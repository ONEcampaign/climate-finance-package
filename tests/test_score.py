"""The truth-group classifier, its links and the provider profile."""

from collections.abc import Iterator

import polars as pl
import pytest
from threadpoolctl import threadpool_limits

from climate_finance import schema
from climate_finance.build.link.keys import build_units
from climate_finance.build.link.levels import LEVELS
from climate_finance.build.link.score import (
    ELIGIBLE,
    HELDOUT,
    MIN_TRUE_PAIRS,
    SPLINK_MIN_COMMITMENTS,
    SPLINK_MIN_PAIRS,
    TRAIN,
    TruthScores,
    classifier_links,
    id_truth,
    is_heldout,
    provider_profile,
    score_truth_group,
    scoring_groups,
    scoring_profile,
    split_key,
    with_split,
)

_PAIRS_SCHEMA = {
    "left_id": pl.String,
    "right_id": pl.String,
    "donor_code": pl.Int32,
    "year_gap": pl.Int16,
    "programme_level": pl.Int8,
    "same_crs_id": pl.Boolean,
    "id_persistent": pl.Boolean,
    "eligible": pl.Boolean,
    "split_key": pl.String,
    "placed_year": pl.Int16,
    "split": pl.String,
    **dict.fromkeys(LEVELS, pl.Int8),
    "log_ratio": pl.Float64,
    "later_commit": pl.Int8,
}
# Levels of a pair that continues its commitment, and of one that does not.
_SAME = {
    "gamma_project_number": 3,
    "gamma_text": 5,
    "gamma_purpose": 2,
    "gamma_recipient_code": 1,
    "gamma_amount": 4,
}
_OTHER = {
    "gamma_project_number": 0,
    "gamma_text": 0,
    "gamma_purpose": 0,
    "gamma_recipient_code": 0,
    "gamma_amount": 2,
}


def _pair(
    left: str,
    right: str,
    *,
    donor: int,
    split: str,
    same: bool,
    levels: dict[str, int],
    programme_level: int = 0,
    placed_year: int = 2020,
) -> dict[str, object]:
    return {
        "left_id": left,
        "right_id": right,
        "donor_code": donor,
        "year_gap": 2,
        "programme_level": programme_level,
        "same_crs_id": same,
        "id_persistent": split != ELIGIBLE,
        "eligible": split == ELIGIBLE,
        "split_key": f"{donor}|{right}",
        "placed_year": placed_year,
        "split": split,
        **dict.fromkeys(LEVELS, 1),
        **levels,
        "log_ratio": -0.2,
        "later_commit": 0,
    }


def _labelled(donor: int, commitments: int, *, false_pairs: int) -> list[dict]:
    """`commitments` labelled commitments of `donor`, alternating halves, each with
    one true pair and `false_pairs` false ones. One commitment in five has a true
    pair that looks like a false one, and one in five a false pair that looks
    true, in both halves."""
    rows = []
    for index in range(commitments):
        right = f"c{donor}-{index}"
        split = TRAIN if index % 2 else HELDOUT
        noisy = index % 10 in {0, 3}
        rows.append(
            _pair(
                f"d{donor}-{index}",
                right,
                donor=donor,
                split=split,
                same=True,
                levels=_OTHER if noisy else _SAME,
            )
        )
        for other in range(false_pairs):
            looks_true = index % 10 in {5, 8} and other == 0
            rows.append(
                _pair(
                    f"x{donor}-{index}-{other}",
                    right,
                    donor=donor,
                    split=split,
                    same=False,
                    levels=_SAME if looks_true else _OTHER,
                )
            )
    return rows


def _frame(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=_PAIRS_SCHEMA)


# Donor 1 has its own calibration; donor 2's pairs are all true, so it borrows the
# pooled map; donor 3 has no labelled pairs.
_ELIGIBLE = [
    _pair("e1", "o1", donor=1, split=ELIGIBLE, same=False, levels=_SAME),
    _pair("e2", "o2", donor=1, split=ELIGIBLE, same=False, levels=_OTHER),
    _pair("e3", "o3", donor=3, split=ELIGIBLE, same=False, levels=_SAME),
]
_FEATURES = _frame(
    [
        *_labelled(1, 400, false_pairs=4),
        *_labelled(2, 2 * MIN_TRUE_PAIRS + 10, false_pairs=0),
        *_ELIGIBLE,
    ]
)
_NO_MISSED_TRUTH = pl.DataFrame(
    schema={"left_id": pl.String, "right_id": pl.String, "donor_code": pl.Int32}
)


@pytest.fixture(scope="module", autouse=True)
def _one_thread() -> Iterator[None]:
    # OpenMP threads spin at barriers, so on a busy machine gradient boosting on a
    # few thousand pairs runs faster on one thread than on many.
    with threadpool_limits(limits=1, user_api="openmp"):
        yield


@pytest.fixture(scope="module")
def scores() -> TruthScores:
    # Slow-ish: six gradient-boosting fits on 2,070 pairs, a few seconds.
    groups = scoring_groups(_FEATURES)
    return score_truth_group(_FEATURES, groups=groups, truth=_truth(_FEATURES))


def _truth(pairs: pl.DataFrame) -> pl.DataFrame:
    return pairs.filter("same_crs_id").select("left_id", "right_id", "donor_code")


def _p(scores: TruthScores, left: str) -> float:
    return scores.pairs.filter(left_id=left)["p_model"].item()


def test_split_keys_fall_in_fixed_halves() -> None:
    keys = pl.Series(["5|ABC", "5|ABD", "12|2021000123", "12|2021000124", "5|ABC"])
    assert is_heldout(keys).to_list() == [True, False, False, True, True]


def test_the_split_key_is_donor_and_crs_id_else_the_unit() -> None:
    frame = pl.DataFrame(
        {"donor_code": [5, 5], "crs_id": ["ABC", None], "unit_id": ["u1", "u2"]}
    )
    keys = frame.select(
        split_key(pl.col("donor_code"), pl.col("crs_id"), pl.col("unit_id"))
    )
    assert keys.to_series().to_list() == ["5|ABC", "5|u2"]


def test_with_split_halves_labelled_pairs_by_their_commitment() -> None:
    units = pl.DataFrame(
        {
            "unit_id": ["c1", "c2"],
            "donor_code": [5, 5],
            "crs_id": ["ABC", "ABD"],
            "placed_year": [2019, 2020],
        },
        schema={
            "unit_id": pl.String,
            "donor_code": pl.Int32,
            "crs_id": pl.String,
            "placed_year": pl.Int16,
        },
    )
    candidates = pl.DataFrame(
        {
            "left_id": ["d1", "d2", "d3"],
            "right_id": ["c1", "c2", "c1"],
            "id_persistent": [True, True, False],
        }
    )
    pairs = with_split(candidates, units=units)
    assert pairs["split"].to_list() == [HELDOUT, TRAIN, ELIGIBLE]
    assert pairs["placed_year"].to_list() == [2019, 2020, 2019]


@pytest.mark.parametrize(
    ("true_pairs", "commitments", "pairs", "group"),
    [
        (MIN_TRUE_PAIRS, 0, 0, "truth"),
        (MIN_TRUE_PAIRS - 1, SPLINK_MIN_COMMITMENTS, SPLINK_MIN_PAIRS, "splink"),
        (0, SPLINK_MIN_COMMITMENTS - 1, SPLINK_MIN_PAIRS, "pooled_splink"),
        (0, SPLINK_MIN_COMMITMENTS, SPLINK_MIN_PAIRS - 1, "pooled_splink"),
    ],
)
def test_scoring_groups(
    true_pairs: int, commitments: int, pairs: int, group: str
) -> None:
    rows = [
        _pair(f"t{index}", f"c{index}", donor=7, split=TRAIN, same=True, levels={})
        for index in range(true_pairs)
    ]
    # A held-out true pair and a false training pair add no truth.
    rows += [
        _pair("h", "ch", donor=7, split=HELDOUT, same=True, levels={}),
        _pair("f", "cf", donor=7, split=TRAIN, same=False, levels={}),
    ]
    rows += [
        _pair(
            f"e{index}",
            f"o{index % commitments}",
            donor=7,
            split=ELIGIBLE,
            same=False,
            levels={},
        )
        for index in range(pairs)
    ]
    profile = scoring_groups(_frame(rows)).row(0, named=True)
    assert profile["scoring_group"] == group
    assert profile["true_pairs"] == true_pairs


def test_id_truth_follows_the_crs_id_into_later_disbursements() -> None:
    record = {
        "donor_code": 5,
        "agency_code": 1,
        "crs_id": "A",
        "project_number": None,
        "recipient_code": 238,
        "purpose_code": 14030,
        "commitment_date": None,
        "commitment_usd": None,
        "disbursement_usd": None,
    }
    records = [
        record | {"record_id": "c", "year": 2020, "commitment_usd": 10.0},
        record | {"record_id": "d", "year": 2022, "disbursement_usd": 4.0},
        # Same year, another id, an agency whose ids do not persist.
        record
        | {"record_id": "s", "year": 2020, "disbursement_usd": 4.0, "purpose_code": 1},
        record
        | {"record_id": "o", "year": 2022, "disbursement_usd": 4.0, "crs_id": "B"},
        record
        | {"record_id": "n", "year": 2020, "commitment_usd": 9.0, "agency_code": 2},
    ]
    crs = pl.LazyFrame(
        records,
        schema={
            "record_id": pl.String,
            "donor_code": pl.Int32,
            "agency_code": pl.Int32,
            "crs_id": pl.String,
            "project_number": pl.String,
            "recipient_code": pl.Int32,
            "purpose_code": pl.Int32,
            "year": pl.Int16,
            "commitment_date": pl.Date,
            "commitment_usd": pl.Float64,
            "disbursement_usd": pl.Float64,
        },
    )
    units = build_units(crs).collect()
    unit = {
        ids[0]: unit_id
        for unit_id, ids in units.select("unit_id", "record_ids").iter_rows()
    }
    links = pl.DataFrame(
        {"task": ["crdf_crs", "crdf_crs"], "right_id": [unit["c"], unit["n"]]}
    )
    id_practice = pl.DataFrame(
        {
            "donor_code": [5, 5],
            "agency_code": [1, 2],
            "period": ["2019-2022", "2019-2022"],
            "id_class": ["id_persistent", "mixed"],
        },
        schema={
            "donor_code": pl.Int32,
            "agency_code": pl.Int32,
            "period": pl.String,
            "id_class": pl.String,
        },
    )
    truth = id_truth(units, links=links, id_practice=id_practice, first_year=2010)
    assert truth.select("left_id", "right_id").rows() == [(unit["d"], unit["c"])]


def test_the_classifier_scores_only_the_truth_group(scores: TruthScores) -> None:
    assert set(scores.pairs["donor_code"]) == {1, 2}
    assert 0 < scores.threshold < 1
    assert _p(scores, "e1") >= scores.threshold > _p(scores, "e2")


def test_a_provider_with_enough_labels_takes_its_own_map(scores: TruthScores) -> None:
    # Donor 2 has 35 true training pairs and no false ones.
    assert dict(scores.calibration.iter_rows()) == {1: "own", 2: "pooled"}


def test_held_out_labels_never_reach_the_model(scores: TruthScores) -> None:
    flipped = _FEATURES.with_columns(
        same_crs_id=pl.when(pl.col("split") == HELDOUT)
        .then(~pl.col("same_crs_id"))
        .otherwise(pl.col("same_crs_id"))
    )
    again = score_truth_group(
        flipped, groups=scoring_groups(flipped), truth=_truth(_FEATURES)
    )
    assert again.threshold == scores.threshold
    assert again.pairs["p_model"].equals(scores.pairs["p_model"])


def test_missed_truth_counts_against_recall(scores: TruthScores) -> None:
    # Many true pairs blocking never produced make each false link cheap against
    # the truth, so the threshold falls.
    missed = pl.DataFrame(
        {
            "left_id": [f"m{index}" for index in range(20_000)],
            "right_id": ["c1-1"] * 20_000,
            "donor_code": [1] * 20_000,
        },
        schema=_NO_MISSED_TRUTH.schema,
    )
    truth = pl.concat([_truth(_FEATURES), missed])
    again = score_truth_group(_FEATURES, groups=scoring_groups(_FEATURES), truth=truth)
    assert again.threshold < scores.threshold


def _scores(*rows: dict[str, object], threshold: float = 0.5) -> TruthScores:
    pairs = _frame(list(rows)).with_columns(
        p_model=pl.Series([row["p"] for row in rows], dtype=pl.Float64)
    )
    calibration = pl.DataFrame(
        {"provider_code": [1], "calibration": ["own"]},
        schema={"provider_code": pl.Int32, "calibration": pl.String},
    )
    return TruthScores(pairs=pairs, threshold=threshold, calibration=calibration)


def _eligible(left: str, right: str, p: float, **fields: object) -> dict[str, object]:
    return _pair(
        left, right, donor=1, split=ELIGIBLE, same=False, levels={}, **fields
    ) | {"p": p}


def test_each_disbursement_links_its_best_commitment() -> None:
    links = classifier_links(
        _scores(
            _eligible("d1", "c1", 0.6),
            _eligible("d1", "c2", 0.8),
            # Ties go to the earlier placed year.
            _eligible("d2", "c3", 0.7, placed_year=2019),
            _eligible("d2", "c4", 0.7, placed_year=2018),
            # Exactly at the threshold links; below does not.
            _eligible("d3", "c5", 0.5),
            _eligible("d4", "c6", 0.49),
            # Siblings never link.
            _eligible("d5", "c7", 0.99, programme_level=1),
            _eligible("d6", "c2", 0.9, programme_level=2),
        )
    )
    schema.validate(schema.LINKS, links)
    assert links.select("left_id", "right_id").rows() == [
        ("d1", "c2"),
        ("d2", "c4"),
        ("d3", "c5"),
        ("d6", "c2"),
    ]
    # One group per commitment, shared by every disbursement linked to it.
    assert links.group_by("group_id").agg(pl.col("right_id").n_unique())[
        "right_id"
    ].to_list() == [1, 1, 1]
    assert set(links["link_method"]) == {"classifier"}
    assert links["probability"].to_list() == links["p_model"].to_list()


def test_training_and_held_out_pairs_never_link() -> None:
    labelled = _pair("d1", "c1", donor=1, split=TRAIN, same=True, levels={})
    held = _pair("d2", "c2", donor=1, split=HELDOUT, same=True, levels={})
    links = classifier_links(_scores(labelled | {"p": 0.9}, held | {"p": 0.9}))
    assert links.is_empty()


def test_the_profile_states_calibration_and_threshold() -> None:
    groups = pl.DataFrame(
        {
            "provider_code": [1, 3],
            "scoring_group": ["truth", "splink"],
            "true_pairs": [400, 0],
            "commitments": [5, 400],
            "pairs": [9, 3_000],
        },
        schema={
            "provider_code": pl.Int32,
            "scoring_group": pl.String,
            "true_pairs": pl.Int32,
            "commitments": pl.Int32,
            "pairs": pl.Int32,
        },
    )
    # Provider 2 has climate value but no candidate pairs.
    crosswalk = pl.DataFrame(
        {
            "provider_code": [1, 2, 3],
            "climate_usd": [5.0, 6.0, 7.0],
            "crosswalk_usd": [None, None, 1.0],
            "crosswalk_share": [None, None, 1 / 7],
        },
        schema={
            "provider_code": pl.Int32,
            "climate_usd": pl.Float64,
            "crosswalk_usd": pl.Float64,
            "crosswalk_share": pl.Float64,
        },
    )
    scoring = scoring_profile(groups, scores=_scores(threshold=0.4))
    profile = provider_profile(crosswalk, scoring=scoring)
    schema.validate(schema.PROVIDER_PROFILE, profile)
    assert profile.select(
        "provider_code", "climate_usd", "scoring_group", "calibration", "threshold"
    ).rows() == [
        (1, 5.0, "truth", "own", 0.4),
        (2, 6.0, None, None, None),
        (3, 7.0, "splink", None, None),
    ]
    empty = provider_profile(crosswalk, scoring=scoring_profile(groups, scores=None))
    schema.validate(schema.PROVIDER_PROFILE, empty)
    assert empty["threshold"].null_count() == 3

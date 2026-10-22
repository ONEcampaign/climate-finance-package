"""The truth-group classifier, its links and the provider profile."""

import datetime as dt
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
    UNLABELLED,
    TruthScores,
    best_commitments,
    classifier_links,
    contradicted_commitments,
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
    "true_pair": pl.Boolean,
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
        "true_pair": same,
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
# Pairs whose labels the project contradicts: true ones that look false and false
# ones that look true.
_UNLABELLED = [
    _pair(f"u{index}", "cu", donor=1, split=UNLABELLED, same=same, levels=levels)
    for index, (same, levels) in enumerate([(True, _OTHER), (False, _SAME)] * 20)
]
_FEATURES = _frame(
    [
        *_labelled(1, 400, false_pairs=4),
        *_labelled(2, 2 * MIN_TRUE_PAIRS + 10, false_pairs=0),
        *_UNLABELLED,
        *_ELIGIBLE,
    ]
)
_NONE_CONTRADICTED = pl.DataFrame(
    schema={"unit_id": pl.String, "ambiguous": pl.Boolean}
)
_TRUTH_SCHEMA = {
    "left_id": pl.String,
    "right_id": pl.String,
    "donor_code": pl.Int32,
    "ambiguous": pl.Boolean,
}


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
    return pairs.filter("true_pair").select(
        "left_id", "right_id", "donor_code", ambiguous=pl.lit(value=False)
    )


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
            "unit_id": ["c1", "c2", "c3"],
            "donor_code": [5, 5, 5],
            "crs_id": ["ABC", "ABD", "ABE"],
            "placed_year": [2019, 2020, 2021],
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
            "left_id": ["d1", "d2", "d3", "d4", "d5"],
            "right_id": ["c1", "c2", "c1", "c3", "c3"],
            "id_persistent": [True, True, False, True, False],
        }
    )
    truth = pl.DataFrame(
        {
            "left_id": ["d2", "d4"],
            "right_id": ["c2", "c3"],
            "donor_code": [5, 5],
            "ambiguous": [False, True],
        }
    )
    pairs = with_split(candidates, units=units, truth=truth)
    assert pairs["split"].to_list() == [HELDOUT, TRAIN, ELIGIBLE, UNLABELLED, ELIGIBLE]
    assert pairs["true_pair"].to_list() == [False, True, False, False, False]
    assert pairs["placed_year"].to_list() == [2019, 2020, 2019, 2021, 2021]


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
    # Held-out and unlabelled true pairs and a false training pair add no truth.
    rows += [
        _pair("h", "ch", donor=7, split=HELDOUT, same=True, levels={}),
        _pair("u", "cu", donor=7, split=UNLABELLED, same=True, levels={}),
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
    truth = id_truth(
        units,
        links=links,
        id_practice=id_practice,
        first_year=2010,
        contradicted=_NONE_CONTRADICTED,
    )
    assert truth.select("left_id", "right_id").rows() == [(unit["d"], unit["c"])]


_CRS_SCHEMA = {
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
}


def _line(
    record_id: str,
    crs_id: str,
    *,
    recipient: int,
    year: int,
    committed: bool,
    project: str = "P1",
    commitment_date: dt.date | None = None,
) -> dict[str, object]:
    """A record of one line of a project of donor 301."""
    return {
        "record_id": record_id,
        "donor_code": 301,
        "agency_code": 3,
        "crs_id": crs_id,
        "project_number": project,
        "recipient_code": recipient,
        "purpose_code": 23240,
        "year": year,
        "commitment_date": commitment_date,
        "commitment_usd": 10.0 if committed else None,
        "disbursement_usd": None if committed else 4.0,
    }


# Project P1 commits two lines in 2018, one per recipient.
_P1 = [
    _line("a", "20182009630001", recipient=358, year=2018, committed=True),
    _line("b", "20182009630002", recipient=489, year=2018, committed=True),
]


@pytest.mark.parametrize(
    ("later", "contradicted"),
    [
        # Each line disburses later under its own id and recipient.
        (
            [
                _line("c", "20182009630001", recipient=358, year=2020, committed=False),
                _line("d", "20182009630002", recipient=489, year=2021, committed=False),
            ],
            False,
        ),
        # A recipient's money later runs under a line no record commits.
        (
            [_line("c", "20182009631", recipient=358, year=2019, committed=False)],
            True,
        ),
        # A line passes to the other line's recipient.
        (
            [_line("c", "20182009630001", recipient=489, year=2021, committed=False)],
            True,
        ),
        # A line passes to a recipient no line of the project committed to.
        (
            [_line("c", "20182009630001", recipient=389, year=2021, committed=False)],
            False,
        ),
        # An uncommitted line of another recipient, of the same year, of another
        # project, or committed before the first year says nothing.
        (
            [
                _line("c", "20182009635", recipient=389, year=2019, committed=False),
                _line("d", "20182009636", recipient=358, year=2018, committed=False),
                _line(
                    "e",
                    "20182009637",
                    recipient=358,
                    year=2019,
                    committed=False,
                    project="P2",
                ),
                _line(
                    "f",
                    "20182009638",
                    recipient=358,
                    year=2019,
                    committed=False,
                    commitment_date=dt.date(2009, 5, 1),
                ),
            ],
            False,
        ),
        # A later tranche under its own committed id is no contradiction.
        (
            [
                _line("c", "20192009630001", recipient=358, year=2019, committed=True),
                _line("d", "20192009630001", recipient=358, year=2020, committed=False),
            ],
            False,
        ),
    ],
)
def test_a_project_whose_lines_move_contradicts_its_ids(
    later: list[dict[str, object]], contradicted: bool
) -> None:
    units = build_units(pl.LazyFrame([*_P1, *later], schema=_CRS_SCHEMA)).collect()
    p1_2018 = units.filter(pl.col("year") == 2018, pl.col("commitment_usd") > 0)
    found = contradicted_commitments(units, first_year=2010)
    # Both lines of the project lose their labels, or neither does.
    assert found["unit_id"].to_list() == (
        sorted(p1_2018["unit_id"]) if contradicted else []
    )
    assert not found["ambiguous"].any()


# Line 0001 passes to recipient 489, and 358 continues under an unpadded id.
_PASSED = _line("c", "20182009630001", recipient=489, year=2021, committed=False)
_UNPADDED = _line("d", "20182009631", recipient=358, year=2019, committed=False)


@pytest.mark.parametrize(
    ("later", "expected"),
    [
        (
            [_PASSED, _UNPADDED],
            {("c", "b", False), ("d", "a", False)},
        ),
        # A second line committed to 358 in 2018 leaves 358's new line ambiguous.
        (
            [
                _PASSED,
                _UNPADDED,
                _line("e", "20182009630003", recipient=358, year=2018, committed=True),
            ],
            {("c", "b", False), ("d", "a", True), ("d", "e", True)},
        ),
        # A line that committed to 358 before it disburses there names its own
        # commitment, and so does a line recorded before the project committed.
        (
            [
                _UNPADDED,
                _line("f", "20182009630003", recipient=358, year=2019, committed=True),
                _line("g", "20182009630003", recipient=358, year=2020, committed=False),
                _line("h", "20172009639", recipient=358, year=2017, committed=False),
                _line("i", "20172009639", recipient=358, year=2019, committed=False),
            ],
            {("d", "a", False)},
        ),
    ],
)
def test_a_contradicted_project_continues_by_project_and_recipient(
    later: list[dict[str, object]], expected: set[tuple[str, str, bool]]
) -> None:
    units = build_units(pl.LazyFrame([*_P1, *later], schema=_CRS_SCHEMA)).collect()
    unit = {
        ids[0]: unit_id
        for unit_id, ids in units.select("unit_id", "record_ids").iter_rows()
    }
    truth = id_truth(
        units,
        links=units.filter(pl.col("commitment_usd") > 0).select(
            task=pl.lit("crdf_crs"), right_id="unit_id"
        ),
        id_practice=pl.DataFrame(
            {
                "donor_code": [301],
                "agency_code": [3],
                "period": ["2015-2018"],
                "id_class": ["id_persistent"],
            },
            schema={
                "donor_code": pl.Int32,
                "agency_code": pl.Int32,
                "period": pl.String,
                "id_class": pl.String,
            },
        ),
        first_year=2010,
        contradicted=contradicted_commitments(units, first_year=2010),
    )
    assert set(truth.select("left_id", "right_id", "ambiguous").iter_rows()) == {
        (unit[left], unit[right], ambiguous) for left, right, ambiguous in expected
    }


def test_the_classifier_scores_only_the_truth_group(scores: TruthScores) -> None:
    assert set(scores.pairs["donor_code"]) == {1, 2}
    assert 0 < scores.threshold < 1
    assert _p(scores, "e1") >= scores.threshold > _p(scores, "e2")


def test_a_provider_with_enough_labels_takes_its_own_map(scores: TruthScores) -> None:
    # Donor 2 has 35 true training pairs and no false ones.
    assert dict(scores.calibration.iter_rows()) == {1: "own", 2: "pooled"}


def test_held_out_and_unlabelled_labels_never_reach_the_model(
    scores: TruthScores,
) -> None:
    flipped = _FEATURES.with_columns(
        true_pair=pl.when(pl.col("split").is_in([HELDOUT, UNLABELLED]))
        .then(~pl.col("true_pair"))
        .otherwise(pl.col("true_pair"))
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
            "ambiguous": [False] * 20_000,
        },
        schema=_TRUTH_SCHEMA,
    )
    truth = pl.concat([_truth(_FEATURES), missed])
    again = score_truth_group(_FEATURES, groups=scoring_groups(_FEATURES), truth=truth)
    assert again.threshold < scores.threshold


def test_recall_counts_disbursements_not_pairs(scores: TruthScores) -> None:
    # A disbursement links one commitment, so more true commitments for the
    # disbursements that already count, or ambiguous ones, leave the threshold.
    train = _truth(_FEATURES.filter(split=TRAIN, donor_code=1))
    commitments = train["right_id"].unique().sort()
    extra = train.join(commitments.head(20).to_frame("other"), how="cross").select(
        "left_id", right_id="other", donor_code="donor_code", ambiguous="ambiguous"
    )
    open_pairs = pl.DataFrame(
        {
            "left_id": [f"a{index}" for index in range(20_000)],
            "right_id": ["c1-1"] * 20_000,
            "donor_code": [1] * 20_000,
            "ambiguous": [True] * 20_000,
        },
        schema=_TRUTH_SCHEMA,
    )
    truth = pl.concat([_truth(_FEATURES), extra, open_pairs]).unique()
    again = score_truth_group(_FEATURES, groups=scoring_groups(_FEATURES), truth=truth)
    assert again.threshold == scores.threshold


def test_best_commitments_follow_the_link_rule() -> None:
    best = best_commitments(
        _scores(
            _eligible("d1", "c1", 0.6),
            _eligible("d1", "c2", 0.8),
            _eligible("d2", "c3", 0.7, placed_year=2019),
            _eligible("d2", "c4", 0.7, placed_year=2018),
            _eligible("d3", "c5", 0.99, programme_level=1),
            _eligible("d3", "c6", 0.1),
        ).pairs
    )
    assert best.select("left_id", "right_id").rows() == [
        ("d1", "c2"),
        ("d2", "c4"),
        ("d3", "c6"),
    ]


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

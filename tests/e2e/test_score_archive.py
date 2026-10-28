"""The truth-group classifier on the archived OECD files.

The archive builds through link with key repair and recoveries on, as in
`build.toml`. The classifier is measured on the held-out half of the labelled pairs.
Each held-out disbursement unit takes its best pair over both halves
(`best_commitments`), which links at the threshold and is right when true. A frame
of years or providers reads precision on the links (`heldout_links`) whose
commitment is in the frame. Recall counts every disbursement unit with a true
held-out commitment in the frame, blocked or not, and a hit when the unit's link is
right, whatever year or half its commitment is in: a disbursement that continues two
commitments of one line links to one of them and is right in both frames. F1 is the
harmonic mean of the two. The expected calibration error reads ten equal bins of
probability over the held-out pairs in the frame scored at 0.05 or more. Provider
classes are the CRDF provider types of the commitment's (provider, agency), DAC and
non-DAC members counting as bilateral.
"""

import os
from pathlib import Path

import numpy as np
import polars as pl
import pytest
from archive_build import archive_config

from climate_finance import schema
from climate_finance.build.allocate import run_allocate
from climate_finance.build.link.disagreement import flag_disagreement
from climate_finance.build.link.score import (
    HELDOUT,
    METHOD,
    TRAIN,
    Scored,
    TruthScores,
    best_commitments,
    contradicted_commitments,
    heldout_links,
    id_truth,
)
from climate_finance.build.stages import StageContext, stage_dir, table_path

pytestmark = pytest.mark.slow

# The prototype's commitment years, and the CRDF years the build covers.
PROTOTYPE_YEARS = (2021, 2022)
CRDF_YEARS = (2015, 2024)
F1_TARGET = 0.89
ECE_TARGET = 0.02
# Calibration is read where a link is in question.
ECE_FLOOR = 0.05
ECE_BINS = 10
# 2021 to 2022 climate value in USD bn that the classifier's links move between
# statuses with `sibling_lines` counted over the candidate pairs' units, what
# counting it over every link-stage unit adds, and what one unit per line
# (`build_units`) adds.
CLASSIFIER_MOVED_BN = {
    "disbursed": 0.00301,
    "partly_disbursed": 0.00336,
    "no_disbursement_record": -0.00637,
}
SIBLING_LINES_MOVED_BN = {"disbursed": -0.00261, "partly_disbursed": 0.00262}
LINE_UNITS_MOVED_BN = {"disbursed": -0.00011, "partly_disbursed": 0.00010}
_CATEGORY = {
    "DAC member": "bilateral",
    "Non-DAC member": "bilateral",
    "Multilateral development bank": "banks",
    "Other multilateral": "other_multilateral",
    "Private donor": "private",
}


def _read(release_dir: Path, stage: str, table: str) -> pl.DataFrame:
    return pl.read_parquet(table_path(release_dir, stage, table))


def _scores(scored: Scored) -> TruthScores:
    assert scored.scores is not None
    return scored.scores


@pytest.fixture(scope="module")
def heldout(
    release_dir: Path,
    scored: Scored,
    keyed_links: pl.DataFrame,
    crs: pl.LazyFrame,
    archive: Path,
    first_year: int,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Held-out pairs, their links (`heldout_links`), and every true pair of their
    commitments, blocked or not, each with the commitment's record year and
    provider class; and each held-out unit's best pair over both halves."""
    scores = _scores(scored).pairs
    pairs = scores.filter(split=HELDOUT)
    links = heldout_links(scores)
    best = best_commitments(
        scores.filter(pl.col("split").is_in([TRAIN, HELDOUT])).join(
            pairs.select("left_id").unique(), on="left_id", how="semi"
        )
    ).select("left_id", "true_pair", "p_model")
    units = _read(release_dir, "link", "units")
    truth = id_truth(
        units,
        crs=crs,
        links=keyed_links,
        id_practice=_read(release_dir, "link", "id_practice"),
        first_year=first_year,
        contradicted=contradicted_commitments(units, first_year=first_year),
    ).join(pairs.select("right_id").unique(), on="right_id", how="semi")
    categories = (
        pl.scan_parquet(archive / "oecd" / "crdf_recipient.parquet")
        .select(
            pl.col("provider_code", "agency_code").cast(pl.Int32),
            category=pl.col("provider_type").replace_strict(_CATEGORY),
        )
        .unique()
        .collect()
    )
    about = units.select(
        right_id="unit_id",
        provider_code="donor_code",
        agency_code="agency_code",
        year="year",
    ).join(categories, on=["provider_code", "agency_code"], how="left")
    return (
        pairs.join(about, on="right_id", how="left"),
        links.join(about, on="right_id", how="left"),
        truth.join(about, on="right_id", how="left"),
        best,
    )


def _ece(p: np.ndarray, y: np.ndarray) -> float:
    bins = np.minimum((p * ECE_BINS).astype(int), ECE_BINS - 1)
    gaps = [
        (bins == b).mean() * abs(p[bins == b].mean() - y[bins == b].mean())
        for b in range(ECE_BINS)
        if (bins == b).any()
    ]
    return float(sum(gaps))


def _measure(
    heldout: tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame],
    *,
    where: pl.Expr,
    threshold: float,
) -> dict[str, float]:
    pairs, links, truth, best = heldout
    pairs, links, truth = (frame.filter(where) for frame in (pairs, links, truth))
    links = links.filter(pl.col("p_model") >= threshold)
    right = best.filter(pl.col("true_pair"), pl.col("p_model") >= threshold)
    true = truth.filter(~pl.col("ambiguous")).select("left_id").unique()
    precision = links["true_pair"].sum() / links.height
    recall = true.join(right, on="left_id", how="semi").height / true.height
    p = pairs["p_model"].to_numpy()
    y = pairs["true_pair"].to_numpy().astype(float)
    scored = p >= ECE_FLOOR
    return {
        "precision": precision,
        "recall": recall,
        "f1": 2 * precision * recall / (precision + recall),
        "ece": _ece(p[scored], y[scored]),
    }


def test_the_classifier_meets_its_targets_on_the_prototype_years(
    scored: Scored,
    heldout: tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame],
) -> None:
    scores = _scores(scored)
    threshold = scores.threshold
    prototype = _measure(
        heldout,
        where=pl.col("year").is_between(*PROTOTYPE_YEARS),
        threshold=threshold,
    )
    every_year = _measure(
        heldout, where=pl.col("year").is_between(*CRDF_YEARS), threshold=threshold
    )
    print(
        {
            "threshold": threshold,
            "best_threshold": scores.best_threshold,
            "pairs": scores.pairs.height,
            "every_year": every_year,
            "prototype": prototype,
        },
        flush=True,
    )

    # The scored pairs depend on the EIB recovery routes (`recover`). A recovered
    # link makes its EIB unit a commitment, which adds candidate pairs
    # (`score_candidates`) and labelled truth pairs (`id_truth`), so a change to the
    # routes refits the best threshold and moves every figure below. Rerun this file
    # after one.
    # Id truth follows a line through its forms of id (`line_id`), Canada's
    # disbursements continue the commitments of their own purpose (`purpose_lines`),
    # and the classifier reads start and completion dates and no commitment date,
    # which refits the best threshold. Template lines (`template_lines`) leave the
    # training pairs, which refits it again. Truth starts in `FIRST_MEASURED_YEAR`,
    # and the classifier reads how many sibling lines a commitment has
    # (`sibling_lines`, counted over every link-stage unit), and a line filed under
    # several purposes is one unit (`build_units`), which refits it to 0.41. Pairs
    # link at the fixed cut of 0.40, whatever the best threshold.
    assert threshold == 0.40
    assert scores.best_threshold == 0.41
    assert prototype["f1"] >= F1_TARGET
    assert every_year["ece"] <= ECE_TARGET
    # Commitments from 2010 train the classifier, and their projects commit more
    # sibling lines than later ones, which `sibling_lines` lets it weigh. A line
    # filed under several purposes is one unit, so the held-out pairs hold no false
    # sibling of a Canadian line's other purposes.
    assert every_year == pytest.approx(
        {"precision": 0.94081, "recall": 0.94234, "f1": 0.94157, "ece": 0.00587},
        abs=1e-5,
    )
    assert prototype == pytest.approx(
        {"precision": 0.95538, "recall": 0.95505, "f1": 0.95521, "ece": 0.01074},
        abs=1e-5,
    )


def test_held_out_figures_by_provider_class(
    scored: Scored,
    heldout: tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame],
) -> None:
    threshold = _scores(scored).threshold
    measured = {
        category: _measure(
            heldout, where=pl.col("category") == category, threshold=threshold
        )
        for category in ("bilateral", "private", "banks", "other_multilateral")
    }
    print(measured, flush=True)
    assert measured == {
        # The UK's lettered copies carry their line's id (`line_id`) and count as
        # true. The class frames hold every commitment year that trains, from 2010.
        "bilateral": pytest.approx(
            {"precision": 0.92392, "recall": 0.92360, "f1": 0.92376, "ece": 0.00537},
            abs=1e-5,
        ),
        # Private donors write a line suffix with and without zero padding, which
        # `line_id` reads as one line.
        "private": pytest.approx(
            {"precision": 0.99212, "recall": 0.99506, "f1": 0.99359, "ece": 0.02108},
            abs=1e-5,
        ),
        "banks": pytest.approx(
            {"precision": 0.99206, "recall": 0.97656, "f1": 0.98425, "ece": 0.01695},
            abs=1e-5,
        ),
        # The class's held-out pairs are the Adaptation Fund's. Truth follows its
        # renumbered projects by title (`carried_continuations`). The fund files
        # a project's line under several purposes, which is one unit
        # (`build_units`), and leaves it 35 true and 12 false training pairs, under
        # `MIN_CALIBRATION_PAIRS`, so its pairs take the pooled map. 20 of its 56
        # true disbursement units link, and none of its 20 links is false.
        "other_multilateral": pytest.approx(
            {"precision": 1.0, "recall": 0.35714, "f1": 0.52632, "ece": 0.53803},
            abs=1e-5,
        ),
    }


def test_the_link_stage_writes_the_classifier_links_and_profile(
    release_dir: Path, scored: Scored
) -> None:
    written = _read(release_dir, "link", "links").filter(link_method=METHOD)
    profile = _read(release_dir, "link", "provider_profile")
    schema.validate(schema.PROVIDER_PROFILE, profile)
    units = _read(release_dir, "link", "units")

    scoring = profile.drop_nulls("scoring_group").select(scored.profile.columns)
    groups = dict(scoring.group_by("scoring_group").len().iter_rows())
    print({"links": written.height, "scoring_groups": groups}, flush=True)

    # The link stage flags the classifier's links whose records disagree after
    # scoring them (`flag_disagreement`).
    assert written.equals(flag_disagreement(scored.links, units=units))
    assert scoring.equals(scored.profile)
    # The key tiers (the date tier and the purpose tier's own guard) and key repair
    # take units the classifier would otherwise link, and the private-donor rule
    # (`confirmable_pairs`) drops some of donor 1624's pairs. Commitments from 2000
    # are candidates, and 990 of the links have a commitment from before 2015.
    # Counted over the candidate pairs' units, `sibling_lines` gives 1,520 links;
    # counted over every link-stage unit, it gives 104 more, and one unit per line
    # (`build_units`) takes 270.
    assert written.height == 1_520 + 104 - 270
    # The AsDB (915) has 179 commitments in eligible pairs, under
    # `SPLINK_MIN_COMMITMENTS`, and scores with the pooled model, as does donor 988,
    # whose 1,881 eligible pairs fall under `SPLINK_MIN_PAIRS`. Portugal (76) has 26
    # true training pairs and no eligible pair. `template_lines` takes every
    # eligible pair of the Development Bank of Latin America and the New Development
    # Bank. Commitments from 2000 give the EBRD (990) 404 commitments, a model of its
    # own, and the United Arab Emirates (576) 50 true training pairs, a place in the
    # truth group.
    assert groups == {
        "truth": 25,
        "splink": 4,
        "pooled_splink": 36,
    }


def _statuses(
    release_dir: Path, *, links: pl.DataFrame | None, out: Path
) -> dict[str, float]:
    """2021 to 2022 counted climate value in USD bn by status, allocating `links`
    in place of the link stage's when given."""
    if links is not None:
        copy = out / "r"
        for stage in ("fetch", "normalise", "link"):
            (copy / stage).mkdir(parents=True)
            for table in stage_dir(release_dir, stage).glob("*.parquet"):
                (copy / stage / table.name).symlink_to(table)
        (copy / "link" / "links.parquet").unlink()
        links.write_parquet(copy / "link" / "links.parquet")
        release_dir = copy
    archive = Path(os.environ["CF_E2E_DATA"]) / "raw"
    config = archive_config(archive, key_repair=True, recoveries=True)
    context = StageContext(
        release="r", release_dir=release_dir, config=config, offline=True
    )
    commitments = run_allocate(context)["commitments"].lazy().collect()
    crdf = _read(release_dir, "normalise", "crdf").filter(
        pl.col("year").is_between(*PROTOTYPE_YEARS)
    )
    totals = (
        commitments.join(crdf.select("row_id"), on="row_id", how="semi")
        .group_by("status")
        .agg(pl.col("climate_usd").sum() / 1e9)
    )
    return dict(totals.iter_rows())


def test_classifier_links_move_statuses(
    release_dir: Path,
    keyed_links: pl.DataFrame,
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    # Slow: allocates the archive twice, about two minutes.
    linked = _statuses(release_dir, links=None, out=tmp_path_factory.mktemp("a"))
    keyed = _statuses(release_dir, links=keyed_links, out=tmp_path_factory.mktemp("b"))
    moved = {
        status: linked.get(status, 0.0) - keyed.get(status, 0.0)
        for status in schema.STATUSES
    }
    expected = {
        status: CLASSIFIER_MOVED_BN.get(status, 0.0)
        + SIBLING_LINES_MOVED_BN.get(status, 0.0)
        + LINE_UNITS_MOVED_BN.get(status, 0.0)
        for status in schema.STATUSES
    }
    print({"moved": moved, "expected": expected}, flush=True)

    assert sum(moved.values()) == pytest.approx(0.0, abs=1e-9)
    # 53 of the classifier's 1,354 links reach a 2021 or 2022 commitment.
    assert moved == pytest.approx(expected, abs=1e-5)

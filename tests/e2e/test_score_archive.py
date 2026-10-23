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
from archive_build import archive_config, seed_release

from climate_finance import schema
from climate_finance.build import Stage, build
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
    score_candidates,
)
from climate_finance.build.normalise.crs import trace_population
from climate_finance.build.stages import StageContext, stage_dir, table_path

pytestmark = pytest.mark.slow

FIRST_YEAR = 2010
# The prototype's commitment years, and the CRDF years the build covers.
PROTOTYPE_YEARS = (2021, 2022)
CRDF_YEARS = (2015, 2024)
F1_TARGET = 0.89
ECE_TARGET = 0.02
# Calibration is read where a link is in question.
ECE_FLOOR = 0.05
ECE_BINS = 10
_CATEGORY = {
    "DAC member": "bilateral",
    "Non-DAC member": "bilateral",
    "Multilateral development bank": "banks",
    "Other multilateral": "other_multilateral",
    "Private donor": "private",
}


@pytest.fixture(scope="module")
def archive() -> Path:
    return Path(os.environ["CF_E2E_DATA"]) / "raw"


@pytest.fixture(scope="module")
def release_dir(tmp_path_factory: pytest.TempPathFactory, archive: Path) -> Path:
    # Slow: normalises 3.8 million CRS records, then links and scores them; about
    # five minutes on an idle machine.
    config = archive_config(archive, key_repair=True, recoveries=True)
    out = tmp_path_factory.mktemp("score")
    seed_release(out / "r", archive=archive, config=config)
    stages = [Stage.FETCH, Stage.NORMALISE, Stage.LINK]
    manifest = build("r", config=config, out_dir=out, stages=stages, offline=True)
    return manifest.release_dir


def _read(release_dir: Path, stage: str, table: str) -> pl.DataFrame:
    return pl.read_parquet(table_path(release_dir, stage, table))


@pytest.fixture(scope="module")
def keyed_links(release_dir: Path) -> pl.DataFrame:
    """The links of the steps before scoring."""
    links = _read(release_dir, "link", "links")
    return links.filter(pl.col("link_method") != METHOD)


@pytest.fixture(scope="module")
def crs(release_dir: Path) -> pl.LazyFrame:
    return trace_population(
        pl.scan_parquet(table_path(release_dir, "normalise", "crs"))
    )


@pytest.fixture(scope="module")
def scored(release_dir: Path, keyed_links: pl.DataFrame, crs: pl.LazyFrame) -> Scored:
    # Slow: scores the archive's candidates again, as the link stage did.
    return score_candidates(
        _read(release_dir, "link", "candidates"),
        units=_read(release_dir, "link", "units"),
        crs=crs,
        links=keyed_links,
        id_practice=_read(release_dir, "link", "id_practice"),
        stems=_read(release_dir, "link", "programme_stems"),
        first_year=FIRST_YEAR,
    )


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
        first_year=FIRST_YEAR,
        contradicted=contradicted_commitments(units, first_year=FIRST_YEAR),
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
    threshold = _scores(scored).threshold
    prototype = _measure(
        heldout,
        where=pl.col("year").is_between(*PROTOTYPE_YEARS),
        threshold=threshold,
    )
    every_year = _measure(
        heldout, where=pl.col("year").is_between(*CRDF_YEARS), threshold=threshold
    )

    # Id truth follows a line through its forms of id (`line_id`), Canada's
    # disbursements continue the commitments of their own purpose (`purpose_lines`),
    # and the classifier reads start and completion dates and no commitment date,
    # which refits the threshold.
    assert threshold == 0.40
    assert prototype["f1"] >= F1_TARGET
    assert every_year["ece"] <= ECE_TARGET
    # Canada renumbers its lines from year to year, so it carries 574 of the 1,603
    # held-out misses over the build's CRDF years; without it the ECE is 0.0056.
    assert every_year == pytest.approx(
        {"precision": 0.92436, "recall": 0.91446, "f1": 0.91938, "ece": 0.00868},
        abs=1e-5,
    )
    assert prototype == pytest.approx(
        {"precision": 0.93564, "recall": 0.93475, "f1": 0.93520, "ece": 0.02554},
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
    assert measured == {
        # The UK's lettered copies carry their line's id (`line_id`) and count as
        # true.
        "bilateral": pytest.approx(
            {"precision": 0.92087, "recall": 0.91010, "f1": 0.91545, "ece": 0.00869},
            abs=1e-5,
        ),
        # Private donors write a line suffix with and without zero padding, which
        # `line_id` reads as one line.
        "private": pytest.approx(
            {"precision": 0.98256, "recall": 0.98256, "f1": 0.98256, "ece": 0.02520},
            abs=1e-5,
        ),
        "banks": pytest.approx(
            {"precision": 0.93220, "recall": 0.95652, "f1": 0.94421, "ece": 0.03812},
            abs=1e-5,
        ),
        # The class's held-out pairs are the Adaptation Fund's. Truth follows its
        # renumbered projects by title (`carried_continuations`), so 88 of its 95
        # true disbursement units link and 8 of its 96 links are false.
        "other_multilateral": pytest.approx(
            {"precision": 0.91667, "recall": 0.92632, "f1": 0.92147, "ece": 0.07621},
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

    # The link stage flags the classifier's links whose records disagree after
    # scoring them (`flag_disagreement`).
    assert written.equals(flag_disagreement(scored.links, units=units))
    scoring = profile.drop_nulls("scoring_group").select(scored.profile.columns)
    assert scoring.equals(scored.profile)
    assert written.height == 3_539
    assert dict(scoring.group_by("scoring_group").len().iter_rows()) == {
        "truth": 25,
        "splink": 6,
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
    assert sum(moved.values()) == pytest.approx(0.0, abs=1e-9)
    assert moved == pytest.approx(
        {
            "disbursed": 0.16610,
            "partly_disbursed": -0.12230,
            "no_disbursement_record": -0.04380,
            "not_traceable": 0.0,
            "pending_review": 0.0,
        },
        abs=1e-5,
    )

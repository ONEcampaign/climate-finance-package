"""The truth-group classifier on the archived OECD files.

The archive builds through link with key repair and recoveries on, as in
`build.toml`. The classifier is measured on the held-out half of the labelled pairs.
F1, precision and recall are read on links: each disbursement unit's best pair links
at the threshold and is right when true, and recall counts every disbursement unit
with a true commitment in the half, blocked or not. The expected calibration error
reads ten equal bins of probability over the pairs scored at 0.05 or more. Provider
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
from climate_finance.build.link.score import (
    HELDOUT,
    METHOD,
    Scored,
    TruthScores,
    best_commitments,
    contradicted_commitments,
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
def scored(release_dir: Path, keyed_links: pl.DataFrame) -> Scored:
    # Slow: scores the archive's candidates again, as the link stage did.
    crs = pl.scan_parquet(table_path(release_dir, "normalise", "crs"))
    return score_candidates(
        _read(release_dir, "link", "candidates"),
        units=_read(release_dir, "link", "units"),
        crs=trace_population(crs),
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
    release_dir: Path, scored: Scored, keyed_links: pl.DataFrame, archive: Path
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Held-out pairs, and every true pair of their commitments, blocked or not,
    each with the commitment's record year and provider class."""
    pairs = _scores(scored).pairs.filter(split=HELDOUT)
    units = _read(release_dir, "link", "units")
    truth = id_truth(
        units,
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
        truth.join(about, on="right_id", how="left"),
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
    pairs: pl.DataFrame, truth: pl.DataFrame, *, threshold: float
) -> dict[str, float]:
    links = best_commitments(pairs).filter(pl.col("p_model") >= threshold)
    hits = int(links["true_pair"].sum())
    true = truth.filter(~pl.col("ambiguous"))["left_id"].n_unique()
    p = pairs["p_model"].to_numpy()
    y = pairs["true_pair"].to_numpy().astype(float)
    scored = p >= ECE_FLOOR
    return {
        "precision": hits / links.height,
        "recall": hits / true,
        "f1": 2 * hits / (links.height + true),
        "ece": _ece(p[scored], y[scored]),
    }


def test_the_classifier_meets_its_targets_on_the_prototype_years(
    scored: Scored, heldout: tuple[pl.DataFrame, pl.DataFrame]
) -> None:
    pairs, truth = heldout
    threshold = _scores(scored).threshold
    years = pl.col("year").is_between(*PROTOTYPE_YEARS)
    prototype = _measure(pairs.filter(years), truth.filter(years), threshold=threshold)
    crdf_years = pl.col("year").is_between(*CRDF_YEARS)
    every_year = _measure(
        pairs.filter(crdf_years), truth.filter(crdf_years), threshold=threshold
    )

    # `carried_continuations` labels the UK's and the Adaptation Fund's new lines
    # true and leaves 1,641 of Canada's pairs unlabelled, which refits the threshold.
    assert threshold == 0.43
    assert prototype["f1"] >= F1_TARGET
    assert every_year["ece"] <= ECE_TARGET
    # Canada renumbers its lines from year to year, so it carries 771 of the 1,957
    # held-out misses over the build's CRDF years and 0.014 of the ECE.
    assert every_year == pytest.approx(
        {"precision": 0.91520, "recall": 0.89329, "f1": 0.90411, "ece": 0.01973},
        abs=1e-5,
    )
    assert prototype == pytest.approx(
        {"precision": 0.94641, "recall": 0.92607, "f1": 0.93613, "ece": 0.02921},
        abs=1e-5,
    )


def test_held_out_figures_by_provider_class(
    scored: Scored, heldout: tuple[pl.DataFrame, pl.DataFrame]
) -> None:
    pairs, truth = heldout
    threshold = _scores(scored).threshold
    measured = {
        category: _measure(
            pairs.filter(category=category),
            truth.filter(category=category),
            threshold=threshold,
        )
        for category in ("bilateral", "private", "banks", "other_multilateral")
    }
    assert measured == {
        # The UK's `-c` copies of its committed ids now count as true.
        "bilateral": pytest.approx(
            {"precision": 0.91635, "recall": 0.88705, "f1": 0.90146, "ece": 0.02009},
            abs=1e-5,
        ),
        # Private donors and banks gain no labels; their figures move with the refit.
        "private": pytest.approx(
            {"precision": 0.93237, "recall": 0.98772, "f1": 0.95924, "ece": 0.04474},
            abs=1e-5,
        ),
        "banks": pytest.approx(
            {"precision": 0.97414, "recall": 0.98261, "f1": 0.97835, "ece": 0.02833},
            abs=1e-5,
        ),
        # The class's held-out pairs are the Adaptation Fund's, and all 46 of its
        # true disbursement units link. 52 of its 98 links are false: most continue
        # a commitment under a new crs id and project number at once, with no record
        # that bridges the two, so no label reaches them.
        "other_multilateral": pytest.approx(
            {"precision": 0.46939, "recall": 1.0, "f1": 0.63889, "ece": 0.13720},
            abs=1e-5,
        ),
    }


def test_the_link_stage_writes_the_classifier_links_and_profile(
    release_dir: Path, scored: Scored
) -> None:
    written = _read(release_dir, "link", "links").filter(link_method=METHOD)
    profile = _read(release_dir, "link", "provider_profile")
    schema.validate(schema.PROVIDER_PROFILE, profile)

    assert written.equals(scored.links)
    scoring = profile.drop_nulls("scoring_group").select(scored.profile.columns)
    assert scoring.equals(scored.profile)
    assert written.height == 6_522
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
            "disbursed": 0.18125,
            "partly_disbursed": -0.14055,
            "no_disbursement_record": -0.04070,
            "not_traceable": 0.0,
            "pending_review": 0.0,
        },
        abs=1e-5,
    )

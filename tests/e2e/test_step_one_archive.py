"""Step 1 of the allocation target, and the bits of a repeated build, on the archive.

Both build offline from the archived source files with recoveries on.
"""

import os
import shutil
from pathlib import Path

import polars as pl
import pytest
from archive_build import archive_config, seed_release

from climate_finance.build import BuildManifest, Stage, build
from climate_finance.build.stages import REGISTRY, table_path

pytestmark = pytest.mark.slow

YEARS = (2021, 2022)
# USD bn of 2021 to 2022 climate value per status under the target's counting, with
# the EIB crosswalk route removed. With it, 1.26 bn leave not traceable.
STEP_ONE_BN = {
    "disbursed": 55.9387,
    "partly_disbursed": 106.9182,
    "no_disbursement_record": 34.1908,
    "not_traceable": 32.5254,
}
# The target's prototype has no GEF crosswalk (`link.crosswalks`). Its links give
# five GEF rows worth USD 0.0042 bn a disbursement, so they move from not traceable
# to partly disbursed.
GEF_CROSSWALK_BN = {"partly_disbursed": 0.0042, "not_traceable": -0.0042}
# The target keys World Bank disbursements on their crs ids, which name projects. The
# build's `wb_loan` tier keys them on their loan or credit numbers, which moves IBRD
# and IDA value and no other provider's.
LOAN_TIER_MOVED_BN = {
    "disbursed": 1.5995,
    "partly_disbursed": 1.1822,
    "no_disbursement_record": -2.7817,
    "not_traceable": 0.0,
}
# The target links by key alone. The truth-group classifier (`link.score`) links
# disbursements the keys left to commitments, which moves value out of no record,
# partly disbursed and not traceable into disbursed.
CLASSIFIER_MOVED_BN = {
    "disbursed": 0.1851,
    "partly_disbursed": -0.0060,
    "no_disbursement_record": -0.1785,
    "not_traceable": -0.0007,
}
TOLERANCE_BN = 0.001


@pytest.fixture(scope="module")
def archive() -> Path:
    return Path(os.environ["CF_E2E_DATA"]) / "raw"


@pytest.fixture(scope="module")
def without_crosswalk(tmp_path_factory: pytest.TempPathFactory, archive: Path) -> Path:
    # Slow: normalises 3.8 million CRS records and links and allocates them, about
    # a minute.
    config = archive_config(archive, key_repair=False, recoveries=True)
    config = config.model_copy(update={"skip_eib_routes": ("crosswalk",)})
    out = tmp_path_factory.mktemp("step_one")
    seed_release(out / "r", archive=archive, config=config)
    stages = [Stage.FETCH, Stage.NORMALISE, Stage.LINK, Stage.ALLOCATE]
    return build(
        "r", config=config, out_dir=out, stages=stages, offline=True
    ).release_dir


def _step_one(release_dir: Path) -> dict[str, float]:
    """2021 to 2022 recipient-perspective climate value in USD bn by status.

    Aggregate children are out and repeats are in. A parent takes the status its
    children's allocations give: disbursed from nine tenths of its face value,
    partly from any, then no record or not traceable by whether a child traces.
    """
    crdf = pl.read_parquet(table_path(release_dir, "normalise", "crdf")).filter(
        pl.col("year").is_between(*YEARS), pl.col("perspective") == "recipient"
    )
    commitments = pl.read_parquet(table_path(release_dir, "allocate", "commitments"))
    children = (
        crdf.filter(pl.col("parent_row_id").is_not_null())
        .join(
            commitments.select("row_id", "disbursed_usd", "traceable"),
            on="row_id",
            how="left",
        )
        .group_by("parent_row_id")
        .agg(
            pl.col("disbursed_usd").sum().fill_null(0),
            pl.col("traceable").any().fill_null(False),
        )
    )
    parents = (
        crdf.filter(pl.col("parent_row_id").is_null())
        .join(commitments.select("row_id", "status"), on="row_id", how="left")
        .join(children, left_on="row_id", right_on="parent_row_id", how="left")
    )
    ratio = (
        pl.when(pl.col("face_usd") > 0)
        .then(pl.col("disbursed_usd") / pl.col("face_usd"))
        .otherwise(0.0)
    )
    status = (
        pl.when(pl.col("status").is_not_null())
        .then(pl.col("status"))
        .when(pl.col("disbursed_usd").is_null())
        .then(pl.lit("not_traceable"))
        .when(ratio >= 0.9)
        .then(pl.lit("disbursed"))
        .when(ratio > 0)
        .then(pl.lit("partly_disbursed"))
        .when(~pl.col("traceable"))
        .then(pl.lit("not_traceable"))
        .otherwise(pl.lit("no_disbursement_record"))
    )
    totals = parents.group_by(status.alias("status")).agg(
        pl.col("climate_usd").sum() / 1e9
    )
    return dict(totals.iter_rows())


def test_step_one_matches_the_target_plus_the_gef_crosswalk(
    without_crosswalk: Path,
) -> None:
    expected = {
        status: usd_bn
        + LOAN_TIER_MOVED_BN[status]
        + GEF_CROSSWALK_BN.get(status, 0.0)
        + CLASSIFIER_MOVED_BN[status]
        for status, usd_bn in STEP_ONE_BN.items()
    }
    assert _step_one(without_crosswalk) == pytest.approx(expected, abs=TOLERANCE_BN)


def _build_release(out: Path, *, archive: Path, manifest: Path | None) -> BuildManifest:
    config = archive_config(archive, key_repair=True, recoveries=True)
    seed_release(out / "r", archive=archive, config=config)
    if manifest is not None:
        shutil.copy(manifest, out / "r" / "fetch" / "raw_manifest.parquet")
    return build("r", config=config, out_dir=out, offline=True)


def test_two_builds_from_one_raw_manifest_give_identical_tables(
    tmp_path_factory: pytest.TempPathFactory, archive: Path
) -> None:
    # Slow: two full builds, about 90 seconds together.
    first = _build_release(
        tmp_path_factory.mktemp("first"), archive=archive, manifest=None
    )
    second = _build_release(
        tmp_path_factory.mktemp("second"),
        archive=archive,
        manifest=table_path(first.release_dir, "fetch", "raw_manifest"),
    )

    assert set(first.tables) == {stage.name for stage in REGISTRY}
    assert first.tables == second.tables

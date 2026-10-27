"""The archive release, built once per session for the e2e tests that read it, and
the scored link stage that the classifier's measurements share.

Key repair and recoveries are off in the session release. The raw directory is
seeded from the archive in CF_E2E_DATA with a matching `raw_manifest`
(`archive_build.seed_raw`), so the fetch stage runs offline over every year the
archive's recipient CRDF holds. The scored link stage (`release_dir`, `scored`) is
built per module through link with key repair and recoveries on, as in `build.toml`.
"""

import os
from pathlib import Path

import polars as pl
import pytest
from archive_build import archive_config, seed_raw

from climate_finance.build import Stage, build
from climate_finance.build.fetch import read_years
from climate_finance.build.link.score import METHOD, Scored, score_candidates
from climate_finance.build.normalise.crs import trace_population
from climate_finance.build.stages import table_path

RELEASE = "e2e"


@pytest.fixture(scope="session")
def release(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Build the archive offline and return the release directory."""
    archive = Path(os.environ["CF_E2E_DATA"]) / "raw"
    config = archive_config(archive, key_repair=False, recoveries=False)
    out_dir = tmp_path_factory.mktemp("build")
    seed_raw(out_dir / RELEASE, archive=archive, config=config)
    # Slow: runs every stage over 5.1 million CRS records.
    build(RELEASE, config=config, out_dir=out_dir, offline=True)
    return out_dir / RELEASE / "assemble"


@pytest.fixture(scope="module")
def archive() -> Path:
    return Path(os.environ["CF_E2E_DATA"]) / "raw"


@pytest.fixture(scope="module")
def release_dir(tmp_path_factory: pytest.TempPathFactory, archive: Path) -> Path:
    # Slow: normalises 3.8 million CRS records, then links and scores them, about
    # five minutes on an idle machine.
    config = archive_config(archive, key_repair=True, recoveries=True)
    out = tmp_path_factory.mktemp("score")
    seed_raw(out / "r", archive=archive, config=config)
    stages = [Stage.FETCH, Stage.NORMALISE, Stage.LINK]
    manifest = build("r", config=config, out_dir=out, stages=stages, offline=True)
    return manifest.release_dir


def _read(release_dir: Path, stage: str, table: str) -> pl.DataFrame:
    return pl.read_parquet(table_path(release_dir, stage, table))


@pytest.fixture(scope="module")
def first_year(release_dir: Path) -> int:
    """The first CRS year the build loads, as the link stage reads it."""
    return read_years(_read(release_dir, "fetch", "raw_manifest"))["crs"][0]


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
def scored(
    release_dir: Path, keyed_links: pl.DataFrame, crs: pl.LazyFrame, first_year: int
) -> Scored:
    # Slow: scores the archive's candidates again, as the link stage did.
    return score_candidates(
        _read(release_dir, "link", "candidates"),
        units=_read(release_dir, "link", "units"),
        crs=crs,
        links=keyed_links,
        id_practice=_read(release_dir, "link", "id_practice"),
        stems=_read(release_dir, "link", "programme_stems"),
        first_year=first_year,
    )

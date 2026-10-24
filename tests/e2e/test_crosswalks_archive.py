"""The GEF crosswalk and the provider profile on the archive."""

import os
from pathlib import Path

import polars as pl
import pytest
from archive_build import archive_config, seed_release

from climate_finance.build import Stage, build
from climate_finance.build.link.crosswalks import GEF_DONOR, crosswalk_profile
from climate_finance.build.stages import table_path

pytestmark = pytest.mark.slow


@pytest.fixture(scope="module")
def linked(tmp_path_factory: pytest.TempPathFactory) -> Path:
    # Slow: normalises 3.8 million CRS records and links them, about 40 seconds.
    archive = Path(os.environ["CF_E2E_DATA"]) / "raw"
    config = archive_config(archive, key_repair=False, recoveries=True)
    out = tmp_path_factory.mktemp("crosswalks")
    seed_release(out / "r", archive=archive, config=config)
    stages = [Stage.FETCH, Stage.NORMALISE, Stage.LINK]
    return build(
        "r", config=config, out_dir=out, stages=stages, offline=True
    ).release_dir


def _gef(profile: pl.DataFrame) -> tuple[float, float, float]:
    row = profile.filter(provider_code=GEF_DONOR)
    return row.select("climate_usd", "crosswalk_usd", "crosswalk_share").row(0)


def test_the_gef_crosswalk_reaches_a_tenth_of_gef_climate_value(linked: Path) -> None:
    """Over 2015 to 2024 the crosswalk reaches USD 0.954bn of GEF's 8.674bn, almost
    all of it committed before 2021: the GEF IATI file names agency ids for few
    recent projects."""
    profile = pl.read_parquet(table_path(linked, "link", "provider_profile"))
    climate, crosswalk, share = _gef(profile)
    assert climate == pytest.approx(8.6745e9, rel=1e-4)
    assert crosswalk == pytest.approx(0.95404e9, rel=1e-4)
    assert share == pytest.approx(0.1100, abs=1e-4)
    others = profile.filter(pl.col("provider_code") != GEF_DONOR)
    assert others["crosswalk_usd"].is_null().all()


def test_the_gef_crosswalk_reaches_little_of_2021_and_2022(linked: Path) -> None:
    """USD 4.2m of the 780.6m GEF committed in 2021 and 2022."""
    crdf = pl.scan_parquet(table_path(linked, "normalise", "crdf")).filter(
        pl.col("year").is_between(2021, 2022)
    )
    links = pl.read_parquet(table_path(linked, "link", "links"))
    profile = crosswalk_profile(crdf, links=links, crosswalks=True)
    climate, crosswalk, share = _gef(profile)
    assert climate == pytest.approx(780.55e6, rel=1e-4)
    assert crosswalk == pytest.approx(4.2e6, rel=1e-4)
    assert share == pytest.approx(0.0054, abs=1e-4)

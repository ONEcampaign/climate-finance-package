"""The GEF crosswalk and the provider profile on the archive."""

import os
from pathlib import Path

import polars as pl
import pytest
from archive_build import archive_config, seed_raw

from climate_finance.build import Stage, build
from climate_finance.build.link.crosswalks import GEF_DONOR, crosswalk_profile
from climate_finance.build.stages import table_path

pytestmark = pytest.mark.slow

# GEF's counted climate value and the part the crosswalk reaches over 2015 to 2024,
# in USD.
CLIMATE_2015_USD = 8.6745e9
CROSSWALK_2015_USD = 1.08036e9
# What the CRDF's earlier years add: the build reads every year the recipient CRDF
# holds, and GEF reports from 2013. Its 2013 and 2014 rows hold USD 1.455bn, and
# the crosswalk reaches 0.435bn of them.
EARLIER_YEARS_CLIMATE_USD = 1.454634e9
EARLIER_YEARS_CROSSWALK_USD = 0.435155e9


@pytest.fixture(scope="module")
def linked(tmp_path_factory: pytest.TempPathFactory) -> Path:
    # Slow: normalises 3.8 million CRS records and links them, about 40 seconds.
    archive = Path(os.environ["CF_E2E_DATA"]) / "raw"
    config = archive_config(archive, key_repair=False, recoveries=True)
    out = tmp_path_factory.mktemp("crosswalks")
    seed_raw(out / "r", archive=archive, config=config)
    stages = [Stage.FETCH, Stage.NORMALISE, Stage.LINK]
    return build(
        "r", config=config, out_dir=out, stages=stages, offline=True
    ).release_dir


def _gef(profile: pl.DataFrame) -> tuple[float, float, float]:
    row = profile.filter(provider_code=GEF_DONOR)
    return row.select("climate_usd", "crosswalk_usd", "crosswalk_share").row(0)


def test_the_gef_crosswalk_reaches_a_tenth_of_gef_climate_value(linked: Path) -> None:
    """Over 2013 to 2024 the crosswalk reaches USD 1.516bn of GEF's 10.129bn, almost
    all of it committed before 2021. The GEF IATI file names agency ids for few
    recent projects. USD 126.3m of it comes from 2024 disbursements whose crs id
    the GEF reuses for another country's commitment; `crs_key` keys such an id by
    recipient, so the crosswalk links them to their own country's project."""
    profile = pl.read_parquet(table_path(linked, "link", "provider_profile"))
    climate, crosswalk, share = _gef(profile)
    expected_climate = CLIMATE_2015_USD + EARLIER_YEARS_CLIMATE_USD
    expected_crosswalk = CROSSWALK_2015_USD + EARLIER_YEARS_CROSSWALK_USD
    assert climate == pytest.approx(expected_climate, rel=1e-4)
    assert crosswalk == pytest.approx(expected_crosswalk, rel=1e-4)
    assert share == pytest.approx(expected_crosswalk / expected_climate, abs=1e-4)
    others = profile.filter(pl.col("provider_code") != GEF_DONOR)
    assert others["crosswalk_usd"].is_null().all()


def test_the_gef_crosswalk_reaches_little_of_2021_and_2022(linked: Path) -> None:
    """USD 8.25m of the 780.6m GEF committed in 2021 and 2022, 4.05m of it an
    Armenian project whose 2024 disbursement shares its crs id with another
    country's commitment."""
    crdf = pl.scan_parquet(table_path(linked, "normalise", "crdf")).filter(
        pl.col("year").is_between(2021, 2022)
    )
    links = pl.read_parquet(table_path(linked, "link", "links"))
    profile = crosswalk_profile(crdf, links=links, crosswalks=True)
    climate, crosswalk, share = _gef(profile)
    assert climate == pytest.approx(780.55e6, rel=1e-4)
    assert crosswalk == pytest.approx(8.25e6, rel=1e-4)
    assert share == pytest.approx(0.0106, abs=1e-4)

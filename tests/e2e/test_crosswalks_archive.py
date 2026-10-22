"""The GEF crosswalk and the provider profile on the archive."""

import os
from pathlib import Path

import polars as pl
import pytest
from archive_build import archive_config, seed_release

from climate_finance.build import Stage, build
from climate_finance.build.link.crosswalks import (
    GEF_DONOR,
    US_DONOR,
    crosswalk_profile,
    read_us_crosswalk,
)
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
    units = pl.read_parquet(table_path(linked, "link", "units"))
    profile = crosswalk_profile(crdf, links=links, units=units, us_awards=None)
    climate, crosswalk, share = _gef(profile)
    assert climate == pytest.approx(780.55e6, rel=1e-4)
    assert crosswalk == pytest.approx(4.2e6, rel=1e-4)
    assert share == pytest.approx(0.0054, abs=1e-4)


def test_the_us_crosswalk_keys_half_of_2021_and_2022_and_its_ledger_a_quarter(
    linked: Path,
) -> None:
    """Of USD 6.750bn of US climate value committed in 2021 and 2022, 3.534bn sits on
    CRS project numbers that ForeignAssistance.gov gives one award, and 1.923bn on
    awards whose USAID IATI activity reports disbursements or expenditure (read
    from the prototype's parse, since the build reads no USAID IATI)."""
    data = Path(os.environ["CF_E2E_DATA"])
    awards = read_us_crosswalk(data / "raw" / "external" / "foreignassistance")
    parsed = data / "raw" / "bilateral-gate" / "parsed" / "usaid.tx.parquet"
    spending = (
        pl.read_parquet(parsed)
        .filter(pl.col("type").is_in(["3", "4"]))
        .select(award=pl.col("iati_id").str.replace("^US-GOV-1-", ""))
        .unique()
    )
    crdf = pl.scan_parquet(table_path(linked, "normalise", "crdf")).filter(
        pl.col("year").is_between(2021, 2022)
    )
    links = pl.read_parquet(table_path(linked, "link", "links"))
    units = pl.read_parquet(table_path(linked, "link", "units"))

    def us(us_awards: pl.DataFrame) -> tuple[float, float, float]:
        profile = crosswalk_profile(crdf, links=links, units=units, us_awards=us_awards)
        row = profile.filter(provider_code=US_DONOR)
        return row.select("climate_usd", "crosswalk_usd", "crosswalk_share").row(0)

    climate, keyed, _ = us(awards)
    assert climate == pytest.approx(6.7498e9, rel=1e-4)
    assert keyed == pytest.approx(3.5338e9, rel=1e-4)
    _, ledgered, share = us(awards.join(spending, on="award", how="semi"))
    assert ledgered == pytest.approx(1.9233e9, rel=1e-4)
    assert share == pytest.approx(0.2849, abs=1e-4)

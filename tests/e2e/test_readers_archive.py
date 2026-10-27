"""The build readers on the archived raw files, whose figures the repo cannot hold."""

import os
from pathlib import Path

import polars as pl
import pytest

from climate_finance.build.readers.gcf import read_gcf_projects
from climate_finance.build.readers.iati import read_iati_activities
from climate_finance.build.readers.imputed_shares import read_imputed_shares

pytestmark = pytest.mark.slow


@pytest.fixture(scope="module")
def raw() -> Path:
    return Path(os.environ["CF_E2E_DATA"]) / "raw"


def _value(frame: pl.DataFrame, **where: object) -> float:
    match = frame.filter(*(pl.col(k) == v for k, v in where.items()))
    assert match.height == 1, match
    return match["share"].item()


def test_imputed_shares_read_the_oecd_workbook(raw: Path) -> None:
    frame = read_imputed_shares(raw / "Imputed_multilateral_shares.xlsx")

    # The 2020 sheet gives the African Development Bank one share.
    assert _value(frame, channel_code=46002, year=2020) == pytest.approx(0.35769)
    # CGIAR in 2024: mitigation only 0.0693, adaptation only 0.1778 and both 0.7362;
    # adaptation and mitigation each include both.
    cgiar = {"channel_code": 47015, "year": 2024}
    both = 0.7362004712504528
    assert _value(frame, **cgiar, objective="cross_cutting") == pytest.approx(both)
    assert _value(frame, **cgiar, objective="mitigation") == pytest.approx(
        0.06934312576919095 + both
    )
    assert _value(frame, **cgiar, objective="adaptation") == pytest.approx(
        0.1778427968687458 + both
    )
    # A key holds split rows or a climate row, never both.
    climate = frame.filter(pl.col("objective") == "climate")
    split = frame.filter(pl.col("objective") != "climate")
    assert climate.join(split, on=["channel_code", "year"]).is_empty()


def test_iati_reads_every_archived_publisher(raw: Path) -> None:
    frame = read_iati_activities(raw / "external" / "iati")

    assert frame.height == 28_787
    gef = frame.filter(publisher="gef")
    # GEF's disbursements as published, cumulative figures included.
    published = gef.filter(transaction_type="disbursement", currency="USD")["amount"]
    assert published.sum() == pytest.approx(10_220_413_469)
    assert gef.filter("cumulative_declared")["activity_id"].n_unique() == 4_013


def test_gcf_projects_read_the_api_file(raw: Path) -> None:
    frame = read_gcf_projects(raw / "external" / "gcf_api" / "projects.json")

    fp001 = frame.filter(
        activity_id="FP001", year=2022, transaction_type="disbursement"
    )
    assert fp001["amount"].item() == 1_990_182

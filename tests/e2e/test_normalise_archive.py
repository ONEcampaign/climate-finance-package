"""The CRS and CRDF normalisers on the archived OECD files."""

import os
from pathlib import Path

import polars as pl
import pytest

from climate_finance import schema
from climate_finance.build.normalise.crdf import normalise_crdf
from climate_finance.build.normalise.crs import (
    normalise_crs,
    psi_flags,
    trace_population,
)

pytestmark = pytest.mark.slow


@pytest.fixture(scope="module")
def oecd() -> Path:
    return Path(os.environ["CF_E2E_DATA"]) / "raw" / "oecd"


@pytest.fixture(scope="module")
def crs(oecd: Path) -> pl.DataFrame:
    # Slow: digests 3.8 million records.
    raw = pl.scan_parquet(oecd / "CRS_v20261006.parquet")
    return normalise_crs(raw, years=(2010, 2025)).collect()


def test_crs_2022_trace_population(crs: pl.DataFrame) -> None:
    schema.validate(schema.CRS, crs)
    assert crs["record_id"].is_unique().all()

    with_flow = (pl.col("commitment_usd").fill_null(0) != 0) | (
        pl.col("disbursement_usd").fill_null(0) != 0
    )
    traced_2022 = trace_population(crs.lazy()).filter(pl.col("year") == 2022)
    assert traced_2022.filter(with_flow).collect().height == 318_554
    # Records with only a grant equivalent stay in the table.
    assert traced_2022.collect().height == 324_528


@pytest.mark.parametrize("perspective", ["recipient", "provider"])
def test_crdf_validates(oecd: Path, crs: pl.DataFrame, perspective: str) -> None:
    raw = pl.scan_parquet(oecd / f"crdf_{perspective}.parquet")
    crdf = normalise_crdf(
        raw,
        perspective=perspective,
        crs=crs.lazy(),
        psi_flags=psi_flags(pl.scan_parquet(oecd / "CRS_v20261006.parquet")),
    ).collect()

    schema.validate(schema.CRDF, crdf)
    assert crdf["row_id"].is_unique().all()


def test_eib_id_scheme_for_every_vintage(oecd: Path, crs: pl.DataFrame) -> None:
    raw = pl.scan_parquet(oecd / "crdf_recipient.parquet")
    crdf = normalise_crdf(
        raw,
        perspective="recipient",
        crs=crs.lazy(),
        psi_flags=psi_flags(pl.scan_parquet(oecd / "CRS_v20261006.parquet")),
    ).collect()

    eib = crdf.filter(pl.col("id_scheme").is_not_null())
    counts = {
        (vintage, scheme): n
        for vintage, scheme, n in eib.group_by("vintage", "id_scheme").len().iter_rows()
    }
    assert counts == {
        (2015, "none"): 70,
        (2016, "eib_serial"): 75,
        (2017, "eib_serial"): 93,
        (2018, "eib_serial"): 111,
        (2019, "eib_serial"): 46,
        (2019, "sequence"): 48,
        (2020, "eib_serial"): 30,
        (2020, "sequence"): 5,
        (2021, "sequence"): 80,
        (2022, "sequence"): 67,
        (2023, "sequence"): 99,
        (2024, "bulk"): 39,
    }
    assert eib.height == crdf.filter(provider_code=918, agency_code=3).height

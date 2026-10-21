"""Agency climate shares and the modelled line on the archived OECD files."""

import os
from pathlib import Path

import polars as pl
import pytest

from climate_finance import schema
from climate_finance.build.measure import (
    agency_shares,
    counted_commitments,
    load_provider_facts,
    modelled_ranges,
)
from climate_finance.build.normalise.aggregates import rebuild_aggregates
from climate_finance.build.normalise.crdf import normalise_crdf
from climate_finance.build.normalise.crs import normalise_crs, trace_population
from climate_finance.build.normalise.vintages import mark_repeats
from climate_finance.build.readers.imputed_shares import read_imputed_shares

pytestmark = pytest.mark.slow

# Banks that report climate components, which ONE takes as reported, as the OECD
# does, so their ONE shares should equal the OECD's: IBRD, IDA, IFC, AfDB, AfDF,
# CABEI, CAF, EBRD and AIIB.
COMPONENT_BANKS = (44001, 44002, 44004, 46002, 46003, 46007, 46008, 46015, 46026)
IFC, CAF, IDB_INVEST, NDB, EU = 903, 1015, 1019, 1044, 918


@pytest.fixture(scope="module")
def raw() -> Path:
    return Path(os.environ["CF_E2E_DATA"]) / "raw"


@pytest.fixture(scope="module")
def crs(raw: Path) -> pl.LazyFrame:
    # Slow: digests 3.8 million records.
    bulk = pl.scan_parquet(raw / "oecd" / "CRS_v20261006.parquet")
    return trace_population(normalise_crs(bulk, years=(2010, 2025)).collect().lazy())


@pytest.fixture(scope="module")
def crdf(raw: Path, crs: pl.LazyFrame) -> pl.LazyFrame:
    normalised = pl.concat(
        normalise_crdf(
            pl.scan_parquet(raw / "oecd" / f"crdf_{perspective}.parquet"),
            perspective=perspective,
            crs=crs,
        )
        for perspective in ("recipient", "provider")
    )
    exchange = pl.read_parquet(raw / "dac_exchange.parquet")
    rebuilt = rebuild_aggregates(
        mark_repeats(normalised, crs=crs, exchange=exchange), crs=crs
    )
    return counted_commitments(rebuilt).collect().lazy()


def _climate(shares: pl.DataFrame) -> pl.DataFrame:
    """Each channel, year and source's climate share: adaptation + mitigation -
    cross-cutting, or the unsplit share."""

    def part(objective: str) -> pl.Expr:
        return pl.col("share").filter(pl.col("objective") == objective).sum()

    return shares.group_by("channel_code", "year", "source").agg(
        climate=part("adaptation")
        + part("mitigation")
        - part("cross_cutting")
        + part("climate")
    )


def test_one_shares_equal_the_oecds_for_component_banks(
    raw: Path, crs: pl.LazyFrame, crdf: pl.LazyFrame
) -> None:
    shares = agency_shares(
        crdf,
        crs=crs,
        imputed_shares=read_imputed_shares(
            raw / "oecd" / "Imputed_multilateral_shares.xlsx"
        ).lazy(),
        agencies=load_provider_facts().agencies,
        years=(2015, 2024),
    ).collect()
    schema.validate(schema.AGENCY_SHARES, shares)

    compared = (
        _climate(shares)
        .filter(
            pl.col("channel_code").is_in(COMPONENT_BANKS),
            pl.col("year").is_between(2021, 2022),
        )
        .pivot(on="source", index=["channel_code", "year"], values="climate")
    )

    assert compared.height == 2 * len(COMPONENT_BANKS)
    # Both divide two years of climate commitments by two years of total
    # commitments; the OECD's own vintage of the files differs by a few rows.
    assert (compared["one"] - compared["oecd"]).abs().max() < 0.004


def test_modelled_ranges(crs: pl.LazyFrame, crdf: pl.LazyFrame) -> None:
    providers = load_provider_facts().modelled
    not_traceable = crdf.select("row_id", status=pl.lit("not_traceable"))
    modelled = modelled_ranges(
        crdf, crs=crs, status=not_traceable, providers=providers, last_year=2024
    ).collect()
    schema.validate(schema.MODELLED, modelled)

    # The EIB's line covers its own agency of the EU Institutions.
    covered = crdf.filter(
        pl.col("year").is_between(2021, 2022),
        pl.col("face_usd") > 0,
        (pl.col("provider_code") != EU) | (pl.col("agency_code") == 3),
    )
    climate = covered.group_by("provider_code").agg(climate=pl.col("climate_usd").sum())
    totals = {
        provider: (low / total, high / total)
        for provider, low, high, total in (
            modelled.lazy()
            .filter(pl.col("year").is_between(2021, 2022))
            .group_by("provider_code")
            .agg(pl.col("low_usd").sum(), pl.col("high_usd").sum())
            .join(climate, on="provider_code")
            .collect()
            .iter_rows()
        )
    }

    # Low and high over 2021 to 2022 climate commitments. IFC: its annual report's
    # 0.92 and 1.05, capped. IDB Invest: 2020 to 2024 cumulative 0.75, window 0.87
    # and 0.88. CAF and the NDB by recipient, the NDB widened by its report's 0.47 and
    # its two cancelled projects at zero. The EIB at its vintage ratios, 0.77 for
    # 2021 and 0.79 for 2022.
    expected = {
        IFC: (0.920, 1.000),
        IDB_INVEST: (0.747, 0.875),
        CAF: (0.530, 0.692),
        NDB: (0.439, 0.839),
        EU: (0.782, 0.782),
    }
    assert totals.keys() == expected.keys()
    for provider, bounds in expected.items():
        assert totals[provider] == pytest.approx(bounds, abs=0.001), provider

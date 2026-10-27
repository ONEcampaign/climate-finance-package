"""Cross-vintage repeats and the aggregate-row rebuild on the archived OECD files."""

import os
from pathlib import Path

import polars as pl
import pytest

from climate_finance import schema
from climate_finance.build.normalise.aggregates import (
    FLOW_CATEGORY,
    continent,
    rebuild_aggregates,
)
from climate_finance.build.normalise.crdf import normalise_crdf
from climate_finance.build.normalise.crs import normalise_crs
from climate_finance.build.normalise.vintages import mark_repeats

pytestmark = pytest.mark.slow


@pytest.fixture(scope="module")
def raw() -> Path:
    return Path(os.environ["CF_E2E_DATA"]) / "raw"


@pytest.fixture(scope="module")
def bulk(raw: Path) -> pl.LazyFrame:
    return pl.scan_parquet(raw / "oecd" / "CRS_v20261006.parquet")


@pytest.fixture(scope="module")
def crs(bulk: pl.LazyFrame) -> pl.LazyFrame:
    # Slow: digests 3.8 million records.
    return normalise_crs(bulk, years=(2010, 2025)).collect().lazy()


@pytest.fixture(scope="module")
def crdf(raw: Path, crs: pl.LazyFrame) -> pl.DataFrame:
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
    ).collect()
    schema.validate(schema.CRDF, rebuilt)
    assert rebuilt["row_id"].is_unique().all()
    return rebuilt


@pytest.fixture(scope="module")
def recipient(crdf: pl.DataFrame) -> pl.DataFrame:
    return crdf.filter(pl.col("perspective") == "recipient")


def test_repeats_across_vintages(recipient: pl.DataFrame) -> None:
    repeats = recipient.filter(
        pl.col("year").is_between(2015, 2024), pl.col("repeat_of").is_not_null()
    )

    # Every group has a row after its first year, so its first row names it.
    assert repeats["repeat_of"].n_unique() == 79
    assert repeats["climate_usd"].sum() / 1e9 == pytest.approx(2.69, abs=0.005)

    agra = recipient.filter(pl.col("title").str.starts_with("AGRA METRO RAIL"))
    first = agra.filter(year=2021)["row_id"].item()
    assert agra.filter(year=2024)["repeat_of"].to_list() == [first]


def test_aggregate_rows_rebuild_from_bulk_records(recipient: pl.DataFrame) -> None:
    children = recipient.filter(pl.col("parent_row_id").is_not_null())
    parents = recipient.join(
        children.select(row_id="parent_row_id").unique(), on="row_id", how="semi"
    )

    in_2021_2022 = parents.filter(pl.col("year").is_between(2021, 2022))
    assert in_2021_2022.height == 98
    assert in_2021_2022["climate_usd"].sum() / 1e9 == pytest.approx(5.64, abs=0.005)

    norway = parents.filter(
        provider_code=8, year=2021, recipient_code=298, rio_mitigation=2
    ).filter((pl.col("climate_usd") / 1e6).round(2) == 224.47)
    rebuilt = children.filter(parent_row_id=norway["row_id"].item())
    assert rebuilt.height == 10
    assert rebuilt["agency_code"].unique().to_list() == [8]  # Norfund
    assert rebuilt["climate_usd"].sum() == pytest.approx(norway["climate_usd"].item())


def test_continents_and_categories_agree_with_the_bulk_file(bulk: pl.LazyFrame) -> None:
    region = pl.col("region_name")
    by_name = (
        pl.when(region.str.contains("Africa|Sahara"))
        .then(298)
        .when(region.str.contains("America|Caribbean"))
        .then(498)
        .when(region.str.contains("Asia|Middle East"))
        .then(798)
        .when(region.str.contains("Oceania"))
        .then(889)
        .when(region.str.contains("Europe"))
        .then(89)
        .otherwise(998)
    )
    pairs = (
        bulk.filter(pl.col("year").is_between(2010, 2025), region.is_not_null())
        .select("recipient_code", "region_name", "flow_code", "category")
        .unique()
        .collect()
    )

    regions = pairs.select("recipient_code", by_name=by_name).unique()
    disagree = regions.filter(continent(pl.col("recipient_code")) != pl.col("by_name"))
    assert disagree.is_empty(), disagree
    flows = pairs.select("flow_code", "category").unique()
    assert dict(flows.iter_rows()) == FLOW_CATEGORY

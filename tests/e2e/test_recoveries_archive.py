"""Organisation resolution and the recovery routes on the archived OECD and IATI
files."""

import os
import shutil
from pathlib import Path

import polars as pl
import pytest

from climate_finance import schema
from climate_finance.build.link.crdf_crs import link_within_year, recover
from climate_finance.build.link.keys import build_units
from climate_finance.build.normalise.aggregates import rebuild_aggregates
from climate_finance.build.normalise.crdf import normalise_crdf
from climate_finance.build.normalise.crs import normalise_crs, trace_population
from climate_finance.build.normalise.external import normalise_external
from climate_finance.build.normalise.orgs import DFI_PACK, org_names, resolve_orgs
from climate_finance.build.normalise.vintages import mark_repeats
from climate_finance.build.readers.iati import read_iati_activities

pytestmark = pytest.mark.slow

YEARS = (2010, 2025)


@pytest.fixture(scope="module")
def raw() -> Path:
    return Path(os.environ["CF_E2E_DATA"]) / "raw"


@pytest.fixture(scope="module")
def bulk(raw: Path) -> pl.LazyFrame:
    return pl.scan_parquet(raw / "oecd" / "CRS_v20261006.parquet").filter(
        pl.col("year").is_between(*YEARS)
    )


@pytest.fixture(scope="module")
def raw_crdf(raw: Path) -> list[pl.LazyFrame]:
    return [
        pl.scan_parquet(raw / "oecd" / f"crdf_{perspective}.parquet")
        for perspective in ("recipient", "provider")
    ]


@pytest.fixture(scope="module")
def orgs(bulk: pl.LazyFrame, raw_crdf: list[pl.LazyFrame]) -> pl.DataFrame:
    orgs = resolve_orgs(
        org_names(crs=bulk, crdf=raw_crdf),
        channels=bulk.select("channel_code", "channel_name", "channel_reported_name"),
    )
    schema.validate(schema.ORGS, orgs)
    return orgs


def test_every_crs_donor_with_an_oecd_provider_code_resolves(
    bulk: pl.LazyFrame, orgs: pl.DataFrame
) -> None:
    donors = (
        bulk.select("donor_name", truth=pl.col("donor_code").cast(pl.Int32))
        .unique()
        .collect()
        .join(
            orgs.filter(role="provider"),
            left_on="donor_name",
            right_on="source_name",
            how="left",
        )
    )

    assert donors.height == 169
    resolved = donors.drop_nulls("donor_code")
    assert resolved.height == 168
    assert (resolved["donor_code"] == resolved["truth"]).all()
    # resolvekit's bundled packs give it no `oecd:provider` code.
    assert set(donors.filter(pl.col("donor_code").is_null())["donor_name"]) == {
        "EU Institutions"
    }


def test_agency_names_resolve_to_the_codes_their_sources_print(
    bulk: pl.LazyFrame, raw_crdf: list[pl.LazyFrame], orgs: pl.DataFrame
) -> None:
    printed = pl.concat(
        [
            bulk.select(
                "agency_name", provider_code="donor_code", agency_code="agency_code"
            ),
            *(
                frame.select("agency_name", "provider_code", "agency_code")
                for frame in raw_crdf
            ),
        ]
    )
    pairs = (
        printed.select(
            source_name=pl.col("agency_name").str.strip_chars(),
            donor_code=pl.col("provider_code").cast(pl.Int32),
            truth=pl.col("agency_code").cast(pl.Int32),
        )
        .drop_nulls()
        .unique()
        .collect()
        .join(orgs.filter(role="agency"), on=["source_name", "donor_code"])
    )

    correct = (pairs["agency_code"] == pairs["truth"]).sum()
    # 691 of 693. The org pack files IDB's "Ordinary Capital" under agency 4, and
    # "BPI France" under 95.
    assert (pairs.height, correct) == (693, 691)
    assert correct / pairs.height >= 0.99


def test_the_dfi_pack_names_agencies_the_crs_reports(bulk: pl.LazyFrame) -> None:
    reported = set(
        bulk.select("donor_code", "agency_code").unique().collect().iter_rows()
    )

    assert {(donor, agency) for donor, agency, _, _ in DFI_PACK} <= reported


@pytest.fixture(scope="module")
def links(
    raw: Path, bulk: pl.LazyFrame, raw_crdf: list[pl.LazyFrame], tmp_path_factory
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """(links, recipient-perspective CRDF rows) with recoveries on."""
    # Slow: digests 3.8 million records.
    crs = normalise_crs(bulk, years=YEARS).collect().lazy()
    exchange = pl.read_parquet(raw / "dac_exchange.parquet")
    perspectives = ("recipient", "provider")
    crdf = rebuild_aggregates(
        mark_repeats(
            pl.concat(
                normalise_crdf(frame, perspective=perspective, crs=crs)
                for frame, perspective in zip(raw_crdf, perspectives, strict=True)
            ),
            crs=crs,
            exchange=exchange,
        ),
        crs=crs,
    ).filter(perspective="recipient")
    iati = tmp_path_factory.mktemp("iati")
    shutil.copytree(raw / "external" / "iati" / "eib", iati / "eib")
    external = normalise_external(read_iati_activities(iati), exchange=exchange)
    units = build_units(trace_population(crs)).collect()
    links = recover(
        crdf,
        within_year=link_within_year(crdf, units=units.lazy()).collect(),
        units=units.lazy(),
        crs=crs,
        external=external.lazy(),
        exchange=exchange,
    )
    schema.validate(schema.LINKS, links)
    return links, crdf.collect()


def _recovered(links: pl.DataFrame, crdf: pl.DataFrame) -> dict[str, tuple]:
    """Rows and USD bn of climate value each route links in 2021 and 2022."""
    rows = (
        links.filter(task="crdf_crs")
        .select(row_id="left_id", method="link_method")
        .unique()
        .join(crdf, on="row_id")
        .filter(pl.col("year").is_between(2021, 2022))
        .group_by("method")
        .agg(pl.len(), (pl.col("climate_usd").sum() / 1e9).round(3))
    )
    return {method: (n, usd) for method, n, usd in rows.iter_rows()}


def test_the_recovery_routes_link_the_eib_and_aggregate_rows(
    links: tuple[pl.DataFrame, pl.DataFrame],
) -> None:
    recovered = _recovered(*links)

    # The EIB's project list: as the prototype, 42 rows.
    assert recovered["eib_list"] == (42, 1.741)
    # The IATI crosswalk links contracts within 0.01% in euros; the title route
    # finds no project within 10% in these years.
    assert recovered["crosswalk"] == (25, 1.264)
    assert "title_recipient" not in recovered
    # Six children of records without a crs id, among them German finance
    # ministry's USD 1.43bn.
    assert recovered["aggregate_partition"] == (6, 1.79)

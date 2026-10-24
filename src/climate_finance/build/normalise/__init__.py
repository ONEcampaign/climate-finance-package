"""The normalise stage: CRDF, CRS and side sources into the stage schemas."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from climate_finance.build.fetch import (
    CRDF_FILES,
    CRS_FILE,
    DAC_EXCHANGE_FILE,
    IATI_DIR,
    IMPUTED_SHARES_FILE,
    MULTISYSTEM_FILE,
    fetched_years,
    ledger_years,
)
from climate_finance.build.normalise.aggregates import rebuild_aggregates
from climate_finance.build.normalise.crdf import normalise_crdf
from climate_finance.build.normalise.crs import normalise_crs
from climate_finance.build.normalise.external import normalise_external
from climate_finance.build.normalise.ledgers import normalise_ledgers
from climate_finance.build.normalise.orgs import org_names, resolve_orgs
from climate_finance.build.normalise.vintages import mark_repeats
from climate_finance.build.readers.gcf import PROJECTS_FILE, read_gcf_projects
from climate_finance.build.readers.iati import read_iati_activities
from climate_finance.build.readers.imputed_shares import read_imputed_shares
from climate_finance.build.readers.multisystem import read_multisystem

if TYPE_CHECKING:
    from climate_finance.build.stages import StageContext, StageOutput


def run_normalise(context: StageContext) -> StageOutput:
    """Normalise the files fetch wrote under `raw/` into the normalise tables.

    Reads only those files, and of the CRS, each CRDF perspective and MULTI only
    the years `raw_manifest` records for it, which an offline fetch may have
    narrowed. It writes no `btr` table, which the release treats as optional.
    `crdf` marks rows repeated across vintages (`repeat_of`) and gains child rows
    for the bilateral aggregate rows the bulk records rebuild (`parent_row_id`).
    `crdf_span` holds the recipient rows of the year before the first CRDF year,
    which the recipient file holds where the CRDF does, less those a row of the
    first CRDF year repeats, with their aggregates rebuilt and `repeat_of` null.
    `orgs` resolves the CRS's and CRDF's provider, agency and channel names to DAC
    codes, and is written only when recoveries are on. `ledger` holds the bank
    ledgers for the years `raw_manifest` records for them, and is written only
    when the build config names them.
    """
    raw = context.raw_dir
    years = fetched_years(context)
    raw_crdf = {
        perspective: pl.scan_parquet(raw / path).filter(
            pl.col("year").is_between(*years[f"crdf_{perspective}"])
        )
        for perspective, path in CRDF_FILES.items()
    }
    first = years["crdf_recipient"][0]
    raw_crs = pl.scan_parquet(raw / CRS_FILE)
    crs = normalise_crs(raw_crs, years=years["crs"]).collect()
    exchange = pl.read_parquet(raw / DAC_EXCHANGE_FILE)
    crdf = pl.concat(
        normalise_crdf(frame, perspective=perspective, crs=crs.lazy())
        for perspective, frame in raw_crdf.items()
    )
    crdf = rebuild_aggregates(
        mark_repeats(crdf, crs=crs.lazy(), exchange=exchange), crs=crs.lazy()
    )
    crdf_span = rebuild_aggregates(
        _span_rows(
            pl.scan_parquet(raw / CRDF_FILES["recipient"]).filter(
                pl.col("year") == first - 1
            ),
            recipient=raw_crdf["recipient"],
            crs=crs.lazy(),
            exchange=exchange,
            before=first,
        ),
        crs=crs.lazy(),
    )
    external = normalise_external(
        pl.concat(
            [
                read_iati_activities(raw / IATI_DIR),
                read_gcf_projects(raw / PROJECTS_FILE),
            ]
        ),
        exchange=exchange,
    )
    tables: dict[str, pl.DataFrame | pl.LazyFrame] = {
        "crdf": crdf,
        "crdf_span": crdf_span,
        "crs": crs,
        "core_contributions": read_multisystem(raw / MULTISYSTEM_FILE).filter(
            pl.col("year").is_between(*years["multisystem"])
        ),
        "imputed_shares": read_imputed_shares(raw / IMPUTED_SHARES_FILE),
        "external": external,
    }
    if (ledgers := context.config.sources.ledgers) is not None:
        tables["ledger"] = normalise_ledgers(
            raw,
            publishers=ledgers.publishers,
            years=ledger_years(context),
            exchange=exchange,
        )
    if context.config.recoveries:
        in_years = raw_crs.filter(pl.col("year").is_between(*years["crs"]))
        tables["orgs"] = resolve_orgs(
            org_names(crs=in_years, crdf=raw_crdf.values()),
            channels=in_years.select(
                "channel_code", "channel_name", "channel_reported_name"
            ),
        )
    return tables


def _span_rows(
    span: pl.LazyFrame,
    *,
    recipient: pl.LazyFrame,
    crs: pl.LazyFrame,
    exchange: pl.DataFrame,
    before: int,
) -> pl.LazyFrame:
    """The normalised `span` rows of years before `before`, less those a `recipient`
    row of `before` repeats.

    They are normalised and matched beside the `recipient` rows of `before`, so that
    `vintage` names each workbook's latest year, as it does in `crdf`, and a
    commitment reported in both years counts once across the span, under `before`
    in `crdf`.
    """
    rows = mark_repeats(
        normalise_crdf(
            pl.concat(
                [span, recipient.filter(pl.col("year") == before)],
                how="diagonal_relaxed",
            ),
            perspective="recipient",
            crs=crs,
        ),
        crs=crs,
        exchange=exchange,
    ).collect()
    repeated = rows.select(row_id="repeat_of").drop_nulls()
    return (
        rows.lazy()
        .filter(pl.col("year") < before)
        .join(repeated.lazy(), on="row_id", how="anti")
    )

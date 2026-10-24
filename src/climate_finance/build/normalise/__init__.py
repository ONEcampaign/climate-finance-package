"""The normalise stage: CRDF, CRS and side sources into the stage schemas."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from climate_finance.build.fetch import (
    BTR_DIR,
    CRDF_FILES,
    CRDF_SPAN_FILE,
    CRS_FILE,
    DAC_EXCHANGE_FILE,
    GCF_FILE,
    IATI_DIR,
    IMPUTED_SHARES_FILE,
    MULTISYSTEM_FILE,
)
from climate_finance.build.normalise.aggregates import rebuild_aggregates
from climate_finance.build.normalise.crdf import normalise_crdf
from climate_finance.build.normalise.crs import normalise_crs
from climate_finance.build.normalise.external import normalise_external, to_usd
from climate_finance.build.normalise.ledgers import normalise_ledgers
from climate_finance.build.normalise.orgs import org_names, resolve_orgs
from climate_finance.build.normalise.vintages import mark_repeats
from climate_finance.build.readers.btr import read_btr_archive
from climate_finance.build.readers.gcf import read_gcf_projects
from climate_finance.build.readers.iati import read_iati_activities
from climate_finance.build.readers.imputed_shares import read_imputed_shares
from climate_finance.build.readers.multisystem import read_multisystem

if TYPE_CHECKING:
    from climate_finance.build.stages import StageContext, StageOutput


def run_normalise(context: StageContext) -> StageOutput:
    """Normalise the files fetch wrote under `raw/` into the normalise tables.

    Reads only those files. `btr` holds the BTR workbooks' Table III.1 rows with the
    DAC rate of each row's currency and year and their amounts restated at it, and
    `btr_workbooks` what was read from
    each workbook. Both are written only when the build config names a BTR archive.
    `crdf` marks rows
    repeated across vintages (`repeat_of`) and gains child rows for the bilateral
    aggregate rows the bulk records rebuild (`parent_row_id`). `crdf_span` holds the
    recipient rows of the year before `years.crdf`, from
    `raw/oecd/crdf_span.parquet`, less those a row of the first `crdf` year repeats,
    with their aggregates rebuilt and `repeat_of` null. `orgs` resolves the CRS's and
    CRDF's provider, agency and channel names to DAC codes, and is written only when
    recoveries are on.
    `ledger` holds the bank ledgers, and is written only when the build config names
    them.
    """
    raw = context.raw_dir
    raw_crdf = {
        perspective: pl.scan_parquet(raw / path)
        for perspective, path in CRDF_FILES.items()
    }
    raw_crs = pl.scan_parquet(raw / CRS_FILE)
    first, last = context.config.years.crs
    crs = normalise_crs(raw_crs, years=(first, last)).collect()
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
            pl.scan_parquet(raw / CRDF_SPAN_FILE),
            recipient=raw_crdf["recipient"],
            crs=crs.lazy(),
            exchange=exchange,
            before=context.config.years.crdf[0],
        ),
        crs=crs.lazy(),
    )
    external = normalise_external(
        pl.concat(
            [read_iati_activities(raw / IATI_DIR), read_gcf_projects(raw / GCF_FILE)]
        ),
        exchange=exchange,
    )
    tables: dict[str, pl.DataFrame | pl.LazyFrame] = {
        "crdf": crdf,
        "crdf_span": crdf_span,
        "crs": crs,
        "core_contributions": read_multisystem(raw / MULTISYSTEM_FILE),
        "imputed_shares": read_imputed_shares(raw / IMPUTED_SHARES_FILE),
        "external": external,
    }
    if context.config.sources.btr_archive is not None:
        btr = read_btr_archive(raw / BTR_DIR)
        tables["btr"] = btr_at_dac_rates(btr.rows, exchange=exchange)
        tables["btr_workbooks"] = btr.workbooks
    if (ledgers := context.config.sources.ledgers) is not None:
        tables["ledger"] = normalise_ledgers(raw, ledgers=ledgers, exchange=exchange)
    if context.config.recoveries:
        in_years = raw_crs.filter(pl.col("year").is_between(first, last))
        tables["orgs"] = resolve_orgs(
            org_names(crs=in_years, crdf=raw_crdf.values()),
            channels=in_years.select(
                "channel_code", "channel_name", "channel_reported_name"
            ),
        )
    return tables


def btr_at_dac_rates(btr: pl.DataFrame, *, exchange: pl.DataFrame) -> pl.DataFrame:
    """`btr` with `dac_rate`, the DAC rate of each row's currency and year in that
    currency per USD as `to_usd` finds it, and the domestic amounts restated at it.
    A row without the domestic amount or the rate keeps the party's USD."""
    rates = to_usd(
        btr.select("currency", "year")
        .unique()
        .with_columns(amount=pl.lit(1.0), amount_usd=pl.lit(None, pl.Float64)),
        exchange=exchange,
    ).select("currency", "year", dac_rate=1 / pl.col("amount_usd"))
    return (
        btr.drop("dac_rate")
        .join(rates, on=["currency", "year"], how="left", nulls_equal=True)
        .with_columns(
            face_usd_at_dac=pl.coalesce(
                pl.col("amount_domestic") / pl.col("dac_rate"), "face_usd"
            ),
            grant_equiv_usd_at_dac=pl.coalesce(
                pl.col("grant_equiv_domestic") / pl.col("dac_rate"), "grant_equiv_usd"
            ),
        )
        .select(btr.columns)
    )


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

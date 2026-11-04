"""The normalise stage: CRDF, CRS and side sources into the stage schemas."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

import polars as pl

from climate_finance import schema
from climate_finance.build.btr_institutions import institution_map
from climate_finance.build.fetch import (
    BTR_DIR,
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
from climate_finance.build.normalise.crs import (
    normalise_crs,
    normalise_reflows,
    psi_flags,
)
from climate_finance.build.normalise.external import normalise_external, to_usd
from climate_finance.build.normalise.ledgers import normalise_ledgers
from climate_finance.build.normalise.orgs import org_names, resolve_orgs
from climate_finance.build.normalise.vintages import mark_repeats
from climate_finance.build.readers.btr import read_btr_archive
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
    narrowed. `reflows` holds the CRS records that report principal repaid or
    interest paid, and `crs` those that report an amount. A record with both is in
    both under one `record_id`. `btr` holds the BTR workbooks' Table III.1 rows
    with the DAC rate of each row's currency and year and their amounts restated at
    it, and `btr_workbooks` what was read from each workbook. `btr_multilateral`
    and `btr_multilateral_workbooks` hold the same for Table III.2, each row with
    its institution group. The four are written only when the build config names a
    BTR archive. `crdf` marks rows repeated across
    vintages (`repeat_of`) and gains child rows for the bilateral aggregate rows
    the bulk records rebuild (`parent_row_id`). `crdf_span` holds the recipient
    rows of the year before the first CRDF year, which the recipient file holds
    where the CRDF does, less those a row of the first CRDF year repeats, with
    their aggregates rebuilt and `repeat_of` null. `orgs` resolves the CRS's and
    CRDF's provider, agency and channel names to DAC codes, and is written only
    when recoveries are on. `ledger` holds the bank ledgers for the years
    `raw_manifest` records for them, and is written only when the build config
    names them.
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
    flags = psi_flags(raw_crs).collect().lazy()
    exchange = pl.read_parquet(raw / DAC_EXCHANGE_FILE)
    crdf = pl.concat(
        normalise_crdf(frame, perspective=perspective, crs=crs.lazy(), psi_flags=flags)
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
            psi_flags=flags,
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
        "reflows": normalise_reflows(raw_crs, years=years["crs"]),
        "core_contributions": read_multisystem(raw / MULTISYSTEM_FILE).filter(
            pl.col("year").is_between(*years["multisystem"])
        ),
        "imputed_shares": read_imputed_shares(raw / IMPUTED_SHARES_FILE),
        "external": external,
    }
    if context.config.sources.btr_archive is not None:
        btr = read_btr_archive(raw / BTR_DIR)
        tables["btr"] = btr_at_dac_rates(btr.rows, exchange=exchange)
        tables["btr_workbooks"] = btr.workbooks
        tables["btr_multilateral"] = btr_multilateral(
            btr.multilateral, exchange=exchange
        )
        tables["btr_multilateral_workbooks"] = btr.multilateral_workbooks
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


def btr_at_dac_rates(
    btr: pl.DataFrame, *, exchange: pl.DataFrame, groups: Sequence[str] = ("",)
) -> pl.DataFrame:
    """`btr` with `dac_rate`, the DAC rate of each row's currency and year in that
    currency per USD as `to_usd` finds it, and the domestic amounts restated at it.
    A row without the domestic amount or the rate keeps the party's USD. Each of
    `groups` prefixes one set of amounts: `{group}amount_domestic` restates as
    `{group}face_usd_at_dac` and `{group}grant_equiv_domestic` as
    `{group}grant_equiv_usd_at_dac`."""
    rates = to_usd(
        btr.select("currency", "year")
        .unique()
        .with_columns(amount=pl.lit(1.0), amount_usd=pl.lit(None, pl.Float64)),
        exchange=exchange,
    ).select("currency", "year", dac_rate=1 / pl.col("amount_usd"))
    restated = {
        f"{group}{kind}_usd_at_dac": pl.coalesce(
            pl.col(f"{group}{domestic}") / pl.col("dac_rate"), f"{group}{kind}_usd"
        )
        for group in groups
        for kind, domestic in (
            ("face", "amount_domestic"),
            ("grant_equiv", "grant_equiv_domestic"),
        )
    }
    return (
        btr.drop("dac_rate")
        .join(rates, on=["currency", "year"], how="left", nulls_equal=True)
        .with_columns(**restated)
        .select(btr.columns)
    )


def btr_multilateral(rows: pl.DataFrame, *, exchange: pl.DataFrame) -> pl.DataFrame:
    """The Table III.2 rows of `read_btr_archive` as `btr_multilateral`: each with
    the group `institution_map` gives its institution, and its amounts restated at
    DAC rates as `btr_at_dac_rates` restates Table III.1's."""
    names = institution_map()
    groups = {
        name: group
        for name in rows["institution"].drop_nulls().unique()
        if (group := names.group(name)) is not None
    }
    table = schema.BTR_MULTILATERAL
    filled = rows.with_columns(
        institution_group=pl.col("institution").replace_strict(
            groups, default=None, return_dtype=pl.String
        ),
        **{
            name: pl.lit(None, pl.Float64)
            for name in table.columns
            if name == "dac_rate" or name.endswith("_at_dac")
        },
    ).select(list(table.columns))
    return btr_at_dac_rates(
        filled, exchange=exchange, groups=("core_", "climate_specific_")
    )


def _span_rows(
    span: pl.LazyFrame,
    *,
    recipient: pl.LazyFrame,
    crs: pl.LazyFrame,
    psi_flags: pl.LazyFrame,
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
            psi_flags=psi_flags,
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

"""The normalise stage: CRDF, CRS and side sources into the stage schemas."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

import polars as pl

from climate_finance.build.fetch import (
    CRDF_FILES,
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
from climate_finance.build.normalise.external import normalise_external
from climate_finance.build.normalise.orgs import resolve_orgs
from climate_finance.build.normalise.vintages import mark_repeats
from climate_finance.build.readers.gcf import read_gcf_projects
from climate_finance.build.readers.iati import read_iati_activities
from climate_finance.build.readers.imputed_shares import read_imputed_shares
from climate_finance.build.readers.multisystem import read_multisystem

if TYPE_CHECKING:
    from climate_finance.build.stages import StageContext, StageOutput


def run_normalise(context: StageContext) -> StageOutput:
    """Normalise the files fetch wrote under `raw/` into the normalise tables.

    Reads only those files, and the ingested BTR table when the config names one
    (`sources.btr_table`); `btr` is left out otherwise. `crdf` marks rows repeated
    across vintages (`repeat_of`) and gains child rows for the bilateral aggregate
    rows the bulk records rebuild (`parent_row_id`). `orgs` resolves the CRDF's
    provider, agency and channel names, and is written only when recoveries are on,
    the only routes that read it.
    """
    raw = context.raw_dir
    raw_crdf = {
        perspective: pl.scan_parquet(raw / path)
        for perspective, path in CRDF_FILES.items()
    }
    crs = normalise_crs(
        pl.scan_parquet(raw / CRS_FILE), years=context.config.years.crs
    ).collect()
    exchange = pl.read_parquet(raw / DAC_EXCHANGE_FILE)
    crdf = pl.concat(
        normalise_crdf(frame, perspective=perspective, crs=crs.lazy())
        for perspective, frame in raw_crdf.items()
    )
    crdf = rebuild_aggregates(
        mark_repeats(crdf, crs=crs.lazy(), exchange=exchange), crs=crs.lazy()
    )
    external = normalise_external(
        pl.concat(
            [read_iati_activities(raw / IATI_DIR), read_gcf_projects(raw / GCF_FILE)]
        ),
        exchange=exchange,
    )
    tables: dict[str, pl.DataFrame | pl.LazyFrame] = {
        "crdf": crdf,
        "crs": crs,
        "core_contributions": read_multisystem(raw / MULTISYSTEM_FILE),
        "imputed_shares": read_imputed_shares(raw / IMPUTED_SHARES_FILE),
        "external": external,
    }
    if context.config.recoveries:
        tables["orgs"] = resolve_orgs(_org_names(raw_crdf.values()))
    # BuildConfig declares no `btr_table` until the BTR reader is built.
    if (btr_table := context.config.sources.btr_table) is not None:
        tables["btr"] = pl.scan_parquet(btr_table)
    return tables


def _org_names(frames: Iterable[pl.LazyFrame]) -> pl.LazyFrame:
    """Every provider, agency and channel name the CRDF prints, once."""
    columns = ("provider_name", "agency_name", "channel_name")
    return (
        pl.concat(
            frame.select(source_name=pl.col(name))
            for frame in frames
            for name in columns
        )
        .drop_nulls()
        .unique()
    )

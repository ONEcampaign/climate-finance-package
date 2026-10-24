"""The normalise stage: CRDF, CRS and side sources into the stage schemas."""

from __future__ import annotations

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
from climate_finance.build.normalise.orgs import org_names, resolve_orgs
from climate_finance.build.normalise.vintages import mark_repeats
from climate_finance.build.readers.gcf import read_gcf_projects
from climate_finance.build.readers.iati import read_iati_activities
from climate_finance.build.readers.imputed_shares import read_imputed_shares
from climate_finance.build.readers.multisystem import read_multisystem

if TYPE_CHECKING:
    from climate_finance.build.stages import StageContext, StageOutput


def run_normalise(context: StageContext) -> StageOutput:
    """Normalise the files fetch wrote under `raw/` into the normalise tables.

    Reads only those files, and of the CRDF and MULTI only the configured CRDF
    years, which an offline fetch may have narrowed. It writes no `btr` table, which
    the release treats as optional. `crdf` marks rows repeated across vintages
    (`repeat_of`) and gains child rows for the bilateral aggregate rows the bulk
    records rebuild (`parent_row_id`). `orgs` resolves the CRS's and CRDF's
    provider, agency and channel names to DAC codes, and is written only when
    recoveries are on.
    """
    raw = context.raw_dir
    in_crdf_years = pl.col("year").is_between(*context.config.years.crdf)
    raw_crdf = {
        perspective: pl.scan_parquet(raw / path).filter(in_crdf_years)
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
    external = normalise_external(
        pl.concat(
            [read_iati_activities(raw / IATI_DIR), read_gcf_projects(raw / GCF_FILE)]
        ),
        exchange=exchange,
    )
    tables: dict[str, pl.DataFrame | pl.LazyFrame] = {
        "crdf": crdf,
        "crs": crs,
        "core_contributions": read_multisystem(raw / MULTISYSTEM_FILE).filter(
            in_crdf_years
        ),
        "imputed_shares": read_imputed_shares(raw / IMPUTED_SHARES_FILE),
        "external": external,
    }
    if context.config.recoveries:
        in_years = raw_crs.filter(pl.col("year").is_between(first, last))
        tables["orgs"] = resolve_orgs(
            org_names(crs=in_years, crdf=raw_crdf.values()),
            channels=in_years.select(
                "channel_code", "channel_name", "channel_reported_name"
            ),
        )
    return tables

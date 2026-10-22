"""The link stage: keys, id practice, candidates and scoring into `links`."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from climate_finance.build.fetch import DAC_EXCHANGE_FILE
from climate_finance.build.link.crdf_crs import link_within_year, recover
from climate_finance.build.link.id_practice import classify_id_practice
from climate_finance.build.link.key_repair import repair_keys
from climate_finance.build.link.keys import build_units, link_by_keys
from climate_finance.build.normalise.crs import trace_population

if TYPE_CHECKING:
    from climate_finance.build.stages import StageContext, StageOutput


def run_link(context: StageContext) -> StageOutput:
    """Link CRDF rows to CRS records and later disbursements to commitments.

    Units sum the CRS trace population. Recipient-perspective CRDF rows join
    commitment units within their year, and disbursement units link to commitments
    through the exact key tiers under each agency's id practice. Key repair runs when
    `key_repair` is on. When `recoveries` is on, the EIB recovery routes link the
    EIB rows the within-year join leaves out, reading the `external` table and the
    DAC exchange rates fetch wrote.
    """
    config = context.config
    crs = context.read("normalise", "crs")
    crdf = context.read("normalise", "crdf").filter(perspective="recipient")
    units = build_units(trace_population(crs)).collect()
    id_practice = classify_id_practice(units)
    trace = link_by_keys(units, first_year=config.years.crs[0])
    crdf_crs = link_within_year(crdf, units=units.lazy()).collect()
    if config.recoveries:
        crdf_crs = recover(
            crdf,
            within_year=crdf_crs,
            units=units.lazy(),
            crs=crs,
            external=context.read("normalise", "external"),
            exchange=pl.read_parquet(context.raw_dir / DAC_EXCHANGE_FILE),
        )
    links = [trace, crdf_crs]
    if config.key_repair:
        links.append(
            repair_keys(units, crs=crs, links=trace, first_year=config.years.crs[0])
        )
    return {"links": pl.concat(links), "units": units, "id_practice": id_practice}

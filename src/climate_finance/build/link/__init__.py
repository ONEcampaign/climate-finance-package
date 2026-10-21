"""The link stage: keys, id practice, candidates and scoring into `links`."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

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
    `key_repair` is on, and the recovery routes, with the tables only they read,
    when `recoveries` is on.
    """
    config = context.config
    crs = context.read("normalise", "crs")
    crdf = context.read("normalise", "crdf").filter(perspective="recipient")
    units = build_units(trace_population(crs)).collect()
    id_practice = classify_id_practice(units)
    trace = link_by_keys(units, id_practice=id_practice, first_year=config.years.crs[0])
    links = [trace, link_within_year(crdf, units=units.lazy()).collect()]
    if config.key_repair:
        links.append(
            repair_keys(
                units.lazy(),
                commitments=units.lazy().filter(pl.col("commitment_usd") > 0),
                links=trace.lazy(),
            ).collect()
        )
    if config.recoveries:
        external = context.read("normalise", "external")
        links.append(recover(crdf, crs=crs, external=external).collect())
    return {"links": pl.concat(links), "units": units, "id_practice": id_practice}

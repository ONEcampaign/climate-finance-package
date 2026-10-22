"""The link stage: keys, id practice, candidates and scoring into `links`."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from climate_finance.build.fetch import DAC_EXCHANGE_FILE, IATI_DIR
from climate_finance.build.link.candidates import (
    generate_candidates,
    learn_programme_stems,
)
from climate_finance.build.link.crdf_crs import link_within_year, recover
from climate_finance.build.link.crosswalks import (
    crosswalk_profile,
    gef_crosswalk,
    link_by_crosswalk,
)
from climate_finance.build.link.id_practice import classify_id_practice
from climate_finance.build.link.key_repair import repair_keys
from climate_finance.build.link.keys import build_units, link_by_keys
from climate_finance.build.link.score import provider_profile, score_candidates
from climate_finance.build.link.splink_models import (
    read_human_labels,
    score_with_splink,
)
from climate_finance.build.normalise.crs import trace_population
from climate_finance.build.readers.iati import read_related_activities

if TYPE_CHECKING:
    from climate_finance.build.stages import StageContext, StageOutput


def run_link(context: StageContext) -> StageOutput:
    """Link CRDF rows to CRS records and later disbursements to commitments.

    Units sum the CRS trace population. Recipient-perspective CRDF rows join
    commitment units within their year, and disbursement units link to commitments
    through the exact key tiers under each agency's id practice. Key repair runs when
    `key_repair` is on. When `recoveries` is on, the EIB recovery routes link the
    EIB rows the within-year join leaves out, reading the `external` table and the
    DAC exchange rates fetch wrote, except the routes `skip_eib_routes` names, and
    GEF disbursements link to GEF commitments through the GEF IATI file's crosswalk
    to agency ids. Programme stems are learned from the trace population, and
    blocking pairs the units the key, repair and crosswalk links left for scoring.
    The provider profile measures each provider's climate value and what its
    crosswalk reaches, and sorts providers into scoring groups; the truth group's
    classifier links what it scores at or above its threshold. Splink scores the
    other groups' pairs into `pair_scores`, calibrated by people's recorded
    decisions and Jev.
    """
    config = context.config
    crs = context.read("normalise", "crs")
    crdf = context.read("normalise", "crdf").filter(perspective="recipient")
    population = trace_population(crs)
    units = build_units(population).collect()
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
            skip=config.skip_eib_routes,
        )
    keyed = [trace]
    if config.key_repair:
        keyed.append(
            repair_keys(units, crs=crs, links=trace, first_year=config.years.crs[0])
        )
    found = [*keyed, crdf_crs]
    if config.recoveries:
        crosswalk = gef_crosswalk(
            read_related_activities(context.raw_dir / IATI_DIR / "gef")
        )
        found.append(
            link_by_crosswalk(
                units,
                crosswalk=crosswalk,
                earlier=pl.concat(keyed),
                first_year=config.years.crs[0],
            )
        )
    links = pl.concat(found)
    stems = learn_programme_stems(population).collect()
    candidates = generate_candidates(
        units,
        crs=population,
        links=links,
        id_practice=id_practice,
        stems=stems,
        first_year=config.years.crs[0],
    )
    scored = score_candidates(
        candidates,
        units=units,
        crs=population,
        links=links,
        id_practice=id_practice,
        stems=stems,
        first_year=config.years.crs[0],
    )
    splink = score_with_splink(
        candidates,
        units=units,
        crs=population,
        stems=stems,
        provider_profile=scored.profile,
        labels=read_human_labels(config.sources.review_decisions),
        client=context.jev,
    )
    return {
        "links": pl.concat([links, scored.links]),
        "units": units,
        "id_practice": id_practice,
        "programme_stems": stems,
        "candidates": candidates,
        "pair_scores": splink.scores,
        "provider_profile": provider_profile(
            crosswalk_profile(
                crdf,
                links=links,
                units=units,
                us_awards=None,
                crosswalks=config.recoveries,
            ),
            scoring=scored.profile,
        ),
    }

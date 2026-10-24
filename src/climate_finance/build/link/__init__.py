"""The link stage: keys, id practice, candidates and scoring into `links`."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from climate_finance.build.fetch import DAC_EXCHANGE_FILE, IATI_DIR
from climate_finance.build.link.btr import link_btr
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
from climate_finance.build.link.decisions import override_links
from climate_finance.build.link.disagreement import flag_disagreement
from climate_finance.build.link.embed import TextEmbedder
from climate_finance.build.link.id_practice import classify_id_practice
from climate_finance.build.link.jev import JevFitError, adjudicate_band, jev_answers
from climate_finance.build.link.key_repair import repair_keys
from climate_finance.build.link.keys import build_units, link_by_keys
from climate_finance.build.link.score import (
    classifier_links,
    provider_profile,
    score_candidates,
)
from climate_finance.build.link.splink_models import (
    read_decision_labels,
    score_with_splink,
    splink_links,
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
    blocking pairs the units the key, repair and crosswalk links left for scoring,
    with embedding neighbours when `embed` is on.
    The provider profile measures each provider's climate value and what its
    crosswalk reaches, and sorts providers into scoring groups; the truth group's
    classifier links what it scores at or above its threshold. Splink scores the
    other groups' pairs into `pair_scores`, calibrated by people's recorded
    decisions and Jev. With `jev_links` on, Jev adjudicates the classifier's band,
    answers each Splink model that people have not calibrated, and the calibrated
    Splink models link. Each trace link records whether its two sides disagree on
    recipient or project number (`flag_disagreement`). People's and agents'
    queue decisions then override the links they decided (`override_links`). When the
    build has BTR rows, each links to the same-year CRS records of its party
    (`link_btr`).

    Raises:
        JevFitError: when `jev_links` is on and the build has no truth group to
            set Jev's scale.
    """
    config = context.config
    decided = read_decision_labels(config.sources.review_decisions)
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
    embedder = (
        TextEmbedder(
            model=config.embedding.model,
            revision=config.embedding.revision,
            cache_dir=config.embedding.cache_dir,
            offline=context.offline,
        )
        if config.embed
        else None
    )
    candidates = generate_candidates(
        units,
        crs=population,
        links=links,
        id_practice=id_practice,
        stems=stems,
        first_year=config.years.crs[0],
        embedder=embedder,
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
    classifier = scored.links
    answer = None
    if config.jev_links:
        if scored.scores is None:
            raise JevFitError("jev_links needs a truth group to set Jev's scale")
        band = adjudicate_band(
            scored.scores,
            units=units,
            crs=population,
            stems=stems,
            band=config.jev.band,
            client=context.jev,
        )
        classifier = classifier_links(scored.scores, jev=band)
        answer = jev_answers(client=context.jev, scale=band.scale)
    splink = score_with_splink(
        candidates,
        units=units,
        crs=population,
        stems=stems,
        provider_profile=scored.profile,
        labels=decided,
        client=context.jev,
        answer=answer,
    )
    scored_links = [classifier]
    if config.jev_links:
        scored_links.append(
            splink_links(
                splink.scores,
                candidates=candidates,
                units=units,
                jev_model=context.jev.model,
            )
        )
    linked = [flag_disagreement(pl.concat([links, *scored_links]), units=units)]
    if config.sources.btr_archive is not None:
        linked.append(link_btr(context.read("normalise", "btr"), crs=crs))
    return {
        "links": override_links(pl.concat(linked), labels=decided),
        "units": units,
        "id_practice": id_practice,
        "programme_stems": stems,
        "candidates": candidates,
        "pair_scores": splink.scores,
        "provider_profile": provider_profile(
            crosswalk_profile(crdf, links=links, crosswalks=config.recoveries),
            scoring=scored.profile,
        ),
    }

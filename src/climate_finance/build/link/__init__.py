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
from climate_finance.build.link.decisions import (
    accepted_pairs,
    drop_pairs,
    override_links,
    rejected_pairs,
)
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
    DAC exchange rates fetch wrote, except the routes `skip_eib_routes` names. When
    `recoveries` is on and `sources.iati_publishers` holds `gef`, GEF disbursements
    link to GEF commitments through the GEF IATI file's crosswalk to agency ids.
    Each tier drops the links people and agents rejected (`drop_pairs`) before the
    next one runs, so key repair, the EIB routes and blocking see a rejected link's
    units as unlinked, and no tier links a rejected pair again. Programme stems are
    learned from the trace population, and blocking pairs the units the key, repair
    and crosswalk links left for scoring, with embedding neighbours when `embed` is
    on.
    The provider profile measures each provider's climate value and what its
    crosswalk reaches, and sorts providers into scoring groups. The truth group's
    classifier links what it scores at or above its threshold, and each scored
    disbursement whose best pair was rejected links its next best. Splink scores the
    other groups' pairs into `pair_scores`, calibrated by people's recorded
    decisions, and by Jev only with `jev_links` on. With `jev_links` on, Jev
    adjudicates the classifier's band, answers each Splink model that people have
    not calibrated, and the calibrated Splink models link. Without it the link stage
    makes no Jev client. Each trace link records whether its two sides disagree on
    recipient or project number (`flag_disagreement`). People's and agents'
    queue decisions then override the links they decided (`override_links`).

    Raises:
        JevFitError: when `jev_links` is on and the build has no truth group to
            set Jev's scale.
    """
    config = context.config
    decided = read_decision_labels(config.sources.review_decisions)
    rejected, accepted = rejected_pairs(decided), accepted_pairs(decided)
    rejected_trace = rejected.filter(task="trace").drop("task")

    def unrejected(tier: pl.DataFrame) -> pl.DataFrame:
        return drop_pairs(tier, pairs=rejected, accepted=accepted)

    crs = context.read("normalise", "crs")
    crdf = context.read("normalise", "crdf").filter(perspective="recipient")
    population = trace_population(crs)
    units = build_units(population).collect()
    id_practice = classify_id_practice(units)
    trace = unrejected(link_by_keys(units, first_year=config.years.crs[0]))
    crdf_crs = unrejected(link_within_year(crdf, units=units.lazy()).collect())
    if config.recoveries:
        crdf_crs = unrejected(
            recover(
                crdf,
                within_year=crdf_crs,
                units=units.lazy(),
                crs=crs,
                external=context.read("normalise", "external"),
                exchange=pl.read_parquet(context.raw_dir / DAC_EXCHANGE_FILE),
                skip=config.skip_eib_routes,
            )
        )
    keyed = [trace]
    if config.key_repair:
        keyed.append(
            unrejected(
                repair_keys(units, crs=crs, links=trace, first_year=config.years.crs[0])
            )
        )
    found = [*keyed, crdf_crs]
    gef = config.recoveries and "gef" in config.sources.iati_publishers
    if gef:
        crosswalk = gef_crosswalk(
            read_related_activities(context.raw_dir / IATI_DIR / "gef")
        )
        found.append(
            unrejected(
                link_by_crosswalk(
                    units,
                    crosswalk=crosswalk,
                    earlier=pl.concat(keyed),
                    first_year=config.years.crs[0],
                )
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
        rejected=rejected_trace,
    )
    classifier = scored.links
    client = context.jev if config.jev_links else None
    answer = None
    if client is not None:
        if scored.scores is None:
            raise JevFitError("jev_links needs a truth group to set Jev's scale")
        band = adjudicate_band(
            scored.scores,
            units=units,
            crs=population,
            stems=stems,
            band=config.jev.band,
            client=client,
        )
        classifier = classifier_links(scored.scores, jev=band, rejected=rejected_trace)
        answer = jev_answers(client=client, scale=band.scale)
    splink = score_with_splink(
        candidates,
        units=units,
        crs=population,
        stems=stems,
        provider_profile=scored.profile,
        labels=decided,
        client=client,
        answer=answer,
    )
    scored_links = [classifier]
    if client is not None:
        scored_links.append(
            splink_links(
                splink.scores,
                candidates=candidates,
                units=units,
                jev_model=client.model,
                rejected=rejected_trace,
            )
        )
    return {
        "links": override_links(
            flag_disagreement(pl.concat([links, *scored_links]), units=units),
            labels=decided,
        ),
        "units": units,
        "id_practice": id_practice,
        "programme_stems": stems,
        "candidates": candidates,
        "pair_scores": splink.scores,
        "provider_profile": provider_profile(
            crosswalk_profile(crdf, links=links, crosswalks=gef),
            scoring=scored.profile,
        ),
    }

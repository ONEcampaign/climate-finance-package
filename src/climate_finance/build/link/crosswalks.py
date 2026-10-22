"""Crosswalks: links by key for a provider whose commitments and disbursements carry
different ids, and the climate value they reach.

GEF commits under its GEF project id, while from 2017 the CRS records the
disbursements its implementing agencies make under the agencies' own project ids,
so no GEF commitment shares a key with those disbursements. The GEF IATI file
names, for some projects, the agency activity that carries it as a related
activity: a UNDP output, an FAO project, a World Bank project or an IDB operation.
`gef_crosswalk` reads those pairs, and `link_by_crosswalk` links each GEF
disbursement unit whose project number is such an agency id, and which no exact
key tier or key-repair rule links, to the commitment units of its GEF projects in
the same recipient.

A GEF commitment's project number is its GEF id, or in 2013 and 2014 the GEF id
followed by a dash and a suffix. An agency id can stand for several GEF projects
(19 do in the archive's GEF file), and allocation pools a group over every
commitment it holds, so a group is a component: the GEF projects that GEF ids and
agency ids connect, in one recipient. Every disbursement unit of a component
reaches all its commitment units, less those its record year or floor rules out,
and each commitment unit falls in one group.

The United States reports each CRS project number as `<submission id>_<activity
id>` of its ForeignAssistance.gov submission, as `76_62884`. ForeignAssistance.gov
gives each such activity a project number, which for USAID is the award number its
IATI activity ids carry. `read_us_crosswalk` reads that pairing; it keys the US
commitments to a ledger, and adds no trace link.

`crosswalk_profile` writes, per provider, the climate value of the counted CRDF
commitments and the part that links by key through a crosswalk, into
`provider_profile`.
"""

from pathlib import Path

import polars as pl

from climate_finance import schema
from climate_finance.build.link.keys import as_exact_links
from climate_finance.build.sums import ordered_sum

GEF_DONOR = 811
US_DONOR = 302
METHOD = "crosswalk"
US_CROSSWALK_FILE = "crosswalk.parquet"

_GEF_ACTIVITY = r"^XI-BRIDGE-6385676864-0*(\d+)$"
_GEF_PROJECT_NUMBER = r"^0*(\d+)(?:-|$)"
# ForeignAssistance.gov project numbers that name no award: empty, redacted, or an
# administrative placeholder.
_NO_AWARD = r"(?i)REDACT|^(N/?A\b.*|NULL|NONE|OE)?$"
# Each agency's id as the GEF IATI file references it and as the CRS project number
# carries it. UNDP numbers are zero-padded in some years and not in others.
_AGENCY_IDS = {
    "UNDP": (r"^XM-DAC-41114-OUTPUT-0*(\d+)$", r"^0*(\d{4,})$"),
    "FAO": (r"^XM-DAC-41301-0*(\d+)$", r"^PRJ_0*(\d+)$"),
    "WB": (r"^44000-(P\d{6})$", r"^(P\d{6})"),
    "IDB": (r"^XI-IATI-IADB-([A-Z]{2}-[A-Z]\d{4})$", r"^([A-Z]{2}-[A-Z]\d{4})(?:&|$)"),
}


def gef_crosswalk(related: pl.DataFrame) -> pl.DataFrame:
    """GEF ids with the agency ids that carry them, and the group each pair falls in.

    Args:
        related: `readers.iati.read_related_activities` over the GEF IATI files.

    Returns:
        One row per (`gef_id`, `agency_id`) pair. `agency_id` is the agency and
        its id, as `UNDP:107251`. `component` names the set of pairs that GEF ids
        and agency ids connect.
    """
    pairs = (
        related.select(
            gef_id=pl.col("activity_id").str.extract(_GEF_ACTIVITY),
            agency_id=_agency_id(pl.col("related_id"), side=0),
        )
        .drop_nulls()
        .unique()
        .sort("gef_id", "agency_id")
    )
    return _components(pairs)


def link_by_crosswalk(
    units: pl.DataFrame,
    *,
    crosswalk: pl.DataFrame,
    earlier: pl.DataFrame,
    first_year: int,
) -> pl.DataFrame:
    """`crosswalk` trace links from GEF disbursement units to the commitment units of
    the GEF projects their agency id's component holds, in the same recipient.

    Args:
        units: The link stage's units.
        crosswalk: `gef_crosswalk` output.
        earlier: The `trace` links of the exact key tiers and key-repair rules,
            which allocation runs first. A disbursement unit they link is left to
            them, so no unit pair carries two methods.
        first_year: The first CRS year the build loads. A disbursement whose floor
            precedes it links to nothing, as in the exact tiers.

    Returns:
        `links` rows, `group_id` naming the method, the crosswalk component and the
        recipient.
    """
    gef = units.filter(pl.col("donor_code") == GEF_DONOR)
    disbursements = (
        gef.filter(
            pl.col("disbursement_usd") > 0,
            pl.col("floor_year").is_null() | (pl.col("floor_year") >= first_year),
        )
        .join(earlier.select(unit_id="left_id"), on="unit_id", how="anti")
        .select(
            "unit_id",
            "recipient_code",
            "year",
            "floor_year",
            agency_id=_agency_id(pl.col("project_number"), side=1),
        )
        .drop_nulls("agency_id")
    )
    commitments = gef.filter(pl.col("commitment_usd") > 0).select(
        "recipient_code",
        "placed_year",
        commitment="unit_id",
        gef_id=pl.col("project_number").str.extract(_GEF_PROJECT_NUMBER),
    )
    eligible = (pl.col("placed_year") <= pl.col("year")) & (
        pl.col("floor_year").is_null() | (pl.col("placed_year") >= pl.col("floor_year"))
    )
    components = crosswalk.select("agency_id", "component").unique()
    projects = crosswalk.select("gef_id", "component").unique()
    pairs = (
        disbursements.join(components, on="agency_id")
        .join(projects, on="component")
        .join(commitments, on=["gef_id", "recipient_code"])
        .filter(eligible)
        .select(
            left_id="unit_id",
            right_id="commitment",
            group_id=pl.concat_str(
                [pl.lit(METHOD), "component", "recipient_code"], separator="|"
            ),
            link_method=pl.lit(METHOD),
        )
        .unique()
    )
    return as_exact_links(pairs.lazy(), task="trace").collect()


def read_us_crosswalk(path: Path) -> pl.DataFrame:
    """CRS project numbers of US activities with the award each names.

    A project number that names no award (`_NO_AWARD`) is left out, and so is a CRS
    project number ForeignAssistance.gov gives more than one award.

    Args:
        path: The directory holding `crosswalk.parquet`, as
            `sources.foreignassistance.download_foreign_assistance` writes it.

    Returns:
        `project_number`, as the CRS prints it, and `award`, upper-cased without
        spaces.

    Raises:
        FileNotFoundError: when `path` holds no `crosswalk.parquet`.
    """
    award = pl.col("project_number").str.replace_all(r"\s+", "").str.to_uppercase()
    return (
        pl.read_parquet(path / US_CROSSWALK_FILE)
        .filter(~pl.col("project_number").fill_null("").str.contains(_NO_AWARD))
        .select(
            project_number=pl.concat_str(
                "submission_id", "submission_activity_id", separator="_"
            ),
            award=award,
        )
        .unique()
        .filter(pl.len().over("project_number") == 1)
        .sort("project_number")
    )


def crosswalk_profile(
    crdf: pl.LazyFrame,
    *,
    links: pl.DataFrame,
    units: pl.DataFrame,
    us_awards: pl.DataFrame | None,
) -> pl.DataFrame:
    """`provider_profile`: each provider's counted climate value, and for a provider
    with a crosswalk the part that links by key through it.

    The rows counted are those `measure.counted_commitments` counts: recipient
    perspective, no repeat of an earlier vintage, and no aggregate parent whose
    rebuilt children stand in for it. A row links by key when a `crdf_crs` link
    takes it to a commitment unit that a `crosswalk` trace link fills (GEF), or to a
    US commitment unit whose project number names an award (`read_us_crosswalk`).
    The crosswalk columns are null for every other provider, and for the US when
    `us_awards` is None.

    Args:
        crdf: The normalise stage's `crdf`.
        links: The link stage's `links`.
        units: The link stage's `units`.
        us_awards: `read_us_crosswalk` output, or None where the build has no US
            crosswalk.
    """
    reached = links.filter(
        pl.col("task") == "trace", pl.col("link_method") == METHOD
    ).select(unit_id="right_id")
    measured = [GEF_DONOR]
    if us_awards is not None:
        awarded = units.filter(pl.col("donor_code") == US_DONOR).join(
            us_awards, on="project_number", how="semi"
        )
        reached = pl.concat([reached, awarded.select("unit_id")])
        measured.append(US_DONOR)
    reached = reached.unique()
    reached_rows = (
        links.filter(pl.col("task") == "crdf_crs")
        .select(row_id="left_id", unit_id="right_id")
        .join(reached, on="unit_id")
        .select("row_id", crosswalk=pl.lit(value=True))
        .unique()
    )
    recipient = crdf.filter(pl.col("perspective") == "recipient")
    parents = recipient.select(row_id="parent_row_id").drop_nulls().unique()
    climate = pl.col("climate_usd")
    has_crosswalk = pl.col("provider_code").is_in(measured)
    return (
        recipient.filter(pl.col("repeat_of").is_null())
        .join(parents, on="row_id", how="anti")
        .select("row_id", "provider_code", "climate_usd")
        .join(reached_rows.lazy(), on="row_id", how="left")
        .group_by("provider_code")
        .agg(
            climate_usd=ordered_sum(climate),
            crosswalk_usd=ordered_sum(climate.filter(pl.col("crosswalk"))),
        )
        .with_columns(
            crosswalk_usd=pl.when(has_crosswalk).then("crosswalk_usd"),
            crosswalk_share=pl.when(has_crosswalk & (climate != 0)).then(
                pl.col("crosswalk_usd") / climate
            ),
        )
        .select(list(schema.PROVIDER_PROFILE.columns))
        .collect()
    )


def _agency_id(ids: pl.Expr, *, side: int) -> pl.Expr:
    """`ids` as `<agency>:<id>` where one of `_AGENCY_IDS`' patterns matches, side 0
    reading IATI references and side 1 CRS project numbers."""
    return pl.coalesce(
        pl.concat_str(pl.lit(f"{agency}:"), ids.str.extract(patterns[side]))
        for agency, patterns in _AGENCY_IDS.items()
    )


def _components(pairs: pl.DataFrame) -> pl.DataFrame:
    """`pairs` with `component`, the least GEF id that GEF ids and agency ids
    connect it to."""
    labelled = pairs.with_columns(component=pl.col("gef_id"))
    while True:
        spread = labelled.with_columns(
            component=pl.col("component").min().over("agency_id")
        ).with_columns(component=pl.col("component").min().over("gef_id"))
        if spread.equals(labelled):
            return labelled
        labelled = spread

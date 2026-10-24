"""CRS records into units, and the exact key tiers that link them, first match wins.

A unit sums the CRS records that share every key field (`UNIT_KEY`). Each disbursement
unit takes the first tier in `TIERS` whose key reaches an eligible commitment unit,
meaning one placed at or before the disbursement's record year and at or after its floor
year. A disbursement under the commitment's own crs id passes its floor when no
commitment unit of that crs id or of its project number sits in its floor year, since
the floor then names no commitment the build can see, as with KfW's 2021-dated
disbursements under its 2020 commitment `2020142415`. An EU contract record is exempt
from the floor in the parent-id tier, since it carries the contract's own date, which
falls on or after its decision's commitment. A lettered record (`parent_id`) reaches its
parent only in the same recipient under the same project number, a missing number
matching only a missing one. A disbursement whose floor precedes the first CRS year
continues a commitment the build cannot see, and links to nothing.

The project-number tiers come last, so they see only what the crs-id tiers left, and
they link only where (donor, agency, award, recipient) maps to one crs id on the
commitment side, since shared framework numbers are right 43% of the time and
unique keys 84.5%. They hold for every agency: one whose ids mostly persist, such as
Korea's EDCF or KfW, still files some projects under a new crs id each year.
The award is the project number, joined to the purpose code where the award commits
its components under separate crs ids and purposes (`split_components`).
"""

from dataclasses import dataclass
from typing import overload

import polars as pl

from climate_finance import schema
from climate_finance.build.link.id_practice import (
    LETTER_SUFFIX_DONORS,
    PN_KEY,
    parent_id,
)
from climate_finance.build.normalise.ids import content_id
from climate_finance.build.normalise.project_numbers import normalise_pn

UNIT_KEY = (
    "donor_code",
    "agency_code",
    "crs_id",
    "project_number",
    "recipient_code",
    "purpose_code",
    "year",
    "commitment_year",
)
# The project-number tiers' key: one award of one agency in one recipient.
AWARD_KEY = ("donor_code", "agency_code", "award", "recipient_code")


@dataclass(frozen=True)
class Tier:
    """A key tier. It holds the commitment key, the disbursement key it must equal,
    whether it is a project-number tier, which reaches only unique award keys
    (`_unique_awards`), the disbursement column holding the floor it keeps, and
    whether its key holds the crs id, so that a disbursement under the
    commitment's own crs id may pass its floor."""

    name: str
    commitment_key: tuple[str, ...]
    unit_key: tuple[str, ...]
    project_number: bool = False
    floor: str = "floor_year"
    same_crs_id: bool = False


TIERS = (
    Tier(
        "crs_id_purpose",
        ("donor_code", "crs_id", "purpose_code", "recipient_code"),
        ("donor_code", "crs_id", "purpose_code", "recipient_code"),
        same_crs_id=True,
    ),
    Tier(
        "crs_id", ("donor_code", "crs_key"), ("donor_code", "crs_key"), same_crs_id=True
    ),
    Tier(
        "crs_id_parent",
        ("donor_code", "parent_key"),
        ("donor_code", "crs_parent"),
        floor="parent_floor_year",
    ),
    Tier(
        "pn_recipient_purpose",
        (*AWARD_KEY, "purpose_code"),
        (*AWARD_KEY, "purpose_code"),
        project_number=True,
    ),
    Tier("pn_recipient", AWARD_KEY, AWARD_KEY, project_number=True),
)
if tuple(tier.name for tier in TIERS) != schema.KEY_TIERS:
    raise RuntimeError("TIERS must follow schema.KEY_TIERS")


def build_units(crs: pl.LazyFrame) -> pl.LazyFrame:
    """Sum `crs` records into units, in the `units` schema.

    Units whose commitments and disbursements both sum to zero are left out.
    """
    dated = pl.col("commitment_year") < pl.col("year")
    return (
        crs.with_columns(
            project_number=normalise_pn(pl.col("project_number")),
            commitment_year=pl.col("commitment_date").dt.year().cast(pl.Int16),
        )
        .group_by(UNIT_KEY)
        # Amounts are summed as lists in record order. A partitioned group sum
        # adds in whatever order threads finish, which changes the last bit.
        .agg(
            pl.col("commitment_usd", "disbursement_usd").sort_by("record_id"),
            record_ids=pl.col("record_id").sort(),
        )
        .with_columns(pl.col("commitment_usd", "disbursement_usd").list.sum())
        .filter((pl.col("commitment_usd") != 0) | (pl.col("disbursement_usd") != 0))
        .with_columns(
            unit_id=content_id(UNIT_KEY, order_by=UNIT_KEY),
            placed_year=pl.when(dated)
            .then(pl.col("commitment_year"))
            .otherwise(pl.col("year")),
            floor_year=pl.when(dated).then(pl.col("commitment_year")),
        )
        .select(list(schema.UNITS.columns))
    )


def link_by_keys(units: pl.DataFrame, *, first_year: int) -> pl.DataFrame:
    """Trace links from each disbursement unit to every eligible commitment unit
    under its first matching tier, as `links` rows.

    `first_year` is the first CRS year the build loads. `group_id` names the tier
    and its key, so the disbursement units of one group reach the same commitments,
    less those their own record year or floor rules out.
    """
    units = units.with_columns(
        crs_key=crs_key(units),
        parent_key=_parent_key(pl.col("crs_id")),
        award=split_components(units, number="project_number"),
    )
    commitments = units.filter(pl.col("commitment_usd") > 0)
    remaining = units.filter(
        pl.col("disbursement_usd") > 0,
        pl.col("floor_year").is_null() | (pl.col("floor_year") >= first_year),
    ).with_columns(
        crs_parent=_parent_key(parent_id(pl.col("donor_code"), pl.col("crs_id"))),
        parent_floor_year=pl.when(pl.col("donor_code") != schema.EU_INSTITUTIONS).then(
            pl.col("floor_year")
        ),
    )
    remaining = remaining.with_columns(
        floor_held=_floor_held(remaining, commitments=commitments)
    )
    unique_awards = _unique_awards(commitments)

    pairs = []
    for tier in TIERS:
        tier_pairs = _eligible_pairs(
            remaining.drop_nulls(tier.unit_key),
            unique_awards if tier.project_number else commitments,
            tier=tier,
        )
        pairs.append(tier_pairs)
        remaining = remaining.join(
            tier_pairs.select(unit_id="left_id"), on="unit_id", how="anti"
        )
    return as_exact_links(pl.concat(pairs), task="trace")


def crs_key(units: pl.DataFrame) -> pl.Series:
    """Each unit's crs id, with its recipient code where the (donor, crs id) names
    several projects: units in more than one recipient under more than one project
    number, a missing number counting as one more. The AsDB, the IDB and the GEF
    reuse an id this way, as for the AsDB's `LN3375-IND` in India and `TA9143-NAU`
    in Nauru."""
    several = (
        (pl.col("recipient_code").n_unique() > 1)
        & (pl.col("project_number").n_unique() > 1)
    ).over("donor_code", "crs_id")
    return units.select(
        pl.when(several)
        .then(pl.concat_str("crs_id", "recipient_code", separator="|"))
        .otherwise(pl.col("crs_id"))
    ).to_series()


def _parent_key(crs_id: pl.Expr) -> pl.Expr:
    """`crs_id` as the parent-id tier matches it: for a `LETTER_SUFFIX_DONORS` unit
    with its recipient and project number, a missing number matching a missing
    one, since a lettered record continues one project in one recipient."""
    lettered = pl.concat_str(
        crs_id,
        pl.col("recipient_code"),
        pl.col("project_number").fill_null(""),
        separator="|",
    )
    return (
        pl.when(pl.col("donor_code").is_in(LETTER_SUFFIX_DONORS))
        .then(lettered)
        .otherwise(crs_id)
    )


def split_components(units: pl.DataFrame, *, number: str) -> pl.Series:
    """Each unit's `number`, joined to its purpose code under a component award: a
    (donor, agency, `number`) whose commitment units of one placed year carry more
    than one crs id and more than one purpose.

    The US commits each component of an award under its own crs id and purpose, as
    award `76_48621` commits its biosphere (41020), legislative (15152) and trade
    policy (33110) components in Viet Nam in 2015 under three crs ids, and
    disburses its HIV/AIDS component (13040) under a fourth. A link under the award
    then stays within one component.
    """
    award = ["donor_code", "agency_code", number]
    components = (
        units.filter(pl.col("commitment_usd") > 0)
        .drop_nulls(award)
        .group_by(*award, "placed_year")
        .agg(
            ids=pl.col("crs_id").n_unique(), purposes=pl.col("purpose_code").n_unique()
        )
        .filter(pl.col("ids") > 1, pl.col("purposes") > 1)
        .select(award)
        .unique()
        .with_columns(component=pl.lit(True))
    )
    return (
        units.select(*award, "purpose_code")
        .join(components, on=award, how="left", maintain_order="left")
        .select(
            pl.when(pl.col("component"))
            .then(pl.concat_str(number, "purpose_code", separator="|"))
            .otherwise(pl.col(number))
            .alias(number)
        )
        .to_series()
    )


def _unique_awards(commitments: pl.DataFrame) -> pl.DataFrame:
    """Commitments whose (donor, agency, award, recipient) maps to one crs id, a
    null crs id counting as one."""
    return commitments.drop_nulls(AWARD_KEY).filter(
        pl.col("crs_id").n_unique().over(AWARD_KEY) == 1
    )


def _floor_held(disbursements: pl.DataFrame, *, commitments: pl.DataFrame) -> pl.Series:
    """Whether a commitment unit sits in each disbursement's floor year under its
    crs id or its (donor, agency, project number, recipient). The floor then names
    a commitment the build can see, and holds in every tier."""
    held = pl.lit(False)
    for key in (("donor_code", "crs_id"), PN_KEY):
        placed = (
            commitments.select(*key, floor_year="placed_year")
            .drop_nulls()
            .unique()
            .with_columns(held=pl.lit(True))
        )
        found = disbursements.select(*key, "floor_year").join(
            placed, on=[*key, "floor_year"], how="left", maintain_order="left"
        )
        held = held | found["held"].fill_null(False)
    return disbursements.select(held.alias("floor_held")).to_series()


def _eligible_pairs(
    disbursements: pl.DataFrame, commitments: pl.DataFrame, *, tier: Tier
) -> pl.DataFrame:
    """(disbursement, commitment) unit pairs equal on the tier's key, the commitment
    placed no later than the record year and no earlier than the tier's floor. In a
    tier keyed on the crs id, the floor holds only where it names a commitment
    (`_floor_held`)."""
    floor = pl.col(tier.floor)
    passes = floor.is_null() | (pl.col("placed_year") >= floor)
    if tier.same_crs_id:
        passes |= ~pl.col("floor_held")
    eligible = (pl.col("placed_year") <= pl.col("year")) & passes
    return (
        disbursements.select(
            "unit_id", "year", tier.floor, "floor_held", *tier.unit_key
        )
        .join(
            commitments.select(
                "unit_id", "placed_year", *tier.commitment_key
            ).drop_nulls(tier.commitment_key),
            left_on=list(tier.unit_key),
            right_on=list(tier.commitment_key),
            suffix="_commitment",
        )
        .filter(eligible)
        .select(
            left_id="unit_id",
            right_id="unit_id_commitment",
            group_id=pl.concat_str(
                [pl.lit(tier.name), *(pl.col(key) for key in tier.unit_key)],
                separator="|",
            ),
            link_method=pl.lit(tier.name),
        )
    )


@overload
def as_exact_links(pairs: pl.DataFrame, *, task: str) -> pl.DataFrame: ...
@overload
def as_exact_links(pairs: pl.LazyFrame, *, task: str) -> pl.LazyFrame: ...
def as_exact_links(
    pairs: pl.DataFrame | pl.LazyFrame, *, task: str
) -> pl.DataFrame | pl.LazyFrame:
    """`pairs` (left_id, right_id, group_id, link_method) as rule `links` rows,
    eager or lazy as `pairs` is.

    `probability` is 1.0 on a certain link: a `CERTAIN_METHODS` tier, a
    `CERTAIN_ROUTES` route, or a unit linked to itself, whose records commit and
    disburse. It is null on every other rule link, which no model scored.
    """
    certain = pl.col("link_method").is_in(
        (*schema.CERTAIN_METHODS, *schema.CERTAIN_ROUTES)
    ) | (pl.col("left_id") == pl.col("right_id"))
    given = {name: pl.col(name) for name in pairs.collect_schema().names()}
    fixed = {
        "task": pl.lit(task),
        "probability": pl.when(certain).then(1.0),
        "review": pl.lit(False),
    }
    return pairs.select(
        given.get(name, fixed.get(name, pl.lit(None, dtype=column.dtype))).alias(name)
        for name, column in schema.LINKS.columns.items()
    )

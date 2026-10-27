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
continues a commitment the build sees only where a later record carries that
commitment's date, so a tier links it only where its key reaches a commitment placed
no later than the floor (`floor_seen`).

The World Bank tier comes first. IBRD and IDA crs ids name projects, and a project
holds several loans and credits, so a tier on the crs id lets allocation spend one
loan's disbursement on another loan's commitment. The loan or credit number in the
project number (`loan_number`) keeps it on its own loan, and reaches additional
financing booked under the parent project's number.

The project-number tiers come last, so they see only what the crs-id tiers left, and
each links only where its own key maps to one crs id on the commitment side
(`_unique_pn`). Shared framework numbers are right 43% of the time at the (number,
recipient) key and unique keys 84.5%. A key with the purpose or the commitment date is
narrower, so the tiers that use one do not inherit the coarser key's refusal: the AsDB
books one loan number under a new crs id each year, and Canada files one line of a
recipient under several crs ids split by purpose. They hold for every agency, since one
whose ids mostly persist, such as Korea's EDCF or KfW, still files some projects under
a new crs id each year. They compare awards (`AWARD_KEY`): the project number without
`-` and `_` (`pn_key`), joined to the purpose code where the award commits its
components under separate crs ids and purposes and one of its units disburses
without committing (`split_components`).

The first project-number tier adds the commitment date (`line_date`). A disbursement
record that repeats its line's commitment date continues that line, where the
project number and recipient alone reach the project's later lines too. The tier
links only where its key reaches one commitment unit, since a line split by purpose
under one date is left to the purpose tier. A date on 1 January or 31 December names
no line, since donors write it as a placeholder for the year and 38% of CRS records
carry 31 December against the 0.3% a real date would.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from functools import cache
from typing import overload

import polars as pl

from climate_finance import catalogue, schema
from climate_finance.build.link.id_practice import (
    LETTER_SUFFIX_DONORS,
    PN_KEY,
    canonical_crs_id,
    parent_id,
)
from climate_finance.build.link.text import largest_record
from climate_finance.build.normalise.ids import content_id
from climate_finance.build.normalise.project_numbers import normalise_pn

# Takes the review decisions to a tier's `links` rows (`decisions.drop_pairs`).
type Decide = Callable[[pl.DataFrame], pl.DataFrame]

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
# IBRD and IDA, whose project numbers carry the loan or credit number.
WORLD_BANK_DONORS = (901, 905)
# The project-number tiers' key: one award of one agency in one recipient.
AWARD_KEY = ("donor_code", "agency_code", "award", "recipient_code")
# The month and day of commitment dates that stand for a year (`line_date`).
PLACEHOLDER_DAYS = ("01-01", "12-31")


@dataclass(frozen=True)
class Tier:
    """A key tier. It holds the commitment key, the disbursement key it must equal
    where that differs (`parent_key`), the column whose values a project-number
    tier's commitment keys must hold one of (`unique_by`, `_unique_pn`), the
    disbursement column holding the floor it keeps, and whether its key holds the
    crs id, so that a disbursement under the commitment's own crs id may pass its
    floor."""

    name: str
    commitment_key: tuple[str, ...]
    parent_key: tuple[str, ...] | None = None
    unique_by: str | None = None
    floor: str = "floor_year"
    same_crs_id: bool = False

    @property
    def unit_key(self) -> tuple[str, ...]:
        """The disbursement key that must equal `commitment_key`."""
        return self.parent_key or self.commitment_key


TIERS = (
    Tier("wb_loan", ("donor_code", "loan")),
    Tier(
        "crs_id_purpose",
        ("donor_code", "crs_id", "purpose_code", "recipient_code"),
        same_crs_id=True,
    ),
    Tier("crs_id", ("donor_code", "crs_key"), same_crs_id=True),
    Tier(
        "crs_id_parent",
        ("donor_code", "parent_key"),
        parent_key=("donor_code", "crs_parent"),
        floor="parent_floor_year",
    ),
    Tier("pn_recipient_date", (*AWARD_KEY, "line_date"), unique_by="unit_id"),
    Tier("pn_recipient_purpose", (*AWARD_KEY, "purpose_code"), unique_by="crs_id"),
    Tier("pn_recipient", AWARD_KEY, unique_by="crs_id"),
)
if tuple(tier.name for tier in TIERS) != schema.KEY_TIERS:
    raise RuntimeError("TIERS must follow schema.KEY_TIERS")


def separator_free_pn(project_number: pl.Expr) -> pl.Expr:
    """A normalised project number without `-` and `_`, or null where fewer than
    two characters remain. Donors write one number with and without them, as
    Germany's `01LE1907A1_L` and `01LE1907A1L`."""
    stripped = project_number.str.replace_all(r"[-_]", "")
    return pl.when(stripped.str.len_chars() >= 2).then(stripped)


def loan_number(donor_code: pl.Expr, project_number: pl.Expr) -> pl.Expr:
    """The loan or credit number an IBRD or IDA project number carries after its
    project id (`P163452.IDA61960.CRS3`), or null for other donors."""
    return pl.when(donor_code.is_in(WORLD_BANK_DONORS)).then(
        project_number.str.split(".").list.get(1, null_on_oob=True)
    )


def build_units(crs: pl.LazyFrame) -> pl.LazyFrame:
    """Sum `crs` records into units, in the `units` schema.

    A unit's commitment date is its largest record's, by commitment plus
    disbursement, ties to the smaller record id. Units whose commitments and
    disbursements both sum to zero are left out.
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
            largest_record("commitment_date"),
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


def link_by_keys(
    units: pl.DataFrame, *, first_year: int, decide: Decide | None = None
) -> pl.DataFrame:
    """Trace links from each disbursement unit to every eligible commitment unit
    under its first matching tier, as `links` rows.

    `first_year` is the first CRS year the build loads. The crs-id tiers compare
    `canonical_crs_id`s and the project-number tiers awards (`AWARD_KEY`).
    `group_id` names the tier and its key, so the disbursement units of one group
    reach the same commitments, less those their own record year or floor rules
    out. `decide` applies the review decisions to each tier's links before the next
    tier runs, so a unit whose links a tier's decisions reject all goes on to the
    later tiers.
    """
    keyed = key_columns(units)
    return link_by_tiers(
        keyed.filter(pl.col("disbursement_usd") > 0),
        commitments=keyed.filter(pl.col("commitment_usd") > 0),
        first_year=first_year,
        decide=decide,
    )


def key_columns(units: pl.DataFrame) -> pl.DataFrame:
    """`units` with the columns the tiers key on.

    `units` may hold reflow units (`reflows.build_reflow_units`) beside the trace
    units, as rows that neither commit nor disburse. They take their keys from the
    trace units and change none of theirs: `crs_key` counts trace units only, and
    `_pn_key` and `split_components` read only commitments and disbursements.
    """
    units = units.with_columns(
        crs_id=canonical_crs_id(pl.col("crs_id")),
        line_date=line_date(pl.col("commitment_date")),
        loan=loan_number(pl.col("donor_code"), pl.col("project_number")),
    )
    units = units.with_columns(
        crs_key=crs_key(),
        parent_key=_parent_key(pl.col("crs_id")),
        pn_key=_pn_key(units),
    )
    return units.with_columns(award=split_components(units, number="pn_key"))


def link_by_tiers(
    units: pl.DataFrame,
    *,
    commitments: pl.DataFrame,
    first_year: int,
    decide: Decide | None = None,
) -> pl.DataFrame:
    """`trace` links from each of `units` to every eligible unit of `commitments`
    under its first matching tier, both keyed by `key_columns`, as `links` rows.

    `decide` applies to each tier's links before the next tier runs, as in
    `link_by_keys`.
    """
    remaining = units.with_columns(
        crs_parent=_parent_key(parent_id(pl.col("donor_code"), pl.col("crs_id"))),
        parent_floor_year=pl.when(pl.col("donor_code") != schema.EU_INSTITUTIONS).then(
            pl.col("floor_year")
        ),
    )
    remaining = remaining.with_columns(
        floor_held=_floor_held(remaining, commitments=commitments)
    )

    pairs = []
    for tier in TIERS:
        tier_pairs = _eligible_pairs(
            remaining.drop_nulls(tier.unit_key),
            commitments
            if tier.unique_by is None
            else _unique_pn(commitments, key=tier.commitment_key, by=tier.unique_by),
            tier=tier,
            first_year=first_year,
        )
        tier_links = as_exact_links(tier_pairs, task="trace")
        if decide is not None:
            tier_links = decide(tier_links)
        pairs.append(tier_links)
        remaining = remaining.join(
            tier_links.select(unit_id="left_id"), on="unit_id", how="anti"
        )
    return pl.concat(pairs)


def floor_loaded(*, first_year: int) -> pl.Expr:
    """Whether a unit's floor year is null or not before `first_year`, so that the
    build loads the year its commitment was made in."""
    floor = pl.col("floor_year")
    return floor.is_null() | (floor >= first_year)


def floor_seen(placed_year: pl.Expr, *, first_year: int, over: list[str]) -> pl.Expr:
    """Whether a pair's disbursement continues a commitment the build holds: its
    floor is loaded (`floor_loaded`), or a pair of the same `over` places its
    commitment no later than the floor year.

    The build loads no record before `first_year`, so a commitment from before it
    is placed there only by a later record that carries its date. Where the pairs
    reach only commitments placed after the floor, the disbursement continues a
    commitment the build cannot see."""
    return floor_loaded(first_year=first_year) | (
        (placed_year <= pl.col("floor_year")).any().over(over)
    )


def line_date(commitment_date: pl.Expr) -> pl.Expr:
    """The commitment date, or null on a `PLACEHOLDER_DAYS` day."""
    placeholder = commitment_date.dt.strftime("%m-%d").is_in(PLACEHOLDER_DAYS)
    return pl.when(~placeholder).then(commitment_date)


def crs_key() -> pl.Expr:
    """A unit's crs id, with its recipient code where the (donor, crs id) names
    several lines: units in more than one country, or in more than one recipient
    under more than one project number, a missing number counting as one more.

    Canada numbers a project's lines inside the crs id and gives a line to another
    country from one year to the next, so `P007307001` files Tanzania under
    `20202000510007` in 2020 and Guinea in 2023, when Guinea's line is `...0003`.
    The AsDB, the IDB and the GEF reuse an id across projects, as for the AsDB's
    `LN3375-IND` in India and `TA9143-NAU` in Nauru. An id that holds one country
    and regional or unspecified recipients (`regional_codes`) names one project, and
    keeps its regional lines.

    Only trace units, which commit or disburse, count, so a reflow unit
    (`key_columns`) changes no trace unit's key."""
    traced = (pl.col("commitment_usd") != 0) | (pl.col("disbursement_usd") != 0)
    country = ~pl.col("recipient_code").is_in(regional_codes())
    recipients = pl.col("recipient_code").filter(traced)
    several = (
        (pl.col("recipient_code").filter(traced & country).n_unique() > 1)
        | (
            (recipients.n_unique() > 1)
            & (pl.col("project_number").filter(traced).n_unique() > 1)
        )
    ).over("donor_code", "crs_id")
    return (
        pl.when(several)
        .then(pl.concat_str("crs_id", "recipient_code", separator="|"))
        .otherwise(pl.col("crs_id"))
    )


def partner_recipient(*, commitment: pl.Expr, disbursement: pl.Expr) -> pl.Expr:
    """Whether a line to the `disbursement` recipient can continue a commitment to
    the `commitment` recipient: the same recipient, or a commitment to a region or
    an unspecified recipient (`regional_codes`), which disburses to its countries."""
    return (disbursement == commitment) | commitment.is_in(regional_codes())


@cache
def regional_codes() -> tuple[int, ...]:
    """The DAC recipient codes of regions and of unspecified recipients."""
    regions = pl.col("type") != "country"
    return tuple(catalogue.recipients.list().filter(regions)["code"])


@cache
def country_codes() -> frozenset[int]:
    """The DAC recipient codes of single countries: those in the recipient list that
    are not regional or unspecified (`regional_codes`)."""
    codes = catalogue.recipients.list()["code"]
    return frozenset(codes) - frozenset(regional_codes())


def _pn_key(units: pl.DataFrame) -> pl.Series:
    """Each unit's `separator_free_pn`, or its project number as written where the
    separator-free key holds commitments under more than one crs id. Dropping
    separators can merge two numbers into one key, as Denmark's `403.KOSOVO.1-1-01`
    and `403.KOSOVO.1-10-1`, which `_unique_pn` would then refuse though each
    number names one project."""
    key = ("donor_code", "agency_code", "pn_key", "recipient_code")
    keyed = units.with_columns(pn_key=separator_free_pn(pl.col("project_number")))
    shared = (
        keyed.filter(pl.col("commitment_usd") > 0)
        .drop_nulls(key)
        .group_by(key)
        .agg(shared=pl.col("crs_id").n_unique() > 1)
        .filter("shared")
    )
    return (
        keyed.select(*key, "project_number")
        .join(shared, on=key, how="left", maintain_order="left")
        .select(
            pl.when("shared")
            .then(pl.col("project_number"))
            .otherwise(pl.col("pn_key"))
            .alias("pn_key")
        )
        .to_series()
    )


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
    than one crs id and more than one purpose, and one of whose units disburses
    without committing.

    The US commits each component of an award under its own crs id and purpose, as
    award `76_48621` commits its biosphere (41020), legislative (15152) and trade
    policy (33110) components in Viet Nam in 2015 under three crs ids, and
    disburses its HIV/AIDS component (13040) under a fourth. A link under the award
    then stays within one component. The split keeps a disbursement with no
    commitment of its own out of another purpose's commitment; where every unit
    that disburses commits too, as Canada files one project's lines by purpose,
    each is a line of the project, and what it disbursed beyond its own commitment
    may go to its sibling lines.
    """
    award = ["donor_code", "agency_code", number]
    committed = pl.col("commitment_usd") > 0
    uncommitted = (
        units.filter(pl.col("disbursement_usd") > 0, ~committed.fill_null(False))
        .drop_nulls(award)
        .select(award)
        .unique()
    )
    components = (
        units.filter(committed)
        .drop_nulls(award)
        .group_by(*award, "placed_year")
        .agg(
            ids=pl.col("crs_id").n_unique(), purposes=pl.col("purpose_code").n_unique()
        )
        .filter(pl.col("ids") > 1, pl.col("purposes") > 1)
        .select(award)
        .unique()
        .join(uncommitted, on=award, how="semi")
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


def _unique_pn(
    commitments: pl.DataFrame, *, key: tuple[str, ...], by: str
) -> pl.DataFrame:
    """Commitments whose `key` maps to one value of `by`, a null counting as one."""
    return commitments.drop_nulls(key).filter(pl.col(by).n_unique().over(key) == 1)


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
    disbursements: pl.DataFrame,
    commitments: pl.DataFrame,
    *,
    tier: Tier,
    first_year: int,
) -> pl.DataFrame:
    """(disbursement, commitment) unit pairs equal on the tier's key, the commitment
    placed no later than the record year and no earlier than the tier's floor, for
    disbursements whose floor the tier sees (`floor_seen`). In a tier keyed on the
    crs id, the floor holds only where it names a commitment (`_floor_held`)."""
    floor = pl.col(tier.floor)
    if tier.same_crs_id:
        floor = pl.when(pl.col("floor_held")).then(floor)
    dates = dict.fromkeys(("year", "floor_year", tier.floor, "floor_held"))
    return (
        disbursements.select("unit_id", *dates, *tier.unit_key)
        .join(
            commitments.select(
                "unit_id", "placed_year", *tier.commitment_key
            ).drop_nulls(tier.commitment_key),
            left_on=list(tier.unit_key),
            right_on=list(tier.commitment_key),
            suffix="_commitment",
        )
        .filter(placed_in_window(pl.col("placed_year"), floor=floor))
        .filter(
            floor_seen(pl.col("placed_year"), first_year=first_year, over=["unit_id"])
        )
        .select(
            left_id="unit_id",
            right_id="unit_id_commitment",
            group_id=group_id(pl.lit(tier.name), *tier.unit_key),
            link_method=pl.lit(tier.name),
        )
    )


def placed_in_window(placed_year: pl.Expr, *, floor: pl.Expr | None = None) -> pl.Expr:
    """Whether a commitment placed in `placed_year` falls no later than the
    disbursement's record year (`year`) and no earlier than its `floor` (by default
    `floor_year`), a null floor holding no commitment out."""
    floor = pl.col("floor_year") if floor is None else floor
    return (placed_year <= pl.col("year")) & (floor.is_null() | (placed_year >= floor))


def group_id(method: pl.Expr, *key: str | pl.Expr) -> pl.Expr:
    """A link group's id: its method and the values of its `key`, joined by `|`."""
    return pl.concat_str([method, *key], separator="|")


@overload
def links_frame(frame: pl.DataFrame, given: Mapping[str, pl.Expr]) -> pl.DataFrame: ...
@overload
def links_frame(frame: pl.LazyFrame, given: Mapping[str, pl.Expr]) -> pl.LazyFrame: ...
def links_frame(
    frame: pl.DataFrame | pl.LazyFrame, given: Mapping[str, pl.Expr]
) -> pl.DataFrame | pl.LazyFrame:
    """`frame` as `links` rows: each column of `schema.LINKS`, in its order and
    type, from `given`, else null."""
    return frame.select(
        given.get(name, pl.lit(None)).cast(column.dtype).alias(name)
        for name, column in schema.LINKS.columns.items()
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

    `probability` is 1.0 on a certain link: a `crdf_crs` link by a
    `CERTAIN_ROUTES` route, a link of another task by a `CERTAIN_METHODS` tier, or
    a unit linked to itself, whose records commit and disburse. It is null on every
    other rule link, which no model scored. The task decides, since the GEF's
    `crosswalk` trace links share their name with the EIB's certain route.
    """
    methods = schema.CERTAIN_ROUTES if task == "crdf_crs" else schema.CERTAIN_METHODS
    certain = pl.col("link_method").is_in(methods) | (
        pl.col("left_id") == pl.col("right_id")
    )
    # A column `pairs` carries wins over the defaults.
    return links_frame(
        pairs,
        {
            "task": pl.lit(task),
            "probability": pl.when(certain).then(1.0),
            "review": pl.lit(False),
            **{name: pl.col(name) for name in pairs.collect_schema().names()},
        },
    )

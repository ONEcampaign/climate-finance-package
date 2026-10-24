"""CRS records into units, and the exact key tiers that link them, first match wins.

A unit sums the CRS records that share every key field (`UNIT_KEY`). Each disbursement
unit takes the first tier in `TIERS` whose key reaches an eligible commitment unit,
meaning one placed at or before the disbursement's record year and, outside the
parent-id tier, at or after its floor year. The parent-id tier ignores the floor, since
an EU contract record carries the contract's own date, which falls on or after its
decision's commitment, and a lettered record (`parent_id`) its own. A disbursement
whose floor precedes the first CRS year continues a commitment the build sees only
where a later record carries that commitment's date, so a tier links it only where
its key reaches a commitment placed no later than the floor (`floor_seen`).

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
recipient under several crs ids split by purpose. They compare project numbers without
`-` and `_` (`pn_key`). They hold for every agency, since one whose ids mostly persist,
such as Korea's EDCF or KfW, still files some projects under a new crs id each year.

The first project-number tier adds the commitment date (`line_date`). A disbursement
record that repeats its line's commitment date continues that line, where the
project number and recipient alone reach the project's later lines too. The tier
links only where its key reaches one commitment unit, since a line split by purpose
under one date is left to the purpose tier. A date on 1 January or 31 December names
no line, since donors write it as a placeholder for the year and 38% of CRS records
carry 31 December against the 0.3% a real date would.
"""

from dataclasses import dataclass
from functools import cache
from importlib.resources import files

import polars as pl

from climate_finance import schema
from climate_finance.build.link.id_practice import (
    canonical_crs_id,
    parent_id,
)
from climate_finance.build.normalise.ids import content_id

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
# Project numbers that name no project, as `normalise_pn` leaves them.
PLACEHOLDER_PNS = (
    "NOTAPPLICABLE",
    "N.A.",
    "N.A",
    "NA",
    "N/A",
    "NONE",
    "NULL",
    "TBD",
    "UNKNOWN",
    "0",
    "-",
    ".",
    "..",
    "X",
)
# A numeric project number at least this long can be one a spreadsheet rounded.
ROUNDED_MIN_DIGITS = 10
_LONG_NUMBER = rf"^\d{{{ROUNDED_MIN_DIGITS},}}$"
_SCIENTIFIC = r"^(\d)\.(\d+)E\+(\d+)$"
# IBRD and IDA, whose project numbers carry the loan or credit number.
WORLD_BANK_DONORS = (901, 905)
# The project-number tiers' key, on `separator_free_pn`.
PN_TIER_KEY = ("donor_code", "agency_code", "pn_key", "recipient_code")
# The month and day of commitment dates that stand for a year (`line_date`).
PLACEHOLDER_DAYS = ("01-01", "12-31")


@dataclass(frozen=True)
class Tier:
    """A key tier. It holds the commitment key, the disbursement key it must equal,
    whether it is a project-number tier, which reaches only commitment keys that
    hold one value of `unique_by` (`_unique_pn`), and whether it holds to the
    disbursement's floor."""

    name: str
    commitment_key: tuple[str, ...]
    unit_key: tuple[str, ...]
    project_number: bool = False
    unique_by: str = "crs_id"
    floored: bool = True


TIERS = (
    Tier("wb_loan", ("donor_code", "loan"), ("donor_code", "loan")),
    Tier(
        "crs_id_purpose",
        ("donor_code", "crs_id", "purpose_code", "recipient_code"),
        ("donor_code", "crs_id", "purpose_code", "recipient_code"),
    ),
    Tier("crs_id", ("donor_code", "crs_key"), ("donor_code", "crs_key")),
    Tier(
        "crs_id_parent",
        ("donor_code", "crs_id"),
        ("donor_code", "crs_parent"),
        floored=False,
    ),
    Tier(
        "pn_recipient_date",
        (*PN_TIER_KEY, "line_date"),
        (*PN_TIER_KEY, "line_date"),
        project_number=True,
        unique_by="unit_id",
    ),
    Tier(
        "pn_recipient_purpose",
        (*PN_TIER_KEY, "purpose_code"),
        (*PN_TIER_KEY, "purpose_code"),
        project_number=True,
    ),
    Tier("pn_recipient", PN_TIER_KEY, PN_TIER_KEY, project_number=True),
)
if tuple(tier.name for tier in TIERS) != schema.KEY_TIERS:
    raise RuntimeError("TIERS must follow schema.KEY_TIERS")


def normalise_pn(project_number: pl.Expr) -> pl.Expr:
    """Upper-case, drop a float artefact `.0` and every space. Placeholders, numbers
    a spreadsheet rounded (`rounded_digits`) and values shorter than two characters
    or holding no letter or digit become null.

    A rounded number such as the AfDB's `2.0002E+12` stands for hundreds of
    projects across dozens of recipients, so no key may join on it."""
    pn = _cleaned(project_number)
    usable = (
        ~pn.is_in(PLACEHOLDER_PNS)
        & pn.str.contains("[A-Z0-9]")
        & (pn.str.len_chars() >= 2)
        & rounded_digits(pn).is_null()
    )
    return pl.when(usable).then(pn)


def separator_free_pn(project_number: pl.Expr) -> pl.Expr:
    """A normalised project number without `-` and `_`, or null where fewer than
    two characters remain. Donors write one number with and without them, as
    Germany's `01LE1907A1_L` and `01LE1907A1L`."""
    stripped = project_number.str.replace_all(r"[-_]", "")
    return pl.when(stripped.str.len_chars() >= 2).then(stripped)


def rounded_digits(project_number: pl.Expr) -> pl.Expr:
    """For a project number a spreadsheet rounded, as `2.10015E+12` or
    `2100150000000`, the digits it kept (`prefix`) and the length of the number it
    stands for (`digits`), or null for other numbers."""
    project_number = _cleaned(project_number)
    scientific = project_number.str.extract_groups(_SCIENTIFIC)
    lead, rest, exponent = (scientific.struct.field(name) for name in ("1", "2", "3"))
    zeros = project_number.str.contains(_LONG_NUMBER) & project_number.str.ends_with(
        "0000"
    )
    prefix = (
        pl.when(lead.is_not_null())
        .then(lead + rest)
        .when(zeros)
        .then(project_number.str.strip_chars_end("0"))
    )
    digits = (
        pl.when(lead.is_not_null())
        .then(exponent.cast(pl.Int32) + 1)
        .when(zeros)
        .then(project_number.str.len_chars().cast(pl.Int32))
    )
    return pl.when(prefix.is_not_null()).then(pl.struct(prefix=prefix, digits=digits))


def _cleaned(project_number: pl.Expr) -> pl.Expr:
    return (
        project_number.str.to_uppercase()
        .str.strip_chars()
        .str.replace(r"\.0$", "")
        .str.replace_all(r"\s+", "")
    )


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
    size = pl.col("commitment_usd").abs().fill_null(0) + pl.col(
        "disbursement_usd"
    ).abs().fill_null(0)
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
            commitment_date=pl.col("commitment_date")
            .sort_by(size, "record_id", descending=[True, False])
            .first(),
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

    `first_year` is the first CRS year the build loads. The crs-id tiers compare
    `canonical_crs_id`s and the project-number tiers `pn_key`s (`_with_pn_key`).
    `group_id` names the tier and its key, so the disbursement
    units of one group reach the same commitments, less those their own record
    year or floor rules out.
    """
    units = units.with_columns(
        crs_id=canonical_crs_id(pl.col("crs_id")),
        line_date=line_date(pl.col("commitment_date")),
    )
    units = units.with_columns(crs_key=crs_key(units))
    remaining = units.filter(pl.col("disbursement_usd") > 0).with_columns(
        crs_parent=parent_id(pl.col("donor_code"), pl.col("crs_id")),
        loan=loan_number(pl.col("donor_code"), pl.col("project_number")),
    )
    commitments = units.filter(pl.col("commitment_usd") > 0).with_columns(
        loan=loan_number(pl.col("donor_code"), pl.col("project_number"))
    )
    remaining, commitments = _with_pn_key(remaining, commitments)

    pairs = []
    for tier in TIERS:
        tier_pairs = _eligible_pairs(
            remaining.drop_nulls(tier.unit_key),
            _unique_pn(commitments, key=tier.commitment_key, by=tier.unique_by)
            if tier.project_number
            else commitments,
            tier=tier,
            first_year=first_year,
        )
        pairs.append(tier_pairs)
        remaining = remaining.join(
            tier_pairs.select(unit_id="left_id"), on="unit_id", how="anti"
        )
    return as_exact_links(pl.concat(pairs).lazy(), task="trace").collect()


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


def crs_key(units: pl.DataFrame) -> pl.Series:
    """Each unit's crs id, with its recipient code where the (donor, crs id) names
    several lines: units in more than one country, or in more than one recipient
    under more than one project number.

    Canada numbers a project's lines inside the crs id and gives a line to another
    country from one year to the next, so `P007307001` files Tanzania under
    `20202000510007` in 2020 and Guinea in 2023, when Guinea's line is `...0003`.
    The AsDB, the IDB and the GEF reuse an id across projects, as for the AsDB's
    `LN3375-IND` in India and `TA9143-NAU` in Nauru. An id that holds one country
    and regional or unspecified recipients (`regional_codes`) names one project, and
    keeps its regional lines."""
    country = ~pl.col("recipient_code").is_in(regional_codes())
    several = (
        (pl.col("recipient_code").filter(country).n_unique() > 1)
        | (
            (pl.col("recipient_code").n_unique() > 1)
            & (pl.col("project_number").drop_nulls().n_unique() > 1)
        )
    ).over("donor_code", "crs_id")
    return units.select(
        pl.when(several)
        .then(pl.concat_str("crs_id", "recipient_code", separator="|"))
        .otherwise(pl.col("crs_id"))
    ).to_series()


def partner_recipient(*, commitment: pl.Expr, disbursement: pl.Expr) -> pl.Expr:
    """Whether a line to the `disbursement` recipient can continue a commitment to
    the `commitment` recipient: the same recipient, or a commitment to a region or
    an unspecified recipient (`regional_codes`), which disburses to its countries."""
    return (disbursement == commitment) | commitment.is_in(regional_codes())


@cache
def regional_codes() -> tuple[int, ...]:
    """The DAC recipient codes of regions and of unspecified recipients."""
    recipients = pl.read_csv(
        (files("climate_finance") / "data" / "recipients.csv").read_bytes(),
        schema_overrides={"code": pl.Int32},
    )
    return tuple(
        recipients.filter(pl.col("name").str.contains("regional|unspecified"))["code"]
    )


def _with_pn_key(
    disbursements: pl.DataFrame, commitments: pl.DataFrame
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Both frames with `pn_key`: the `separator_free_pn`, or the project number as
    written where the separator-free key holds commitments under more than one crs
    id. Dropping separators can merge two numbers into one key, as Denmark's
    `403.KOSOVO.1-1-01` and `403.KOSOVO.1-10-1`, which `_unique_pn` would then
    refuse though each number names one project."""
    pn_key = separator_free_pn(pl.col("project_number"))
    shared = (
        commitments.with_columns(pn_key=pn_key)
        .drop_nulls(PN_TIER_KEY)
        .group_by(PN_TIER_KEY)
        .agg(shared=pl.col("crs_id").n_unique() > 1)
        .filter("shared")
    )

    def keyed(frame: pl.DataFrame) -> pl.DataFrame:
        return (
            frame.with_columns(pn_key=pn_key)
            .join(shared, on=PN_TIER_KEY, how="left", maintain_order="left")
            .with_columns(
                pn_key=pl.when("shared")
                .then(pl.col("project_number"))
                .otherwise(pl.col("pn_key"))
            )
            .drop("shared")
        )

    return keyed(disbursements), keyed(commitments)


def _unique_pn(
    commitments: pl.DataFrame, *, key: tuple[str, ...], by: str
) -> pl.DataFrame:
    """Commitments whose `key` maps to one value of `by`, a null counting as one."""
    return commitments.drop_nulls(key).filter(pl.col(by).n_unique().over(key) == 1)


def _eligible_pairs(
    disbursements: pl.DataFrame,
    commitments: pl.DataFrame,
    *,
    tier: Tier,
    first_year: int,
) -> pl.DataFrame:
    """(disbursement, commitment) unit pairs equal on the tier's key, the commitment
    placed no later than the record year and, in a floored tier, no earlier than
    the disbursement's floor year, for disbursements whose floor the tier sees
    (`floor_seen`)."""
    eligible = pl.col("placed_year") <= pl.col("year")
    if tier.floored:
        eligible &= pl.col("floor_year").is_null() | (
            pl.col("placed_year") >= pl.col("floor_year")
        )
    return (
        disbursements.select("unit_id", "year", "floor_year", *tier.unit_key)
        .join(
            commitments.select(
                "unit_id", "placed_year", *tier.commitment_key
            ).drop_nulls(tier.commitment_key),
            left_on=list(tier.unit_key),
            right_on=list(tier.commitment_key),
            suffix="_commitment",
        )
        .filter(eligible)
        .filter(
            floor_seen(pl.col("placed_year"), first_year=first_year, over=["unit_id"])
        )
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


def as_exact_links(pairs: pl.LazyFrame, *, task: str) -> pl.LazyFrame:
    """`pairs` (left_id, right_id, group_id, link_method) as certain `links` rows."""
    given = {name: pl.col(name) for name in pairs.collect_schema().names()}
    fixed = {"task": pl.lit(task), "probability": pl.lit(1.0), "review": pl.lit(False)}
    return pairs.select(
        given.get(name, fixed.get(name, pl.lit(None, dtype=column.dtype))).alias(name)
        for name, column in schema.LINKS.columns.items()
    )

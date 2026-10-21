"""Each (donor, agency)'s id practice per period, from how open commitments continue.

An open commitment is a (donor, agency, crs id, year) with commitments recorded in the
period and less than 95% disbursed in its own year. It continues, checked in this
order: by id when a later year disburses under the same (donor, crs id); by parent id
when a later disbursement's crs id begins with its crs id as an EU decision id; by
project number when a later disbursement under another crs id shares (donor, agency,
project number, recipient). Shares are of open commitment value.

An agency is `unclassified` below `MIN_OPEN` open commitments, `neither` when under
`MIN_CONTINUED` of its value continues, and otherwise `id_persistent`,
`parent_id_persistent` or `pn_persistent` when that route carries at least
`PERSISTENT_SHARE` of the continued value (the parent class counting id and parent
together, with parent the larger), else `mixed`. A disbursement takes the class of its
record year's period. Later years reuse the last period.
"""

from collections.abc import Mapping

import polars as pl

from climate_finance import schema

PERIODS = {
    "2010-2014": (2010, 2014),
    "2015-2018": (2015, 2018),
    "2019-2022": (2019, 2022),
}
# Agencies whose project-number tiers and scoring are off.
ID_PERSISTENT_CLASSES = ("id_persistent", "parent_id_persistent")
# A commitment is open when more than this share is left after its own year.
OPEN_SHARE = 0.05
PERSISTENT_SHARE = 0.8
MIN_CONTINUED = 0.2
MIN_OPEN = 10

# An EU contract's crs id is its 10-character decision id plus a contract number.
_PARENT_ID_LENGTH = 10
_AGENCY = ("donor_code", "agency_code")
# The project-number key: one project of one agency in one recipient.
PN_KEY = ("donor_code", "agency_code", "project_number", "recipient_code")
_EVENT = ("donor_code", "crs_id", "year")
_ROUTES = ("id", "parent", "pn")


def parent_id(crs_id: pl.Expr) -> pl.Expr:
    """The EU decision id a contract's crs id begins with, or null for a shorter id."""
    return pl.when(crs_id.str.len_chars() > _PARENT_ID_LENGTH).then(
        crs_id.str.slice(0, _PARENT_ID_LENGTH)
    )


def period_of(year: pl.Expr) -> pl.Expr:
    """The id-practice period a record year falls in. Later years take the last."""
    (first_name, (_, first_last)), *middle, (last_name, _) = PERIODS.items()
    period = pl.when(year <= first_last).then(pl.lit(first_name))
    for name, (_, last) in middle:
        period = period.when(year <= last).then(pl.lit(name))
    return period.otherwise(pl.lit(last_name))


def classify_id_practice(
    units: pl.DataFrame, *, periods: Mapping[str, tuple[int, int]] = PERIODS
) -> pl.DataFrame:
    """Class each (donor, agency) in each of `periods`, in the `id_practice` schema.

    `units` is the `units` table, and `periods` a subset of `PERIODS`. An agency
    with no open commitment in a period gets no row for it.
    """
    disbursements = units.filter(pl.col("disbursement_usd") > 0).with_columns(
        crs_parent=parent_id(pl.col("crs_id"))
    )
    return pl.concat(
        _classify(units, disbursements, first=first, last=last).with_columns(
            period=pl.lit(name)
        )
        for name, (first, last) in periods.items()
    ).select(list(schema.ID_PRACTICE.columns))


def _classify(
    units: pl.DataFrame, disbursements: pl.DataFrame, *, first: int, last: int
) -> pl.DataFrame:
    commitments = units.filter(
        pl.col("commitment_usd") > 0,
        pl.col("year").is_between(first, last),
        pl.col("crs_id").is_not_null(),
    )
    flagged = _continuations(
        _open(commitments, disbursements), commitments, disbursements
    )
    value = pl.col("commitment_usd")
    shares = flagged.group_by(_AGENCY).agg(
        open_commitments=pl.len().cast(pl.Int32),
        **{
            f"{route}_share": value.filter(pl.col(f"by_{route}")).sum() / value.sum()
            for route in _ROUTES
        },
    )
    continued = pl.sum_horizontal(f"{route}_share" for route in _ROUTES)
    base = pl.when(continued > 0).then(continued).otherwise(1.0)
    id_share, parent_share = pl.col("id_share"), pl.col("parent_share")
    return shares.with_columns(
        id_class=pl.when(pl.col("open_commitments") < MIN_OPEN)
        .then(pl.lit("unclassified"))
        .when(continued < MIN_CONTINUED)
        .then(pl.lit("neither"))
        .when(id_share / base >= PERSISTENT_SHARE)
        .then(pl.lit("id_persistent"))
        .when(
            ((id_share + parent_share) / base >= PERSISTENT_SHARE)
            & (parent_share > id_share)
        )
        .then(pl.lit("parent_id_persistent"))
        .when(pl.col("pn_share") / base >= PERSISTENT_SHARE)
        .then(pl.lit("pn_persistent"))
        .otherwise(pl.lit("mixed"))
    )


def _open(commitments: pl.DataFrame, disbursements: pl.DataFrame) -> pl.DataFrame:
    """Commitments by (donor, agency, crs id, year) with more than `OPEN_SHARE` left
    after the same (donor, crs id)'s disbursements that year."""
    same_year = disbursements.group_by(_EVENT).agg(
        same_year_usd=pl.col("disbursement_usd").sum()
    )
    return (
        commitments.group_by(*_AGENCY, "crs_id", "year")
        .agg(pl.col("commitment_usd").sum())
        .join(same_year, on=_EVENT, how="left")
        .filter(
            pl.col("commitment_usd") - pl.col("same_year_usd").fill_null(0.0)
            > OPEN_SHARE * pl.col("commitment_usd")
        )
    )


def _continuations(
    open_commitments: pl.DataFrame,
    commitments: pl.DataFrame,
    disbursements: pl.DataFrame,
) -> pl.DataFrame:
    """Flag how each open commitment continues: `by_id`, `by_parent` or `by_pn`,
    each only where no earlier route applies."""
    last_by_id = (
        disbursements.drop_nulls("crs_id")
        .group_by("donor_code", "crs_id")
        .agg(last_id_year=pl.col("year").max())
    )
    last_by_parent = (
        disbursements.drop_nulls("crs_parent")
        .group_by("donor_code", crs_id="crs_parent")
        .agg(last_child_year=pl.col("year").max())
    )
    later = (*PN_KEY, "crs_id", "year")
    by_pn = (
        commitments.drop_nulls(PN_KEY)
        .select(later)
        .unique()
        .join(
            disbursements.drop_nulls(later).select(later).unique(),
            on=PN_KEY,
            suffix="_later",
        )
        .filter(
            pl.col("year_later") > pl.col("year"),
            pl.col("crs_id_later") != pl.col("crs_id"),
        )
        .select(_EVENT)
        .unique()
        .with_columns(pn_hit=pl.lit(value=True))
    )
    by_id = (pl.col("last_id_year") > pl.col("year")).fill_null(value=False)
    by_parent = (pl.col("last_child_year") > pl.col("year")).fill_null(value=False)
    return (
        open_commitments.join(last_by_id, on=["donor_code", "crs_id"], how="left")
        .join(last_by_parent, on=["donor_code", "crs_id"], how="left")
        .join(by_pn, on=_EVENT, how="left")
        .with_columns(by_id=by_id, by_parent=by_parent & ~by_id)
        .with_columns(
            by_pn=pl.col("pn_hit").fill_null(value=False)
            & ~pl.col("by_id")
            & ~pl.col("by_parent")
        )
    )

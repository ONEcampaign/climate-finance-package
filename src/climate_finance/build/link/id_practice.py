"""Each (donor, agency)'s id practice per period, from how open commitments continue.

An open commitment is a (donor, agency, crs id, year) with commitments recorded in the
period and less than 95% disbursed in its own year. It continues, checked in this
order: by id when a later year disburses under the same (donor, crs id); by parent id
when a later disbursement's crs id names its crs id as parent (`parent_id`); by
project number when a later disbursement under another crs id shares (donor, agency,
project number, recipient). Shares are of open commitment value.

An agency is `unclassified` below `MIN_OPEN` open commitments, `neither` when under
`MIN_CONTINUED` of its value continues, and otherwise `id_persistent`,
`parent_id_persistent` or `pn_persistent` when that route carries at least
`PERSISTENT_SHARE` of the continued value (the parent class counting id and parent
together, with parent the larger), else `mixed`. A disbursement takes the class of its
record year's period. Earlier years take the first period and later years the last;
open commitments of years outside every period class no agency.

`keyed_disbursement_share` is the agency's disbursement value in the period on records
with a usable key, over all its disbursement value there: a project number the
normaliser accepts, or a crs id that a commitment of the same donor carries. Its
disbursements fall in periods as its records do, by record year. An agency is
`untraceable` in a period when it is classed `neither` or keys under
`MIN_KEYED_DISBURSED` of its disbursement value, since the CRS cannot then show
whether its commitments were paid.
"""

from collections.abc import Mapping
from dataclasses import dataclass

import polars as pl

from climate_finance import schema

PERIODS = {
    "2010-2014": (2010, 2014),
    "2015-2018": (2015, 2018),
    "2019-2022": (2019, 2022),
}
# Classes whose crs ids persist from commitment to disbursement.
ID_PERSISTENT_CLASSES = ("id_persistent", "parent_id_persistent")
# A commitment is open when more than this share is left after its own year.
OPEN_SHARE = 0.05
PERSISTENT_SHARE = 0.8
MIN_CONTINUED = 0.2
MIN_OPEN = 10
# On the archive, agencies with at least USD 1bn disbursed in a period split at this
# share. In 2019 to 2022 IFAD keys 0.18 of its disbursement value, IDB Invest 0.32,
# the EBRD 0.34 and the NDB 0.37; the next agency keys 0.53, and 90 of the 108 key
# 0.8 or more. In 2015 to 2018 the gap runs from 0.35 to 0.57.
MIN_KEYED_DISBURSED = 0.45

# An EU contract's crs id is its 10-character decision id plus a contract number.
_DECISION_ID_LENGTH = 10
# Denmark, Germany, the UK, the US and the AsDB file a later record of a project
# under its crs id plus up to three letters (`2019001320a`, `2021000329-c`,
# `2015003432/b`), and the two records share a title or project number. Other
# donors' suffixes name other projects: France and 5RDB006 write 11-digit sequence
# numbers, Sweden and New Zealand letters that start a new agreement.
LETTER_SUFFIX_DONORS = (3, 5, 12, 302, 915)
_LETTER_SUFFIX = r"^(\d{10})[-/]?[A-Za-z]{1,3}$"
_AGENCY = ("donor_code", "agency_code")
# The project-number key: one project of one agency in one recipient.
PN_KEY = ("donor_code", "agency_code", "project_number", "recipient_code")
_EVENT = ("donor_code", "crs_id", "year")
_ROUTES = ("id", "parent", "pn")


def parent_id(donor_code: pl.Expr, crs_id: pl.Expr) -> pl.Expr:
    """The crs id of the project a record belongs to under another crs id: the EU
    decision id a contract's crs id begins with, or the crs id a
    `LETTER_SUFFIX_DONORS` id adds letters to. Null for every other id."""
    return (
        pl.when(
            (donor_code == schema.EU_INSTITUTIONS)
            & (crs_id.str.len_chars() > _DECISION_ID_LENGTH)
        )
        .then(crs_id.str.slice(0, _DECISION_ID_LENGTH))
        .when(donor_code.is_in(LETTER_SUFFIX_DONORS))
        .then(crs_id.str.extract(_LETTER_SUFFIX, 1))
    )


def period_of(year: pl.Expr) -> pl.Expr:
    """The id-practice period a record year falls in. Earlier years take the first
    and later years the last."""
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
    disbursed = _disbursed(units)
    return (
        pl.concat(
            _classify(units, disbursed, first=first, last=last).with_columns(
                period=pl.lit(name)
            )
            for name, (first, last) in periods.items()
        )
        .join(_keyed_shares(units), on=[*_AGENCY, "period"], how="left")
        .select(list(schema.ID_PRACTICE.columns))
    )


def untraceable() -> pl.Expr:
    """True for an `id_practice` row whose agency the CRS cannot trace: classed
    `neither`, or keying under `MIN_KEYED_DISBURSED` of its disbursement value."""
    return (pl.col("id_class") == "neither") | (
        pl.col("keyed_disbursement_share") < MIN_KEYED_DISBURSED
    ).fill_null(value=False)


def _keyed_shares(units: pl.DataFrame) -> pl.DataFrame:
    """`keyed_disbursement_share` by (donor, agency, period)."""
    committed_ids = (
        units.filter(pl.col("commitment_usd") > 0)
        .drop_nulls("crs_id")
        .select("donor_code", "crs_id")
        .unique()
        .with_columns(committed_id=pl.lit(value=True))
    )
    value = pl.col("disbursement_usd")
    keyed = pl.col("project_number").is_not_null() | pl.col("committed_id")
    return (
        units.filter(value > 0)
        .join(committed_ids, on=["donor_code", "crs_id"], how="left")
        .with_columns(pl.col("committed_id").fill_null(value=False))
        .group_by(*_AGENCY, period=period_of(pl.col("year")))
        .agg(keyed_disbursement_share=value.filter(keyed).sum() / value.sum())
    )


@dataclass(frozen=True)
class _Disbursed:
    """What the disbursement units show, the same in every period.

    `same_year` sums disbursement by (donor, crs id, year). `last_by_id` and
    `last_by_parent` give the last disbursement year under a (donor, crs id) and
    under a (donor, parent id). `pn_events` holds the distinct (donor, agency,
    project number, recipient, crs id, year) of disbursements.
    """

    same_year: pl.DataFrame
    last_by_id: pl.DataFrame
    last_by_parent: pl.DataFrame
    pn_events: pl.DataFrame


def _disbursed(units: pl.DataFrame) -> _Disbursed:
    disbursements = units.filter(pl.col("disbursement_usd") > 0).with_columns(
        crs_parent=parent_id(pl.col("donor_code"), pl.col("crs_id"))
    )
    later = (*PN_KEY, "crs_id", "year")
    return _Disbursed(
        same_year=disbursements.group_by(_EVENT).agg(
            same_year_usd=pl.col("disbursement_usd").sum()
        ),
        last_by_id=disbursements.drop_nulls("crs_id")
        .group_by("donor_code", "crs_id")
        .agg(last_id_year=pl.col("year").max()),
        last_by_parent=disbursements.drop_nulls("crs_parent")
        .group_by("donor_code", crs_id="crs_parent")
        .agg(last_child_year=pl.col("year").max()),
        pn_events=disbursements.drop_nulls(later).select(later).unique(),
    )


def _classify(
    units: pl.DataFrame, disbursed: _Disbursed, *, first: int, last: int
) -> pl.DataFrame:
    commitments = units.filter(
        pl.col("commitment_usd") > 0,
        pl.col("year").is_between(first, last),
        pl.col("crs_id").is_not_null(),
    )
    flagged = _continuations(
        _open(commitments, disbursed.same_year), commitments, disbursed
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


def _open(commitments: pl.DataFrame, same_year: pl.DataFrame) -> pl.DataFrame:
    """Commitments by (donor, agency, crs id, year) with more than `OPEN_SHARE` left
    after the same (donor, crs id)'s disbursements that year (`same_year`)."""
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
    open_commitments: pl.DataFrame, commitments: pl.DataFrame, disbursed: _Disbursed
) -> pl.DataFrame:
    """Flag how each open commitment continues: `by_id`, `by_parent` or `by_pn`,
    each only where no earlier route applies."""
    later = (*PN_KEY, "crs_id", "year")
    by_pn = (
        commitments.drop_nulls(PN_KEY)
        .select(later)
        .unique()
        .join(disbursed.pn_events, on=PN_KEY, suffix="_later")
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
        open_commitments.join(
            disbursed.last_by_id, on=["donor_code", "crs_id"], how="left"
        )
        .join(disbursed.last_by_parent, on=["donor_code", "crs_id"], how="left")
        .join(by_pn, on=_EVENT, how="left")
        .with_columns(by_id=by_id, by_parent=by_parent & ~by_id)
        .with_columns(
            by_pn=pl.col("pn_hit").fill_null(value=False)
            & ~pl.col("by_id")
            & ~pl.col("by_parent")
        )
    )

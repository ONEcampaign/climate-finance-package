"""CRDF rows to CRS commitments, within a year and then by the recovery routes.

Within a year, a CRDF row joins the commitment units that share its provider, crs
id, purpose and year. The recovery routes link rows that join reaches no unit for.

Aggregate children (`normalise.aggregates`) take the crs id of the bulk record they
stand for, so most join within the year. A child of a record without a crs id links
to that record's commitment units (`aggregate_partition`), and those units link to
themselves as a trace, so the record's own disbursement fills its own commitment.

The EIB reports to the CRDF and the CRS as agency 3 of EU Institutions (918), but
its CRDF ids rarely reach the CRS: 2021 rows carry a contract number, 2022 rows a
project serial the CRS never prints, and 2023 rows a placeholder sequence and no
project number. The CRS dates the commitments late: of the 2013-2018 EIB value
that links, 37% has a CRS commitment dated in the row's year or the next, and a
third four or more years later. Five routes link the EIB rows, in this order:

- `eib_list`: the EIB's project list, by project number and recipient, or recipient,
  euro amount within 3% and a specific title (`vintages.match_eib`).
- `crosswalk`: the EIB's IATI activity ids pair each project serial with its finance
  contract numbers, which the CRS uses as project numbers.
- `title_recipient`: a bulk project with the same recipient and nearly the same
  title.
- `eib_text_amount`: a commitment unit with the same recipient, text much like the
  row's, and a commitment within about 10% of the row's face value.
- `eib_amount`: a commitment unit with the same recipient, a commitment within
  0.02% of the row's face value, and text that does not contradict the row's.

The first three link a row to the commitment units of the bulk records it
matches. `crosswalk` and `title_recipient` take a project whose commitments from
the row's year on come within 10% of the row's face value, in euros, pairing rows
and projects closest amount first. A crosswalk pair within 0.01% keeps the method
`crosswalk`, a certain route (`schema.CERTAIN_ROUTES`). A wider one takes
`crosswalk_amount`, since the amount then only suggests the contract.

The last two compare texts by TF-IDF cosine and link one commitment unit dated in
the row's year or later, its amount in euros at the rate of the year the CRS
reports it. A unit's amount can match the row's face value or the sum of its
description group, the rows of its year that share its recipient and description
(`vintages.EIB_DESCRIPTION_GROUP`, the groups the repeat rule pairs). Those rows
then share the unit and one group. A row links only when the units its rule
accepts belong to one project, and then to the accepted unit with the highest
cosine.
"""

from collections.abc import Callable, Collection

import numpy as np
import polars as pl
from sklearn.feature_extraction.text import TfidfVectorizer

from climate_finance.build.errors import LinkInputError
from climate_finance.build.link.keys import as_exact_links, group_id
from climate_finance.build.normalise.crs import trace_population
from climate_finance.build.normalise.pairing import pair_one_to_one
from climate_finance.build.normalise.project_numbers import normalise_pn
from climate_finance.build.normalise.vintages import (
    EIB_DESCRIPTION_GROUP,
    eib_description_key,
    match_eib,
)
from climate_finance.schema import EIB_AGENCY, EU_INSTITUTIONS, EURO_ISSUER

# CRDF rows and CRS commitments that agree on these, the CRDF provider standing for
# the CRS donor, are one commitment. 99.5% of such keys pair one to one.
WITHIN_YEAR_KEY = ("donor_code", "crs_id", "year", "purpose_code")

# The EIB's IATI activity id carries its project serial, then one finance contract
# number.
_EIB_ACTIVITY = r"^XM-DAC-918-3-(?<serial>\d{8})-(?<contract>\d{5})$"
# A project's commitments match a row's face value within this share, in euros.
_AMOUNT_TOLERANCE = 0.10
# A crosswalk pair this close is the row's own contract.
_EXACT_TOLERANCE = 1e-4
# The title route needs word sets that overlap this much, on a title that at most
# this many EIB commitments carry.
_TITLE_OVERLAP = 0.8
_GENERIC_TITLE = 10
# `eib_text_amount` needs a TF-IDF cosine of at least this between the row's and the
# unit's texts, and an absolute log ratio of the unit's commitment to the row's (or
# its description group's) face value of at most this, about 10%.
_TEXT_MATCH = 0.7
_TEXT_AMOUNT_GAP = 0.10
# `eib_amount` needs an absolute log ratio of at most this, about 0.02%, and a cosine
# of at least this, unless the unit's text is generic: empty, or carried by more
# than this many EIB commitment units, as "PSI Investment Loan" is (154 units on the
# archive build, the next most shared text 5), and so names no project.
_EXACT_AMOUNT_GAP = 2e-4
_TEXT_CONSISTENT = 0.3
_GENERIC_TEXT_UNITS = 10

type _Route = Callable[[pl.DataFrame], pl.DataFrame]


def link_within_year(crdf: pl.LazyFrame, *, units: pl.LazyFrame) -> pl.LazyFrame:
    """Link each CRDF row to every commitment unit of the same year sharing its
    (donor, crs id, purpose), as `links` rows.

    `group_id` names the key, so a commitment's face value is the sum of its group's
    commitment units.
    """
    commitments = units.filter(pl.col("commitment_usd") > 0).select(
        "unit_id", *WITHIN_YEAR_KEY
    )
    return as_exact_links(
        crdf.select("row_id", *WITHIN_YEAR_KEY[1:], donor_code="provider_code")
        .drop_nulls(WITHIN_YEAR_KEY)
        .join(commitments, on=WITHIN_YEAR_KEY)
        .select(
            left_id="row_id",
            right_id="unit_id",
            group_id=group_id(pl.lit("within_year"), *WITHIN_YEAR_KEY),
            link_method=pl.lit("within_year"),
        ),
        task="crdf_crs",
    )


def recover(
    crdf: pl.LazyFrame,
    *,
    within_year: pl.DataFrame,
    units: pl.LazyFrame,
    crs: pl.LazyFrame,
    external: pl.LazyFrame,
    exchange: pl.DataFrame,
    skip: Collection[str] = (),
) -> pl.DataFrame:
    """Add the recovery routes' links to the within-year `crdf_crs` links.

    The EIB routes run in order on the EIB rows that no earlier link reaches,
    leaving out rows that repeat an earlier row (`repeat_of`). A recovered EIB row's
    links form its own group, `<link_method>|<row_id>`, except that rows matched
    through their description group's sum share the group of the first of them. A
    row whose units another row's group already holds stays unlinked, with the
    rows that share its group, unless every holder repeats a row of that group.
    The earliest rows count, so the group takes over its repeats' groups.

    Args:
        crdf: Recipient-perspective CRDF rows.
        within_year: `link_within_year` output.
        units: The link stage's units.
        crs: Normalised CRS records.
        external: The `external` table, read for the EIB's IATI activity ids.
        exchange: DAC exchange rates with `iso_code`, `year` and `exchange_rate`.
        skip: Names of EIB routes (`eib_list`, `crosswalk`, `title_recipient`,
            `eib_text_amount`, `eib_amount`) to leave out, to measure what one
            route adds. A release builds with none.

    Returns:
        `within_year`, with groups taken over re-pointed to the row that took them,
        the recovered `crdf_crs` links, and the `trace` links of id-less aggregate
        records to themselves.

    Raises:
        LinkInputError: when the euro has no DAC rate for a year an EIB row or bulk
            EIB commitment needs, or `skip` names a route that does not exist.
    """
    rates = exchange.filter(pl.col("iso_code") == EURO_ISSUER).select(
        year=pl.col("year").cast(pl.Int16), euro_per_usd=pl.col("exchange_rate")
    )
    rows = _in_euro(
        crdf.filter(
            pl.col("provider_code") == EU_INSTITUTIONS,
            pl.col("agency_code") == EIB_AGENCY,
            pl.col("repeat_of").is_null(),
            pl.col("face_usd") > 0,
        ).collect(),
        rates,
        amount="face_usd",
    )
    records = _eib_commitments(crs, units=units, rates=rates)
    crosswalk = _eib_crosswalk(external)
    scored = _scored_pairs(rows, _eib_units(crs, units=units, rates=rates))
    routes: tuple[tuple[str, _Route], ...] = (
        (
            "eib_list",
            lambda left: _by_project_list(
                left, crs=crs, exchange=exchange, records=records
            ).with_columns(link_method=pl.lit("eib_list")),
        ),
        ("crosswalk", lambda left: _by_crosswalk(left, records, crosswalk)),
        (
            "title_recipient",
            lambda left: _by_title(left, records).with_columns(
                link_method=pl.lit("title_recipient")
            ),
        ),
        (
            "eib_text_amount",
            lambda left: _by_text_and_amount(
                left,
                scored,
                gap=_TEXT_AMOUNT_GAP,
                text=pl.col("cosine") >= _TEXT_MATCH,
            ).with_columns(link_method=pl.lit("eib_text_amount")),
        ),
        (
            "eib_amount",
            lambda left: _by_text_and_amount(
                left,
                scored,
                gap=_EXACT_AMOUNT_GAP,
                text=(pl.col("cosine") >= _TEXT_CONSISTENT) | pl.col("generic"),
            ).with_columns(link_method=pl.lit("eib_amount")),
        ),
    )
    if unknown := sorted(set(skip) - {name for name, _ in routes}):
        raise LinkInputError(f"Unknown EIB recovery routes to skip: {unknown}")
    repeats = crdf.select(holder="row_id", first_row="repeat_of").collect()
    links = within_year
    for name, route in routes:
        if name in skip:
            continue
        left = rows.join(links.select(row_id="left_id"), on="row_id", how="anti")
        links = _settle(route(left), links=links, repeats=repeats)
    return pl.concat([links, _by_aggregate_record(crdf, units=units)])


def _by_aggregate_record(crdf: pl.LazyFrame, *, units: pl.LazyFrame) -> pl.DataFrame:
    """Links of aggregate children without a crs id to their record's commitment
    units, and of those units to themselves.

    A child carries its record's donor, agency, project number, recipient, purpose
    and year, and the units with those and no crs id are the record's. Children of
    one such key share a group.
    """
    key = ["donor_code", "agency_code", "project_number", "recipient_code"]
    key += ["purpose_code", "year"]
    children = crdf.filter(
        pl.col("parent_row_id").is_not_null(), pl.col("crs_id").is_null()
    ).select(
        "row_id",
        "agency_code",
        "purpose_code",
        "year",
        "recipient_code",
        donor_code="provider_code",
        project_number=normalise_pn(pl.col("project_number")),
    )
    commitments = units.filter(
        pl.col("crs_id").is_null(), pl.col("commitment_usd") > 0
    ).select("unit_id", "disbursement_usd", *key)
    method = pl.lit("aggregate_partition")
    pairs = (
        children.join(commitments, on=key, nulls_equal=True)
        .with_columns(group_id=group_id(method, pl.col("unit_id").min().over(key)))
        .collect()
    )
    to_records = pairs.select(
        left_id="row_id", right_id="unit_id", group_id="group_id", link_method=method
    )
    own = (
        pairs.filter(pl.col("disbursement_usd") > 0)
        .select(
            left_id="unit_id",
            right_id="unit_id",
            group_id=group_id(method, "unit_id"),
            link_method=method,
        )
        .unique()
    )
    return pl.concat(
        [
            as_exact_links(to_records, task="crdf_crs"),
            as_exact_links(own, task="trace"),
        ]
    )


def _eib_commitments(
    crs: pl.LazyFrame, *, units: pl.LazyFrame, rates: pl.DataFrame
) -> pl.DataFrame:
    """Bulk EIB commitment records with their commitment unit, amount in euros and
    normalised project number."""
    eib = (pl.col("donor_code") == EU_INSTITUTIONS) & (
        pl.col("agency_code") == EIB_AGENCY
    )
    unit_of = (
        units.filter(eib, pl.col("commitment_usd") > 0)
        .select("unit_id", record_id="record_ids")
        .explode("record_id")
    )
    records = (
        trace_population(crs)
        .filter(eib, pl.col("commitment_usd") > 0)
        .select(
            "record_id",
            "year",
            "recipient_code",
            "title",
            "commitment_usd",
            project_number=normalise_pn(pl.col("project_number")),
        )
        .join(unit_of, on="record_id")
        .collect()
    )
    return _in_euro(records, rates, amount="commitment_usd")


def _eib_crosswalk(external: pl.LazyFrame) -> pl.DataFrame:
    """Each EIB project serial with its finance contract numbers, from the EIB's
    IATI activity ids."""
    return (
        external.filter(pl.col("source") == "iati", pl.col("publisher") == "eib")
        .select(pl.col("activity_id").str.extract_groups(_EIB_ACTIVITY).struct.unnest())
        .drop_nulls()
        .unique()
        .collect()
    )


def _by_project_list(
    rows: pl.DataFrame,
    *,
    crs: pl.LazyFrame,
    exchange: pl.DataFrame,
    records: pl.DataFrame,
) -> pl.DataFrame:
    return (
        match_eib(rows, crs=crs, exchange=exchange)
        .join(records.select("record_id", "unit_id"), on="record_id")
        .select("row_id", "unit_id")
    )


def _by_crosswalk(
    rows: pl.DataFrame, records: pl.DataFrame, crosswalk: pl.DataFrame
) -> pl.DataFrame:
    """Projects under the row's own project number or a contract of its serial, as
    `crosswalk` pairs within `_EXACT_TOLERANCE` and `crosswalk_amount` pairs
    beyond it."""
    own = rows.select("row_id", project_number=normalise_pn(pl.col("project_number")))
    contracts = own.join(crosswalk, left_on="project_number", right_on="serial").select(
        "row_id", project_number="contract"
    )
    keys = pl.concat([own, contracts]).drop_nulls().unique()
    pairs = _pair_projects(
        rows.select("row_id", "year", "euro").join(keys, on="row_id"), records
    )
    return pairs.select(
        "row_id",
        "unit_id",
        link_method=pl.when(pl.col("gap") <= _EXACT_TOLERANCE)
        .then(pl.lit("crosswalk"))
        .otherwise(pl.lit("crosswalk_amount")),
    )


def _by_title(rows: pl.DataFrame, records: pl.DataFrame) -> pl.DataFrame:
    """Projects with the row's recipient and a specific title whose words overlap
    the row's title's by at least `_TITLE_OVERLAP`."""
    title = pl.col("title").str.to_uppercase()
    specific = records.filter(
        pl.col("project_number").is_not_null(),
        pl.len().over(title) <= _GENERIC_TITLE,
    ).select(
        "recipient_code",
        "project_number",
        record_year="year",
        record_words=_words("title"),
    )
    shared = pl.col("words").list.set_intersection("record_words").list.len()
    either = pl.col("words").list.set_union("record_words").list.len()
    candidates = (
        rows.select("row_id", "year", "euro", "recipient_code", words=_words("title"))
        .join(specific, on="recipient_code")
        .filter(
            pl.col("record_year") >= pl.col("year"),
            either > 0,
            shared >= _TITLE_OVERLAP * either,
        )
        .select("row_id", "year", "euro", "project_number")
        .unique()
    )
    return _pair_projects(candidates, records).select("row_id", "unit_id")


def _eib_units(
    crs: pl.LazyFrame, *, units: pl.LazyFrame, rates: pl.DataFrame
) -> pl.DataFrame:
    """Bulk EIB commitment units with a commitment date, with their commitment in
    euros at the rate of the year the CRS reports it, their project (normalised
    project number, else the unit itself), their first record's title and long
    description as `text`, and whether that text is generic."""
    eib = (pl.col("donor_code") == EU_INSTITUTIONS) & (
        pl.col("agency_code") == EIB_AGENCY
    )
    texts = crs.filter(eib).select("record_id", "title", "long_description")
    dated = (
        units.filter(
            eib, pl.col("commitment_usd") > 0, pl.col("commitment_year").is_not_null()
        )
        .select(
            "unit_id",
            "recipient_code",
            "commitment_usd",
            "year",
            "commitment_year",
            project=pl.coalesce("project_number", "unit_id"),
            record_id=pl.col("record_ids").list.first(),
        )
        .join(texts, on="record_id", how="left")
        .with_columns(
            text=pl.concat_str(
                "title", "long_description", separator=". ", ignore_nulls=True
            ).fill_null("")
        )
        .with_columns(
            words=pl.col("text").str.to_lowercase().str.replace_all(r"\s+", " ")
        )
        .select(
            "unit_id",
            "recipient_code",
            "commitment_usd",
            "year",
            "commitment_year",
            "project",
            "text",
            generic=(pl.col("words").str.strip_chars() == "")
            | (pl.len().over("words") > _GENERIC_TEXT_UNITS),
        )
        .collect()
    )
    # The CRS converts a commitment at the rate of the year it reports it in, so
    # that rate gives back its euros; the commitment year only dates it.
    return _in_euro(dated, rates, amount="commitment_usd")


def _scored_pairs(rows: pl.DataFrame, units: pl.DataFrame) -> pl.DataFrame:
    """Each row's units with its recipient, committed in the row's year or later, as
    (row_id, unit_id, project, unit_euro, generic, cosine).

    `cosine` compares the row's title, and its description where that differs,
    with the unit's text, as TF-IDF vectors fitted on every row's and unit's text.
    """
    rows = rows.sort("row_id").with_columns(row=pl.int_range(pl.len()))
    units = units.sort("unit_id").with_columns(unit=pl.int_range(pl.len()))
    pairs = (
        rows.select("row", "row_id", "year", "recipient_code")
        .join(
            units.select(
                "unit",
                "unit_id",
                "recipient_code",
                "project",
                "generic",
                unit_year="commitment_year",
                unit_euro="euro",
            ),
            on="recipient_code",
        )
        .filter(pl.col("unit_year") >= pl.col("year"))
        .sort("row", "unit")
    )
    columns = ["row_id", "unit_id", "project", "unit_euro", "generic"]
    if pairs.is_empty():
        return pairs.select(columns).with_columns(cosine=pl.lit(None, pl.Float64))
    described = pl.col("description") != pl.col("title").fill_null("")
    row_texts = rows.select(
        pl.concat_str(
            "title",
            pl.when(described).then("description"),
            separator=". ",
            ignore_nulls=True,
        ).fill_null("")
    ).to_series()
    vectoriser = TfidfVectorizer(
        sublinear_tf=True, stop_words="english", ngram_range=(1, 2)
    )
    try:
        vectoriser.fit([*row_texts, *units["text"]])
    except ValueError:
        # Every text is empty or stop words, so no text agrees.
        cosine = np.zeros(pairs.height)
    else:
        row_vectors = vectoriser.transform(row_texts)[pairs["row"].to_numpy()]
        unit_vectors = vectoriser.transform(units["text"])[pairs["unit"].to_numpy()]
        # The vectors have unit length, so their dot product is the cosine.
        cosine = np.asarray(row_vectors.multiply(unit_vectors).sum(axis=1)).ravel()
    return pairs.select(columns).with_columns(
        cosine=pl.Series(cosine, dtype=pl.Float64)
    )


def _by_text_and_amount(
    rows: pl.DataFrame, scored: pl.DataFrame, *, gap: float, text: pl.Expr
) -> pl.DataFrame:
    """Each row's unit that passes `text` and whose commitment is within `gap` (an
    absolute log ratio) of the row's face value or of its description group's, as
    (row_id, unit_id, group_row).

    A row links only when the units passing belong to one project, and then to the
    one with the highest cosine, ties to the smaller unit id. Rows of a description
    group matched through its sum share their unit, under `group_row`, the first of
    them. A unit two matches take goes to the one with the higher cosine, ties to
    the smaller `group_row`.

    A description group (`vintages.EIB_DESCRIPTION_GROUP`) holds only `rows`, the
    rows no earlier route linked, since a linked row's money is already accounted
    for and summing it in would match a unit on it a second time. A row with no
    description is a group of its own, as it says nothing about which operation it
    belongs to.
    """
    key = pl.col("description_key")
    faces = (
        rows.with_columns(description_key=eib_description_key())
        .with_columns(
            group=pl.when(key.is_null())
            .then(pl.col("row_id"))
            .otherwise(pl.col("row_id").min().over("year", *EIB_DESCRIPTION_GROUP))
        )
        .select(
            "row_id",
            "group",
            row_euro="euro",
            group_euro=pl.col("euro").sum().over("group"),
            group_rows=pl.len().over("group"),
        )
    )
    passing = (
        scored.join(faces, on="row_id")
        .with_columns(
            own=_log_gap("row_euro") <= gap,
            pooled=(pl.col("group_rows") > 1) & (_log_gap("group_euro") <= gap),
        )
        .filter(pl.col("own") | pl.col("pooled"), text)
    )
    picked = (
        passing.filter(pl.col("project").n_unique().over("row_id") == 1)
        .sort(["row_id", "cosine", "unit_id"], descending=[False, True, False])
        .group_by("row_id", maintain_order=True)
        .first()
        .with_columns(
            group_row=pl.when("own")
            .then(pl.col("row_id"))
            .otherwise(pl.col("row_id").min().over("group", "unit_id", "own"))
        )
    )
    return (
        picked.sort(["cosine", "group_row"], descending=[True, False])
        .filter(pl.col("group_row") == pl.col("group_row").first().over("unit_id"))
        .select("row_id", "unit_id", "group_row")
        .sort("row_id")
    )


def _pair_projects(candidates: pl.DataFrame, records: pl.DataFrame) -> pl.DataFrame:
    """Pair rows with candidate projects one to one, closest amount first, as
    (row_id, unit_id, gap).

    A project's amount is the sum of its commitments from the row's year on, and
    `gap` its relative distance from the row's. Pairs more than `_AMOUNT_TOLERANCE`
    apart are dropped first.
    """
    matched = candidates.join(
        records.select(
            "project_number", "unit_id", record_year="year", record_euro="euro"
        ),
        on="project_number",
    ).filter(pl.col("record_year") >= pl.col("year"))
    sums = (
        matched.group_by("row_id", "project_number")
        .agg(pl.col("euro").first(), pl.col("record_euro").sum())
        .with_columns(gap=(pl.col("record_euro") / pl.col("euro") - 1).abs())
        .filter(pl.col("gap") <= _AMOUNT_TOLERANCE)
        .sort("gap", "row_id", "project_number")
    )
    paired = pair_one_to_one(sums, left=["row_id"], right=["project_number"])
    return (
        matched.join(
            paired.select("row_id", "project_number", "gap"),
            on=["row_id", "project_number"],
        )
        .select("row_id", "unit_id", "gap")
        .unique()
    )


def _settle(
    pairs: pl.DataFrame, *, links: pl.DataFrame, repeats: pl.DataFrame
) -> pl.DataFrame:
    """Add a route's (row_id, unit_id, link_method) pairs to `links`, each as a
    `<link_method>|<group_row>` group, where `group_row` defaults to the row.

    A row loses its pairs when another row's group holds one of its units, unless
    that holder repeats a row of the row's group, in which case the group takes
    over the holder's group. Rows that share a group lose their pairs together.
    """
    # Only the EIB text and amount routes emit `group_row`.
    if "group_row" not in pairs.columns:
        pairs = pairs.with_columns(group_row="row_id")
    holders = links.select(
        unit_id="right_id", group_id="group_id", holder="left_id"
    ).join(repeats, on="holder", how="left")
    members = pairs.select("group_row", first_row="row_id").unique()
    repeated = members.with_columns(repeats_group=pl.lit(True))
    claimed = pairs.join(holders, on="unit_id", how="left").join(
        repeated, on=["group_row", "first_row"], how="left"
    )
    blocked = claimed.filter(
        pl.col("holder").is_not_null(), pl.col("repeats_group").is_null()
    )
    claimed = claimed.join(blocked.select("group_row"), on="group_row", how="anti")
    taken = (
        claimed.drop_nulls("holder")
        .select("group_id", "row_id", "group_row", method="link_method")
        .unique()
    )
    group = group_id(pl.col("method"), "group_row")
    moved = links.join(taken, on="group_id").select(
        left_id="row_id", right_id="right_id", group_id=group, link_method="method"
    )
    new = claimed.select(
        left_id="row_id",
        right_id="unit_id",
        group_id=group_id(pl.col("link_method"), "group_row"),
        link_method="link_method",
    )
    moved_and_new = as_exact_links(pl.concat([moved, new]), task="crdf_crs")
    return pl.concat(
        [links.join(taken.select("group_id"), on="group_id", how="anti"), moved_and_new]
    ).unique(["left_id", "right_id"], keep="first", maintain_order=True)


def _in_euro(frame: pl.DataFrame, rates: pl.DataFrame, *, amount: str) -> pl.DataFrame:
    """`frame` with `euro`, `amount` at the euro's DAC rate of the row's year."""
    joined = frame.join(rates, on="year", how="left", maintain_order="left")
    missing = joined.filter(pl.col("euro_per_usd").is_null())["year"].unique().sort()
    if missing.len():
        raise LinkInputError(
            f"The euro has no DAC exchange rate for {missing.to_list()}, so EIB "
            "amounts of those years cannot be compared"
        )
    return joined.with_columns(euro=pl.col(amount) * pl.col("euro_per_usd")).drop(
        "euro_per_usd"
    )


def _log_gap(face: str) -> pl.Expr:
    """The absolute log ratio of the unit's commitment to `face`, both in euros."""
    return (pl.col("unit_euro") / pl.col(face)).log().abs()


def _words(column: str) -> pl.Expr:
    """The title's distinct upper-case words of two or more letters or digits."""
    return (
        pl.col(column)
        .fill_null("")
        .str.to_uppercase()
        .str.extract_all(r"[A-Z0-9]{2,}")
        .list.unique()
    )

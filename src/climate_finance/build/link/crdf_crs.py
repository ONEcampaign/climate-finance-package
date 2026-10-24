"""CRDF rows to CRS commitments, within a year and then by the recovery routes.

Within a year, a CRDF row joins the commitment units that share its provider, crs
id, purpose and year. The recovery routes link rows that join reaches no unit for.

Aggregate children (`normalise.aggregates`) take the crs id of the bulk record they
stand for, so most join within the year. A child of a record without a crs id links
to that record's commitment units (`aggregate_partition`), and those units link to
themselves as a trace, so the record's own disbursement fills its own commitment.

The EIB reports to the CRDF and the CRS as agency 3 of EU Institutions (918), but
its CRDF ids rarely reach the CRS: 2021 rows carry a contract number, 2022 rows a
project serial the CRS never prints, and the CRS lags by one to three years. Three
routes link the EIB rows, each to the commitment units of the bulk records it
matches:

- `eib_list`: the EIB's project list, by project number and recipient, or recipient,
  euro amount within 3% and a specific title (`vintages.match_eib`).
- `crosswalk`: the EIB's IATI activity ids pair each project serial with its finance
  contract numbers, which the CRS uses as project numbers.
- `title_recipient`: a bulk project with the same recipient and nearly the same
  title.

The last two take a project whose commitments from the row's year on come within
10% of the row's face value, in euros, pairing rows and projects closest amount
first.
"""

from collections.abc import Callable, Collection

import polars as pl

from climate_finance.build.errors import LinkInputError
from climate_finance.build.link.keys import as_exact_links, normalise_pn
from climate_finance.build.normalise.crs import trace_population
from climate_finance.build.normalise.vintages import match_eib, pair_one_to_one

# CRDF rows and CRS commitments that agree on these, the CRDF provider standing for
# the CRS donor, are one commitment. 99.5% of such keys pair one to one.
WITHIN_YEAR_KEY = ("donor_code", "crs_id", "year", "purpose_code")

_EIB_DONOR = 918
_EIB_AGENCY = 3
# The EIB's IATI activity id carries its project serial, then one finance contract
# number.
_EIB_ACTIVITY = r"^XM-DAC-918-3-(?<serial>\d{8})-(?<contract>\d{5})$"
# DAC exchange rates are by country, and the euro's is held by EU Institutions.
_EURO_ISSUER = "EUI"
# A project's commitments match a row's face value within this share, in euros.
_AMOUNT_TOLERANCE = 0.10
# The title route needs word sets that overlap this much, on a title that at most
# this many EIB commitments carry.
_TITLE_OVERLAP = 0.8
_GENERIC_TITLE = 10

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
            group_id=pl.concat_str(
                [pl.lit("within_year"), *WITHIN_YEAR_KEY], separator="|"
            ),
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
    links form its own group, `<link_method>|<row_id>`. A row whose units another
    row's group already holds stays unlinked, unless every holder repeats it. The
    earliest row counts, so it takes over its repeats' group.

    Args:
        crdf: Recipient-perspective CRDF rows.
        within_year: `link_within_year` output.
        units: The link stage's units.
        crs: Normalised CRS records.
        external: The `external` table, read for the EIB's IATI activity ids.
        exchange: DAC exchange rates with `iso_code`, `year` and `exchange_rate`.
        skip: Names of EIB routes (`eib_list`, `crosswalk`, `title_recipient`) to
            leave out, to measure what one route adds. A release builds with none.

    Returns:
        `within_year`, with groups taken over re-pointed to the row that took them,
        the recovered `crdf_crs` links, and the `trace` links of id-less aggregate
        records to themselves.

    Raises:
        LinkInputError: when the euro has no DAC rate for a year an EIB row or bulk
            EIB commitment needs, or `skip` names a route that does not exist.
    """
    rates = exchange.filter(pl.col("iso_code") == _EURO_ISSUER).select(
        year=pl.col("year").cast(pl.Int16), euro_per_usd=pl.col("exchange_rate")
    )
    rows = _in_euro(
        crdf.filter(
            pl.col("provider_code") == _EIB_DONOR,
            pl.col("agency_code") == _EIB_AGENCY,
            pl.col("repeat_of").is_null(),
            pl.col("face_usd") > 0,
        ).collect(),
        rates,
        amount="face_usd",
    )
    records = _eib_commitments(crs, units=units, rates=rates)
    crosswalk = _eib_crosswalk(external)
    routes: tuple[tuple[str, _Route], ...] = (
        (
            "eib_list",
            lambda left: _by_project_list(
                left, crs=crs, exchange=exchange, records=records
            ),
        ),
        ("crosswalk", lambda left: _by_crosswalk(left, records, crosswalk)),
        ("title_recipient", lambda left: _by_title(left, records)),
    )
    if unknown := sorted(set(skip) - {method for method, _ in routes}):
        raise LinkInputError(f"Unknown EIB recovery routes to skip: {unknown}")
    repeats = crdf.select(holder="row_id", first_row="repeat_of").collect()
    links = within_year
    for method, route in routes:
        if method in skip:
            continue
        left = rows.join(links.select(row_id="left_id"), on="row_id", how="anti")
        links = _settle(route(left), links=links, repeats=repeats, method=method)
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
        .with_columns(
            group_id=pl.concat_str(
                [method, pl.col("unit_id").min().over(key)],
                separator="|",
            )
        )
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
            group_id=pl.concat_str([method, pl.col("unit_id")], separator="|"),
            link_method=method,
        )
        .unique()
    )
    return pl.concat(
        [
            as_exact_links(to_records.lazy(), task="crdf_crs").collect(),
            as_exact_links(own.lazy(), task="trace").collect(),
        ]
    )


def _eib_commitments(
    crs: pl.LazyFrame, *, units: pl.LazyFrame, rates: pl.DataFrame
) -> pl.DataFrame:
    """Bulk EIB commitment records with their commitment unit, amount in euros and
    normalised project number."""
    eib = (pl.col("donor_code") == _EIB_DONOR) & (pl.col("agency_code") == _EIB_AGENCY)
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
    """Projects under the row's own project number or a contract of its serial."""
    own = rows.select("row_id", project_number=normalise_pn(pl.col("project_number")))
    contracts = own.join(crosswalk, left_on="project_number", right_on="serial").select(
        "row_id", project_number="contract"
    )
    keys = pl.concat([own, contracts]).drop_nulls().unique()
    return _pair_projects(
        rows.select("row_id", "year", "euro").join(keys, on="row_id"), records
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
    return _pair_projects(candidates, records)


def _pair_projects(candidates: pl.DataFrame, records: pl.DataFrame) -> pl.DataFrame:
    """Pair rows with candidate projects one to one, closest amount first.

    A project's amount is the sum of its commitments from the row's year on. Pairs
    more than `_AMOUNT_TOLERANCE` apart are dropped first.
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
            paired.select("row_id", "project_number"), on=["row_id", "project_number"]
        )
        .select("row_id", "unit_id")
        .unique()
    )


def _settle(
    pairs: pl.DataFrame, *, links: pl.DataFrame, repeats: pl.DataFrame, method: str
) -> pl.DataFrame:
    """Add a route's (row_id, unit_id) pairs to `links` as `method` groups.

    A row loses its pairs when another row's group holds one of its units, unless
    that holder repeats it, in which case the row takes over the holder's group.
    """
    holders = links.select(
        unit_id="right_id", group_id="group_id", holder="left_id"
    ).join(repeats, on="holder", how="left")
    claimed = pairs.join(holders, on="unit_id", how="left")
    other = pl.col("first_row").is_null() | (pl.col("first_row") != pl.col("row_id"))
    blocked = claimed.filter(pl.col("holder").is_not_null() & other).select("row_id")
    claimed = claimed.join(blocked, on="row_id", how="anti")
    taken = claimed.drop_nulls("holder").select("group_id", "row_id").unique()
    group = pl.concat_str([pl.lit(method), pl.col("row_id")], separator="|")
    moved = (
        links.join(taken, on="group_id")
        .with_columns(left_id="row_id", group_id=group, link_method=pl.lit(method))
        .select(links.columns)
    )
    new = as_exact_links(
        claimed.lazy().select(
            left_id="row_id",
            right_id="unit_id",
            group_id=group,
            link_method=pl.lit(method),
        ),
        task="crdf_crs",
    ).collect()
    return pl.concat(
        [links.join(taken.select("group_id"), on="group_id", how="anti"), moved, new]
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


def _words(column: str) -> pl.Expr:
    """The title's distinct upper-case words of two or more letters or digits."""
    return (
        pl.col(column)
        .fill_null("")
        .str.to_uppercase()
        .str.extract_all(r"[A-Z0-9]{2,}")
        .list.unique()
    )

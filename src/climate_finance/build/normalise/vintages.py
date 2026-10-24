"""Rows repeated across CRDF vintages.

A commitment the OECD reports in one year's CRDF can reappear in a later year's,
under a new id when the provider changed its id scheme. Four rules pair rows of
different years, within one perspective and provider:

- same id: a row whose crs id the CRS knows, but with no CRS commitment under its
  own key (provider, crs id, year, purpose), shares provider, crs id and purpose
  with a row of an earlier year. A row of a later year that the CRS commits money
  under is new money under the old id, such as a yearly contribution to a fund.
- EIB bulk record: an EIB row without a CRS commitment of its own matches a bulk
  EIB commitment (`_match_eib`) whose key, or crs id, an EIB row of another year
  carries.
- project id: same recipient and project number (`normalise_pn`, so placeholders
  such as `UNKNOWN` name no project), different crs ids, face values within 10%.
- title: same recipient and a specific title (at most three of the provider's rows
  carry it), different crs ids, face values within 10%.

A project-id or title pair repeats only when at least one of its rows has no CRS
commitment under its own key; when both have one, the CRS itself reports two
commitments (a tranche or a top-up). Paired rows form groups, and the rows of a
group's earliest year count: every later row gets `repeat_of`, the row id of the
group's first row by year and row id.
"""

import polars as pl

from climate_finance.build.normalise.crdf import CRDFNormaliseError
from climate_finance.build.normalise.crs import trace_population
from climate_finance.build.normalise.project_numbers import normalise_pn

# Face values of a project-id or title pair agree within this share of the larger.
_FACE_TOLERANCE = 0.10
# A title is specific when it keeps at least this many characters once normalised
# and at most this many of the provider's rows carry it.
_TITLE_MIN_CHARS = 15
_TITLE_MAX_ROWS = 3
_PROJECT_ID_MIN_CHARS = 3

# The EIB reports to the CRDF as agency 3 of EU Institutions (918).
_EIB = {"donor_code": 918, "agency_code": 3}
# DAC exchange rates are by country. The euro's is held by EU Institutions.
_EURO_ISSUER = "EUI"
# An EIB match needs euro amounts within 3%, and an equal project number, a title
# overlap of at least 0.5 or an equal purpose. Purpose-only matches are dropped once
# pairs are chosen, because they tie on round euro amounts.
_EIB_AMOUNT_TOLERANCE = 0.03
_EIB_CLOSE_AMOUNT = 0.005
_EIB_TITLE_OVERLAP = 0.5
# A bulk title used by more than this many EIB commitments gives no title evidence.
_EIB_GENERIC_TITLE = 10
_TITLE_STOP_WORDS = (
    "the",
    "and",
    "for",
    "des",
    "les",
    "del",
    "loan",
    "project",
    "programme",
    "program",
    "projet",
    "facility",
    "fl",
    "de",
    "la",
    "du",
    "et",
)


def mark_repeats(
    crdf: pl.LazyFrame, *, crs: pl.LazyFrame, exchange: pl.DataFrame
) -> pl.LazyFrame:
    """Set `repeat_of` on later rows that repeat an earlier vintage's row.

    Args:
        crdf: Normalised CRDF rows, both perspectives.
        crs: Normalised CRS records.
        exchange: DAC exchange rates with `iso_code`, `year` and `exchange_rate`.
            The euro's rate puts EIB amounts of different years on one footing.

    Raises:
        CRDFNormaliseError: when the euro has no DAC rate for a year that an EIB row
            or bulk EIB commitment needs.
    """
    rows = _with_keys(crdf.collect(), crs=crs)
    groups = repeat_groups(rows, crs=crs, exchange=exchange).join(
        rows.select("row_id", "year"), on="row_id"
    )
    first = groups.sort("year", "row_id").group_by("group", maintain_order=True).first()
    repeats = groups.join(
        first.select("group", first_row="row_id", first_year="year"), on="group"
    ).filter(pl.col("year") > pl.col("first_year"))
    return (
        crdf.join(
            repeats.lazy().select("row_id", "first_row"),
            on="row_id",
            how="left",
            maintain_order="left",
        )
        .with_columns(repeat_of=pl.coalesce("first_row", "repeat_of"))
        .drop("first_row")
    )


def repeat_groups(
    rows: pl.DataFrame, *, crs: pl.LazyFrame, exchange: pl.DataFrame
) -> pl.DataFrame:
    """Every paired row's group, named by the smallest row id in it."""
    edges = pl.concat(
        [
            _same_id_edges(rows, crs=crs),
            _eib_edges(rows, crs=crs, exchange=exchange),
            _similar_edges(rows, key="project_id"),
            _similar_edges(rows, key="title_key"),
        ]
    )
    return _union(edges)


def commitment_keys(crs: pl.LazyFrame) -> pl.LazyFrame:
    """The (provider, crs id, year, purpose) keys the CRS commits money under.

    Records are summed by donor, agency, crs id, project number, recipient, purpose,
    year and commitment year, and a key has a commitment when one of its sums is
    positive.
    """
    units = [
        "donor_code",
        "agency_code",
        "crs_id",
        "project_number",
        "recipient_code",
        "purpose_code",
        "year",
        pl.col("commitment_date").dt.year().alias("commitment_year"),
    ]
    return (
        _crs_units(crs)
        .group_by(units)
        .agg(pl.col("commitment_usd").sum().alias("unit_commitment"))
        .filter(pl.col("unit_commitment") > 0, pl.col("crs_id").is_not_null())
        .select(
            provider_code="donor_code",
            crs_id="crs_id",
            year="year",
            purpose_code="purpose_code",
        )
        .unique()
    )


def crs_ids(crs: pl.LazyFrame) -> pl.LazyFrame:
    """Every (provider, crs id) the CRS reports in any year."""
    return (
        _crs_units(crs)
        .filter(pl.col("crs_id").is_not_null())
        .select(provider_code="donor_code", crs_id="crs_id")
        .unique()
    )


def _crs_units(crs: pl.LazyFrame) -> pl.LazyFrame:
    """Trace-population records that report a commitment or a disbursement."""
    return trace_population(crs).filter(
        pl.col("commitment_usd").is_not_null()
        | pl.col("disbursement_usd").is_not_null()
    )


def _with_keys(rows: pl.DataFrame, *, crs: pl.LazyFrame) -> pl.DataFrame:
    """Add `backed`, `id_in_crs`, `project_id` and `title_key` to the rows."""
    backed = commitment_keys(crs).with_columns(backed=pl.lit(True)).collect()
    known = crs_ids(crs).with_columns(id_in_crs=pl.lit(True)).collect()
    project_id = normalise_pn(pl.col("project_number"))
    title_key = (
        pl.col("title").str.to_uppercase().str.extract_all(r"[A-Z0-9]+").list.join(" ")
    )
    rows = (
        rows.join(
            backed,
            on=["provider_code", "crs_id", "year", "purpose_code"],
            how="left",
            maintain_order="left",
        )
        .join(known, on=["provider_code", "crs_id"], how="left", maintain_order="left")
        .with_columns(
            pl.col("backed", "id_in_crs").fill_null(False),
            project_id=pl.when(
                project_id.str.len_chars() >= _PROJECT_ID_MIN_CHARS
            ).then(project_id),
            title_key=pl.when(title_key.str.len_chars() >= _TITLE_MIN_CHARS).then(
                title_key
            ),
        )
    )
    title_rows = pl.len().over("perspective", "provider_code", "title_key")
    return rows.with_columns(
        title_key=pl.when(title_rows <= _TITLE_MAX_ROWS).then("title_key")
    )


def _same_id_edges(rows: pl.DataFrame, *, crs: pl.LazyFrame) -> pl.DataFrame:
    """Rows whose id the CRS knows but commits nothing under, and earlier rows."""
    key = ["perspective", "provider_code", "crs_id", "purpose_code"]
    unbacked = rows.filter(pl.col("id_in_crs"), ~pl.col("backed"))
    return _edges(
        unbacked.select("row_id", "year", *key)
        .join(rows.select(*key, other="row_id", other_year="year"), on=key)
        .filter(pl.col("other_year") < pl.col("year"))
    )


def _eib_edges(
    rows: pl.DataFrame, *, crs: pl.LazyFrame, exchange: pl.DataFrame
) -> pl.DataFrame:
    """EIB rows paired through the bulk commitment one of them matches."""
    eib = rows.filter(
        pl.col("provider_code") == _EIB["donor_code"],
        pl.col("agency_code") == _EIB["agency_code"],
    )
    matches = match_eib(eib.filter(~pl.col("backed")), crs=crs, exchange=exchange)
    carriers = eib.select(
        "perspective",
        other="row_id",
        other_year="year",
        record_crs_id="crs_id",
        record_year="year",
        record_purpose="purpose_code",
    )
    by_key = matches.join(
        carriers, on=["perspective", "record_crs_id", "record_year", "record_purpose"]
    )
    by_id = matches.join(
        carriers.drop("record_year", "record_purpose"),
        on=["perspective", "record_crs_id"],
    )
    columns = ["row_id", "year", "other", "other_year"]
    return _edges(pl.concat([by_key.select(columns), by_id.select(columns)]))


def match_eib(
    rows: pl.DataFrame, *, crs: pl.LazyFrame, exchange: pl.DataFrame
) -> pl.DataFrame:
    """Each EIB row's bulk EIB commitment, one to one, best evidence first.

    A candidate is a commitment to the row's recipient in the row's year or later,
    with euro amounts within 3%. It scores on an equal project number, how close the
    amounts are, title overlap, an equal purpose and an equal year. Rows and
    commitments pair best score first, and pairs that agree on purpose alone are then
    dropped.
    """
    rates = exchange.filter(pl.col("iso_code") == _EURO_ISSUER).select(
        year=pl.col("year").cast(pl.Int16), euro_per_usd=pl.col("exchange_rate")
    )
    records = (
        trace_population(crs)
        .filter(
            pl.col("donor_code") == _EIB["donor_code"],
            pl.col("agency_code") == _EIB["agency_code"],
            pl.col("commitment_usd") > 0,
            pl.col("year") >= pl.lit(rows["year"].min(), pl.Int16),
        )
        .select(
            "record_id",
            "recipient_code",
            record_year="year",
            record_crs_id="crs_id",
            record_purpose="purpose_code",
            record_project_number="project_number",
            record_title="title",
            record_commitment="commitment_usd",
        )
        .collect()
    )
    title = pl.col("record_title").str.to_lowercase()
    records = records.with_columns(
        generic_title=title.is_null() | (pl.len().over(title) > _EIB_GENERIC_TITLE),
        record_tokens=_title_tokens("record_title"),
    )
    rows = rows.select(
        "row_id",
        "perspective",
        "year",
        "recipient_code",
        "purpose_code",
        "face_usd",
        project_id=pl.col("project_number").str.replace(r"\.0$", ""),
        tokens=_title_tokens("title"),
    )
    rows = _in_euro(rows, rates, year="year", amount="face_usd", into="euro")
    records = _in_euro(
        records,
        rates,
        year="record_year",
        amount="record_commitment",
        into="record_euro",
    )

    candidates = (
        rows.join(records, on="recipient_code")
        .filter(pl.col("record_year") >= pl.col("year"))
        .with_columns(
            same_purpose=(pl.col("purpose_code") == pl.col("record_purpose")).fill_null(
                False
            ),
            same_project=(
                pl.col("project_id") == pl.col("record_project_number")
            ).fill_null(False),
            amount_gap=(pl.col("record_euro") / pl.col("euro") - 1).abs(),
            title_overlap=pl.when(~pl.col("generic_title"))
            .then(_jaccard("tokens", "record_tokens"))
            .otherwise(0.0),
        )
        .filter(
            pl.col("amount_gap") <= _EIB_AMOUNT_TOLERANCE,
            pl.col("same_project")
            | (pl.col("title_overlap") >= _EIB_TITLE_OVERLAP)
            | pl.col("same_purpose"),
        )
        .with_columns(
            score=4
            + 4 * pl.col("same_project")
            + pl.when(pl.col("amount_gap") <= _EIB_CLOSE_AMOUNT).then(3).otherwise(1.5)
            + 2 * pl.col("title_overlap")
            + pl.col("same_purpose")
            + 0.5 * (pl.col("year") == pl.col("record_year"))
        )
        .sort("score", "row_id", "record_id", descending=[True, False, False])
    )
    paired = pair_one_to_one(
        candidates, left=["perspective", "row_id"], right=["perspective", "record_id"]
    )
    return paired.filter(
        pl.col("same_project") | (pl.col("title_overlap") >= _EIB_TITLE_OVERLAP)
    ).select(
        "perspective",
        "row_id",
        "year",
        "record_id",
        "record_crs_id",
        "record_year",
        "record_purpose",
    )


def _in_euro(
    frame: pl.DataFrame, rates: pl.DataFrame, *, year: str, amount: str, into: str
) -> pl.DataFrame:
    joined = frame.join(
        rates.rename({"year": year}), on=year, how="left", maintain_order="left"
    )
    missing = joined.filter(pl.col("euro_per_usd").is_null())[year].unique().sort()
    if missing.len():
        raise CRDFNormaliseError(
            f"The euro has no DAC exchange rate for {missing.to_list()}, so EIB "
            "amounts of those years cannot be compared"
        )
    return joined.with_columns(
        (pl.col(amount) * pl.col("euro_per_usd")).alias(into)
    ).drop("euro_per_usd")


def _title_tokens(column: str) -> pl.Expr:
    """The title's lower-case words of two or more characters, stop words left out."""
    words = (
        pl.col(column)
        .fill_null("")
        .str.to_lowercase()
        .str.replace_all("&", " and ", literal=True)
        .str.extract_all(r"[a-z0-9]+")
    )
    return words.list.eval(
        pl.element().filter(
            (pl.element().str.len_chars() >= 2) & ~pl.element().is_in(_TITLE_STOP_WORDS)
        )
    ).list.unique()


def _jaccard(left: str, right: str) -> pl.Expr:
    union = pl.col(left).list.set_union(pl.col(right)).list.len()
    shared = pl.col(left).list.set_intersection(pl.col(right)).list.len()
    return pl.when(union > 0).then(shared / union).otherwise(0.0)


def _similar_edges(rows: pl.DataFrame, *, key: str) -> pl.DataFrame:
    """Rows of different years and ids that share a recipient, `key` and face value."""
    on = ["perspective", "provider_code", "recipient_code", key]
    side = rows.filter(pl.col(key).is_not_null()).select(
        "row_id", "year", "crs_id", "face_usd", "backed", *on
    )
    larger = pl.max_horizontal(pl.col("face_usd").abs(), pl.col("face_usd_other").abs())
    pairs = side.join(side, on=on, suffix="_other").filter(
        pl.col("year") < pl.col("year_other"),
        pl.col("crs_id").fill_null("") != pl.col("crs_id_other").fill_null(""),
        pl.col("face_usd") > 0,
        (pl.col("face_usd") - pl.col("face_usd_other")).abs()
        <= _FACE_TOLERANCE * larger,
        ~(pl.col("backed") & pl.col("backed_other")),
    )
    return _edges(
        pairs.select("row_id", "year", other="row_id_other", other_year="year_other")
    )


def _edges(pairs: pl.DataFrame) -> pl.DataFrame:
    """Pairs of rows from different years, as `row_id` and `other`."""
    return pairs.filter(pl.col("year") != pl.col("other_year")).select(
        "row_id", "other"
    )


def _union(edges: pl.DataFrame) -> pl.DataFrame:
    """Each row's connected group, named by the smallest row id in it."""
    parent: dict[str, str] = {}

    def find(row: str) -> str:
        parent.setdefault(row, row)
        while parent[row] != row:
            parent[row] = parent[parent[row]]
            row = parent[row]
        return row

    for left, right in edges.iter_rows():
        a, b = find(left), find(right)
        if a != b:
            parent[max(a, b)] = min(a, b)
    return pl.DataFrame(
        {"row_id": list(parent), "group": [find(row) for row in parent]},
        schema={"row_id": pl.String, "group": pl.String},
    )


def pair_one_to_one(
    candidates: pl.DataFrame, *, left: list[str], right: list[str]
) -> pl.DataFrame:
    """Keep each candidate whose two sides are both still free, in the given order.

    `candidates` arrive sorted best first, ties broken by the sort.
    """
    taken_left: set[tuple[object, ...]] = set()
    taken_right: set[tuple[object, ...]] = set()
    keep: list[bool] = []
    for a, b in zip(
        candidates.select(left).iter_rows(),
        candidates.select(right).iter_rows(),
        strict=True,
    ):
        free = a not in taken_left and b not in taken_right
        if free:
            taken_left.add(a)
            taken_right.add(b)
        keep.append(free)
    return candidates.filter(pl.Series(keep, dtype=pl.Boolean))

"""The allocate stage, dated FIFO allocation and commitment status.

The link stage sums CRS records into `units` and links them. A `trace` link runs
from a disbursement unit (left) to each commitment unit (right) it may fill. A
disbursement unit carries at most one `group_id` per `link_method`, and a group
holds one key of one method, so the groups of one method never share a commitment.
Every disbursement unit of a group reaches the same commitments, less those its
record year or floor rules out, since the group pools their disbursement. A
`crdf_crs` link runs from a CRDF row to a commitment unit, and its `group_id` holds
every row and unit of one commitment.

Allocation runs on the CRS records inside those units (`units.record_ids`). A record
with a positive commitment in a commitment unit is a commitment at the unit's
`placed_year`, and a record with a positive disbursement in a linked unit is a
disbursement. Negative commitments are cancellations. They leave a commitment's
headroom unchanged and are reported by provider and year.

Passes run by split rank first (see below), then certain groups first, then by
record year, then link method in `LINK_METHODS` order, then class: records with a
date floor first, oldest floor first, then a dated donor's records dated in their
own year, then the rest. A group's pass is certain when every record pair of it is,
so certain links fill a commitment before estimated links of any year, and an
estimated link of an earlier year takes only what they left. Within a pass a group
pools its records' disbursement and fills its commitments oldest `placed_year`
first, pro rata to headroom within a year, capped at face value. A record whose unit
carries groups of several methods enters one pass per group, in that order, and each
later pass receives only what the earlier ones left: the exact key tiers first, then
the key-repair rules. What the last pass leaves is excess.

A trace group that review decisions split (`decisions.drop_pairs`) runs as one group
per part. A part ranked after another (`LINKS.split_path`) runs, with its own
commitments, in later passes, whatever its certainty and record year, so parts of one
group may share a commitment. A part holding an accepted link ranks before the part
holding none, and so claims a shared commitment's headroom first. A group that is not
split ranks with the first part of a split one, so a build without decisions passes
as it would with no split rank at all.

A disbursement record's own commitment in a group fills before the rest of the group
(`_own_commitments`). That is the record itself where it also commits, else the one
commitment record of the group whose real commitment date it carries and whose title
shares at least `OWN_TITLE_SHARE` of its words. Each own commitment runs as a group of
its own, in a pass just before the group's, so a donor that files a new module or phase
under an old crs id keeps the new module's money on the new commitment, and the old
module keeps its headroom. What the own commitment cannot take enters the group's pass.

Two per-donor switches change the order. A dated donor's record dated in its own
year fills that year's commitments first. A window donor's other records fill first
the commitments whose expected start to completion window, plus `WINDOW_GRACE`
years, covers the record year.

A `crdf_crs` group's allocation is split over its CRDF rows by face value, over the
larger of the group's CRS and CRDF face, so no row receives more than its face and
no record gives more than it disbursed. Amounts stay at face value. The release
query applies the climate share.

An allocation's low bound is its amount when a certain link carried it (see
`schema.CERTAIN_METHODS`) or the record disbursed against its own commitment, and
the CRDF rows reach that commitment by certain routes alone
(`schema.CERTAIN_ROUTES`). It is 0 otherwise. Its high bound is its amount.
"""

from __future__ import annotations

from collections.abc import Collection
from typing import TYPE_CHECKING

import numpy as np
import polars as pl

from climate_finance.build.errors import AllocationInputError
from climate_finance.build.link.candidates import word_share
from climate_finance.build.link.id_practice import period_of
from climate_finance.build.link.keys import line_date
from climate_finance.build.link.levels import title_words
from climate_finance.build.normalise.crs import trace_population
from climate_finance.build.sums import ordered_sum
from climate_finance.schema import CERTAIN_METHODS, CERTAIN_ROUTES, LINK_METHODS

if TYPE_CHECKING:
    from climate_finance.build.stages import StageContext, StageOutput

# A donor is dated when this share of its disbursement-only dollars in these record
# years carries a commitment date before the record year.
DATED_SHARE = 0.6
DATED_YEARS = (2020, 2025)
# A donor is a window donor when this share of later same-id disbursement, over at
# least WINDOW_MIN_USD tested, falls in [expected start, completion + WINDOW_GRACE],
# tested on ids with one commitment year in WINDOW_TEST_YEARS.
WINDOW_SHARE = 0.8
WINDOW_MIN_USD = 1e8
WINDOW_TEST_YEARS = (2015, 2021)
WINDOW_GRACE = 1
# Disbursed at this share of face value or more.
DISBURSED_RATIO = 0.9
# The method that links EU contract records to their decision, exempt from floors.
NO_FLOOR_METHODS = ("crs_id_parent",)
# Order offset that puts a commitment after every commitment-year order.
_LATE = 10_000
# A disbursement record's commitment record of the same real date is its own when
# their titles share at least this word share.
OWN_TITLE_SHARE = 0.5

_PAIRS_SCHEMA = pl.Schema(
    {
        "commitment_id": pl.String(),
        "record_id": pl.String(),
        "year": pl.Int16(),
        "allocated_usd": pl.Float64(),
        "rule": pl.String(),
        "certain": pl.Boolean(),
        "pass": pl.UInt32(),
        "link_method": pl.String(),
        "commitment_unit_id": pl.String(),
    }
)


def run_allocate(context: StageContext) -> StageOutput:
    """Allocate linked CRS disbursement to commitments and set statuses."""
    crs = trace_population(context.read("normalise", "crs"))
    crdf = counted_rows(context.read("normalise", "crdf"))
    units = context.read("link", "units")
    links = context.read("link", "links")

    pairs = allocate(
        units,
        crs=crs,
        links=links,
        dated=dated_donors(units),
        windowed=window_donors(units, crs=crs),
    )
    allocations = allocate_to_rows(pairs, crdf=crdf, units=units, crs=crs, links=links)
    basis = status_basis(
        crdf,
        units=units,
        crs=crs,
        links=links,
        pairs=pairs,
        id_practice=context.read("link", "id_practice"),
    )
    return {
        "allocations": allocations,
        "commitments": compute_status(commitments=basis, allocations=allocations),
        "cancellations": cancellations(crs),
        "years_since_commitment": years_since_commitment(
            allocations, crdf=crdf, last_year=context.config.years.crs_last_complete
        ),
    }


def counted_rows(crdf: pl.LazyFrame) -> pl.LazyFrame:
    """The CRDF commitments allocation follows: recipient-perspective rows, with an
    aggregate parent left out where its rebuilt children stand in for it."""
    recipient = crdf.filter(pl.col("perspective") == "recipient")
    parents = recipient.select(row_id="parent_row_id").drop_nulls().unique()
    return recipient.join(parents, on="row_id", how="anti")


def _year(date: str) -> pl.Expr:
    return pl.col(date).dt.year().cast(pl.Int16)


def _unit_records(units: pl.LazyFrame, *, crs: pl.LazyFrame) -> pl.LazyFrame:
    """Each unit's records with their own amounts and window dates."""
    return (
        units.select("unit_id", record_id="record_ids")
        .explode("record_id")
        .join(
            crs.select(
                "record_id",
                "commitment_usd",
                "disbursement_usd",
                start_year=_year("expected_start_date"),
                end_year=_year("completion_date"),
            ),
            on="record_id",
        )
    )


def _commitment_records(units: pl.LazyFrame, *, crs: pl.LazyFrame) -> pl.DataFrame:
    """Records with a positive commitment in commitment units, as (record_id,
    unit_id, face)."""
    return (
        _unit_records(units.filter(pl.col("commitment_usd") > 0), crs=crs)
        .filter(pl.col("commitment_usd") > 0)
        .select("record_id", "unit_id", face="commitment_usd")
        .collect()
    )


def _unit_windows(units: pl.LazyFrame, *, crs: pl.LazyFrame) -> pl.LazyFrame:
    """Each unit's earliest expected start year and latest completion year."""
    return (
        _unit_records(units, crs=crs)
        .group_by("unit_id")
        .agg(pl.col("start_year").min(), pl.col("end_year").max())
    )


def dated_donors(units: pl.LazyFrame) -> list[int]:
    """Donors whose disbursement-only units carry the original commitment's date.

    Measured on units with disbursement and no commitment in `DATED_YEARS`. The
    dollar share whose commitment year precedes the record year must reach
    `DATED_SHARE`.
    """
    disbursed = pl.col("disbursement_usd")
    early = (pl.col("commitment_year") < pl.col("year")).fill_null(False)
    donors = (
        units.filter(
            disbursed > 0,
            pl.col("commitment_usd") <= 0,
            pl.col("year").is_between(*DATED_YEARS),
        )
        .group_by("donor_code")
        .agg(share=ordered_sum(disbursed.filter(early)) / ordered_sum(disbursed))
        .filter(pl.col("share") >= DATED_SHARE)
        .collect()
    )
    return sorted(donors["donor_code"])


def window_donors(units: pl.LazyFrame, *, crs: pl.LazyFrame) -> list[int]:
    """Donors whose expected start to completion windows bracket later disbursement.

    Tested on (donor, crs id) pairs whose commitment units sit in one year of
    `WINDOW_TEST_YEARS` and carry a completion year. The dollar share of later
    same-id disbursement whose record year lies in [start year (or commitment year),
    completion year + `WINDOW_GRACE`] must reach `WINDOW_SHARE`, over at least
    `WINDOW_MIN_USD`.
    """
    ids = ["donor_code", "crs_id"]
    tested = (
        units.filter(pl.col("commitment_usd") > 0, pl.col("crs_id").is_not_null())
        .join(_unit_windows(units, crs=crs), on="unit_id")
        .group_by(ids)
        .agg(
            years=pl.col("placed_year").n_unique(),
            cyear=pl.col("placed_year").min(),
            start_year=pl.col("start_year").min(),
            end_year=pl.col("end_year").max(),
        )
        .filter(
            pl.col("years") == 1,
            pl.col("cyear").is_between(*WINDOW_TEST_YEARS),
            pl.col("end_year").is_not_null(),
        )
    )
    year = pl.col("year")
    in_window = (year >= pl.coalesce("start_year", "cyear")) & (
        year <= pl.col("end_year") + WINDOW_GRACE
    )
    usd = pl.col("disbursement_usd")
    donors = (
        units.filter(usd > 0)
        .group_by(*ids, "year")
        .agg(ordered_sum(usd))
        .join(tested, on=ids)
        .filter(year > pl.col("cyear"))
        .group_by("donor_code")
        .agg(tested=ordered_sum(usd), inside=ordered_sum(usd.filter(in_window)))
        .filter(
            pl.col("tested") >= WINDOW_MIN_USD,
            pl.col("inside") / pl.col("tested") >= WINDOW_SHARE,
        )
        .collect()
    )
    return sorted(donors["donor_code"])


def _trace_links(links: pl.LazyFrame, *, units: pl.LazyFrame) -> pl.DataFrame:
    """The `trace` links as (disbursement unit, commitment unit, group, method,
    certain, split), `split` ordering the parts of split groups (`_split_order`).

    Raises:
        AllocationInputError: when a link lacks a group, a group spans two
            methods, a disbursement unit carries two groups of one method, or a
            link names a unit that is not a positive disbursement (left) or
            commitment (right).
    """
    trace = (
        links.filter(pl.col("task") == "trace")
        .select(
            d_unit="left_id",
            c_unit="right_id",
            group_id="group_id",
            method="link_method",
            certain=pl.col("link_method").is_in(CERTAIN_METHODS)
            & ~pl.col("records_disagree").fill_null(value=False),
            split="split_path",
        )
        .collect()
    )
    trace = trace.with_columns(split=_split_order(trace["split"]))
    if trace["group_id"].null_count():
        raise AllocationInputError("trace links without a group_id")
    spans = trace.group_by("group_id").agg(pl.col("method").n_unique())
    if (bad := spans.filter(pl.col("method") > 1)).height:
        raise AllocationInputError(
            f"{bad.height} trace groups span more than one link method, such as "
            f"{bad['group_id'].sort().head(3).to_list()}"
        )
    split = trace.group_by("d_unit", "method").agg(pl.col("group_id").n_unique())
    if (bad := split.filter(pl.col("group_id") > 1)).height:
        raise AllocationInputError(
            f"{bad['d_unit'].n_unique()} disbursement units carry two groups of one "
            f"link method, such as {bad['d_unit'].unique().sort().head(3).to_list()}"
        )
    amounts = units.select("unit_id", "commitment_usd", "disbursement_usd").collect()
    disbursing = amounts.filter(pl.col("disbursement_usd") > 0).select(d_unit="unit_id")
    committing = amounts.filter(pl.col("commitment_usd") > 0).select(c_unit="unit_id")
    valid = trace.join(disbursing, on="d_unit").join(committing, on="c_unit")
    if valid.height != trace.height:
        raise AllocationInputError(
            f"{trace.height - valid.height} trace links name a unit that is not a "
            "positive disbursement (left) or commitment (right)"
        )
    return trace


def _split_order(paths: pl.Series) -> pl.Series:
    """Each split path's rank among `paths`, 0 for a group that is not split.

    Paths compare rank by rank, outermost first, with the ranks a shorter path
    lacks read as 0. So the parts of one split group take distinct ranks, a part
    split again ranks its parts inside its own place, and a group that is not split
    ranks with the first part of each split group.
    """
    depth = paths.list.len().max()
    if not isinstance(depth, int) or depth == 0:
        return pl.Series(paths.name, [0] * len(paths), dtype=pl.UInt32)
    padded = pl.struct(
        [
            pl.col(paths.name)
            .list.get(level, null_on_oob=True)
            .fill_null(0)
            .alias(str(level))
            for level in range(depth)
        ]
    )
    return paths.to_frame().select(padded.rank("dense") - 1).to_series()


def _trace_pairs(
    units: pl.LazyFrame,
    *,
    crs: pl.LazyFrame,
    links: pl.LazyFrame,
    dated: Collection[int],
    windowed: Collection[int],
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Every (commitment record, disbursement record) pair the links allow, with its
    pass, order and rule, and the commitment records by `c_idx`.

    Order, class and rule are facts of the unit pair, as a unit's records share
    their years and keys.

    Raises:
        AllocationInputError: when the links break the contract in the module
            docstring, including disbursement units of one group and pass that
            reach different commitments.
    """
    trace = _trace_links(links, units=units)
    disbursements = units.select(
        d_unit="unit_id",
        year="year",
        donor_code="donor_code",
        date_year="commitment_year",
        floor_year="floor_year",
    ).collect()
    commitment_units = (
        units.select(c_unit="unit_id", cyear="placed_year")
        .join(_unit_windows(units, crs=crs).rename({"unit_id": "c_unit"}), on="c_unit")
        .collect()
    )
    year, date_year, cyear = pl.col("year"), pl.col("date_year"), pl.col("cyear")
    floored = ~pl.col("method").is_in(NO_FLOOR_METHODS)
    floor = pl.when(floored).then(pl.col("floor_year"))
    dated_record = (
        pl.col("donor_code").is_in(list(dated)) & floored & (date_year <= year)
    ).fill_null(False)
    same_year = dated_record & (date_year == year)
    window = pl.col("donor_code").is_in(list(windowed)) & ~dated_record
    active = (
        (pl.coalesce("start_year", "cyear") <= year)
        & (pl.col("end_year") + WINDOW_GRACE >= year)
    ).fill_null(False)
    order = (
        cyear.cast(pl.Int32)
        + pl.when(window & ~active).then(_LATE).otherwise(0)
        + pl.when(same_year & (cyear != year)).then(_LATE).otherwise(0)
    )
    rank = pl.col("method").replace_strict(
        {method: rank for rank, method in enumerate(LINK_METHODS)},
        return_dtype=pl.Int16,
    )
    unit_pairs = (
        trace.join(disbursements, on="d_unit")
        .join(commitment_units, on="c_unit")
        .select(
            "group_id",
            "d_unit",
            "c_unit",
            "year",
            "certain",
            "method",
            "split",
            rank=rank,
            class_year=pl.coalesce(floor, pl.when(same_year).then(year)),
            order=order,
            rule=pl.when(same_year)
            .then(pl.lit("dated"))
            .when(window)
            .then(pl.lit("window"))
            .otherwise(pl.lit("fifo")),
        )
    )
    commitments = (
        _commitment_records(units, crs=crs).sort("record_id").with_row_index("c_idx")
    )
    linked = trace.lazy().select(unit_id="d_unit").unique()
    records = (
        _unit_records(units.join(linked, on="unit_id"), crs=crs)
        .filter(pl.col("disbursement_usd") > 0)
        .select(d_unit="unit_id", record_id="record_id", amount="disbursement_usd")
        .collect()
    )
    # A record that commits and disburses is certain of its own commitment, whatever
    # link joined its unit to itself.
    pairs = (
        unit_pairs.join(records, on="d_unit")
        .join(
            commitments.select("c_idx", c_unit="unit_id", c_record="record_id"),
            on="c_unit",
        )
        .with_columns(
            certain=pl.col("certain") | (pl.col("record_id") == pl.col("c_record"))
        )
        .drop("c_record")
    )
    own = _own_commitments(pairs, commitments=commitments, crs=crs)
    # Each own commitment is a group of its own, holding the records whose own it
    # is, so it shares no commitment with another group of its pass, and it runs
    # before the group's pass (`later`).
    pairs = pl.concat(
        [
            pairs.join(own, on=["group_id", "record_id", "c_idx"]).with_columns(
                group_id=pl.concat_str(
                    [pl.col("group_id"), pl.lit("own"), pl.col("c_idx")],
                    separator="|",
                ),
                later=pl.lit(value=False),
            ),
            pairs.with_columns(later=pl.lit(value=True)),
        ]
    )
    by = ["year", "rank", "class_year", "later"]
    key = ["split", "estimated", *by]
    # A group's pass is certain when all its pairs are, since the pass pools the
    # group's disbursement over all its commitments.
    pairs = pairs.with_columns(estimated=~pl.col("certain").all().over("group_id", *by))
    passes = (
        pairs.select(key).unique().sort(key, nulls_last=True).with_row_index("pass")
    )
    pairs = pairs.join(passes, on=key, nulls_equal=True)
    _check_pooling(
        pairs.filter("later").select("pass", "group_id", "d_unit", "c_unit").unique()
    )
    pairs = pairs.select(
        "pass",
        "group_id",
        "record_id",
        "year",
        "amount",
        "c_idx",
        "order",
        "rule",
        "certain",
        "method",
    )
    return pairs, commitments


def _own_commitments(
    pairs: pl.DataFrame, *, commitments: pl.DataFrame, crs: pl.LazyFrame
) -> pl.DataFrame:
    """Each disbursement record's own commitment record in a group, as (group_id,
    record_id, c_idx). That is the record itself where it also commits, else the one
    commitment record of the group whose real commitment date (`line_date`) it
    carries and whose title shares at least `OWN_TITLE_SHARE` of its words
    (`word_share`). A record with two such commitments in a group has none there."""
    reached = (
        pairs.select("group_id", "record_id", "c_idx")
        .unique()
        .join(commitments.select("c_idx", c_record="record_id"), on="c_idx")
    )
    same = reached.filter(pl.col("record_id") == pl.col("c_record"))
    rest = reached.join(same, on=["group_id", "record_id"], how="anti")
    ids = pl.concat([rest["record_id"], rest["c_record"]]).unique().implode()
    dates = (
        crs.filter(pl.col("record_id").is_in(ids))
        .select("record_id", date=line_date(pl.col("commitment_date")))
        .drop_nulls("date")
        .collect()
    )
    dated = rest.join(dates, on="record_id").join(
        dates.rename({"record_id": "c_record"}), on=["c_record", "date"]
    )
    columns = ["group_id", "record_id", "c_idx"]
    if dated.is_empty():
        return same.select(columns)
    # Each record read as a unit of its own, for its title words.
    involved = pl.concat([dated["record_id"], dated["c_record"]]).unique()
    words = title_words(
        involved.to_frame("unit_id").with_columns(record_ids=pl.concat_list("unit_id")),
        crs=crs,
    )
    agrees = word_share(pl.col("d_words"), pl.col("c_words")) >= OWN_TITLE_SHARE
    matched = (
        dated.join(
            words.select(record_id="unit_id", d_words="title_toks"), on="record_id"
        )
        .join(words.select(c_record="unit_id", c_words="title_toks"), on="c_record")
        .filter(agrees.fill_null(value=False))
        .filter(pl.len().over("group_id", "record_id") == 1)
    )
    return pl.concat([same.select(columns), matched.select(columns)])


def _check_pooling(unit_pairs: pl.DataFrame) -> None:
    """Refuse a group whose disbursement units of one pass reach different
    commitments, since the pass pools their disbursement over all of them.

    Raises:
        AllocationInputError: naming such groups.
    """
    group = ["pass", "group_id"]
    reached = unit_pairs.group_by(*group, "d_unit").agg(pl.col("c_unit").n_unique())
    pooled = unit_pairs.group_by(group).agg(pooled=pl.col("c_unit").n_unique())
    bad = reached.join(pooled, on=group).filter(pl.col("c_unit") != pl.col("pooled"))
    if bad.height:
        raise AllocationInputError(
            f"{bad['group_id'].n_unique()} trace groups pool disbursement units that "
            "reach different commitments in one pass, such as "
            f"{bad['group_id'].unique().sort().head(3).to_list()}"
        )


def allocate(
    units: pl.LazyFrame,
    *,
    crs: pl.LazyFrame,
    links: pl.LazyFrame,
    dated: Collection[int] = (),
    windowed: Collection[int] = (),
) -> pl.DataFrame:
    """Split each linked disbursement record over its groups' commitment records,
    each later group of its unit taking what the earlier ones left.

    Args:
        units: the link stage's `units`.
        crs: the CRS records the trace follows, with `expected_start_date` and
            `completion_date`.
        links: `links` rows, of which the `trace` task is read.
        dated: dated donors (see `dated_donors`).
        windowed: window donors (see `window_donors`).

    Returns:
        One row per pass and (commitment record, disbursement record) that received
        an amount: `commitment_id` (the commitment's CRS record_id), `record_id`,
        `year` (the record's), `allocated_usd`, `rule`, `certain`, whether a
        certain pair carried it, and the trace link that carried it: `pass`, in
        run order, its `link_method`, and `commitment_unit_id`, the commitment's
        unit, which the link joins to the record's unit. It also holds a
        `rule="excess"` row, not certain, with a null `commitment_id`, pass and
        link for each record its groups could not take in full.

    Raises:
        AllocationInputError: when the links break the contract in the module
            docstring, including two groups of one pass sharing a commitment.
    """
    pairs, commitments = _trace_pairs(
        units, crs=crs, links=links, dated=dated, windowed=windowed
    )
    if pairs.is_empty():
        return pl.DataFrame(schema=_PAIRS_SCHEMA)
    passes = ["pass", "group_id"]
    records = (
        pairs.select("record_id", "year", "amount")
        .unique()
        .sort("record_id")
        .with_row_index("r_idx")
    )
    # A record enters one pass per group of its unit, in pass order.
    entries = (
        pairs.select(*passes, "record_id", "rule")
        .unique()
        .join(records.select("record_id", "r_idx", "year"), on="record_id")
        .sort(*passes, "record_id")
    )
    candidates = pairs.select(*passes, "c_idx", "order").unique()
    if candidates.select("pass", "c_idx").is_duplicated().any():
        raise AllocationInputError("Two groups of one pass share a commitment")

    headroom = commitments["face"].to_numpy().astype(np.float64, copy=True)
    # What each record has left to give, its disbursement less earlier passes.
    left = records["amount"].to_numpy().astype(np.float64, copy=True)
    by_pass = candidates.partition_by("pass", as_dict=True)
    entries_by_pass = entries.partition_by("pass", as_dict=True)
    allocated = []
    for key in sorted(by_pass):
        entered = entries_by_pass[key]
        entered = entered.with_columns(
            left=pl.Series(left[entered["r_idx"].to_numpy()])
        ).filter(pl.col("left") > 0)
        if entered.is_empty():
            continue
        totals = entered.group_by("group_id", maintain_order=True).agg(
            total=ordered_sum(pl.col("left"))
        )
        part = by_pass[key].join(totals, on="group_id").sort(*passes, "order", "c_idx")
        filled = _fill(part, headroom[part["c_idx"].to_numpy()])
        headroom[part["c_idx"].to_numpy()] -= filled["fill"].to_numpy()

        capacity = filled.group_by("group_id", maintain_order=True).agg(
            pl.col("capacity").first()
        )
        given = entered.join(totals, on="group_id").join(capacity, on="group_id")
        share = pl.col("left") / pl.col("total")
        # A group that takes its whole total takes each record's whole remainder.
        taken = given.select(
            "r_idx",
            taken=pl.when(pl.col("total") <= pl.col("capacity"))
            .then(pl.col("left"))
            .otherwise(pl.col("capacity") * share),
        )
        index = taken["r_idx"].to_numpy()
        left[index] = np.maximum(left[index] - taken["taken"].to_numpy(), 0.0)
        allocated.append(
            filled.filter(pl.col("fill") > 0)
            .join(given, on="group_id")
            .select(
                "pass",
                "c_idx",
                "record_id",
                "year",
                allocated_usd=pl.col("fill") * share,
                rule="rule",
            )
        )

    commitment_ids = commitments.select(
        "c_idx", commitment_id="record_id", commitment_unit_id="unit_id"
    )
    carried = pairs.select(
        "pass", "record_id", "c_idx", "certain", link_method="method"
    )
    filled_pairs = (
        pl.concat(allocated)
        .join(carried, on=["pass", "record_id", "c_idx"], validate="1:1")
        .join(commitment_ids, on="c_idx")
        .drop("c_idx")
        .select(_PAIRS_SCHEMA.names())
    )
    excess = (
        records.select(
            pl.lit(None).alias("pass"),
            commitment_id=pl.lit(None),
            record_id="record_id",
            year="year",
            allocated_usd=pl.Series(left),
            rule=pl.lit("excess"),
            certain=pl.lit(value=False),
            link_method=pl.lit(None),
            commitment_unit_id=pl.lit(None),
        )
        .filter(pl.col("allocated_usd") > 0)
        .select(_PAIRS_SCHEMA.names())
    )
    return pl.concat([filled_pairs.cast(_PAIRS_SCHEMA), excess.cast(_PAIRS_SCHEMA)])


def _fill(candidates: pl.DataFrame, headroom: np.ndarray) -> pl.DataFrame:
    """One pass's fill per candidate. Each group's `total` fills its candidates in
    ascending `order`, pro rata to headroom within one order. `capacity` is the
    group's headroom, so its excess is whatever of `total` exceeds it."""
    group_order = ["group_id", "order"]
    available = pl.col("headroom").clip(lower_bound=0.0)
    block = pl.col("block")
    return (
        candidates.lazy()
        .with_columns(headroom=pl.Series(headroom))
        .with_columns(
            block=ordered_sum(available).over(group_order),
            capacity=ordered_sum(available).over("group_id"),
        )
        .with_columns(through=available.cum_sum().over("group_id", order_by="order"))
        .with_columns(before=pl.col("through").max().over(group_order) - block)
        .with_columns(block_fill=(pl.col("total") - pl.col("before")).clip(0.0, block))
        .select(
            "pass",
            "group_id",
            "c_idx",
            "capacity",
            fill=pl.when(available > 0)
            .then(pl.col("block_fill") * available / block)
            .otherwise(0.0),
        )
        .collect()
    )


def _commitment_groups(
    crdf: pl.LazyFrame,
    *,
    units: pl.LazyFrame,
    crs: pl.LazyFrame,
    links: pl.LazyFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """The `crdf_crs` groups as (row_id, group_id, weight, crs_face_usd),
    (group_id, spread) and (unit_id, group_id).

    A row's weight is its face value over the larger of its group's CRS commitment
    face (its commitment records' sum) and CRDF face, so a group's weights sum to at
    most 1. `crs_face_usd` is the row's weight times the CRS face, so a row's
    allocations over it are the group's allocation over the CRS face. The two faces
    differ when they convert one currency amount at different years' rates.

    A repeat row whose first row (`repeat_of`) sits in its group takes no weight
    and adds nothing to the CRDF face: `_to_first_rows` would hand its share to the
    first row, whose `crs_face_usd` must then cover the group's whole CRS face.

    Raises:
        AllocationInputError: when a link has no group, or a row or a unit sits in
            two groups.
    """
    joined = (
        links.filter(pl.col("task") == "crdf_crs")
        .select(row_id="left_id", unit_id="right_id", group_id="group_id")
        .join(crdf.select("row_id", "face_usd", "repeat_of"), on="row_id")
        .collect()
    )
    if joined["group_id"].null_count():
        raise AllocationInputError("crdf_crs links without a group_id")
    for side in ("row_id", "unit_id"):
        many = (
            joined.group_by(side)
            .agg(pl.col("group_id").n_unique())
            .filter(pl.col("group_id") > 1)
        )
        if many.height:
            raise AllocationInputError(
                f"{many.height} crdf_crs {side} values sit in more than one group"
            )
    unit_groups = joined.select("unit_id", "group_id").unique()
    rows = joined.select("row_id", "group_id", "face_usd", "repeat_of").unique()
    firsts = rows.select(
        repeat_of="row_id", group_id="group_id", first_in_group=pl.lit(True)
    )
    rows = (
        rows.join(firsts, on=["repeat_of", "group_id"], how="left")
        .with_columns(
            share_face=pl.when(pl.col("first_in_group"))
            .then(0.0)
            .otherwise(pl.col("face_usd").clip(lower_bound=0.0))
        )
        .sort("row_id")
    )
    share_face = pl.col("share_face")
    crs_face = (
        _commitment_records(units, crs=crs)
        .join(unit_groups, on="unit_id")
        .sort("record_id")
        .group_by("group_id", maintain_order=True)
        .agg(crs_face=ordered_sum(pl.col("face")))
    )
    denominator = pl.col("denominator")
    faces = (
        rows.group_by("group_id", maintain_order=True)
        .agg(crdf_face=ordered_sum(share_face))
        .join(crs_face, on="group_id")
        .with_columns(denominator=pl.max_horizontal("crs_face", "crdf_face"))
    )
    weights = rows.join(faces, on="group_id").select(
        "row_id",
        "group_id",
        weight=pl.when(denominator > 0).then(share_face / denominator).otherwise(0.0),
        crs_face_usd=pl.when(pl.col("crs_face") > 0)
        # Divided by the face ratio, so a row whose CRS face covers it keeps its
        # face to the bit.
        .then(pl.col("face_usd") / (denominator / pl.col("crs_face")))
        .otherwise(pl.col("face_usd")),
    )
    # The share of a group's allocation its rows take, exactly 1 when their face
    # covers the CRS face.
    spread = faces.select(
        "group_id",
        spread=pl.when(denominator > 0)
        .then(pl.col("crdf_face") / denominator)
        .otherwise(0.0),
    )
    return weights, spread, unit_groups


def allocate_to_rows(
    pairs: pl.DataFrame,
    *,
    crdf: pl.LazyFrame,
    units: pl.LazyFrame,
    crs: pl.LazyFrame,
    links: pl.LazyFrame,
) -> pl.DataFrame:
    """The `allocations` table, `allocate` output split over CRDF commitment rows.

    A pair's amount goes to the rows of its commitment's `crdf_crs` group by weight
    (see `_commitment_groups`). The rest, and every pair whose commitment has no
    CRDF row, goes to a null `row_id`, so each record's rows sum to what `allocate`
    gave it, excess included. A repeat row's share then moves to the row it repeats
    (see `_to_first_rows`). `allocated_low_usd` sums the amounts certain pairs
    carried to rows of groups whose `crdf_crs` links all take certain routes, and
    `allocated_high_usd` equals `allocated_usd`. `link_method` and
    `commitment_unit_id` name the trace link that carried the row's allocation:
    that of the first pass to carry any of it, and within that pass the link that
    carried the most over its commitment records, then the lowest commitment unit.
    """
    weights, spread, unit_groups = _commitment_groups(
        crdf, units=units, crs=crs, links=links
    )
    routes = (
        links.filter(pl.col("task") == "crdf_crs")
        .group_by("group_id")
        .agg(route_certain=pl.col("link_method").is_in(CERTAIN_ROUTES).all())
        .collect()
    )
    record_groups = (
        _commitment_records(units, crs=crs)
        .join(unit_groups, on="unit_id")
        .select(commitment_id="record_id", group_id="group_id")
    )
    grouped = pairs.join(record_groups, on="commitment_id", how="left")
    carrier = ("pass", "link_method", "commitment_unit_id")
    to_rows = (
        grouped.join(weights, on="group_id")
        .join(routes, on="group_id")
        .select(
            "row_id",
            "record_id",
            "year",
            "rule",
            "commitment_id",
            *carrier,
            certain=pl.col("certain") & pl.col("route_certain"),
            allocated_usd=pl.col("allocated_usd") * pl.col("weight"),
        )
    )
    to_rows = _to_first_rows(to_rows, crdf=crdf)
    rest = grouped.join(spread, on="group_id", how="left").select(
        pl.lit(None, dtype=pl.String).alias("row_id"),
        "record_id",
        "year",
        "rule",
        "commitment_id",
        *carrier,
        "certain",
        allocated_usd=pl.col("allocated_usd") * (1.0 - pl.col("spread").fill_null(0.0)),
    )
    amount = pl.col("allocated_usd")
    row = ["row_id", "record_id", "year", "rule"]
    given = pl.concat([to_rows, rest]).filter(amount > 0)
    carriers = (
        given.group_by(*row, *carrier)
        .agg(carried=ordered_sum(amount))
        .sort(
            *row,
            "pass",
            "carried",
            "commitment_unit_id",
            descending=[False, False, False, False, False, True, False],
            nulls_last=True,
        )
        .group_by(row, maintain_order=True)
        .agg(pl.col("link_method", "commitment_unit_id").first())
    )
    return (
        given.sort(*row, "commitment_id", "pass", "certain")
        .group_by(row, maintain_order=True)
        .agg(
            ordered_sum(amount),
            allocated_low_usd=ordered_sum(amount.filter(pl.col("certain"))),
        )
        .join(carriers, on=row, nulls_equal=True, maintain_order="left")
        .select(
            "row_id",
            "record_id",
            "year",
            "allocated_usd",
            "allocated_low_usd",
            amount.alias("allocated_high_usd"),
            "rule",
            pl.lit("crs").alias("disbursement_source"),
            "link_method",
            "commitment_unit_id",
        )
    )


def _to_first_rows(to_rows: pl.DataFrame, *, crdf: pl.LazyFrame) -> pl.DataFrame:
    """Move each repeat row's allocations to the row it repeats (`repeat_of`).

    A republished commitment is the same commitment, so its disbursement belongs to
    the first row. The first row keeps its own allocations and takes its repeats'
    up to its face, in record order; the rest goes to a null `row_id`, as the part
    of a commitment without a CRDF row does.
    """
    rows = crdf.select("row_id", "repeat_of", "face_usd").collect()
    amount = pl.col("allocated_usd")
    repeat = pl.col("repeat_of").is_not_null()
    headroom = (pl.col("face") - pl.col("before")).clip(lower_bound=0.0)
    ordered = (
        to_rows.join(rows.select("row_id", "repeat_of"), on="row_id", how="left")
        .with_columns(first=pl.coalesce("repeat_of", "row_id"))
        .join(
            rows.select(first="row_id", face=pl.col("face_usd").clip(lower_bound=0.0)),
            on="first",
            how="left",
        )
        .sort(
            "first",
            repeat,
            "row_id",
            "record_id",
            "year",
            "rule",
            "commitment_id",
            "pass",
            "certain",
        )
        .with_columns(before=amount.cum_sum().over("first") - amount)
        .with_columns(
            kept=pl.when(repeat)
            .then(pl.min_horizontal(amount, headroom.fill_null(0.0)))
            .otherwise(amount)
        )
    )
    columns = (
        "record_id",
        "year",
        "rule",
        "commitment_id",
        "pass",
        "link_method",
        "commitment_unit_id",
        "certain",
    )
    return pl.concat(
        [
            ordered.select(
                pl.col("first").alias("row_id"), *columns, amount=pl.col("kept")
            ),
            ordered.select(
                pl.lit(None, dtype=pl.String).alias("row_id"),
                *columns,
                amount=amount - pl.col("kept"),
            ),
        ]
    ).rename({"amount": "allocated_usd"})


def status_basis(
    crdf: pl.LazyFrame,
    *,
    units: pl.LazyFrame,
    crs: pl.LazyFrame,
    links: pl.LazyFrame,
    pairs: pl.DataFrame,
    id_practice: pl.LazyFrame,
) -> pl.DataFrame:
    """The `compute_status` input, each CRDF row with the facts allocation fixes.

    `traceable` is false when the row reaches no commitment unit, or its commitment
    unit's agency is classed `neither` in the period of the row's year.
    `id_disbursing` is true when allocation gave disbursement to a commitment of the
    row's (donor, crs id) placed in a year before the row's commitment.
    `crs_face_usd` is the row's share of its linked CRS commitment's face, which
    status reads its allocations against (see `_commitment_groups`). It is the
    row's own face outside every group.
    """
    weights, _, unit_groups = _commitment_groups(
        crdf, units=units, crs=crs, links=links
    )
    commitment_units = units.filter(pl.col("commitment_usd") > 0).collect()
    group_facts = (
        unit_groups.join(commitment_units, on="unit_id")
        .group_by("group_id")
        .agg(
            pl.col("donor_code").min(),
            pl.col("agency_code").min(),
            pl.col("placed_year").min(),
            pl.col("crs_id").drop_nulls().unique(),
        )
    )
    rows = (
        crdf.select("row_id", "year", "face_usd", "climate_usd")
        .collect()
        .join(
            weights.select("row_id", "group_id", "crs_face_usd"),
            on="row_id",
            how="left",
        )
        .join(group_facts, on="group_id", how="left")
        .with_columns(period=period_of(pl.col("year")))
    )
    neither = (
        id_practice.filter(pl.col("id_class") == "neither")
        .select("donor_code", "agency_code", "period", neither=pl.lit(True))
        .collect()
    )
    disbursing_ids = (
        pairs.filter(pl.col("commitment_id").is_not_null())
        .join(
            _commitment_records(units, crs=crs).select(
                "unit_id", commitment_id="record_id"
            ),
            on="commitment_id",
        )
        .join(commitment_units, on="unit_id")
        .filter(pl.col("crs_id").is_not_null())
        .group_by("donor_code", "crs_id")
        .agg(first_disbursing=pl.col("placed_year").min())
    )
    disbursing = (
        rows.select("row_id", "donor_code", "placed_year", "crs_id")
        .explode("crs_id")
        .join(disbursing_ids, on=["donor_code", "crs_id"])
        .filter(pl.col("first_disbursing") < pl.col("placed_year"))
        .select("row_id", id_disbursing=pl.lit(True))
        .unique()
    )
    return (
        rows.join(neither, on=["donor_code", "agency_code", "period"], how="left")
        .join(disbursing, on="row_id", how="left")
        .select(
            "row_id",
            "face_usd",
            "climate_usd",
            traceable=pl.col("placed_year").is_not_null() & pl.col("neither").is_null(),
            id_disbursing=pl.col("id_disbursing").fill_null(False),
            crs_face_usd=pl.col("crs_face_usd").fill_null(pl.col("face_usd")),
        )
    )


def compute_status(
    *,
    commitments: pl.LazyFrame | pl.DataFrame,
    allocations: pl.LazyFrame | pl.DataFrame,
    pending: pl.LazyFrame | pl.DataFrame | None = None,
) -> pl.LazyFrame:
    """Status, disbursed bounds and `project_disbursing` per commitment.

    Args:
        commitments: `row_id`, `face_usd`, `climate_usd`, `traceable`,
            `id_disbursing` and `crs_face_usd`, as `status_basis` builds them or
            the allocate stage's `commitments` table holds them.
        allocations: the `allocations` table.
        pending: the `row_id`s of commitments pending review.

    Returns:
        The `commitments` stage table. Status uses the linked CRS commitment's face. Its
        ratio is the group's allocation over the CRS face, which is the row's disbursed
        amount over `crs_face_usd`. `disbursed_usd` sums the row's allocations in the
        dollars disbursed, so it can sit below face for a disbursed commitment when
        exchange rates move between the commitment and disbursement years. The ratio
        reads `disbursed` at `DISBURSED_RATIO` or more, `partly_disbursed` above 0. With
        nothing allocated a row is `not_traceable` when not `traceable`, else
        `no_disbursement_record`. A pending row is `pending_review` whatever its ratio.
        `project_disbursing` marks a no-record row whose id is disbursing.
    """
    disbursed = (
        allocations.lazy()
        .filter(pl.col("row_id").is_not_null(), pl.col("rule") != "excess")
        .group_by("row_id")
        .agg(
            disbursed_usd=ordered_sum(pl.col("allocated_usd")),
            disbursed_low_usd=ordered_sum(pl.col("allocated_low_usd")),
            disbursed_high_usd=ordered_sum(pl.col("allocated_high_usd")),
        )
    )
    pending_ids = (
        pl.LazyFrame(schema={"row_id": pl.String})
        if pending is None
        else pending.lazy().select("row_id")
    )
    face = pl.col("crs_face_usd")
    ratio = pl.when(face > 0).then(pl.col("disbursed_usd") / face).otherwise(0.0)
    status = (
        pl.when(pl.col("pending"))
        .then(pl.lit("pending_review"))
        .when(ratio >= DISBURSED_RATIO)
        .then(pl.lit("disbursed"))
        .when(ratio > 0)
        .then(pl.lit("partly_disbursed"))
        .when(~pl.col("traceable"))
        .then(pl.lit("not_traceable"))
        .otherwise(pl.lit("no_disbursement_record"))
    )
    amounts = ("disbursed_usd", "disbursed_low_usd", "disbursed_high_usd")
    return (
        commitments.lazy()
        .select(
            "row_id",
            "face_usd",
            "climate_usd",
            "traceable",
            "id_disbursing",
            "crs_face_usd",
        )
        .join(disbursed, on="row_id", how="left")
        .join(
            pending_ids.unique().with_columns(pending=pl.lit(True)),
            on="row_id",
            how="left",
        )
        .with_columns(
            pl.col(*amounts).fill_null(0.0),
            pl.col("pending").fill_null(False),
        )
        .with_columns(status=status)
        .select(
            "row_id",
            "status",
            "face_usd",
            "climate_usd",
            *amounts,
            project_disbursing=(pl.col("status") == "no_disbursement_record")
            & pl.col("id_disbursing"),
            traceable=pl.col("traceable"),
            id_disbursing=pl.col("id_disbursing"),
            crs_face_usd=pl.col("crs_face_usd"),
        )
    )


def cancellations(crs: pl.LazyFrame) -> pl.LazyFrame:
    """Negative commitments by provider and year, as positive amounts."""
    return (
        crs.filter(pl.col("commitment_usd") < 0)
        .group_by(provider_code="donor_code", year="year")
        .agg(cancelled_usd=-ordered_sum(pl.col("commitment_usd")))
    )


def years_since_commitment(
    allocations: pl.DataFrame, *, crdf: pl.LazyFrame, last_year: int
) -> pl.LazyFrame:
    """Cumulative allocation per commitment row by years since its CRDF year.

    One row per commitment and year from 0 to `last_year` minus its year. A record
    from before the commitment's year counts at 0.
    """
    rows = crdf.select("row_id", "year")
    by_year = (
        allocations.filter(pl.col("row_id").is_not_null(), pl.col("rule") != "excess")
        .join(rows.collect(), on="row_id", suffix="_commitment")
        .sort("row_id", "record_id", "year", "rule")
        .group_by(
            "row_id",
            years_since=(pl.col("year") - pl.col("year_commitment"))
            .clip(lower_bound=0)
            .cast(pl.Int16),
            maintain_order=True,
        )
        .agg(ordered_sum(pl.col("allocated_usd")))
    )
    return (
        rows.filter(pl.col("year") <= last_year)
        .with_columns(
            years_since=pl.int_ranges(0, last_year - pl.col("year") + 1, dtype=pl.Int16)
        )
        .explode("years_since")
        .join(by_year.lazy(), on=["row_id", "years_since"], how="left")
        .sort("row_id", "years_since")
        .select(
            "row_id",
            "years_since",
            cumulative_disbursed_usd=pl.col("allocated_usd")
            .fill_null(0.0)
            .cum_sum()
            .over("row_id"),
        )
    )

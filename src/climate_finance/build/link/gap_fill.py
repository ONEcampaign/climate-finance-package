"""The fill stage: the GCF and GEF IATI routes, then gap fills from bank ledgers.

The GCF and GEF report their disbursements under ids the CRS does not carry, so
their CRDF rows that allocation leaves not traceable take their status from the
funds' own IATI files: the activity's disbursements (type 3) over its own commitment
(type 2), by year (`route_funds`). Allocation rows from this route carry rule and
`disbursement_source` `iati`.

The bank-ledger fill gives a commitment with no CRS disbursement on its ledger key
the ledger's disbursement on that key, in bank-years whose ledger agrees with the
CRS (`bank_year_overlap`, `fill_from_ledgers`). Its allocation rows carry rule and
`disbursement_source` `bank_ledger`. Neither route changes a CRS allocation, and
`compute_status` recomputes status over the CRS and filled allocations together.
Amounts are at face value, as allocation's are.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from climate_finance.build.allocate import compute_status, counted_rows
from climate_finance.build.errors import ConfigError
from climate_finance.build.fetch import IATI_DIR
from climate_finance.build.link.keys import normalise_pn
from climate_finance.build.link.ledger import LEDGER_PUBLISHERS, ledger_key
from climate_finance.build.readers.ledgers.iati import read_iati_ledger
from climate_finance.build.sums import ordered_sum

if TYPE_CHECKING:
    from climate_finance.build.stages import StageContext, StageOutput

logger = logging.getLogger(__name__)

# The funds whose CRDF rows take their status from their own IATI files, by DAC
# provider code, with the IATI publisher that holds the files.
FUND_PUBLISHERS: Mapping[int, str] = {1016: "gcf", 811: "gef"}
# A GCF funded activity or simplified approval number, which GCF project numbers
# and titles carry and GCF IATI identifiers end with.
GCF_PROJECT = r"\b((?:FP|SAP)\d{3})\b"
# A fund reports its disbursements when this share of its activities committed
# before the last complete CRS year show one. A matched row with none is then
# `no_disbursement_record`; otherwise it stays `not_traceable`.
MIN_REPORTING_SHARE = 0.5

# The overlap test, per bank and year, on the ledger keys with CRS disbursement that
# year: at least MIN_KEYS keys, the ledger total within TOTAL_TOLERANCE of the CRS
# total, and at least MIN_KEY_SHARE of keys within KEY_TOLERANCE of their CRS amount.
MIN_KEYS = 30
TOTAL_TOLERANCE = 0.05
KEY_TOLERANCE = 0.10
MIN_KEY_SHARE = 0.80
# A bank-year is blocked when more than this share of its CRS disbursement carries
# no usable key, since a fill there could repeat money the CRS records unkeyed.
MAX_UNKEYED_SHARE = 0.05
# Statuses a ledger fill may change.
GAP_STATUSES = ("no_disbursement_record", "not_traceable")
# AfDB and AfDF keys are 13-digit ids. One ending in five zeros is a truncated float.
_AFDB_KEY = r"^\d{13}$"
_AFDB_TRUNCATED = "00000"


def run_fill(context: StageContext) -> StageOutput:
    """Route GCF and GEF rows through IATI and fill gaps from bank ledgers.

    With `gap_fill` off, pass the allocate stage's allocations and commitments
    through unchanged.

    Raises:
        ConfigError: when `gap_fill` is on and the build config names no ledgers,
            or leaves the GCF or GEF out of `iati_publishers`.
    """
    allocations = context.read("allocate", "allocations")
    commitments = context.read("allocate", "commitments")
    config = context.config
    if not config.gap_fill:
        return {"allocations": allocations, "commitments": commitments}
    ledgers = config.sources.ledgers
    if ledgers is None:
        raise ConfigError(
            "The fill stage fills gaps from bank ledgers; name them under "
            "[sources.ledgers] in build.toml."
        )
    if missing := sorted(
        set(FUND_PUBLISHERS.values()) - set(config.sources.iati_publishers)
    ):
        raise ConfigError(
            f"The fill stage routes GCF and GEF rows through their IATI files; add "
            f"{', '.join(missing)} to sources.iati_publishers in build.toml."
        )
    crdf = counted_rows(context.read("normalise", "crdf")).collect()
    status = commitments.collect()
    units = context.read("link", "units")
    ledger = context.read("normalise", "ledger")

    fund_allocations, fund_basis = route_funds(
        crdf,
        commitments=status,
        activities=fund_activities(context.raw_dir / IATI_DIR),
        last_year=config.years.crs[1],
        committed_before=config.years.crs_last_complete,
    )
    overlap = bank_year_overlap(units, ledger=ledger, years=ledgers.years).collect()
    _log_failures(overlap)
    ledger_allocations = fill_from_ledgers(
        crdf.join(fund_basis, on="row_id", how="anti"),
        commitments=status,
        units=units,
        ledger=ledger,
        overlap=overlap,
        years=ledgers.years,
    )
    filled = pl.concat([allocations.collect(), fund_allocations, ledger_allocations])
    return {
        "allocations": filled,
        "commitments": compute_status(
            commitments=status.update(fund_basis, on="row_id"), allocations=filled
        ),
        "bank_year_overlap": overlap,
    }


def fund_activities(iati_dir: Path) -> pl.DataFrame:
    """The GCF's and GEF's IATI transactions as ledger rows, keyed by the id their
    CRDF rows carry: the funded activity number for the GCF, the GEF id for the GEF.

    The reader leaves out the GCF's co-financing commitments and reads the GEF's
    cumulative disbursements as rises of their running peak.
    """
    return pl.concat(
        read_iati_ledger(iati_dir / publisher, publisher=publisher)
        for publisher in FUND_PUBLISHERS.values()
    )


def route_funds(
    crdf: pl.DataFrame,
    *,
    commitments: pl.DataFrame,
    activities: pl.DataFrame,
    last_year: int,
    committed_before: int,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Allocations and status inputs for GCF and GEF rows from the funds' IATI files.

    A fund's row that allocation leaves `not_traceable`, and that repeats no earlier
    row, joins the fund's activities on its key (`fund_key`). It matches when the
    key's own commitments sum above zero. Its disbursed share by a year is the key's
    disbursements up to that year, to `last_year`, over that commitment, capped at 1
    and non-decreasing; each year's rise in the share, times the row's face value, is
    an allocation of rule `iati` with record id `iati:<publisher>:<key>:<year>`.
    Status reads that share against the row's face, so `crs_face_usd` becomes
    `face_usd`. A matched row with no disbursement is traceable when its fund
    reports disbursements (`MIN_REPORTING_SHARE`); an unmatched row is not.

    Args:
        crdf: Counted CRDF rows.
        commitments: The allocate stage's `commitments`.
        activities: `fund_activities` output.
        last_year: The last year of disbursement read.
        committed_before: Activities committed before this year measure whether a
            fund reports its disbursements.

    Returns:
        The `iati` allocation rows, and for each routed row the `traceable` and
        `crs_face_usd` that replace the allocate stage's.
    """
    publisher = pl.col("provider_code").replace_strict(
        FUND_PUBLISHERS, default=None, return_dtype=pl.String
    )
    amount = pl.col("amount_usd")
    own = (
        activities.filter(transaction_type="commitment")
        .group_by("publisher", "key")
        .agg(commitment_usd=ordered_sum(amount))
        .filter(pl.col("commitment_usd") > 0)
    )
    rows = (
        crdf.filter(pl.col("repeat_of").is_null())
        .with_columns(publisher=publisher)
        .filter(pl.col("publisher").is_not_null())
        .join(commitments.filter(status="not_traceable").select("row_id"), on="row_id")
        .with_columns(
            key=fund_key(pl.col("publisher"), pl.col("project_number"), pl.col("title"))
        )
        .join(own, on=["publisher", "key"], how="left")
        .select("row_id", "publisher", "key", "face_usd", "commitment_usd")
    )
    disbursed = (
        activities.filter(
            pl.col("transaction_type") == "disbursement",
            pl.col("year") <= last_year,
        )
        .group_by("publisher", "key", "year")
        .agg(disbursed_usd=ordered_sum(amount))
    )
    share = (
        (pl.col("disbursed_usd").cum_sum() / pl.col("commitment_usd"))
        .clip(upper_bound=1.0)
        .cum_max()
        .over("row_id", order_by="year")
    )
    previous = pl.col("share").shift(1, fill_value=0.0).over("row_id", order_by="year")
    rise = pl.col("share") - pl.col("previous")
    allocations = (
        rows.join(disbursed, on=["publisher", "key"])
        .with_columns(share=share)
        .with_columns(previous=previous)
        .filter(rise > 0)
        .select(
            "row_id",
            record_id=pl.format("iati:{}:{}:{}", "publisher", "key", "year"),
            year="year",
            allocated_usd=rise * pl.col("face_usd"),
            allocated_low_usd=rise * pl.col("face_usd"),
            allocated_high_usd=rise * pl.col("face_usd"),
            rule=pl.lit("iati"),
            disbursement_source=pl.lit("iati"),
        )
    )
    reports = _reporting_publishers(activities, committed_before=committed_before)
    basis = rows.select(
        "row_id",
        traceable=pl.col("commitment_usd").is_not_null()
        & pl.col("publisher").is_in(reports),
        crs_face_usd="face_usd",
    )
    return allocations, basis


def fund_key(publisher: pl.Expr, project_number: pl.Expr, title: pl.Expr) -> pl.Expr:
    """The IATI key a fund's CRDF row carries: for the GCF, the funded activity
    number in its project number or else its title; for the GEF, its normalised
    project number, the GEF id."""
    return (
        pl.when(publisher == "gcf")
        .then(
            pl.coalesce(
                project_number.str.extract(GCF_PROJECT),
                title.str.extract(GCF_PROJECT),
            )
        )
        .otherwise(normalise_pn(project_number))
    )


def _reporting_publishers(
    activities: pl.DataFrame, *, committed_before: int
) -> list[str]:
    """The publishers that show a disbursement on at least `MIN_REPORTING_SHARE` of
    their activities committed before `committed_before`."""
    disbursing = (
        activities.filter(transaction_type="disbursement")
        .select("activity_id", disburses=pl.lit(True))
        .unique()
    )
    shares = (
        activities.filter(
            pl.col("transaction_type") == "commitment",
            pl.col("year") < committed_before,
        )
        .select("publisher", "activity_id")
        .unique()
        .join(disbursing, on="activity_id", how="left")
        .group_by("publisher")
        .agg(share=pl.col("disburses").is_not_null().mean())
    )
    return shares.filter(pl.col("share") >= MIN_REPORTING_SHARE)["publisher"].to_list()


def keyed_units(units: pl.LazyFrame, *, ledger: pl.LazyFrame) -> pl.LazyFrame:
    """The units of the banks whose ledger `ledger` holds, with `publisher`, `key`
    and `usable`: whether the key can name a ledger activity."""
    key = pl.col("key")
    usable = (
        pl.when(pl.col("publisher") == "afdb")
        .then(key.str.contains(_AFDB_KEY) & ~key.str.ends_with(_AFDB_TRUNCATED))
        .otherwise(key.is_not_null())
        .fill_null(False)
    )
    return (
        units.with_columns(
            publisher=pl.col("donor_code").replace_strict(
                LEDGER_PUBLISHERS, default=None, return_dtype=pl.String
            )
        )
        .join(ledger.select("publisher").unique(), on="publisher", how="semi")
        .with_columns(key=ledger_key(pl.col("publisher"), pl.col("project_number")))
        .with_columns(usable=usable)
    )


def counted_disbursements(ledger: pl.LazyFrame) -> pl.LazyFrame:
    """The ledger's positive US-dollar disbursements.

    An ADB key with a leg in another currency is left out whole, since the CRS
    records those legs in dollars the ledger does not give (0.3% of ADB value).
    Amounts no rate could convert do not count.
    """
    disbursements = ledger.filter(transaction_type="disbursement")
    local_legs = (
        disbursements.filter(pl.col("publisher") == "asdb", pl.col("currency") != "USD")
        .select("publisher", "key")
        .unique()
    )
    return disbursements.filter(pl.col("amount_usd") > 0).join(
        local_legs, on=["publisher", "key"], how="anti"
    )


def ledger_flows(ledger: pl.LazyFrame, *, years: tuple[int, int]) -> pl.LazyFrame:
    """Each ledger key's `counted_disbursements` by year in `years`."""
    return (
        counted_disbursements(ledger)
        .filter(pl.col("year").is_between(*years))
        .group_by("publisher", "key", "year")
        .agg(ledger_usd=ordered_sum(pl.col("amount_usd")))
    )


def bank_year_overlap(
    units: pl.LazyFrame, *, ledger: pl.LazyFrame, years: tuple[int, int]
) -> pl.LazyFrame:
    """The overlap test of each ledger bank in each of `years`.

    The test population is the bank's keys with CRS disbursement in the year that
    show a `counted_disbursements` row in any year.
    `crs_usd` and `ledger_usd` are their totals, `keys` their count and
    `share_keys_within_10pct` the share whose ledger amount is within
    `KEY_TOLERANCE` of the CRS amount (or of one dollar). `unkeyed_share` is the
    share of the bank's CRS disbursement that year with no usable key. A bank-year
    passes with at least `MIN_KEYS` keys, totals within `TOTAL_TOLERANCE`,
    `MIN_KEY_SHARE` of keys within tolerance and at most `MAX_UNKEYED_SHARE`
    unkeyed.
    """
    first, last = years
    keyed = keyed_units(units, ledger=ledger).filter(
        pl.col("year").is_between(first, last)
    )
    disbursement = pl.col("disbursement_usd")
    banks = (
        keyed.select("donor_code")
        .unique()
        .join(
            pl.LazyFrame({"year": range(first, last + 1)}, schema={"year": pl.Int16}),
            how="cross",
        )
    )
    unkeyed = keyed.group_by("donor_code", "year").agg(
        unkeyed_usd=ordered_sum(disbursement.filter(~pl.col("usable"))),
        total_usd=ordered_sum(disbursement),
    )
    crs, published = pl.col("crs_usd"), pl.col("ledger_usd")
    pairs = (
        keyed.filter(pl.col("usable"))
        .group_by("donor_code", "publisher", "key", "year")
        .agg(crs_usd=ordered_sum(disbursement))
        .filter(crs > 0)
        .join(
            counted_disbursements(ledger).select("publisher", "key").unique(),
            on=["publisher", "key"],
            how="semi",
        )
        .join(
            ledger_flows(ledger, years=years),
            on=["publisher", "key", "year"],
            how="left",
        )
        .with_columns(published.fill_null(0.0))
        .group_by("donor_code", "year")
        .agg(
            keys=pl.len().cast(pl.Int32),
            ledger_usd=ordered_sum(published),
            crs_usd=ordered_sum(crs),
            share_keys_within_10pct=(
                (published - crs).abs()
                <= KEY_TOLERANCE * pl.max_horizontal(crs.abs(), pl.lit(1.0))
            ).mean(),
        )
    )
    return (
        banks.join(pairs, on=["donor_code", "year"], how="left")
        .join(unkeyed, on=["donor_code", "year"], how="left")
        .select(
            provider_code="donor_code",
            year="year",
            keys=pl.col("keys").fill_null(0),
            ledger_usd=published.fill_null(0.0),
            crs_usd=crs.fill_null(0.0),
            share_keys_within_10pct=pl.col("share_keys_within_10pct").fill_null(0.0),
            unkeyed_share=pl.when(pl.col("total_usd") > 0)
            .then(pl.col("unkeyed_usd") / pl.col("total_usd"))
            .otherwise(0.0),
        )
        .with_columns(
            passed=(pl.col("keys") >= MIN_KEYS)
            & ((published / crs - 1).abs() <= TOTAL_TOLERANCE).fill_null(False)
            & (pl.col("share_keys_within_10pct") >= MIN_KEY_SHARE)
            & (pl.col("unkeyed_share") <= MAX_UNKEYED_SHARE)
        )
    )


def fill_from_ledgers(
    crdf: pl.DataFrame,
    *,
    commitments: pl.DataFrame,
    units: pl.LazyFrame,
    ledger: pl.LazyFrame,
    overlap: pl.DataFrame,
    years: tuple[int, int],
) -> pl.DataFrame:
    """Bank-ledger allocations for commitments with no CRS disbursement on their key.

    A counted CRDF row of a ledger bank, repeating no earlier row, with a positive
    face value and status `no_disbursement_record` or `not_traceable` is a gap. Its
    key is the ledger key its normalised project number carries. A key's gaps share
    one window: `years` from the earliest gap's commitment year on.
    The key is filled when its gaps belong to one bank, it maps to one ledger
    activity, and the CRS records no disbursement on it in the window. The fill is
    the ledger's disbursement in the window's passing bank-years less what
    allocation gave the key's other rows, capped at the gaps' face value, shared
    over the gaps by face value and over the years by the ledger's amount in each.

    The ledger enters only through keys the CRS carries, so ledger activities with
    no CRS key (the IsDB's trade finance, AfDB trust funds run for others) never
    reach a fill.

    Returns:
        Allocation rows of rule and `disbursement_source` `bank_ledger`, with record
        id `ledger:<publisher>:<key>:<year>`.
    """
    publisher = pl.col("provider_code").replace_strict(
        LEDGER_PUBLISHERS, default=None, return_dtype=pl.String
    )
    keyed = (
        crdf.filter(pl.col("repeat_of").is_null())
        .with_columns(publisher=publisher)
        .filter(pl.col("publisher").is_not_null())
        .join(commitments.select("row_id", "status", "disbursed_usd"), on="row_id")
        .select(
            "row_id",
            "provider_code",
            "publisher",
            "year",
            "face_usd",
            "status",
            "disbursed_usd",
            key=ledger_key(pl.col("publisher"), normalise_pn(pl.col("project_number"))),
        )
        .filter(pl.col("key").is_not_null())
    )
    gap = pl.col("status").is_in(GAP_STATUSES) & (pl.col("face_usd") > 0)
    gaps = keyed.filter(gap)
    on = ["publisher", "key"]
    first, last = years
    windows = (
        gaps.group_by(on)
        .agg(
            pl.col("provider_code").first(),
            banks=pl.col("provider_code").n_unique(),
            start=pl.col("year").min(),
            face=ordered_sum(pl.col("face_usd")),
        )
        .filter(pl.col("banks") == 1)
        .drop("banks")
    )
    in_window = pl.col("year").is_between(pl.col("start"), last)
    crs_disbursing = (
        keyed_units(units, ledger=ledger)
        .filter(pl.col("disbursement_usd") != 0, pl.col("year") >= first)
        .select(*on, "year")
        .unique()
        .collect()
        .join(windows.select(*on, "start"), on=on)
        .filter(in_window)
        .select(on)
        .unique()
    )
    single = (
        ledger.select(*on, "activity_id")
        .unique()
        .group_by(on)
        .agg(activities=pl.len())
        .filter(pl.col("activities") == 1)
        .select(on)
        .collect()
    )
    eligible = windows.join(single, on=on).join(crs_disbursing, on=on, how="anti")
    flows = (
        ledger_flows(ledger, years=years)
        .collect()
        .join(eligible, on=on)
        .filter(in_window)
        .join(
            overlap.filter("passed").select("provider_code", "year"),
            on=["provider_code", "year"],
        )
    )
    taken = (
        keyed.filter(~gap)
        .group_by(on)
        .agg(taken_usd=ordered_sum(pl.col("disbursed_usd")))
    )
    keys = (
        flows.group_by(on)
        .agg(window_usd=ordered_sum(pl.col("ledger_usd")))
        .join(eligible.select(*on, "face"), on=on)
        .join(taken, on=on, how="left")
        .with_columns(
            filled_usd=pl.min_horizontal(
                (pl.col("window_usd") - pl.col("taken_usd").fill_null(0.0)).clip(
                    lower_bound=0.0
                ),
                pl.col("face"),
            )
        )
        .filter(pl.col("filled_usd") > 0)
    )
    amount = (
        pl.col("filled_usd")
        * (pl.col("face_usd") / pl.col("face"))
        * (pl.col("ledger_usd") / pl.col("window_usd"))
    )
    return (
        gaps.select("row_id", "face_usd", *on)
        .join(keys.select(*on, "face", "window_usd", "filled_usd"), on=on)
        .join(flows.select(*on, "year", "ledger_usd"), on=on)
        .select(
            "row_id",
            record_id=pl.format("ledger:{}:{}:{}", "publisher", "key", "year"),
            year="year",
            allocated_usd=amount,
            allocated_low_usd=amount,
            allocated_high_usd=amount,
            rule=pl.lit("bank_ledger"),
            disbursement_source=pl.lit("bank_ledger"),
        )
    )


def _log_failures(overlap: pl.DataFrame) -> None:
    for row in overlap.filter(~pl.col("passed")).iter_rows(named=True):
        logger.info(
            "Bank-year %d %d fails the overlap test, so no ledger fill: %d keys, "
            "ledger USD %.0f against CRS USD %.0f, %.0f%% of keys within 10%%, "
            "%.1f%% of CRS disbursement unkeyed",
            row["provider_code"],
            row["year"],
            row["keys"],
            row["ledger_usd"],
            row["crs_usd"],
            100 * row["share_keys_within_10pct"],
            100 * row["unkeyed_share"],
        )

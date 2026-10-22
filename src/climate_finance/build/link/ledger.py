"""The ledger check: CRS disbursement against the banks' own ledgers.

Each bank in `LEDGER_PUBLISHERS` publishes its disbursements keyed by an id its CRS
project numbers carry, which `ledger_key` cuts from the trace unit's project number.
For every ledger key and calendar year, the check compares the CRS disbursement
recorded under the key, summed over the trace units that carry it, with the ledger's
disbursement transactions for the key in US dollars. They agree when the CRS is
within `TOLERANCE` of the ledger. The CRS stays the disbursement record.

The CRS records are compared, not the dollars allocation gives each link. The World
Bank's crs ids name projects, so the key tiers link at project level and allocation
spreads a project's disbursement over its loans: allocated dollars agree with the
ledger for 67% to 94% of World Bank loan-years in 2021 and 2022, the CRS records for
99.7% to 100% of those the ledger lists.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

import polars as pl

from climate_finance.build.errors import ConfigError
from climate_finance.build.sums import ordered_sum

if TYPE_CHECKING:
    from climate_finance.build.stages import StageContext

# A CRS amount agrees with the ledger within this share of the ledger amount, or of
# one dollar where the ledger amount is smaller.
TOLERANCE = 0.05
# The ledger that checks each provider's CRS rows, by DAC provider code. The AfDB and
# the African Development Fund publish one IATI ledger. The GEF is left out: the CRS
# records its disbursements under its agencies' project ids, so no GEF key carries
# CRS disbursement to compare.
LEDGER_PUBLISHERS: Mapping[int, str] = {
    901: "ibrd",
    905: "ida",
    913: "afdb",
    914: "afdb",
    915: "asdb",
    909: "iadb",
    976: "isdb",
    990: "ebrd",
    910: "bcie",
    1016: "gcf",
}


def ledger_key(publisher: pl.Expr, project_number: pl.Expr) -> pl.Expr:
    """The ledger key a normalised CRS project number carries for `publisher`.

    World Bank numbers carry the loan or credit number after the project id
    (`P163452.IDA61960.CRS3`), the ADB's the loan, grant or TA number before the
    first dash, the IDB's the operation number before `&`, and the GCF's a funded
    activity number (`FP001`, `SAP001`). The other banks' numbers are the key.
    """
    return (
        pl.when(publisher.is_in(["ibrd", "ida"]))
        .then(project_number.str.split(".").list.get(1, null_on_oob=True))
        .when(publisher == "asdb")
        .then(project_number.str.split("-").list.first())
        .when(publisher == "iadb")
        .then(project_number.str.split("&").list.first())
        .when(publisher == "gcf")
        .then(project_number.str.extract(r"\b((?:FP|SAP)\d{3})\b"))
        .otherwise(project_number)
    )


def run_ledger_check(context: StageContext) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """`check_ledgers` on the link stage's links and units and the normalised ledger.

    Raises:
        ConfigError: when the build config names no ledger sources.
        StageInputMissingError: when normalise wrote no `ledger` table.
    """
    ledgers = context.config.sources.ledgers
    if ledgers is None:
        raise ConfigError(
            "The review stage checks links against bank ledgers; name them under "
            "[sources.ledgers] in build.toml."
        )
    return check_ledgers(
        context.read("link", "links"),
        units=context.read("link", "units"),
        ledger=context.read("normalise", "ledger"),
        years=ledgers.years,
    )


def check_ledgers(
    links: pl.LazyFrame,
    *,
    units: pl.LazyFrame,
    ledger: pl.LazyFrame,
    years: tuple[int, int],
) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """Set `ledger_check` on the links of the banks whose ledger `ledger` holds, and
    label the confirmed ones.

    A `trace` link whose disbursement falls in `years` takes the result for its
    commitment's ledger key in the disbursement's year: `confirmed` when the CRS and
    the ledger agree, `contradicted` when they do not, and `none` when the ledger
    does not list the key or holds an amount with no US dollar value that year, or
    when the disbursement unit carries another key or none, so the ledger cannot
    speak to the pair. A key the ledger lists reads as zero in a year it shows no
    disbursement.

    A `crdf_crs` link to a commitment placed in `years` whose key shows no
    disbursement in either source from that year to the last of `years` is
    `confirmed_undisbursed`. Other links keep their `ledger_check`.

    Args:
        links: `links` rows.
        units: the link stage's `units`.
        ledger: the `ledger` table, of which the `disbursement` rows count.
        years: the first and last calendar years checked.

    Returns:
        The links, and a `pair_labels` row with `is_match` true and source `ledger`
        for each confirmed `trace` pair.
    """
    first, last = years
    keyed = (
        units.with_columns(
            publisher=pl.col("donor_code").replace_strict(
                LEDGER_PUBLISHERS, default=None, return_dtype=pl.String
            )
        )
        .join(ledger.select("publisher").unique(), on="publisher", how="semi")
        .with_columns(key=ledger_key(pl.col("publisher"), pl.col("project_number")))
    )
    key_years = _key_years(keyed, ledger=ledger, years=years)
    disbursements = keyed.select(left_id="unit_id", left_key="key", year="year")
    commitments = keyed.select(
        right_id="unit_id", publisher="publisher", key="key", placed_year="placed_year"
    )
    columns = links.collect_schema().names()

    same_key = pl.col("left_key") == pl.col("key")
    trace = (
        links.filter(task="trace")
        .join(disbursements, on="left_id", how="left")
        .join(commitments, on="right_id", how="left")
        .join(key_years, on=["publisher", "key", "year"], how="left")
        .with_columns(
            ledger_check=pl.when(
                pl.col("publisher").is_null() | ~pl.col("year").is_between(first, last)
            )
            .then(pl.col("ledger_check"))
            .when(same_key & pl.col("check").is_not_null())
            .then(pl.col("check"))
            .otherwise(pl.lit("none"))
        )
        .select(columns)
    )
    undisbursed = (
        commitments.join(_last_active(key_years), on=["publisher", "key"])
        .filter(
            pl.col("placed_year").is_between(first, last),
            pl.col("last_active").is_null()
            | (pl.col("last_active") < pl.col("placed_year")),
        )
        .select("right_id", undisbursed=pl.lit(True))
    )
    crdf_crs = (
        links.filter(task="crdf_crs")
        .join(undisbursed, on="right_id", how="left")
        .with_columns(
            ledger_check=pl.when(pl.col("undisbursed"))
            .then(pl.lit("confirmed_undisbursed"))
            .otherwise(pl.col("ledger_check"))
        )
        .select(columns)
    )
    rest = links.filter(~pl.col("task").is_in(["trace", "crdf_crs"]))
    labels = (
        trace.filter(ledger_check="confirmed")
        .select(
            "task",
            "left_id",
            "right_id",
            is_match=pl.lit(True),
            pair_label_source=pl.lit("ledger"),
        )
        .unique()
    )
    return pl.concat([trace, crdf_crs, rest]), labels


def _key_years(
    keyed: pl.LazyFrame, *, ledger: pl.LazyFrame, years: tuple[int, int]
) -> pl.LazyFrame:
    """Each key the ledger lists, in each of `years`, with its CRS and ledger
    amounts and `check`: `confirmed`, `contradicted`, or `none` when a ledger amount
    has no US dollar value."""
    first, last = years
    counted = ledger.filter(
        pl.col("transaction_type") == "disbursement", pl.col("year").is_not_null()
    )
    grid = (
        counted.select("publisher", "key")
        .unique()
        .join(
            pl.LazyFrame({"year": range(first, last + 1)}, schema={"year": pl.Int16}),
            how="cross",
        )
    )
    on = ["publisher", "key", "year"]
    crs = keyed.group_by(on).agg(crs_usd=ordered_sum(pl.col("disbursement_usd")))
    published = counted.group_by(on).agg(
        ledger_usd=ordered_sum(pl.col("amount_usd")),
        converted=pl.col("amount_usd").is_not_null().all(),
    )
    crs_usd, ledger_usd = pl.col("crs_usd"), pl.col("ledger_usd")
    agrees = (crs_usd - ledger_usd).abs() <= TOLERANCE * pl.max_horizontal(
        ledger_usd.abs(), pl.lit(1.0)
    )
    return (
        grid.join(crs, on=on, how="left")
        .join(published, on=on, how="left")
        .with_columns(
            crs_usd.fill_null(0.0),
            ledger_usd.fill_null(0.0),
            pl.col("converted").fill_null(True),
        )
        .with_columns(
            check=pl.when(~pl.col("converted"))
            .then(pl.lit("none"))
            .when(agrees)
            .then(pl.lit("confirmed"))
            .otherwise(pl.lit("contradicted"))
        )
    )


def _last_active(key_years: pl.LazyFrame) -> pl.LazyFrame:
    """Per key, the last year with disbursement in either source, or with no
    comparison; null when every year shows none in both."""
    active = (pl.col("crs_usd") != 0) | (pl.col("check") != "confirmed")
    return key_years.group_by("publisher", "key").agg(
        last_active=pl.col("year").filter(active).max()
    )

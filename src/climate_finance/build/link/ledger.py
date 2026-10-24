"""The ledger check: CRS disbursement against the banks' own ledgers.

Each bank in `LEDGER_PUBLISHERS` publishes its disbursements keyed by an id its CRS
project numbers carry, which `ledger_key` cuts from the trace unit's project number.
For every ledger key and calendar year, the check compares the CRS disbursement
recorded under the key, summed over the trace units that carry it, with the ledger's
disbursement transactions for the key in US dollars. They agree when the CRS is
within `TOLERANCE` of the ledger. The CRS is the disbursement record.

The check compares CRS records, which agree with the ledger for 99.7% to 100% of the
World Bank loan-years it lists in 2021 and 2022. The dollars allocation gives each
link agree for 96% to 99.7%, since the `wb_loan` key tier keeps a loan's disbursement
on its own loan but excess and cross-year timing differ from the ledger.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

import polars as pl

from climate_finance.build.errors import ConfigError
from climate_finance.build.fetch import ledger_years
from climate_finance.build.link.keys import partner_recipient
from climate_finance.build.sums import ordered_sum

if TYPE_CHECKING:
    from climate_finance.build.stages import StageContext

# A CRS amount agrees with the ledger within this share of the ledger amount, or of
# one dollar where the ledger amount is smaller.
TOLERANCE = 0.05
# The ledger that checks each provider's CRS rows, by DAC provider code. The AfDB and
# the African Development Fund publish one IATI ledger. The GEF is left out because the
# CRS records its disbursements under its agencies' project ids, so no GEF key carries
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
# The ledgers whose silence on a key counts as zero disbursement: they can confirm a
# commitment undisbursed, and a key they list with no disbursement transaction is
# checked. These three match the CRS for 99.7% to 100% of loan-years. The others can
# list an activity's commitment and leave out its disbursements, as the AfDB's IATI
# files do for some loans, so their silence on a key proves nothing.
UNDISBURSED_PUBLISHERS = ("ibrd", "ida", "iadb")
# A GCF funded activity or simplified approval number, which GCF project numbers
# and titles carry and GCF IATI identifiers end with.
GCF_PROJECT = r"\b((?:FP|SAP)\d{3})\b"


def ledger_key(publisher: pl.Expr, project_number: pl.Expr) -> pl.Expr:
    """The ledger key a normalised CRS project number carries for `publisher`.

    World Bank numbers carry the loan or credit number after the project id
    (`P163452.IDA61960.CRS3`), the ADB's the loan, grant or TA number before the
    first dash, the IDB's the operation number before `&`, and the GCF's a funded
    activity number (`FP001`, `SAP001`) or a readiness grant's agreement reference
    (`MLI-RS-001`), the key of the GCF readiness list. The other banks' numbers are
    the key.
    """
    return (
        pl.when(publisher.is_in(["ibrd", "ida"]))
        .then(project_number.str.split(".").list.get(1, null_on_oob=True))
        .when(publisher == "asdb")
        .then(project_number.str.split("-").list.first())
        .when(publisher == "iadb")
        .then(project_number.str.split("&").list.first())
        .when(publisher == "gcf")
        .then(
            pl.coalesce(
                project_number.str.extract(GCF_PROJECT),
                project_number.str.extract(r"\b([A-Z]{3}-RS-\d{3})\b"),
            )
        )
        .otherwise(project_number)
    )


def publisher_of(code: pl.Expr, publishers: Mapping[int, str]) -> pl.Expr:
    """The publisher `publishers` maps a DAC provider `code` to, else null."""
    return code.replace_strict(publishers, default=None, return_dtype=pl.String)


def ledger_units(units: pl.LazyFrame, *, ledger: pl.LazyFrame) -> pl.LazyFrame:
    """The units of the banks whose ledger `ledger` holds, with `publisher` and the
    ledger `key` their project number carries (`ledger_key`)."""
    return (
        units.with_columns(
            publisher=publisher_of(pl.col("donor_code"), LEDGER_PUBLISHERS)
        )
        .join(ledger.select("publisher").unique(), on="publisher", how="semi")
        .with_columns(key=ledger_key(pl.col("publisher"), pl.col("project_number")))
    )


def year_range(years: tuple[int, int]) -> pl.LazyFrame:
    """One `year` row for each year from the first to the last of `years`."""
    first, last = years
    return pl.LazyFrame({"year": range(first, last + 1)}, schema={"year": pl.Int16})


def within(value: pl.Expr, base: pl.Expr, *, tolerance: float) -> pl.Expr:
    """Whether `value` lies within `tolerance` of `base`, or of one dollar where
    `base` is smaller, of `base`."""
    return (value - base).abs() <= tolerance * pl.max_horizontal(
        base.abs(), pl.lit(1.0)
    )


def run_ledger_check(context: StageContext) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """`check_ledgers` on the link stage's links and units and the normalised ledger.

    Raises:
        ConfigError: when the build config names no ledger sources.
        StageInputMissingError: when normalise wrote no `ledger` table.
    """
    if context.config.sources.ledgers is None:
        raise ConfigError(
            "The review stage checks links against bank ledgers; name them under "
            "[sources.ledgers] in build.toml."
        )
    return check_ledgers(
        context.read("link", "links"),
        units=context.read("link", "units"),
        ledger=context.read("normalise", "ledger"),
        years=ledger_years(context),
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
    speak to the pair. A key with a disbursement transaction reads as zero in a year
    it shows none. So does a key an `UNDISBURSED_PUBLISHERS` ledger lists under any
    transaction type.

    A `crdf_crs` link to a commitment of an `UNDISBURSED_PUBLISHERS` bank placed in
    `years` whose key shows no disbursement in either source from that year to the
    last of `years` is `confirmed_undisbursed`. Other links keep their
    `ledger_check`.

    Args:
        links: `links` rows.
        units: the link stage's `units`.
        ledger: the `ledger` table, of which the `disbursement` rows count, and
            every row of an `UNDISBURSED_PUBLISHERS` ledger lists its key.
        years: the first and last calendar years checked.

    Returns:
        The links, and a `pair_labels` row with `is_match` true and source `ledger`
        for each confirmed `trace` pair whose disbursement goes to the commitment's
        recipient or to a country of its region (`keys.partner_recipient`). The
        ledger key alone does not show that two units share a recipient, since a
        bank can file one operation number for lines to several countries.
    """
    first, last = years
    keyed = ledger_units(units, ledger=ledger)
    key_years = _key_years(keyed, ledger=ledger, years=years)
    disbursements = keyed.select(
        left_id="unit_id",
        left_key="key",
        year="year",
        left_recipient="recipient_code",
    )
    # Only a key the check lists can show no disbursement.
    undisbursed = (
        pl.col("listed").fill_null(value=False)
        & pl.col("publisher").is_in(UNDISBURSED_PUBLISHERS)
        & pl.col("placed_year").is_between(first, last)
        & (
            pl.col("last_active").is_null()
            | (pl.col("last_active") < pl.col("placed_year"))
        )
    )
    commitments = keyed.join(
        _last_active(key_years).with_columns(listed=pl.lit(value=True)),
        on=["publisher", "key"],
        how="left",
    ).select(
        right_id="unit_id",
        publisher="publisher",
        key="key",
        recipient="recipient_code",
        undisbursed=undisbursed,
    )
    columns = links.collect_schema().names()

    trace = pl.col("task") == "trace"
    same_key = pl.col("left_key") == pl.col("key")
    trace_check = (
        pl.when(pl.col("publisher").is_null() | ~pl.col("year").is_between(first, last))
        .then(pl.col("ledger_check"))
        .when(same_key & pl.col("check").is_not_null())
        .then(pl.col("check"))
        .otherwise(pl.lit("none"))
    )
    # A `crdf_crs` link's left side is a CRDF row, so no disbursement unit joins it.
    checked = (
        links.join(disbursements, on="left_id", how="left", maintain_order="left")
        .join(commitments, on="right_id", how="left", maintain_order="left")
        .join(
            key_years,
            on=["publisher", "key", "year"],
            how="left",
            maintain_order="left",
        )
        .with_columns(
            ledger_check=pl.when(trace)
            .then(trace_check)
            .when((pl.col("task") == "crdf_crs") & pl.col("undisbursed"))
            .then(pl.lit("confirmed_undisbursed"))
            .otherwise(pl.col("ledger_check"))
        )
    )
    labels = (
        checked.filter(
            trace,
            pl.col("ledger_check") == "confirmed",
            partner_recipient(
                commitment=pl.col("recipient"), disbursement=pl.col("left_recipient")
            ),
        )
        .select(
            "task",
            "left_id",
            "right_id",
            is_match=pl.lit(True),
            pair_label_source=pl.lit("ledger"),
        )
        .unique()
    )
    return checked.select(columns), labels


def _key_years(
    keyed: pl.LazyFrame, *, ledger: pl.LazyFrame, years: tuple[int, int]
) -> pl.LazyFrame:
    """Each key the ledger checks, in each of `years`, with its CRS amount, its
    ledger disbursement and `check`: `confirmed`, `contradicted`, or `none` when a
    ledger amount has no US dollar value. The ledger checks a key with a
    disbursement transaction, and every key of an `UNDISBURSED_PUBLISHERS` ledger."""
    disbursement = pl.col("transaction_type") == "disbursement"
    counted = ledger.filter(disbursement, pl.col("year").is_not_null())
    grid = (
        ledger.filter(disbursement | pl.col("publisher").is_in(UNDISBURSED_PUBLISHERS))
        .select("publisher", "key")
        .unique()
        .join(year_range(years), how="cross")
    )
    on = ["publisher", "key", "year"]
    crs = keyed.group_by(on).agg(crs_usd=ordered_sum(pl.col("disbursement_usd")))
    published = counted.group_by(on).agg(
        ledger_usd=ordered_sum(pl.col("amount_usd")),
        converted=pl.col("amount_usd").is_not_null().all(),
    )
    crs_usd, ledger_usd = pl.col("crs_usd"), pl.col("ledger_usd")
    agrees = within(crs_usd, ledger_usd, tolerance=TOLERANCE)
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

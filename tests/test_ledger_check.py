"""The ledger check on hand-built units, links and ledgers with invented values."""

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from climate_finance import schema
from climate_finance.build.config import Ledgers, load_config
from climate_finance.build.link.ledger import check_ledgers
from climate_finance.build.link.review import run_review
from climate_finance.build.stages import StageContext, table_path

IDA, IBRD, AFDB, GERMANY = 905, 901, 913, 5
YEARS = (2021, 2022)

# unit_id: (donor_code, project_number, year, placed_year, disbursement_usd)
UNITS = {
    # One IDA credit split over two CRS rows: 60 + 40 in 2021, 50 in 2022.
    "c_ida1": (IDA, "P000001.IDA10010.CRS1", 2020, 2020, 0.0),
    "d_ida1a": (IDA, "P000001.IDA10010.CRS1", 2021, 2021, 60.0),
    "d_ida1b": (IDA, "P000001.IDA10010.CRS2", 2021, 2021, 40.0),
    "d_ida1c": (IDA, "P000001.IDA10010.CRS1", 2022, 2022, 50.0),
    "d_ida1_old": (IDA, "P000001.IDA10010.CRS1", 2019, 2019, 30.0),
    # Another credit of the same project, and one the ledger does not list.
    "d_ida2": (IDA, "P000001.IDA20020.CRS1", 2021, 2021, 25.0),
    "c_ida3": (IDA, "P000003.IDA30030.CRS1", 2018, 2018, 0.0),
    "d_ida3": (IDA, "P000003.IDA30030.CRS1", 2021, 2021, 70.0),
    # IBRD loans committed in 2021: one never disburses, one does in the ledger only.
    "c_ibrd4": (IBRD, "P000004.IBRD40040.CRS1", 2021, 2021, 0.0),
    "c_ibrd5": (IBRD, "P000005.IBRD50050.CRS1", 2021, 2021, 0.0),
    # An AfDB loan whose ledger is in XDR, which has no DAC rate.
    "c_afdb6": (AFDB, "2100150000006", 2020, 2020, 0.0),
    "d_afdb6": (AFDB, "2100150000006", 2021, 2021, 15.0),
    # A provider with no ledger.
    "c_deu7": (GERMANY, "DE7", 2020, 2020, 0.0),
    "d_deu7": (GERMANY, "DE7", 2021, 2021, 9.0),
}

# (task, left_id, right_id)
LINKS = [
    ("trace", "d_ida1a", "c_ida1"),
    ("trace", "d_ida1b", "c_ida1"),
    ("trace", "d_ida1c", "c_ida1"),
    ("trace", "d_ida1_old", "c_ida1"),
    ("trace", "d_ida2", "c_ida1"),
    ("trace", "d_ida3", "c_ida3"),
    ("trace", "d_afdb6", "c_afdb6"),
    ("trace", "d_deu7", "c_deu7"),
    ("crdf_crs", "row_ibrd4", "c_ibrd4"),
    ("crdf_crs", "row_ibrd5", "c_ibrd5"),
    ("crdf_crs", "row_ida1", "c_ida1"),
    ("crdf_crs", "row_deu7", "c_deu7"),
]

# (publisher, key, year, currency, amount, amount_usd)
LEDGER = [
    ("ida", "IDA10010", 2021, "USD", 104.0, 104.0),
    ("ida", "IDA10010", 2022, "USD", 53.0, 53.0),
    ("ida", "IDA10010", 2022, "USD", 1.0, 1.0),
    ("ida", "IDA20020", 2021, "USD", 25.0, 25.0),
    ("ibrd", "IBRD40040", 2021, "USD", 0.0, 0.0),
    ("ibrd", "IBRD40040", 2022, "USD", 0.0, 0.0),
    ("ibrd", "IBRD50050", 2022, "USD", 10.0, 10.0),
    ("afdb", "2100150000006", 2021, "XDR", 11.0, None),
]


def _units() -> pl.LazyFrame:
    return pl.LazyFrame(
        [(unit_id, *row) for unit_id, row in UNITS.items()],
        schema={
            "unit_id": pl.String,
            "donor_code": pl.Int32,
            "project_number": pl.String,
            "year": pl.Int16,
            "placed_year": pl.Int16,
            "disbursement_usd": pl.Float64,
        },
        orient="row",
    )


def _links(ledger_check: str | None = None) -> pl.LazyFrame:
    columns = schema.LINKS.columns
    frame = pl.DataFrame(
        LINKS, schema=["task", "left_id", "right_id"], orient="row"
    ).with_columns(
        group_id=pl.lit(None),
        probability=pl.lit(1.0),
        link_method=pl.when(pl.col("task") == "trace")
        .then(pl.lit("crs_id"))
        .otherwise(pl.lit("within_year")),
        ledger_check=pl.lit(ledger_check),
        review=pl.lit(False),
    )
    return (
        frame.with_columns(
            pl.lit(None).alias(name) for name in columns if name not in frame.columns
        )
        .select(pl.col(name).cast(column.dtype) for name, column in columns.items())
        .lazy()
    )


def _ledger() -> pl.LazyFrame:
    rows = [
        {
            "publisher": publisher,
            "key": key,
            "activity_id": key,
            "transaction_type": "disbursement",
            "date": date(year, 12, 31),
            "year": year,
            "currency": currency,
            "amount": amount,
            "amount_usd": amount_usd,
            "title": None,
            "other_ids": [],
        }
        for publisher, key, year, currency, amount, amount_usd in LEDGER
    ]
    columns = schema.LEDGER.columns
    return pl.LazyFrame(rows, schema={name: c.dtype for name, c in columns.items()})


def _checked() -> tuple[pl.DataFrame, pl.DataFrame]:
    links, labels = check_ledgers(
        _links(), units=_units(), ledger=_ledger(), years=YEARS
    )
    return links.collect(), labels.collect()


def _check(links: pl.DataFrame, left_id: str, right_id: str) -> str | None:
    return links.filter(left_id=left_id, right_id=right_id)["ledger_check"].item()


@pytest.mark.parametrize(
    ("left_id", "right_id", "expected"),
    [
        # 60 + 40 against 104: within 5% of the ledger.
        ("d_ida1a", "c_ida1", "confirmed"),
        ("d_ida1b", "c_ida1", "confirmed"),
        # 50 against 53 + 1: outside 5%.
        ("d_ida1c", "c_ida1", "contradicted"),
        # Before the years checked.
        ("d_ida1_old", "c_ida1", None),
        # The disbursement carries another credit, which the ledger cannot pair.
        ("d_ida2", "c_ida1", "none"),
        # The ledger does not list the credit.
        ("d_ida3", "c_ida3", "none"),
        # The ledger amount has no US dollar value.
        ("d_afdb6", "c_afdb6", "none"),
        # No ledger for the provider.
        ("d_deu7", "c_deu7", None),
        # A commitment that disburses in neither source from its year on.
        ("row_ibrd4", "c_ibrd4", "confirmed_undisbursed"),
        ("row_ibrd5", "c_ibrd5", None),
        ("row_ida1", "c_ida1", None),
        ("row_deu7", "c_deu7", None),
    ],
)
def test_ledger_check_per_link(left_id: str, right_id: str, expected: str) -> None:
    links, _ = _checked()

    assert _check(links, left_id, right_id) == expected


def test_checked_links_keep_their_rows_and_schema() -> None:
    links, _ = _checked()

    schema.validate(schema.LINKS, links)
    assert sorted(links.select("task", "left_id", "right_id").rows()) == sorted(LINKS)


def test_links_outside_the_check_keep_their_ledger_check() -> None:
    links, _ = check_ledgers(
        _links("contradicted"), units=_units(), ledger=_ledger(), years=YEARS
    )

    assert _check(links.collect(), "d_deu7", "c_deu7") == "contradicted"


def test_confirmed_trace_pairs_become_ledger_labels() -> None:
    _, labels = _checked()

    schema.validate(schema.PAIR_LABELS, labels)
    assert sorted(labels.select("left_id", "right_id").rows()) == [
        ("d_ida1a", "c_ida1"),
        ("d_ida1b", "c_ida1"),
    ]
    assert labels["is_match"].all()
    assert set(labels["pair_label_source"]) == {"ledger"}


def test_a_bank_whose_ledger_was_not_read_is_left_unchecked() -> None:
    ida_only = _ledger().filter(publisher="ida")

    links, _ = check_ledgers(_links(), units=_units(), ledger=ida_only, years=YEARS)

    assert _check(links.collect(), "d_afdb6", "c_afdb6") is None


def test_review_runs_the_ledger_check_before_the_unbuilt_queue(tmp_path: Path) -> None:
    for stage, table, frame in [
        ("link", "links", _links()),
        ("link", "units", _units()),
        ("normalise", "ledger", _ledger()),
    ]:
        path = table_path(tmp_path, stage, table)
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.collect().write_parquet(path)
    config = load_config(Path(__file__).parents[1] / "build.toml")
    sources = config.sources.model_copy(
        update={"ledgers": Ledgers(years=YEARS, publishers=("ida", "ibrd"))}
    )
    context = StageContext(
        release="r",
        release_dir=tmp_path,
        config=config.model_copy(update={"review": True, "sources": sources}),
        offline=True,
    )

    with pytest.raises(NotImplementedError, match="S14"):
        run_review(context)

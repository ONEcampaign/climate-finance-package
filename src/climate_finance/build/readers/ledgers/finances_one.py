"""World Bank Finances One loan disbursements (DS00975 IBRD, DS00976 IDA)."""

import json
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Literal

import polars as pl

from climate_finance.build.readers.ledgers.frame import LEDGER_SCHEMA, complete_ledger
from climate_finance.errors import SourceUnavailableError

type Dataset = Literal["DS00975", "DS00976"]


@dataclass(frozen=True)
class _Columns:
    """Where a dataset keeps the loan number, cumulative disbursement and name."""

    publisher: str
    key: str
    disbursed: str
    title: str


DATASETS: dict[Dataset, _Columns] = {
    "DS00975": _Columns("ibrd", "loan_number", "disbursed_amount", "project_name_"),
    "DS00976": _Columns("ida", "credit_number", "disbursed_amount_us_", "project_name"),
}
# The dataset of each publisher name the ledger frame carries.
PUBLISHERS: dict[str, Dataset] = {
    columns.publisher: dataset for dataset, columns in DATASETS.items()
}


def snapshot_file(dataset: Dataset, year: int) -> str:
    """Name of the file holding the 31 December `year` snapshot of `dataset`."""
    return f"{dataset}_31-Dec-{year}.json"


def snapshot_years(years: Iterable[int]) -> list[int]:
    """The year-end snapshots needed to take a December-to-December flow for `years`."""
    return sorted({year + offset for year in years for offset in (-1, 0)})


def repeated_keys(
    records: Sequence[Mapping[str, object]], *, dataset: Dataset
) -> list[str]:
    """The loan or credit numbers that appear more than once in a snapshot, sorted.

    Finances One lists each loan once per year-end, so a repeat means the snapshot's
    pages overlapped.
    """
    counts = Counter(str(record[DATASETS[dataset].key]) for record in records)
    return sorted(key for key, count in counts.items() if count > 1)


def read_finances_one(
    path: Path, *, dataset: Dataset, years: Iterable[int]
) -> pl.DataFrame:
    """Calendar-year loan disbursements from the cumulative year-end snapshots.

    The flow of a loan in a year is its cumulative disbursement at 31 December minus the
    figure at the previous 31 December, with a loan absent from a snapshot counted as
    zero. This reproduces the CRS at a ratio of 1.000 for 2021 to 2024. The World Bank's
    fiscal-year flows match 14% to 18% of loans. Small negative flows are the World
    Bank's own corrections and are kept. Every loan in either snapshot gets a row, so a
    loan that did not disburse reads as zero. Rows are dated 31 December and keyed by
    loan or credit number.

    Args:
        path: A directory with the snapshot files named by `snapshot_file`.
        dataset: `DS00975` for IBRD loans or `DS00976` for IDA credits.
        years: The calendar years to return. Each needs its own and the previous
            year-end snapshot.

    Raises:
        SourceUnavailableError: when a snapshot file is missing or lists a loan
            twice.
    """
    columns = DATASETS[dataset]
    years = sorted(set(years))
    cumulative = {
        year: _read_snapshot(path / snapshot_file(dataset, year), dataset=dataset)
        for year in snapshot_years(years)
    }
    flows = [
        _flow(cumulative[year - 1], cumulative[year], year=year, columns=columns)
        for year in years
    ]
    return complete_ledger(pl.concat(flows).select(list(LEDGER_SCHEMA))).sort(
        "key", "date"
    )


def _read_snapshot(file: Path, *, dataset: Dataset) -> pl.DataFrame:
    """One snapshot's cumulative disbursement and title per loan.

    Raises:
        SourceUnavailableError: when the file is missing or lists a loan twice,
            which summed would read as a phantom disbursement.
    """
    if not file.is_file():
        raise SourceUnavailableError(f"Finances One snapshot {file} is not there.")
    records = json.loads(file.read_text(encoding="utf-8"))
    if repeated := repeated_keys(records, dataset=dataset):
        raise SourceUnavailableError(
            f"Finances One snapshot {file} lists {len(repeated)} loans more than "
            f"once, such as {repeated[:3]}; fetch it again."
        )
    columns = DATASETS[dataset]
    return pl.DataFrame(
        {
            "key": [r[columns.key] for r in records],
            "cumulative": [r[columns.disbursed] or 0 for r in records],
            "title": [r.get(columns.title) for r in records],
        },
        schema={"key": pl.String, "cumulative": pl.Float64, "title": pl.String},
    )


def _flow(
    before: pl.DataFrame, after: pl.DataFrame, *, year: int, columns: _Columns
) -> pl.DataFrame:
    return before.join(
        after, on="key", how="full", coalesce=True, suffix="_after"
    ).select(
        publisher=pl.lit(columns.publisher),
        key="key",
        activity_id="key",
        transaction_type=pl.lit("disbursement"),
        date=pl.lit(date(year, 12, 31)),
        currency=pl.lit("USD"),
        amount=pl.col("cumulative_after").fill_null(0)
        - pl.col("cumulative").fill_null(0),
        title=pl.coalesce("title_after", "title"),
        other_ids=pl.lit([], dtype=pl.List(pl.String)),
    )

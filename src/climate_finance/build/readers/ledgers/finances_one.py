"""World Bank Finances One loan disbursements (DS00975 IBRD, DS00976 IDA)."""

import json
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Literal

import polars as pl

from climate_finance.build.readers.ledgers import LEDGER_DIR
from climate_finance.build.readers.ledgers.frame import complete_ledger
from climate_finance.errors import SourceUnavailableError

type Dataset = Literal["DS00975", "DS00976"]
FOLDER = Path(LEDGER_DIR) / "finances_one"


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
# Fields every row of one loan in a snapshot must share for its parts to be added up.
_IDENTITY = ("project_id", "end_of_period")
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


def repeated_rows(
    records: Sequence[Mapping[str, object]], *, dataset: Dataset
) -> list[str]:
    """The loan or credit numbers of rows that appear more than once whole, sorted.

    The API can list a loan in more than one row of a snapshot, each row holding a
    part of the loan with its own amounts, and the reader adds them up. A row repeated
    field for field holds no new part, so it means the snapshot's pages overlapped.
    """
    key = DATASETS[dataset].key
    counts = Counter(
        (str(record[key]), json.dumps(record, sort_keys=True)) for record in records
    )
    return sorted({loan for (loan, _), n in counts.items() if n > 1})


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
    loan or credit number. A loan the snapshot lists in several rows, as it lists
    IBRD85600 at the end of 2024, holds the sum of their cumulative disbursements.

    Args:
        path: A directory with the snapshot files named by `snapshot_file`.
        dataset: `DS00975` for IBRD loans or `DS00976` for IDA credits.
        years: The calendar years to return. Each needs its own and the previous
            year-end snapshot.

    Raises:
        SourceUnavailableError: when a snapshot file is missing, repeats a row
            whole, or lists a loan in rows that disagree on its title, project or
            year-end.
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
    return complete_ledger(pl.concat(flows)).sort("key", "date")


def _read_snapshot(file: Path, *, dataset: Dataset) -> pl.DataFrame:
    """One snapshot's cumulative disbursement and title per loan.

    Raises:
        SourceUnavailableError: when the file is missing, repeats a row whole, which
            summed would read as a phantom disbursement, or lists a loan in rows
            that disagree on a field of `_IDENTITY`.
    """
    if not file.is_file():
        raise SourceUnavailableError(f"Finances One snapshot {file} is not there.")
    records = json.loads(file.read_text(encoding="utf-8"))
    if repeated := repeated_rows(records, dataset=dataset):
        raise SourceUnavailableError(
            f"Finances One snapshot {file} repeats a row whole for "
            f"{len(repeated)} loans, such as {repeated[:3]}, so its pages "
            "overlapped; fetch it again."
        )
    columns = DATASETS[dataset]
    rows = pl.DataFrame(
        {
            "key": [r[columns.key] for r in records],
            "cumulative": [r[columns.disbursed] or 0 for r in records],
            "title": [r.get(columns.title) for r in records],
            **{field: [r.get(field) for r in records] for field in _IDENTITY},
        },
        schema={
            "key": pl.String,
            "cumulative": pl.Float64,
            "title": pl.String,
            **dict.fromkeys(_IDENTITY, pl.String),
        },
    )
    identity = ["title", *_IDENTITY]
    split = (
        rows.group_by("key")
        .agg(pl.col(identity).n_unique())
        .filter(pl.any_horizontal(pl.col(identity) > 1))
    )
    if split.height:
        loans = split.sort("key").head(3).iter_rows(named=True)
        raise SourceUnavailableError(
            f"Finances One snapshot {file} lists {split.height} loans in rows that "
            "disagree on "
            + "; ".join(
                f"{loan['key']} ({', '.join(f for f in identity if loan[f] > 1)})"
                for loan in loans
            )
            + ", so their parts cannot be added up."
        )
    return rows.group_by("key", maintain_order=True).agg(
        pl.col("cumulative").sum(), pl.col("title").first()
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

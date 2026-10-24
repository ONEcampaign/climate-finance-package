"""World Bank Finances One: year-end snapshots of IBRD and IDA loan disbursements."""

import json
import time
from collections.abc import Iterable, Sequence
from pathlib import Path

import httpx

from climate_finance.build.readers.ledgers.finances_one import (
    DATASETS,
    FOLDER,
    Dataset,
    repeated_keys,
    snapshot_file,
    snapshot_years,
)
from climate_finance.build.sources.files import (
    RawFile,
    get,
    http_client,
    import_archive,
    save,
)
from climate_finance.errors import SourceUnavailableError

SOURCE = "ledger_finances_one"
API = "https://datacatalogapi.worldbank.org/dexapps/fone/api/apiservice"
_RESOURCE = "RS00905"
_PAGE = 1000


def download_finances_one(
    raw_dir: Path,
    *,
    years: Iterable[int],
    datasets: Sequence[Dataset],
    archive: Path | None = None,
    client: httpx.Client | None = None,
    pause_seconds: float = 1.5,
) -> list[RawFile]:
    """Save the 31 December snapshots that `read_finances_one` needs for `years`.

    The full Finances One file exceeds 300 MB, so each snapshot comes through filtered
    API calls of 1,000 rows, a pause apart, each skipping the rows already read.
    The API serves rows by loan number and takes no sort parameter. Each response
    reports the snapshot's row `count`; the download refuses unless it holds that
    many rows, none of them a repeated loan, which together prove every loan arrived
    exactly once. Rows are saved sorted by loan or credit number, so a snapshot
    downloads to the same bytes whatever order the API served. Files land in
    `<raw_dir>/ledgers/finances_one/`, one per dataset and year-end, each recorded
    under its own source name (`source_name`).

    Args:
        raw_dir: The raw directory.
        years: The calendar years to read; each needs its own and the previous
            year-end.
        datasets: The datasets to save, of IBRD (`DS00975`) and IDA (`DS00976`).
        archive: A directory holding the snapshot files, named as the download names
            them. They are placed (`files.place`) and no request is made.
        client: An HTTP client to use in place of the default.
        pause_seconds: The pause between requests.

    Raises:
        SourceUnavailableError: when the archive lacks a snapshot, or when the API
            returns none for a year-end, other than its own count of rows, or one
            loan twice.
    """
    wanted = [(dataset, year) for dataset in datasets for year in snapshot_years(years)]
    if archive is not None:
        records = []
        for dataset, year in wanted:
            name = snapshot_file(dataset, year)
            records += import_archive(
                {FOLDER / name: archive / name},
                raw_dir=raw_dir,
                source=source_name(dataset, year),
            )
        return records
    records = []
    with http_client(client) as session:
        for dataset, year in wanted:
            rows = _pull(session, dataset, year, pause_seconds=pause_seconds)
            records.append(
                save(
                    json.dumps(rows).encode(),
                    raw_dir=raw_dir,
                    relative=FOLDER / snapshot_file(dataset, year),
                    source=source_name(dataset, year),
                    url=API,
                )
            )
    return records


def source_name(dataset: Dataset, year: int) -> str:
    """The `raw_manifest` source of the 31 December `year` snapshot of `dataset`."""
    return f"{SOURCE}_{dataset}_{year}"


def _pull(
    client: httpx.Client, dataset: Dataset, year: int, *, pause_seconds: float
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    count = None
    while True:
        response = get(
            client,
            API,
            params={
                "datasetId": dataset,
                "resourceId": _RESOURCE,
                "top": _PAGE,
                "skip": len(rows),
                "filter": f"end_of_period='31-Dec-{year}'",
            },
            pause_seconds=pause_seconds,
        ).json()
        if count is None:
            count = response["count"]
        page = response["data"]
        rows += page
        if len(page) < _PAGE:
            break
        time.sleep(pause_seconds)
    if not rows:
        raise SourceUnavailableError(f"{dataset} has no rows for 31 December {year}.")
    if len(rows) != count:
        raise SourceUnavailableError(
            f"{dataset} served {len(rows)} rows for 31 December {year} but counts "
            f"{count}; fetch it again."
        )
    if repeated := repeated_keys(rows, dataset=dataset):
        raise SourceUnavailableError(
            f"{dataset} served {len(repeated)} loans twice for 31 December {year}, "
            f"such as {repeated[:3]}, so its pages overlapped; fetch it again."
        )
    return sorted(rows, key=lambda row: str(row[DATASETS[dataset].key]))

"""World Bank Finances One: year-end snapshots of IBRD and IDA loan disbursements."""

import json
import time
from collections.abc import Iterable
from pathlib import Path

import httpx

from climate_finance.build.readers.ledgers.finances_one import (
    DATASETS,
    snapshot_file,
    snapshot_years,
)
from climate_finance.build.sources.files import (
    LEDGER_DIR,
    RawFile,
    get,
    http_client,
    import_archive,
    require_online,
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
    offline: bool,
    archive: Path | None = None,
    client: httpx.Client | None = None,
    pause_seconds: float = 1.5,
) -> list[RawFile]:
    """Save the 31 December snapshots that `read_finances_one` needs for `years`.

    The full Finances One file exceeds 300 MB, so each snapshot comes through filtered
    API calls of 1,000 rows, a pause apart. Files land in
    `<raw_dir>/ledgers/finances_one/`, one per dataset and year-end.

    Args:
        raw_dir: The raw directory.
        years: The calendar years to read; each needs its own and the previous
            year-end.
        offline: Refuse to call the API. Ignored when `archive` is given.
        archive: A directory holding the snapshot files, named as the download names
            them. They are copied and no request is made.
        client: An HTTP client to use in place of the default.
        pause_seconds: The pause between requests.

    Raises:
        SourceUnavailableError: when offline without an archive, when the archive
            lacks a snapshot, or when the API returns none for a year-end.
    """
    folder = Path(LEDGER_DIR) / "finances_one"
    year_ends = snapshot_years(years)
    wanted = [(dataset, year) for dataset in DATASETS for year in year_ends]
    if archive is not None:
        return import_archive(
            {
                folder / snapshot_file(dataset, year): archive
                / snapshot_file(dataset, year)
                for dataset, year in wanted
            },
            raw_dir=raw_dir,
            source=SOURCE,
        )
    require_online(SOURCE, offline=offline)
    records = []
    with http_client(client) as session:
        for dataset, year in wanted:
            rows = _pull(session, dataset, year, pause_seconds=pause_seconds)
            records.append(
                save(
                    json.dumps(rows).encode(),
                    raw_dir=raw_dir,
                    relative=folder / snapshot_file(dataset, year),
                    source=SOURCE,
                    url=API,
                )
            )
    return records


def _pull(
    client: httpx.Client, dataset: str, year: int, *, pause_seconds: float
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    while True:
        page = get(
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
        ).json()["data"]
        rows += page
        if len(page) < _PAGE:
            break
        time.sleep(pause_seconds)
    if not rows:
        raise SourceUnavailableError(f"{dataset} has no rows for 31 December {year}.")
    return rows

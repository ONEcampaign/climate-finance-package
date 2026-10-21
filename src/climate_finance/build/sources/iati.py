"""IATI activity files of the banks and bilateral donors that publish ledgers."""

import time
from collections.abc import Sequence
from pathlib import Path

import httpx

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

SOURCE = "ledger_iati"
INDEX = "https://bulk-data.iatistandard.org/datasets-minimal"


def download_iati_ledgers(
    raw_dir: Path,
    *,
    publishers: Sequence[str],
    offline: bool,
    archive: Path | None = None,
    client: httpx.Client | None = None,
    pause_seconds: float = 1.0,
) -> list[RawFile]:
    """Save every activity file of the named publishers from the IATI Bulk Data Service.

    The service lists each publisher's files in its dataset index; each file is
    fetched from the service's cache a pause apart. Files land in
    `<raw_dir>/ledgers/iati/<publisher>/`, the directory `read_iati_ledger` takes.

    Args:
        raw_dir: The raw directory.
        publishers: Short names on the service, such as `asdb`, `afdb` or `bmz`.
        offline: Refuse to call the service. Ignored when `archive` is given.
        archive: A directory with one subdirectory of XML files per publisher. The
            files are copied and no request is made.
        client: An HTTP client to use in place of the default.
        pause_seconds: The pause between requests.

    Raises:
        SourceUnavailableError: when offline without an archive, or when a publisher
            has no file in the index or the archive.
    """
    folder = Path(LEDGER_DIR) / "iati"
    if archive is not None:
        return import_archive(
            {
                folder / publisher / file.name: file
                for publisher in publishers
                for file in sorted((archive / publisher).glob("*.xml"))
            },
            raw_dir=raw_dir,
            source=SOURCE,
        )
    require_online(SOURCE, offline=offline)
    records = []
    with http_client(client) as session:
        index = get(session, INDEX, pause_seconds=pause_seconds).json()["datasets"]
        for publisher in publishers:
            urls = [
                dataset["last_known_good_dataset"]["cached_dataset_url_xml"]
                for dataset in index
                if dataset["reporting_org_short_name"] == publisher
                and (dataset.get("last_known_good_dataset") or {}).get(
                    "cached_dataset_url_xml"
                )
            ]
            if not urls:
                raise SourceUnavailableError(
                    f"The IATI index lists no file for publisher {publisher!r}."
                )
            for url in urls:
                time.sleep(pause_seconds)
                response = get(session, url, pause_seconds=pause_seconds)
                records.append(
                    save(
                        response.content,
                        raw_dir=raw_dir,
                        relative=folder / publisher / url.rsplit("/", 1)[-1],
                        source=SOURCE,
                        url=url,
                        etag=response.headers.get("etag"),
                    )
                )
    return records

"""ForeignAssistance.gov's complete US foreign aid file, cut to the ids that key the
US crosswalk.

The US reports each CRS project number as `<submission id>_<activity id>` of its
ForeignAssistance.gov submission, and the file gives each such activity its project
number, which for USAID is the award number its IATI activity ids carry. The file
is 3.75 GB, so the download streams it and keeps only the four id columns.
"""

import csv
import io
import json
from collections.abc import Iterator
from pathlib import Path

import httpx
import polars as pl

from climate_finance.build.sources.files import (
    RawFile,
    http_client,
    import_archive,
    require_online,
    save,
)
from climate_finance.errors import SourceUnavailableError

SOURCE = "foreignassistance"
URL = (
    "https://s3.amazonaws.com/files.explorer.devtechlab.com/us_foreign_aid_complete.csv"
)
FOLDER = Path("external/foreignassistance")
CROSSWALK_FILE = FOLDER / "crosswalk.parquet"
# The upstream file's URL, size and Last-Modified, so a copy names what it was cut
# from.
UPSTREAM_FILE = FOLDER / "upstream.json"
# Upstream column -> crosswalk column.
COLUMNS = {
    "Submission ID": "submission_id",
    "submission_activity_id": "submission_activity_id",
    "Activity Project Number": "project_number",
    "Managing Agency Acronym": "managing_agency",
}


def download_foreign_assistance(
    raw_dir: Path,
    *,
    offline: bool,
    archive: Path | None = None,
    client: httpx.Client | None = None,
) -> list[RawFile]:
    """Stream the complete file and save its distinct id rows as parquet.

    Rows are sorted on every column, so one upstream file gives the same bytes. The
    parquet lands at `<raw_dir>/external/foreignassistance/crosswalk.parquet`, the
    file `link.crosswalks.read_us_crosswalk` takes, beside `upstream.json`.

    Args:
        raw_dir: The raw directory.
        offline: Refuse the download. Ignored when `archive` is given.
        archive: A directory holding `crosswalk.parquet` and `upstream.json`, which
            are copied.
        client: An HTTP client to use in place of the default.

    Raises:
        SourceUnavailableError: when offline without an archive, when the server
            refuses the file, or when the file lacks a column the crosswalk reads.
    """
    if archive is not None:
        return import_archive(
            {
                relative: archive / relative.name
                for relative in (CROSSWALK_FILE, UPSTREAM_FILE)
            },
            raw_dir=raw_dir,
            source=SOURCE,
        )
    require_online(SOURCE, offline=offline)
    with http_client(client) as session, session.stream("GET", URL) as response:
        if not response.is_success:
            raise SourceUnavailableError(
                f"{URL} could not be fetched: HTTP {response.status_code}."
            )
        rows = _id_rows(_lines(response.iter_text()))
        upstream = {
            "url": URL,
            "size": response.headers.get("content-length"),
            "last_modified": response.headers.get("last-modified"),
        }
        etag = response.headers.get("etag")
    names = list(COLUMNS.values())
    crosswalk = pl.DataFrame(
        sorted(rows), schema=dict.fromkeys(names, pl.String), orient="row"
    )
    buffer = io.BytesIO()
    crosswalk.write_parquet(buffer)
    return [
        save(
            buffer.getvalue(),
            raw_dir=raw_dir,
            relative=CROSSWALK_FILE,
            source=SOURCE,
            url=URL,
            etag=etag,
        ),
        save(
            json.dumps(upstream, indent=2).encode(),
            raw_dir=raw_dir,
            relative=UPSTREAM_FILE,
            source=SOURCE,
            url=URL,
            etag=etag,
        ),
    ]


def _id_rows(lines: Iterator[str]) -> set[tuple[str, ...]]:
    """The distinct `COLUMNS` values of each CSV record."""
    reader = csv.reader(lines)
    header = next(reader, [])
    if missing := [name for name in COLUMNS if name not in header]:
        raise SourceUnavailableError(f"{URL} lacks the columns {missing}.")
    positions = [header.index(name) for name in COLUMNS]
    return {tuple(record[i] for i in positions) for record in reader if record}


def _lines(chunks: Iterator[str]) -> Iterator[str]:
    """`chunks` of text as lines that keep their endings, so `csv.reader` joins a
    quoted field that spans lines."""
    rest = ""
    for chunk in chunks:
        lines = (rest + chunk).splitlines(keepends=True)
        rest = lines.pop() if lines and not lines[-1].endswith(("\n", "\r")) else ""
        yield from lines
    if rest:
        yield rest

"""The ForeignAssistance.gov download keeps the id columns the US crosswalk reads."""

import json
from pathlib import Path

import httpx
import polars as pl
import pytest

from climate_finance import schema
from climate_finance.build.link.crosswalks import read_us_crosswalk
from climate_finance.build.sources.foreignassistance import (
    CROSSWALK_FILE,
    UPSTREAM_FILE,
    URL,
    download_foreign_assistance,
)
from climate_finance.errors import SourceUnavailableError

# The upstream layout cut to a few columns, with a description that spans lines
# and a row repeated per transaction.
HEADER = (
    '"Country ID","Submission ID","Activity Name","Activity Description",'
    '"Activity Project Number","Managing Agency Acronym","submission_activity_id"\r\n'
)
WATER = '4,76,"Water","Wells,\r\nphase two",72DFFP19IO00066,USAID,62884\r\n'
ROADS = '4,65,"Roads","Paving",Information Redacted,STATE,17225\r\n'
CSV = HEADER + WATER + WATER + ROADS


def _client(body: str, *, status: int = 200) -> httpx.Client:
    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == URL
        return httpx.Response(
            status,
            content=body.encode(),
            headers={"last-modified": "Wed, 02 Sep 2026 17:40:59 GMT"},
        )

    return httpx.Client(transport=httpx.MockTransport(handler))


def test_the_download_keeps_distinct_id_rows_in_a_stable_order(
    tmp_path: Path,
) -> None:
    first = download_foreign_assistance(
        tmp_path / "a", offline=False, client=_client(CSV)
    )
    # The same rows in another order give the same bytes.
    second = download_foreign_assistance(
        tmp_path / "b", offline=False, client=_client(HEADER + ROADS + WATER)
    )

    # The crosswalk is the first record; `upstream.json` holds the file's size.
    assert first[0]["sha256"] == second[0]["sha256"]
    for record in first:
        assert record["sha256"] == schema.sha256_file(tmp_path / "a" / record["path"])
    crosswalk = pl.read_parquet(tmp_path / "a" / CROSSWALK_FILE)
    assert crosswalk.rows() == [
        ("65", "17225", "Information Redacted", "STATE"),
        ("76", "62884", "72DFFP19IO00066", "USAID"),
    ]
    upstream = json.loads((tmp_path / "a" / UPSTREAM_FILE).read_text())
    assert upstream["url"] == URL
    assert upstream["last_modified"] == "Wed, 02 Sep 2026 17:40:59 GMT"
    assert read_us_crosswalk(tmp_path / "a" / CROSSWALK_FILE.parent).rows() == [
        ("76_62884", "72DFFP19IO00066")
    ]


@pytest.mark.parametrize(
    ("body", "status"),
    [('"Country ID","Submission ID"\r\n4,76\r\n', 200), ("", 404)],
)
def test_the_download_refuses_a_file_it_cannot_read(
    tmp_path: Path, body: str, status: int
) -> None:
    with pytest.raises(SourceUnavailableError):
        download_foreign_assistance(
            tmp_path, offline=False, client=_client(body, status=status)
        )


def test_the_download_makes_no_request_offline(tmp_path: Path) -> None:
    def refuse(request: httpx.Request) -> httpx.Response:
        raise AssertionError(f"offline download requested {request.url}")

    client = httpx.Client(transport=httpx.MockTransport(refuse))
    with pytest.raises(SourceUnavailableError):
        download_foreign_assistance(tmp_path, offline=True, client=client)


def test_the_download_imports_a_local_archive_even_offline(tmp_path: Path) -> None:
    download_foreign_assistance(tmp_path / "online", offline=False, client=_client(CSV))
    archive = tmp_path / "online" / CROSSWALK_FILE.parent

    records = download_foreign_assistance(
        tmp_path / "raw", offline=True, archive=archive
    )

    assert [record["method"] for record in records] == ["manual", "manual"]
    assert (tmp_path / "raw" / CROSSWALK_FILE).read_bytes() == (
        archive / CROSSWALK_FILE.name
    ).read_bytes()

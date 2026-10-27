"""The source downloads write what the ledger readers parse."""

import json
from collections.abc import Callable
from pathlib import Path

import httpx
import pytest

from climate_finance import schema
from climate_finance.build.readers.ledgers.finances_one import (
    DATASETS,
    read_finances_one,
)
from climate_finance.build.readers.ledgers.gcf import (
    read_gcf_disbursements,
    read_gcf_readiness,
)
from climate_finance.build.readers.ledgers.iati import read_iati_ledger
from climate_finance.build.readers.ledgers.imf import read_sdr_rates
from climate_finance.build.sources import files
from climate_finance.build.sources.finances_one import API, download_finances_one
from climate_finance.build.sources.gcf import (
    PROJECTS_URL,
    READINESS_URL,
    download_gcf_projects,
    download_gcf_readiness,
)
from climate_finance.build.sources.iati import INDEX, download_iati_ledgers
from climate_finance.build.sources.imf import URL as IMF_URL
from climate_finance.build.sources.imf import download_sdr_rates
from climate_finance.errors import SourceUnavailableError

LEDGERS = Path(__file__).parent / "fixtures" / "raw" / "ledgers"
Handler = Callable[[httpx.Request], httpx.Response]


def _client(handler: Handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def _refuse(request: httpx.Request) -> httpx.Response:
    raise AssertionError(f"offline download requested {request.url}")


def _manifest_row(record: files.RawFile, raw_dir: Path) -> dict[str, object]:
    """The record as a `raw_manifest` row, checked against the file it describes."""
    assert record["sha256"] == schema.sha256_file(raw_dir / record["path"])
    assert set(record) == set(schema.RAW_MANIFEST.columns)
    return dict(record)


# World Bank ---------------------------------------------------------------------


def _finances_one_api(
    rows_by_snapshot: dict[tuple[str, str], int],
    *,
    order: int = 1,
    overlap: int = 0,
    miscount: int = 0,
) -> Handler:
    """An API serving `n` loans per (dataset, end of period) in pages as asked, each
    page in `order` (-1 reverses it) and every page after the first starting
    `overlap` loans early, with a `count` of `n` plus `miscount`."""

    def handler(request: httpx.Request) -> httpx.Response:
        params = request.url.params
        assert str(request.url).startswith(API)
        assert params["resourceId"] == "RS00905"
        dataset = params["datasetId"]
        period = params["filter"].removeprefix("end_of_period='").removesuffix("'")
        columns = next(c for name, c in DATASETS.items() if name == dataset)
        top, skip = int(params["top"]), int(params["skip"])
        shift = overlap if skip else 0
        total = rows_by_snapshot.get((dataset, period), 0)
        page = [
            {
                columns.key: f"{dataset}-{i:04d}",
                columns.disbursed: 100.0 * (i + 1),
                columns.title: f"Loan {i}",
                "end_of_period": period,
            }
            for i in range(skip - shift, min(skip + top, total) - shift)
        ][::order]
        return httpx.Response(200, json={"count": total + miscount, "data": page})

    return handler


def test_finances_one_pages_each_year_end_and_feeds_the_reader(tmp_path: Path) -> None:
    served = {
        (dataset, period): 1_001 if period == "31-Dec-2021" else 3
        for dataset in DATASETS
        for period in ("31-Dec-2020", "31-Dec-2021")
    }
    requests: list[httpx.Request] = []
    handler = _finances_one_api(served)

    def counting(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return handler(request)

    records = download_finances_one(
        tmp_path,
        years=[2021],
        datasets=tuple(DATASETS),
        client=_client(counting),
        pause_seconds=0,
    )

    assert sorted(r["path"] for r in records) == [
        "ledgers/finances_one/DS00975_31-Dec-2020.json",
        "ledgers/finances_one/DS00975_31-Dec-2021.json",
        "ledgers/finances_one/DS00976_31-Dec-2020.json",
        "ledgers/finances_one/DS00976_31-Dec-2021.json",
    ]
    row = _manifest_row(records[0], tmp_path)
    assert row["method"] == "http"
    assert row["url"] == API
    # 1,001 loans need a second request, 3 need one: 2 + 1 per dataset.
    assert len(requests) == 6
    ibrd = read_finances_one(
        tmp_path / "ledgers" / "finances_one", dataset="DS00975", years=[2021]
    )
    assert ibrd.height == 1_001
    # Loans 0000 to 0002 stand at the same value at both year-ends; loan 1000 is new.
    assert ibrd.filter(key="DS00975-0000")["amount"].item() == 0
    assert ibrd.filter(key="DS00975-1000")["amount"].item() == 100.0 * 1_001


def test_finances_one_saves_a_snapshot_sorted_by_loan(tmp_path: Path) -> None:
    api = _finances_one_api(
        {(d, f"31-Dec-{y}"): 3 for d in DATASETS for y in (2020, 2021)}, order=-1
    )

    download_finances_one(
        tmp_path,
        years=[2021],
        datasets=tuple(DATASETS),
        client=_client(api),
        pause_seconds=0,
    )

    saved = json.loads(
        (tmp_path / "ledgers" / "finances_one" / "DS00975_31-Dec-2021.json").read_text()
    )
    assert [row["loan_number"] for row in saved] == [
        "DS00975-0000",
        "DS00975-0001",
        "DS00975-0002",
    ]


def test_finances_one_refuses_pages_that_overlap(tmp_path: Path) -> None:
    # The second page repeats the first page's last loan and so never reaches the
    # snapshot's last loan.
    api = _finances_one_api(
        {(d, f"31-Dec-{y}"): 1_001 for d in DATASETS for y in (2020, 2021)},
        overlap=1,
    )

    with pytest.raises(SourceUnavailableError, match="DS00975-0999"):
        download_finances_one(
            tmp_path,
            years=[2021],
            datasets=tuple(DATASETS),
            client=_client(api),
            pause_seconds=0,
        )
    assert not (tmp_path / "ledgers").exists()


def test_finances_one_keeps_a_loan_the_source_lists_in_two_rows(
    tmp_path: Path,
) -> None:
    # The source lists loan 0001 at the end of 2021 in two rows with their own
    # amounts, which no refetch changes; they are saved and added up.
    parts = [
        {"loan_number": "L-0001", "disbursed_amount": 70.0, "part": "a"},
        {"loan_number": "L-0001", "disbursed_amount": 30.0, "part": "b"},
    ]

    def api(order: int) -> Handler:
        def handler(request: httpx.Request) -> httpx.Response:
            period = request.url.params["filter"].split("'")[1]
            rows = {
                "31-Dec-2020": [{"loan_number": "L-0001", "disbursed_amount": 40.0}],
                "31-Dec-2021": [{**p, "end_of_period": period} for p in parts[::order]],
            }[period]
            return httpx.Response(200, json={"count": len(rows), "data": rows})

        return handler

    saved = []
    for order in (1, -1):
        folder = tmp_path / str(order)
        download_finances_one(
            folder,
            years=[2021],
            datasets=["DS00975"],
            client=_client(api(order)),
            pause_seconds=0,
        )
        saved.append(
            (folder / "ledgers/finances_one/DS00975_31-Dec-2021.json").read_bytes()
        )

    assert saved[0] == saved[1]
    ibrd = read_finances_one(
        tmp_path / "1" / "ledgers" / "finances_one", dataset="DS00975", years=[2021]
    )
    assert ibrd.filter(key="L-0001")["amount"].item() == 100.0 - 40.0


def test_finances_one_refuses_a_snapshot_short_of_its_count(tmp_path: Path) -> None:
    api = _finances_one_api(
        {(d, f"31-Dec-{y}"): 3 for d in DATASETS for y in (2020, 2021)}, miscount=1
    )

    with pytest.raises(SourceUnavailableError, match=r"served 3 rows .* counts 4"):
        download_finances_one(
            tmp_path,
            years=[2021],
            datasets=tuple(DATASETS),
            client=_client(api),
            pause_seconds=0,
        )
    assert not (tmp_path / "ledgers").exists()


def test_finances_one_retries_a_rate_limit(tmp_path: Path) -> None:
    api = _finances_one_api(
        {(d, f"31-Dec-{y}"): 1 for d in DATASETS for y in (2020, 2021)}
    )
    refusals = {"left": 2}

    def limited(request: httpx.Request) -> httpx.Response:
        if refusals["left"]:
            refusals["left"] -= 1
            return httpx.Response(429)
        return api(request)

    records = download_finances_one(
        tmp_path,
        years=[2021],
        datasets=tuple(DATASETS),
        client=_client(limited),
        pause_seconds=0,
    )

    assert len(records) == 4


def test_finances_one_gives_up_on_a_persistent_server_fault(tmp_path: Path) -> None:
    with pytest.raises(SourceUnavailableError, match="HTTP 503"):
        download_finances_one(
            tmp_path,
            years=[2021],
            datasets=tuple(DATASETS),
            client=_client(lambda request: httpx.Response(503)),
            pause_seconds=0,
        )


def test_finances_one_refuses_a_year_end_the_api_does_not_have(tmp_path: Path) -> None:
    api = _finances_one_api({("DS00975", "31-Dec-2020"): 2})

    with pytest.raises(SourceUnavailableError, match="no rows"):
        download_finances_one(
            tmp_path,
            years=[2021],
            datasets=tuple(DATASETS),
            client=_client(api),
            pause_seconds=0,
        )


def test_finances_one_imports_a_local_archive_without_a_request(tmp_path: Path) -> None:
    records = download_finances_one(
        tmp_path,
        years=[2021, 2022],
        datasets=tuple(DATASETS),
        archive=LEDGERS / "finances_one",
        client=_client(_refuse),
    )

    assert len(records) == 6
    assert {_manifest_row(r, tmp_path)["method"] for r in records} == {"manual"}
    ida = read_finances_one(
        tmp_path / "ledgers" / "finances_one", dataset="DS00976", years=[2022]
    )
    assert ida.filter(key="IDA90001")["amount"].item() == pytest.approx(
        300_000 - 120_000
    )


def test_finances_one_archive_names_the_snapshot_it_lacks(tmp_path: Path) -> None:
    with pytest.raises(SourceUnavailableError, match="31-Dec-2019"):
        download_finances_one(
            tmp_path,
            years=[2020],
            datasets=tuple(DATASETS),
            archive=LEDGERS / "finances_one",
        )


# IATI ----------------------------------------------------------------------------


def _iati_service(publishers: dict[str, list[Path]]) -> Handler:
    """A Bulk Data Service serving each publisher's fixture files from its index."""
    datasets = [
        {
            "reporting_org_short_name": publisher,
            "last_known_good_dataset": {
                "cached_dataset_url_xml": f"https://bulk.example/{publisher}/{f.name}"
            },
        }
        for publisher, paths in publishers.items()
        for f in paths
    ] + [{"reporting_org_short_name": "usaid", "last_known_good_dataset": None}]
    by_url = {
        f"https://bulk.example/{publisher}/{f.name}": f
        for publisher, paths in publishers.items()
        for f in paths
    }

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == INDEX:
            return httpx.Response(200, json={"datasets": datasets})
        return httpx.Response(
            200, content=by_url[url].read_bytes(), headers={"etag": '"abc"'}
        )

    return handler


def test_iati_downloads_the_named_publishers_into_the_directories_readers_take(
    tmp_path: Path,
) -> None:
    iati = LEDGERS / "iati"
    service = _iati_service(
        {
            "asdb": [iati / "asdb" / "asdb-sample.xml"],
            "isdb": [iati / "isdb" / "isdb-sample.xml"],
            "gef": [iati / "gef" / "gef-sample.xml"],
        }
    )

    records = download_iati_ledgers(
        tmp_path,
        publishers=["asdb", "isdb"],
        client=_client(service),
        pause_seconds=0,
    )

    assert sorted(r["path"] for r in records) == [
        "ledgers/iati/asdb/asdb-sample.xml",
        "ledgers/iati/isdb/isdb-sample.xml",
    ]
    row = _manifest_row(records[0], tmp_path)
    assert (row["etag"], row["url"]) == (
        '"abc"',
        "https://bulk.example/asdb/asdb-sample.xml",
    )
    assert not (tmp_path / "ledgers" / "iati" / "gef").exists()
    assert read_iati_ledger(tmp_path / "ledgers" / "iati" / "isdb", publisher="isdb")[
        "key"
    ].unique().to_list() == ["ZZZ9001"]


def test_iati_names_a_publisher_the_index_lacks(tmp_path: Path) -> None:
    service = _iati_service({"asdb": [LEDGERS / "iati" / "asdb" / "asdb-sample.xml"]})

    with pytest.raises(SourceUnavailableError, match="'usaid'"):
        download_iati_ledgers(
            tmp_path,
            publishers=["usaid"],
            client=_client(service),
            pause_seconds=0,
        )


def test_iati_imports_a_local_archive_of_publisher_directories(tmp_path: Path) -> None:
    records = download_iati_ledgers(
        tmp_path,
        publishers=["bmz", "ec-intpa"],
        archive=LEDGERS / "iati",
    )

    assert sorted(r["path"] for r in records) == [
        "ledgers/iati/bmz/bmz-sample.xml",
        "ledgers/iati/ec-intpa/ec-intpa-sample.xml",
    ]
    with pytest.raises(SourceUnavailableError, match="archive"):
        download_iati_ledgers(tmp_path, publishers=["usaid"], archive=LEDGERS / "iati")


# GCF -----------------------------------------------------------------------------


def test_gcf_projects_are_saved_as_served_and_feed_the_reader(tmp_path: Path) -> None:
    body = (LEDGERS / "gcf" / "projects.json").read_bytes()

    def api(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == PROJECTS_URL
        return httpx.Response(200, content=body)

    (record,) = download_gcf_projects(tmp_path, client=_client(api), pause_seconds=0)

    assert record["path"] == "external/gcf/projects.json"
    _manifest_row(record, tmp_path)
    assert (tmp_path / record["path"]).read_bytes() == body
    assert read_gcf_disbursements(tmp_path / record["path"]).height == 8


def test_gcf_projects_refuse_a_reply_that_is_not_a_list(tmp_path: Path) -> None:
    with pytest.raises(SourceUnavailableError, match="list"):
        download_gcf_projects(
            tmp_path,
            client=_client(lambda r: httpx.Response(200, json={"error": "x"})),
            pause_seconds=0,
        )


def test_gcf_readiness_is_cut_from_the_page_data(tmp_path: Path) -> None:
    grants = json.loads((LEDGERS / "gcf" / "readiness.json").read_text())
    page = {"props": {"pageProps": {"data": {"result": True, "data": grants}}}}
    html = (
        '<html><body><script id="__NEXT_DATA__" type="application/json">'
        f"{json.dumps(page)}</script></body></html>"
    )

    def library(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == READINESS_URL
        return httpx.Response(200, text=html)

    (record,) = download_gcf_readiness(
        tmp_path, client=_client(library), pause_seconds=0
    )

    assert record["path"] == "ledgers/gcf/readiness.json"
    _manifest_row(record, tmp_path)
    assert json.loads((tmp_path / record["path"]).read_text()) == grants
    assert read_gcf_readiness(tmp_path / record["path"]).height == 5


def test_gcf_readiness_names_a_page_that_lost_its_table(tmp_path: Path) -> None:
    with pytest.raises(SourceUnavailableError, match="readiness table"):
        download_gcf_readiness(
            tmp_path,
            client=_client(lambda r: httpx.Response(200, text="<html></html>")),
            pause_seconds=0,
        )


def test_gcf_readiness_imports_a_local_archive_without_a_request(
    tmp_path: Path,
) -> None:
    (readiness,) = download_gcf_readiness(tmp_path, archive=LEDGERS / "gcf")

    assert readiness["method"] == "manual"
    assert read_gcf_readiness(tmp_path / "ledgers" / "gcf" / "readiness.json").height


# IMF ----------------------------------------------------------------------------


def test_sdr_rates_are_saved_as_served_and_feed_the_reader(tmp_path: Path) -> None:
    body = (LEDGERS / "imf" / "sdr_usd.xml").read_bytes()

    def api(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == IMF_URL
        return httpx.Response(200, content=body)

    (record,) = download_sdr_rates(tmp_path, client=_client(api), pause_seconds=0)

    assert record["path"] == "ledgers/imf/sdr_usd.xml"
    _manifest_row(record, tmp_path)
    # The 2023 observation carries no value.
    assert read_sdr_rates(tmp_path / record["path"]).rows() == [
        (2020, 1.25),
        (2021, 1.5),
        (2022, 2.0),
    ]


def test_sdr_rates_refuse_a_reply_without_observations(tmp_path: Path) -> None:
    with pytest.raises(SourceUnavailableError, match="no observations"):
        download_sdr_rates(
            tmp_path,
            client=_client(lambda r: httpx.Response(200, text="<html></html>")),
            pause_seconds=0,
        )


def test_sdr_rates_import_an_archive_without_a_request(
    tmp_path: Path,
) -> None:
    (record,) = download_sdr_rates(
        tmp_path, archive=LEDGERS / "imf", client=_client(_refuse)
    )

    assert record["method"] == "manual"

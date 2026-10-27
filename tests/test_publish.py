"""Packing, gating and uploading a release, and `load` downloading it into a cache.

The round trip runs in the fast suite: it packs the fixture release of a few hundred
kB and serves it from a server on loopback, which the socket guard allows, so it
reaches no outside network and takes well under a second.
"""

import json
import shutil
import tarfile
import threading
import tomllib
from collections.abc import Collection, Iterator
from datetime import UTC, datetime
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import polars as pl
import pyarrow as pa
import pytest
from make_fixture import FIXTURE_DIR
from pydantic import ValidationError

import climate_finance as cf
from climate_finance import release as release_module
from climate_finance.build import gate, release
from climate_finance.build.errors import BuildError, GateFailedError
from climate_finance.build.fetch import BTR_SOURCE
from climate_finance.build.measure import PROVIDERS_TOML
from climate_finance.errors import (
    ReleaseIntegrityError,
    SourceUnavailableError,
    UnknownNameError,
    WithheldTableError,
)
from climate_finance.release import IndexEntry, ReleaseIndex
from climate_finance.schema import (
    BTR_WORKBOOKS,
    DATA_FORMAT,
    ReleaseManifest,
    sha256_file,
)

# The fixture packs under a release name of its own, since `fixture` is reserved.
RELEASE = "2026a"
PACK = f"climate-finance-{RELEASE}.tar.zst"
BUILD_TOML = Path(__file__).parents[1] / "build.toml"


def built_release(release_dir: Path, *, sources: tuple[str, ...] = ("crs",)) -> Path:
    """A build's release directory: the fixture as `assemble`, named `RELEASE`, and
    a raw manifest recording `sources` and the BTR archive behind the fixture's BTR
    rows."""
    shutil.copytree(FIXTURE_DIR, release_dir / "assemble")
    path = release_dir / "assemble" / "release.json"
    manifest = ReleaseManifest.model_validate_json(path.read_text())
    path.write_text(manifest.model_copy(update={"release": RELEASE}).model_dump_json())
    sources = (*sources, BTR_SOURCE)
    (release_dir / "fetch").mkdir()
    pl.DataFrame(
        {
            "source": sources,
            "url": [f"https://example.org/{source}" for source in sources],
            "path": [f"{source}.parquet" for source in sources],
            "sha256": ["0" * 64] * len(sources),
            "etag": [None] * len(sources),
            "version": ["fixture"] * len(sources),
            "fetched_at": [datetime(2026, 10, 1, tzinfo=UTC)] * len(sources),
            "method": ["http"] * len(sources),
        }
    ).write_parquet(release_dir / "fetch" / "raw_manifest.parquet")
    return release_dir


PASSED = {"subject": None, "passed": True, "waived": False}


def gated(
    release_dir: Path,
    *,
    rows: dict[str, list[dict[str, object]]] | None = None,
    omit: Collection[str] = (),
    waiver: str | None = None,
) -> Path:
    """`release_dir` with a passed row for every gate in its `release.json`,
    replaced gate by gate by `rows`, without the gates in `omit`, and `waiver`
    recorded."""
    path = release_dir / "assemble" / "release.json"
    manifest = ReleaseManifest.model_validate_json(path.read_text())
    gates = {name: [PASSED] for name in gate.GATES if name not in omit}
    gates["precision"] = [PASSED | {"subject": "4"}]
    path.write_text(
        manifest.model_copy(
            update={
                "gates": gates | (rows or {}),
                "precision_gate_waived": waiver is not None,
                "precision_waiver": waiver,
            }
        ).model_dump_json()
    )
    return release_dir


def members(pack: Path) -> dict[str, bytes]:
    with (
        pa.input_stream(str(pack), compression="zstd") as stream,
        tarfile.open(fileobj=stream, mode="r|") as tar,
    ):
        return {member.name: tar.extractfile(member).read() for member in tar}


@pytest.fixture
def server(tmp_path: Path) -> Iterator[tuple[str, Path]]:
    """A loopback HTTP server that serves and accepts files by PUT in a directory."""
    root = tmp_path / "served"
    root.mkdir()

    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, directory=str(root), **kwargs)

        def do_PUT(self) -> None:
            length = int(self.headers["Content-Length"])
            (root / Path(self.path).name).write_bytes(self.rfile.read(length))
            self.send_response(201)
            self.end_headers()

        def log_message(self, *args: object) -> None:
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as httpd:
        # A short poll lets shutdown return at once at teardown.
        thread = threading.Thread(
            target=httpd.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
        )
        thread.start()
        yield f"http://127.0.0.1:{httpd.server_port}", root
        httpd.shutdown()
        thread.join()


def test_a_pack_holds_the_release_and_its_sources_and_repacks_to_the_same_bytes(
    tmp_path: Path,
) -> None:
    release_dir = built_release(
        tmp_path / RELEASE, sources=("crs", "ledger_iati_44000", "crdf_recipient")
    )
    (release_dir / "assemble" / "stage.json").write_text("{}\n")
    first = release.pack_release(release_dir, out_path=tmp_path / "a" / PACK)
    second = release.pack_release(release_dir, out_path=tmp_path / "b" / PACK)

    packed = members(first)
    assert sha256_file(first) == sha256_file(second)
    # The row-level BTR tables and the stage record stay out; the release tables
    # go in as built.
    assert set(packed) == {
        f"climate-finance-{RELEASE}/{path.name}"
        for path in [*FIXTURE_DIR.iterdir(), Path("sources.json")]
        if path.name not in {"btr.parquet", "btr_links.parquet"}
    }
    built = ReleaseManifest.model_validate_json(
        (release_dir / "assemble" / "release.json").read_text()
    )
    manifest = ReleaseManifest.model_validate_json(
        packed[f"climate-finance-{RELEASE}/release.json"]
    )
    assert manifest.withheld_tables == {
        "btr": release.BTR_WITHHELD_NOTE,
        "btr_links": release.BTR_WITHHELD_NOTE,
    }
    assert manifest.tables == {
        name: entry
        for name, entry in built.tables.items()
        if name not in {"btr", "btr_links"}
    }
    assert manifest.model_dump(exclude={"tables", "withheld_tables"}) == (
        built.model_dump(exclude={"tables", "withheld_tables"})
    )
    sources = json.loads(packed[f"climate-finance-{RELEASE}/sources.json"])
    # The fixture holds IFC's modelled range, so IFC's annual report is listed
    # beside the fetched sources, and the BTR archive once, with the BTR note.
    assert [(row["source"], row["licence"]) for row in sources] == [
        ("crdf_recipient", release.SOURCE_LICENCES["crdf"]),
        ("crs", release.SOURCE_LICENCES["crs"]),
        ("ledger_iati_44000", release.SOURCE_LICENCES["ledger_iati"]),
        (release.REPORTS_SOURCE, release.SOURCE_LICENCES[release.REPORTS_SOURCE]),
        (BTR_SOURCE, release.SOURCE_LICENCES["unfccc"]),
    ]
    assert [row.get("note") for row in sources] == [
        None,
        None,
        None,
        None,
        release.BTR_WITHHELD_NOTE,
    ]
    assert [sources[i]["version"] for i in (1, 4)] == ["fixture", "fixture"]
    assert sources[1]["urls"] == ["https://example.org/crs"]
    assert [citation.split(",")[0] for citation in sources[3]["citations"]] == [
        "IFC Annual Report 2022"
    ] * 2


def test_sources_json_cites_every_report_providers_toml_cites(tmp_path: Path) -> None:
    facts = tomllib.loads(PROVIDERS_TOML.read_text())["modelled"]
    cited = {
        ratio["source"] for entry in facts for ratio in entry.get("report_ratios", [])
    }
    release_dir = built_release(tmp_path / RELEASE)
    modelled = release_dir / "assemble" / "modelled.parquet"
    pl.concat(
        [
            pl.read_parquet(modelled)
            .head(1)
            .with_columns(provider_code=pl.lit(entry["provider_code"], dtype=pl.Int32))
            for entry in facts
        ]
    ).write_parquet(modelled)

    pack = release.pack_release(release_dir, out_path=tmp_path / PACK)

    sources = json.loads(members(pack)[f"climate-finance-{RELEASE}/sources.json"])
    reports = [row for row in sources if row["source"] == release.REPORTS_SOURCE]
    assert {"IFC", "NDB"} <= {citation.split()[0] for citation in cited}
    assert [sorted(row["citations"]) for row in reports] == [sorted(cited)]


def test_sources_list_each_btr_workbook_with_its_rows_kept_and_dropped(
    tmp_path: Path,
) -> None:
    release_dir = built_release(tmp_path / RELEASE)
    workbooks = [
        {
            "file": "Bulgaria__BGR-CTF-FTC-2024-V1.1_started.xlsx",
            "party": "BGR",
            "sheets": [],
            "rows_kept": 0,
            "dropped_blank": 0,
            "dropped_notation": 0,
            "dropped_total": 0,
            "draft_marker": "started",
        },
        {
            "file": "Japan__JPN-CTF-FTC-20241031r.xlsx",
            "party": "JPN",
            "sheets": ["Japan__JPN-CTF-FTC-20241031r.xlsx#Table1_2022"],
            "rows_kept": 4432,
            "dropped_blank": 0,
            "dropped_notation": 37,
            "dropped_total": 1,
            "draft_marker": None,
        },
    ]
    (release_dir / "normalise").mkdir()
    pl.DataFrame(
        workbooks,
        schema={name: column.dtype for name, column in BTR_WORKBOOKS.columns.items()},
    ).write_parquet(release_dir / "normalise" / "btr_workbooks.parquet")

    pack = release.pack_release(release_dir, out_path=tmp_path / PACK)

    sources = json.loads(members(pack)[f"climate-finance-{RELEASE}/sources.json"])
    btr = next(row for row in sources if row["source"] == BTR_SOURCE)
    assert btr["workbooks"] == workbooks


def test_a_published_release_holds_the_btr_totals_and_a_build_the_rows_too(
    tmp_path: Path,
) -> None:
    release_dir = built_release(tmp_path / RELEASE)
    pack = release.pack_release(release_dir, out_path=tmp_path / PACK)
    with (
        pa.input_stream(str(pack), compression="zstd") as stream,
        tarfile.open(fileobj=stream, mode="r|") as tar,
    ):
        tar.extractall(tmp_path / "unpacked", filter="data")

    published = cf.load(path=tmp_path / "unpacked" / f"climate-finance-{RELEASE}")
    built = cf.load(path=release_dir / "assemble")

    for name in ("btr", "btr_links"):
        with pytest.raises(WithheldTableError, match="cf-build build"):
            published.table(name)
        assert built.table(name).collect().height > 0
    totals = published.table("btr_totals").collect()
    assert totals.height > 0
    assert totals.equals(built.table("btr_totals").collect())


def test_a_source_without_a_licence_stops_the_pack(tmp_path: Path) -> None:
    release_dir = built_release(tmp_path / RELEASE, sources=("crs", "mystery"))

    with pytest.raises(BuildError, match=r"\['mystery'\]"):
        release.pack_release(release_dir, out_path=tmp_path / PACK)
    assert not (tmp_path / PACK).exists()


def test_btr_rows_without_the_btr_archive_stop_the_pack(tmp_path: Path) -> None:
    release_dir = built_release(tmp_path / RELEASE)
    raw = release_dir / "fetch" / "raw_manifest.parquet"
    pl.read_parquet(raw).filter(pl.col("source") != BTR_SOURCE).write_parquet(raw)

    with pytest.raises(BuildError, match=BTR_SOURCE):
        release.pack_release(release_dir, out_path=tmp_path / PACK)


@pytest.mark.parametrize("waiver", [None, "labels arrive after the cut"])
def test_cut_release_gates_records_the_results_and_the_waiver_then_packs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, waiver: str | None
) -> None:
    waive = waiver is not None
    calls = []
    config = Path(shutil.copy(BUILD_TOML, tmp_path / "build.toml"))

    def fake_build(name: str, *, config: Path, out_dir: Path) -> None:
        built_release(out_dir / name)

    def fake_gates(
        release_dir: Path,
        *,
        decisions: Path,
        sample: Path | None,
        previous: Path | None,
        first_release: bool,
        waive_precision: bool,
    ) -> pl.DataFrame:
        calls.append((release_dir, decisions, sample, previous, waive_precision))
        return pl.DataFrame({"gate": ["precision", "drift"], "passed": [waive, True]})

    monkeypatch.setattr(release.build_package, "build", fake_build)
    monkeypatch.setattr(gate, "run_gates", fake_gates)

    pack = release.cut_release(
        RELEASE,
        config=config,
        out_dir=tmp_path,
        sample=tmp_path / "sample.parquet",
        previous=FIXTURE_DIR,
        waive_precision=waiver,
    )

    assert pack == tmp_path / PACK
    assert calls == [
        (
            tmp_path / RELEASE,
            tmp_path / "data" / "review_decisions.parquet",
            tmp_path / "sample.parquet",
            FIXTURE_DIR,
            waive,
        )
    ]
    manifest = ReleaseManifest.model_validate_json(
        members(pack)[f"climate-finance-{RELEASE}/release.json"]
    )
    assert manifest.precision_gate_waived is waive
    assert manifest.precision_waiver == waiver
    assert manifest.gates == {
        "precision": [{"passed": waive}],
        "drift": [{"passed": True}],
    }
    assert manifest.previous_release == cf.load(path=FIXTURE_DIR).manifest.release


def test_a_failed_gate_stops_cut_release_before_the_pack(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def failed_gates(release_dir: Path, **options: object) -> pl.DataFrame:
        raise GateFailedError(["precision 4: fail after 50 labels (pps)"])

    monkeypatch.setattr(
        release.build_package,
        "build",
        lambda name, *, config, out_dir: built_release(out_dir / name),
    )
    monkeypatch.setattr(gate, "run_gates", failed_gates)

    with pytest.raises(GateFailedError, match="precision"):
        release.cut_release(
            RELEASE,
            config=Path(shutil.copy(BUILD_TOML, tmp_path / "build.toml")),
            out_dir=tmp_path,
        )
    assert not (tmp_path / PACK).exists()


@pytest.mark.parametrize(
    ("rows", "waiver", "refused"),
    [
        ({}, None, None),
        ({"human_audit": [PASSED | {"passed": False, "waived": True}]}, "late", None),
        ({"human_audit": [PASSED | {"passed": False}]}, None, "human_audit: failed"),
        (
            {"human_audit": [PASSED | {"passed": False, "waived": True}]},
            None,
            "no recorded reason",
        ),
        ({"human_audit": []}, None, "human_audit: release.json records no result"),
        ({"drift": [PASSED | {"subject": "DEU", "passed": False}]}, None, "drift DEU"),
        (
            {"drift": [PASSED | {"passed": False, "waived": True}]},
            "late",
            "drift: waived, but only",
        ),
    ],
)
def test_upload_and_index_take_only_a_release_whose_gates_passed_or_were_waived(
    tmp_path: Path,
    rows: dict[str, list[dict[str, object]]],
    waiver: str | None,
    refused: str | None,
) -> None:
    release_dir = gated(built_release(tmp_path / RELEASE), rows=rows, waiver=waiver)
    pack = release.pack_release(release_dir, out_path=tmp_path / PACK)
    uploads = tmp_path / "uploads"

    def upload() -> str:
        return release.upload_release(pack, target=str(uploads))

    def index() -> ReleaseIndex:
        return release.add_to_index(
            ReleaseIndex(), release=RELEASE, pack=pack, url="file:///p"
        )

    if refused is None:
        upload()
        assert index().latest == RELEASE
    else:
        for publish in (upload, index):
            with pytest.raises(GateFailedError, match=refused):
                publish()
        assert not uploads.exists()


@pytest.mark.parametrize(
    ("omit", "emptied"),
    [
        (set(gate.GATES) - {"precision", "human_audit"}, set()),
        ({"conservation"}, set()),
        ({"drift", "withheld_rows"}, set()),
        (set(), {"listed_codes"}),
        ({"over_allocation"}, {"oecd_reconciliation"}),
    ],
    ids=["only-waivable-gates", "one-omitted", "two-omitted", "empty", "mixed"],
)
def test_a_pack_is_unpublishable_for_every_gate_with_no_result(
    tmp_path: Path, omit: set[str], emptied: set[str]
) -> None:
    missing = omit | emptied
    release_dir = gated(
        built_release(tmp_path / RELEASE),
        rows={name: [] for name in emptied},
        omit=omit,
    )
    pack = release.pack_release(release_dir, out_path=tmp_path / PACK)
    uploads = tmp_path / "uploads"

    def reported(problems: list[str]) -> set[str]:
        return {problem.split(":")[0] for problem in problems}

    assert reported(release.unpublishable(release.packed_manifest(pack))) == missing
    for publish in (
        lambda: release.upload_release(pack, target=str(uploads)),
        lambda: release.add_to_index(
            ReleaseIndex(), release=RELEASE, pack=pack, url="file:///p"
        ),
    ):
        with pytest.raises(GateFailedError) as refused:
            publish()
        assert reported(refused.value.failures) == missing
    assert not uploads.exists()


def test_the_index_refuses_a_pack_under_another_release_name(tmp_path: Path) -> None:
    pack = release.pack_release(
        gated(built_release(tmp_path / RELEASE)), out_path=tmp_path / PACK
    )
    with pytest.raises(BuildError, match="holds release '2026a', not 'other'"):
        release.add_to_index(
            ReleaseIndex(), release="other", pack=pack, url="file:///p"
        )


def test_the_reserved_fixture_name_is_never_cut_uploaded_or_indexed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    release_dir = gated(built_release(tmp_path / RELEASE))
    path = release_dir / "assemble" / "release.json"
    manifest = ReleaseManifest.model_validate_json(path.read_text())
    path.write_text(
        manifest.model_copy(update={"release": "fixture"}).model_dump_json()
    )
    pack = release.pack_release(release_dir, out_path=tmp_path / "fixture.tar.zst")
    monkeypatch.setattr(release.build_package, "build", pytest.fail)

    refusals = (
        lambda: release.upload_release(pack, target=str(tmp_path / "uploads")),
        lambda: release.add_to_index(
            ReleaseIndex(), release="fixture", pack=pack, url="file:///p"
        ),
        lambda: release.cut_release("fixture", config=BUILD_TOML, out_dir=tmp_path),
    )
    for refused in refusals:
        with pytest.raises(BuildError, match="'fixture' is reserved"):
            refused()
    assert not (tmp_path / "uploads").exists()


def test_upload_copies_into_a_directory_or_puts_to_a_server(
    tmp_path: Path, server: tuple[str, Path]
) -> None:
    url, served = server
    pack = release.pack_release(
        gated(built_release(tmp_path / RELEASE)), out_path=tmp_path / PACK
    )

    local = release.upload_release(pack, target=str(tmp_path / "uploads"))
    remote = release.upload_release(pack, target=f"{url}/assets/")

    assert local == (tmp_path / "uploads" / PACK).as_uri()
    assert sha256_file(tmp_path / "uploads" / PACK) == sha256_file(pack)
    assert remote == f"{url}/assets/{PACK}"
    assert sha256_file(served / PACK) == sha256_file(pack)


@pytest.fixture
def published(
    tmp_path: Path, server: tuple[str, Path], monkeypatch: pytest.MonkeyPatch
) -> tuple[ReleaseIndex, Path]:
    """The fixture release packed, uploaded to `server` and listed in a shipped index
    and in the index online. Returns that index and the served directory."""
    url, served = server
    pack = release.pack_release(
        gated(built_release(tmp_path / RELEASE)), out_path=tmp_path / PACK
    )
    index = release.add_to_index(
        ReleaseIndex(),
        release=RELEASE,
        pack=pack,
        url=release.upload_release(pack, target=url),
    )
    shipped = tmp_path / "releases.json"
    shipped.write_text(index.model_dump_json())
    (served / "releases.json").write_text(index.model_dump_json())
    monkeypatch.setattr(release_module, "RELEASE_INDEX", shipped)
    monkeypatch.setattr(release_module, "INDEX_URL", f"{url}/releases.json")
    return index, served


def test_load_downloads_verifies_and_then_runs_offline(
    tmp_path: Path,
    published: tuple[ReleaseIndex, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = tmp_path / "cache"
    expected = cf.load(path=FIXTURE_DIR).spending().table

    loaded = cf.load(cache_dir=cache)

    assert loaded.path == cache / f"climate-finance-{RELEASE}"
    assert sorted(path.name for path in cache.iterdir()) == [
        f"climate-finance-{RELEASE}"
    ]

    def offline(*args: object, **kwargs: object) -> None:
        raise AssertionError("network after download")

    monkeypatch.setattr(release_module.urllib.request, "urlopen", offline)
    assert loaded.spending().table.equals(expected)
    again = cf.load(RELEASE, cache_dir=cache)
    assert again.spending().table.equals(expected)


def test_refresh_reads_the_index_online(
    tmp_path: Path,
    published: tuple[ReleaseIndex, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(release_module, "RELEASE_INDEX", tmp_path / "missing.json")

    loaded = cf.load(cache_dir=tmp_path / "cache", refresh=True)

    assert loaded.manifest.release == RELEASE


def test_a_pack_whose_sha256_differs_from_the_index_is_refused(
    tmp_path: Path, published: tuple[ReleaseIndex, Path]
) -> None:
    _, served = published
    (served / PACK).write_bytes((served / PACK).read_bytes()[:-1] + b"\0")
    cache = tmp_path / "cache"

    with pytest.raises(ReleaseIntegrityError, match="the release index has"):
        cf.load(RELEASE, cache_dir=cache)
    assert list(cache.iterdir()) == []


def test_a_cached_release_that_fails_its_check_downloads_again(
    tmp_path: Path, published: tuple[ReleaseIndex, Path]
) -> None:
    cache = tmp_path / "cache"
    cf.load(cache_dir=cache)
    table = cache / f"climate-finance-{RELEASE}" / "provider_coefficients.parquet"
    table.write_bytes(b"corrupt")

    loaded = cf.load(cache_dir=cache)

    assert sha256_file(table) == loaded.manifest.tables["provider_coefficients"].sha256


def test_a_cached_release_from_another_pack_downloads_again(
    tmp_path: Path,
    published: tuple[ReleaseIndex, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    index, _ = published
    cache = tmp_path / "cache"
    stamp = cf.load(cache_dir=cache).path / release_module.PACK_SHA256
    # The cache holds a pack the index no longer names, as after a re-cut.
    stamp.write_text("0" * 64)
    downloads = []
    fetch = release_module._fetch

    def counted(url: str, *, out_path: Path) -> str:
        downloads.append(url)
        return fetch(url, out_path=out_path)

    monkeypatch.setattr(release_module, "_fetch", counted)

    cf.load(cache_dir=cache)

    assert downloads == [index.releases[RELEASE].url]
    assert stamp.read_text() == index.releases[RELEASE].sha256


@pytest.mark.parametrize("winner", ["same-pack", "other-pack"])
def test_two_loads_racing_on_one_release_both_open_it(
    tmp_path: Path,
    published: tuple[ReleaseIndex, Path],
    monkeypatch: pytest.MonkeyPatch,
    winner: str,
) -> None:
    index, _ = published
    cache = tmp_path / "cache"
    fetch = release_module._fetch
    raced: list[cf.Release] = []

    def racing(url: str, *, out_path: Path) -> str:
        # Another process finishes its own load between this download and rename.
        monkeypatch.setattr(release_module, "_fetch", fetch)
        raced.append(cf.load(cache_dir=cache))
        if winner == "other-pack":  # as from an index listing a re-cut
            (raced[0].path / release_module.PACK_SHA256).write_text("f" * 64)
        return fetch(url, out_path=out_path)

    monkeypatch.setattr(release_module, "_fetch", racing)

    loaded = cf.load(cache_dir=cache)

    assert loaded.path == raced[0].path
    assert [path.name for path in cache.iterdir()] == [f"climate-finance-{RELEASE}"]
    stamp = loaded.path / release_module.PACK_SHA256
    assert stamp.read_text() == index.releases[RELEASE].sha256


@pytest.mark.parametrize("other", ["moved-it-away", "replaced-it"])
def test_a_stale_copy_another_load_handles_first_is_left_to_it(
    tmp_path: Path,
    published: tuple[ReleaseIndex, Path],
    monkeypatch: pytest.MonkeyPatch,
    other: str,
) -> None:
    cache = tmp_path / "cache"
    target = cf.load(cache_dir=cache).path
    (target / release_module.PACK_SHA256).write_text("0" * 64)
    packed = release_module._packed_sha256
    fetch = release_module._fetch
    downloads = []

    def counted(url: str, *, out_path: Path) -> str:
        downloads.append(url)
        return fetch(url, out_path=out_path)

    def checked(path: Path) -> str | None:
        # Another load acts on the stale copy between this check and the discard.
        monkeypatch.setattr(release_module, "_packed_sha256", packed)
        stale = packed(path)
        if other == "moved-it-away":
            shutil.rmtree(path)
        else:
            cf.load(cache_dir=cache)
        downloads.clear()
        return stale

    monkeypatch.setattr(release_module, "_fetch", counted)
    monkeypatch.setattr(release_module, "_packed_sha256", checked)

    loaded = cf.load(cache_dir=cache)

    # A fresh copy another load placed is kept, not downloaded again.
    assert len(downloads) == (1 if other == "moved-it-away" else 0)
    assert loaded.spending().table.height > 0
    assert [path.name for path in cache.iterdir()] == [f"climate-finance-{RELEASE}"]


def test_refresh_reads_an_index_with_fields_this_package_does_not_know(
    tmp_path: Path, published: tuple[ReleaseIndex, Path]
) -> None:
    index, served = published
    newer = json.loads(index.model_dump_json())
    newer["generated"] = "2027-01-01"
    newer["releases"][RELEASE]["requires"] = ">=2.1"
    (served / "releases.json").write_text(json.dumps(newer))

    loaded = cf.load(cache_dir=tmp_path / "cache", refresh=True)

    assert loaded.manifest.release == RELEASE


@pytest.mark.parametrize("body", [b"<html>Not Found</html>", b'{"releases": []}'])
def test_a_malformed_index_online_is_a_source_error(
    tmp_path: Path, published: tuple[ReleaseIndex, Path], body: bytes
) -> None:
    _, served = published
    (served / "releases.json").write_bytes(body)

    with pytest.raises(SourceUnavailableError) as raised:
        cf.load(cache_dir=tmp_path / "cache", refresh=True)
    assert isinstance(raised.value.__cause__, ValidationError)


def test_the_index_url_is_on_the_fixed_data_tag() -> None:
    # GitHub's "latest" release follows code releases, which carry no index.
    assert "/releases/download/data/" in release_module.INDEX_URL


def test_load_names_an_unlisted_release_and_an_unreachable_pack(
    tmp_path: Path,
    published: tuple[ReleaseIndex, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    index, _ = published
    # A release newer than the shipped index is online only, so refresh is named.
    # Suggestions from the shipped index would name only older releases.
    with pytest.raises(UnknownNameError) as shipped:
        cf.load("2026", cache_dir=tmp_path / "cache")
    with pytest.raises(UnknownNameError) as online:
        cf.load("2026", cache_dir=tmp_path / "cache", refresh=True)
    assert shipped.value.suggestions == ()
    assert "refresh=True" in str(shipped.value)
    assert online.value.suggestions == (RELEASE,)
    assert "refresh=True" not in str(online.value)

    gone = index.model_copy(
        update={
            "releases": {
                RELEASE: IndexEntry(
                    url="http://127.0.0.1:9/gone",
                    sha256="0" * 64,
                    data_format=DATA_FORMAT,
                )
            }
        }
    )
    (tmp_path / "releases.json").write_text(gone.model_dump_json())
    with pytest.raises(SourceUnavailableError, match="Could not download"):
        cf.load(cache_dir=tmp_path / "cache")


def test_the_shipped_index_parses() -> None:
    assert isinstance(release_module._shipped_index(), ReleaseIndex)

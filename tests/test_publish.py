"""Packing, gating and uploading a release, and `load` downloading it into a cache.

The round trip runs in the fast suite: it packs the fixture release of a few hundred
kB and serves it from a server on loopback, which the socket guard allows, so it
reaches no outside network and takes well under a second.
"""

import json
import shutil
import tarfile
import threading
from collections.abc import Iterator
from datetime import UTC, datetime
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import polars as pl
import pyarrow as pa
import pytest
from make_fixture import FIXTURE_DIR

import climate_finance as cf
from climate_finance import snapshot
from climate_finance.build import gate, release
from climate_finance.build.errors import BuildError
from climate_finance.errors import (
    ReleaseIntegrityError,
    SourceUnavailableError,
    UnknownNameError,
)
from climate_finance.schema import ReleaseManifest, sha256_file
from climate_finance.snapshot import IndexEntry, ReleaseIndex

PACK = "climate-finance-fixture.tar.zst"


def built_release(release_dir: Path, *, sources: tuple[str, ...] = ("crs",)) -> Path:
    """A build's release directory: the fixture as `assemble`, and a raw manifest
    recording `sources`."""
    shutil.copytree(FIXTURE_DIR, release_dir / "assemble")
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
        tmp_path / "fixture", sources=("crs", "ledger_iati_44000", "crdf_recipient")
    )
    first = release.pack_release(release_dir, out_path=tmp_path / "a" / PACK)
    second = release.pack_release(release_dir, out_path=tmp_path / "b" / PACK)

    packed = members(first)
    assert sha256_file(first) == sha256_file(second)
    assert set(packed) == {
        f"climate-finance-fixture/{path.name}"
        for path in [*FIXTURE_DIR.iterdir(), Path("sources.json")]
    }
    assert packed["climate-finance-fixture/release.json"] == (
        (FIXTURE_DIR / "release.json").read_bytes()
    )
    sources = json.loads(packed["climate-finance-fixture/sources.json"])
    # The fixture holds BTR rows, so `btr` is listed beside the fetched sources.
    assert [(row["source"], row["licence"]) for row in sources] == [
        ("btr", release.SOURCE_LICENCES["btr"]),
        ("crdf_recipient", release.SOURCE_LICENCES["crdf"]),
        ("crs", release.SOURCE_LICENCES["crs"]),
        ("ledger_iati_44000", release.SOURCE_LICENCES["ledger_iati"]),
    ]
    assert sources[2]["version"] == "fixture"
    assert sources[2]["urls"] == ["https://example.org/crs"]


def test_a_source_without_a_licence_stops_the_pack(tmp_path: Path) -> None:
    release_dir = built_release(tmp_path / "fixture", sources=("crs", "mystery"))

    with pytest.raises(BuildError, match=r"\['mystery'\]"):
        release.pack_release(release_dir, out_path=tmp_path / PACK)
    assert not (tmp_path / PACK).exists()


@pytest.mark.parametrize("waive", [False, True])
def test_cut_release_gates_records_the_results_and_the_waiver_then_packs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, waive: bool
) -> None:
    calls = []

    def fake_build(name: str, *, config: Path, out_dir: Path) -> None:
        built_release(out_dir / name)

    def fake_gates(release_dir: Path, *, waive_precision: bool) -> pl.DataFrame:
        calls.append((release_dir, waive_precision))
        return pl.DataFrame({"gate": ["precision"], "passed": [waive]})

    monkeypatch.setattr(release.build_package, "build", fake_build)
    monkeypatch.setattr(gate, "run_gates", fake_gates)

    pack = release.cut_release(
        "fixture",
        config=tmp_path / "build.toml",
        out_dir=tmp_path,
        waive_precision=waive,
    )

    assert pack == tmp_path / PACK
    assert calls == [(tmp_path / "fixture", waive)]
    manifest = ReleaseManifest.model_validate_json(
        members(pack)["climate-finance-fixture/release.json"]
    )
    assert manifest.precision_gate_waived is waive
    assert manifest.gates == {"results": [{"gate": "precision", "passed": waive}]}


def test_a_failed_gate_stops_cut_release_before_the_pack(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def failed_gates(release_dir: Path, *, waive_precision: bool) -> pl.DataFrame:
        raise BuildError("precision gate failed")

    monkeypatch.setattr(
        release.build_package,
        "build",
        lambda name, *, config, out_dir: built_release(out_dir / name),
    )
    monkeypatch.setattr(gate, "run_gates", failed_gates)

    with pytest.raises(BuildError, match="precision"):
        release.cut_release("fixture", config=tmp_path / "build.toml", out_dir=tmp_path)
    assert not (tmp_path / PACK).exists()


def test_upload_copies_into_a_directory_or_puts_to_a_server(
    tmp_path: Path, server: tuple[str, Path]
) -> None:
    url, served = server
    pack = release.pack_release(
        built_release(tmp_path / "fixture"), out_path=tmp_path / PACK
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
        built_release(tmp_path / "fixture"), out_path=tmp_path / PACK
    )
    index = release.add_to_index(
        ReleaseIndex(),
        release="fixture",
        pack=pack,
        url=release.upload_release(pack, target=url),
    )
    shipped = tmp_path / "releases.json"
    shipped.write_text(index.model_dump_json())
    (served / "releases.json").write_text(index.model_dump_json())
    monkeypatch.setattr(snapshot, "RELEASE_INDEX", shipped)
    monkeypatch.setattr(snapshot, "INDEX_URL", f"{url}/releases.json")
    return index, served


def test_load_downloads_verifies_and_then_runs_offline(
    tmp_path: Path,
    published: tuple[ReleaseIndex, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = tmp_path / "cache"
    expected = cf.load(path=FIXTURE_DIR).spending().table.to_polars()

    snap = cf.load(cache_dir=cache)

    assert snap.path == cache / "climate-finance-fixture"
    assert sorted(path.name for path in cache.iterdir()) == ["climate-finance-fixture"]

    def offline(*args: object, **kwargs: object) -> None:
        raise AssertionError("network after download")

    monkeypatch.setattr(snapshot.urllib.request, "urlopen", offline)
    assert snap.spending().table.to_polars().equals(expected)
    again = cf.load("fixture", cache_dir=cache)
    assert again.spending().table.to_polars().equals(expected)


def test_refresh_reads_the_index_online(
    tmp_path: Path,
    published: tuple[ReleaseIndex, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(snapshot, "RELEASE_INDEX", tmp_path / "missing.json")

    snap = cf.load(cache_dir=tmp_path / "cache", refresh=True)

    assert snap.manifest.release == "fixture"


def test_a_pack_whose_sha256_differs_from_the_index_is_refused(
    tmp_path: Path, published: tuple[ReleaseIndex, Path]
) -> None:
    _, served = published
    (served / PACK).write_bytes((served / PACK).read_bytes()[:-1] + b"\0")
    cache = tmp_path / "cache"

    with pytest.raises(ReleaseIntegrityError, match="the release index has"):
        cf.load("fixture", cache_dir=cache)
    assert list(cache.iterdir()) == []


def test_a_cached_release_that_fails_its_check_downloads_again(
    tmp_path: Path, published: tuple[ReleaseIndex, Path]
) -> None:
    cache = tmp_path / "cache"
    cf.load(cache_dir=cache)
    table = cache / "climate-finance-fixture" / "btr.parquet"
    table.write_bytes(b"corrupt")

    snap = cf.load(cache_dir=cache)

    assert sha256_file(table) == snap.manifest.tables["btr"].sha256


def test_load_names_an_unlisted_release_and_an_unreachable_pack(
    tmp_path: Path,
    published: tuple[ReleaseIndex, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    index, _ = published
    with pytest.raises(UnknownNameError, match="Did you mean: fixture"):
        cf.load("fixtur", cache_dir=tmp_path / "cache")

    gone = index.model_copy(
        update={
            "releases": {
                "fixture": IndexEntry(url="http://127.0.0.1:9/gone", sha256="0" * 64)
            }
        }
    )
    (tmp_path / "releases.json").write_text(gone.model_dump_json())
    with pytest.raises(SourceUnavailableError, match="Could not download"):
        cf.load(cache_dir=tmp_path / "cache")


def test_the_shipped_index_parses() -> None:
    assert isinstance(snapshot._shipped_index(), ReleaseIndex)

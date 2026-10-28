"""Packing, gating and publishing a release, and `load` downloading it into a cache.

Publishing runs against `FakeGitHub`, an in-memory stand-in for `gh`, so no test
reaches GitHub. The round trip runs in the fast suite: it packs the fixture release
of a few hundred kB and serves it from a server on loopback, which the socket guard
allows, so it reaches no outside network and takes well under a second.
"""

import hashlib
import json
import re
import shutil
import subprocess
import tarfile
import threading
import tomllib
from collections.abc import Callable, Collection, Iterator, Sequence
from datetime import UTC, datetime
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import httpx
import polars as pl
import pyarrow as pa
import pytest
from make_fixture import FIXTURE_DIR
from normalised import IATI_PUBLISHERS, record_fetch, write_raw
from pydantic import ValidationError
from typer.testing import CliRunner

import climate_finance as cf
import climate_finance.build
from climate_finance import release as release_module
from climate_finance.build import fetch, gate, publish, release
from climate_finance.build.cli import TRACEBACK_VAR, app
from climate_finance.build.config import PUBLISHED_SETTINGS, BuildConfig, load_config
from climate_finance.build.errors import (
    AlreadyPublishedError,
    BuildError,
    DraftReleaseError,
    DriftBaselineError,
    GateFailedError,
    GhError,
    IndexReplacedError,
    MissingIndexError,
    PublishedIndexError,
    ReleaseNameError,
    RepositoryNameError,
    SyntheticReleaseError,
    UnpublishedSettingsError,
)
from climate_finance.build.fetch import BTR_SOURCE
from climate_finance.build.jev_client import API_KEY_ENV, JevClient
from climate_finance.build.measure import PROVIDERS_TOML
from climate_finance.build.provenance import setting_value
from climate_finance.build.stages import (
    REGISTRY,
    Stage,
    StageContext,
    StageDef,
    raw_dir,
)
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
    Column,
    ReleaseManifest,
    TableSchema,
    sha256_file,
)

# The fixture packs under a release name of its own, since `fixture` is reserved.
RELEASE = "2026a"
PACK = f"climate-finance-{RELEASE}.tar.zst"
BUILD_TOML = Path(__file__).parent / "fixtures" / "build.toml"


def release_toml(directory: Path) -> Path:
    """The fixture's `build.toml` in `directory` with the review stage and Jev
    flags on, so it holds `PUBLISHED_SETTINGS` and `cut_release` takes it. Relative
    paths in it resolve against `directory`."""
    path = directory / "build.toml"
    path.write_text(
        BUILD_TOML.read_text(encoding="utf-8").replace(
            "review = false", "review = true\njev_flags = true"
        ),
        encoding="utf-8",
    )
    return path


def built_release(release_dir: Path, *, sources: tuple[str, ...] = ("crs",)) -> Path:
    """A build's release directory: the fixture as `assemble`, named `RELEASE`, and
    a raw manifest recording `sources` and the BTR archive behind the fixture's BTR
    rows."""
    shutil.copytree(FIXTURE_DIR, release_dir / "assemble")
    path = release_dir / "assemble" / "release.json"
    manifest = ReleaseManifest.model_validate_json(path.read_text(encoding="utf-8"))
    path.write_text(
        manifest.model_copy(update={"release": RELEASE}).model_dump_json(),
        encoding="utf-8",
    )
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
    previous: str | None = None,
    previous_sha256: str | None = "0" * 64,
    settings: dict[str, object] | None = None,
) -> Path:
    """`release_dir` with a passed row for every gate in its `release.json`,
    replaced gate by gate by `rows`, without the gates in `omit`, `waiver` and the
    `previous` release with its pack's `previous_sha256` recorded (the sha256
    `indexed` lists), and a published release's settings, replaced setting by
    setting by `settings`."""
    path = release_dir / "assemble" / "release.json"
    manifest = ReleaseManifest.model_validate_json(path.read_text(encoding="utf-8"))
    gates = {name: [PASSED] for name in gate.GATES if name not in omit}
    gates["precision"] = [PASSED | {"subject": "4"}]
    path.write_text(
        manifest.model_copy(
            update={
                "gates": gates | (rows or {}),
                "precision_gate_waived": waiver is not None,
                "precision_waiver": waiver,
                "previous_release": previous,
                "previous_pack_sha256": None if previous is None else previous_sha256,
                "settings": manifest.settings | PUBLISHED_SETTINGS | (settings or {}),
            }
        ).model_dump_json(),
        encoding="utf-8",
    )
    return release_dir


class FakeGitHub:
    """`gh` against one repository held in memory: the assets of its `data`
    release, or None while it has none, beside a code release. Records each call,
    and runs `after_write` after each call that changes the repository."""

    def __init__(
        self,
        assets: dict[str, bytes] | None = None,
        *,
        draft: bool = False,
        after_write: Callable[["FakeGitHub"], None] | None = None,
    ) -> None:
        self.assets = assets
        self.ids = {name: number for number, name in enumerate(assets or {})}
        self.draft = draft
        self.after_write = after_write
        self.calls: list[tuple[str, ...]] = []

    def __call__(self, args: Sequence[str]) -> str:
        call = tuple(args)
        self.calls.append(call)
        assert call[-2:] == ("--repo", publish.PUBLISH_REPO) or call[0] == "api"
        if call[:2] == ("release", "list"):
            data = [] if self.assets is None else [publish.DATA_TAG]
            return json.dumps(
                [{"tagName": "v1.2.3", "isDraft": False}]
                + [{"tagName": tag, "isDraft": self.draft} for tag in data]
            )
        if call[:2] == ("release", "create"):
            assert self.assets is None
            self.assets = {}
        assert self.assets is not None
        match call[:2]:
            case ("release", "view"):
                api = f"https://api.github.com/repos/{publish.PUBLISH_REPO}"
                return json.dumps(
                    {
                        "assets": [
                            {"name": name, "apiUrl": f"{api}/releases/assets/{number}"}
                            for name, number in self.ids.items()
                            if name in self.assets
                        ]
                    }
                )
            case ("release", "download"):
                return self.assets[call[4]].decode()
            case ("release", "upload"):
                name = Path(call[3]).name
                self.assets[name] = Path(call[3]).read_bytes()
                self.ids[name] = len(self.ids)
            case ("release", "delete-asset"):
                del self.assets[call[3]]
            case ("api", "--method"):
                number = int(call[3].rsplit("/", 1)[1])
                old = next(name for name, i in self.ids.items() if i == number)
                new = call[5].removeprefix("name=")
                assert new not in self.assets
                self.assets[new] = self.assets.pop(old)
                self.ids[new] = number
        if call in self.writes and self.after_write is not None:
            self.after_write(self)
        return ""

    @property
    def writes(self) -> list[tuple[str, ...]]:
        """The calls that change the repository."""
        return [
            call
            for call in self.calls
            if call[0] == "api" or call[1] in {"create", "upload", "delete-asset"}
        ]

    def index(self) -> dict[str, Any]:
        assert self.assets is not None
        return json.loads(self.assets[publish.INDEX_ASSET])


def indexed(*releases: str, latest: str | None = None) -> dict[str, bytes]:
    """The assets of a `data` release whose index lists `releases`, the newest of
    them latest unless `latest` is given."""
    index = {
        "latest": latest or (max(releases) if releases else None),
        "releases": {
            name: {
                "url": publish.asset_url(f"climate-finance-{name}.tar.zst"),
                "sha256": "0" * 64,
                "data_format": DATA_FORMAT,
            }
            for name in releases
        },
    }
    return {publish.INDEX_ASSET: json.dumps(index).encode()}


def members(pack: Path) -> dict[str, bytes]:
    with (
        pa.input_stream(str(pack), compression="zstd") as stream,
        tarfile.open(fileobj=stream, mode="r|") as tar,
    ):
        return {member.name: tar.extractfile(member).read() for member in tar}


@pytest.fixture
def server(tmp_path: Path) -> Iterator[tuple[str, Path]]:
    """A loopback HTTP server that serves the files in a directory."""
    root = tmp_path / "served"
    root.mkdir()

    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, directory=str(root), **kwargs)

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
    (release_dir / "assemble" / "stage.json").write_text("{}\n", encoding="utf-8")
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
        (release_dir / "assemble" / "release.json").read_text(encoding="utf-8")
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
    facts = tomllib.loads(PROVIDERS_TOML.read_text(encoding="utf-8"))["modelled"]
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
    config = release_toml(tmp_path)

    def fake_build(
        name: str, *, config: BuildConfig, out_dir: Path, reuse_sources: bool
    ) -> None:
        built_release(out_dir / name)

    def fake_gates(
        release_dir: Path,
        *,
        decisions: Path,
        sample: Path | None,
        previous: cf.Release | None,
        first_release: bool,
        drift_causes: object,
        waive_precision: bool,
    ) -> pl.DataFrame:
        assert previous is not None
        calls.append((release_dir, decisions, sample, previous.path, waive_precision))
        return pl.DataFrame({"gate": ["precision", "drift"], "passed": [waive, True]})

    monkeypatch.setattr(release.build_package, "build", fake_build)
    monkeypatch.setattr(gate, "run_gates", fake_gates)
    previous = built_release(tmp_path / "previous")

    pack = release.cut_release(
        RELEASE,
        config=config,
        out_dir=tmp_path,
        sample=tmp_path / "sample",
        previous=previous,
        waive_precision=waiver,
    )

    assert pack == tmp_path / PACK
    assert calls == [
        (
            tmp_path / RELEASE,
            tmp_path / "data" / "review_decisions.parquet",
            tmp_path / "sample",
            previous / "assemble",
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
    # A build's directory names the release its `assemble/` holds, and with no pack
    # beside it records none.
    assert manifest.previous_release == RELEASE
    assert manifest.previous_pack_sha256 is None


@pytest.mark.parametrize(
    ("configured", "fetched", "offline", "expected"),
    [
        # (config.offline, reuse_sources) the build receives.
        (False, False, None, (False, False)),
        (False, True, None, (False, True)),
        (True, False, None, (True, False)),
        (True, True, None, (True, True)),
        (False, True, False, (False, False)),
        (True, False, False, (False, False)),
        (False, True, True, (True, False)),
    ],
)
def test_cut_release_reuses_the_sources_an_earlier_build_fetched(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    configured: bool,
    fetched: bool,
    offline: bool | None,
    expected: tuple[bool, bool],
) -> None:
    config = release_toml(tmp_path)
    config.write_text(
        config.read_text(encoding="utf-8").replace(
            "offline = false", f"offline = {str(configured).lower()}"
        ),
        encoding="utf-8",
    )
    if fetched:
        (tmp_path / RELEASE / "fetch").mkdir(parents=True)
    builds = []

    def fake_build(
        name: str, *, config: BuildConfig, out_dir: Path, reuse_sources: bool
    ) -> None:
        builds.append((config.offline, reuse_sources))
        raise BuildError("built")

    monkeypatch.setattr(release.build_package, "build", fake_build)

    with pytest.raises(BuildError, match="built"):
        release.cut_release(RELEASE, config=config, out_dir=tmp_path, offline=offline)
    assert builds == [expected]


def test_a_rerun_cut_reuses_the_fetched_sources_and_asks_jev_new_questions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A build fetched its sources and the sample was labelled since, so the link
    # stage asks Jev a question its cache does not hold.
    out_dir = tmp_path / "out"
    config = load_config(release_toml(tmp_path))
    config = config.model_copy(
        update={
            "sources": config.sources.model_copy(
                update={
                    "crs_file": None,
                    "iati_publishers": IATI_PUBLISHERS,
                    "btr_archive": None,
                }
            )
        }
    )
    record_fetch(
        out_dir / RELEASE, write_raw(raw_dir(out_dir / RELEASE)), config=config
    )
    monkeypatch.setattr(fetch, "new_client", pytest.fail)
    sent: list[httpx.Request] = []

    def answer(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        return httpx.Response(
            200,
            json={
                "model": config.jev.model,
                "answers": {"sibling": {"type": "noul", "noul": 0.5}},
                "usage": {"input_tokens": 10, "output_tokens": 1},
            },
        )

    def fake_jev(config: BuildConfig, *, offline: bool) -> JevClient:
        return JevClient(
            model=config.jev.model,
            cost_cap_usd=1.0,
            cache_dir=tmp_path / "jev-cache",
            offline=offline,
            transport=httpx.MockTransport(answer),
        )

    def link(context: StageContext) -> dict[str, pl.DataFrame]:
        context.read(Stage.FETCH, "raw_manifest")
        context.jev.score([{"inputs": {"pair": "new"}, "questions": {}}])
        return {"numbers": pl.DataFrame({"key": ["a"], "value": [1.0]})}

    [fetch_stage] = (stage for stage in REGISTRY if stage.name == Stage.FETCH)
    numbers = TableSchema(
        name="numbers",
        columns={"key": Column(pl.String()), "value": Column(pl.Float64())},
        sort_key=("key",),
    )
    monkeypatch.setenv(API_KEY_ENV, "key")
    monkeypatch.setattr(release, "load_config", lambda path: config)
    monkeypatch.setattr(climate_finance.build, "make_jev_client", fake_jev)
    monkeypatch.setattr(
        climate_finance.build,
        "REGISTRY",
        (fetch_stage, StageDef(name="link", run=link, tables={"numbers": numbers})),
    )
    monkeypatch.setattr(gate, "run_gates", _stop)

    with pytest.raises(BuildError, match="gated"):
        release.cut_release(RELEASE, config=tmp_path / "build.toml", out_dir=out_dir)
    assert len(sent) == 1


def _stop(release_dir: Path, **options: object) -> pl.DataFrame:
    raise BuildError("gated")


def test_cut_release_refuses_a_config_a_published_release_does_not_use(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(release.build_package, "build", pytest.fail)

    with pytest.raises(UnpublishedSettingsError):
        release.cut_release(
            RELEASE,
            config=Path(shutil.copy(BUILD_TOML, tmp_path / "build.toml")),
            out_dir=tmp_path,
        )


def test_a_failed_gate_stops_cut_release_before_the_pack(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def failed_gates(release_dir: Path, **options: object) -> pl.DataFrame:
        raise GateFailedError(["precision 4: fail after 50 labels (pps)"])

    monkeypatch.setattr(
        release.build_package,
        "build",
        lambda name, *, config, out_dir, reuse_sources: built_release(out_dir / name),
    )
    monkeypatch.setattr(gate, "run_gates", failed_gates)

    with pytest.raises(GateFailedError, match="precision"):
        release.cut_release(RELEASE, config=release_toml(tmp_path), out_dir=tmp_path)
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
def test_publish_takes_only_a_release_whose_gates_passed_or_were_waived(
    tmp_path: Path,
    rows: dict[str, list[dict[str, object]]],
    waiver: str | None,
    refused: str | None,
) -> None:
    release_dir = gated(built_release(tmp_path / RELEASE), rows=rows, waiver=waiver)
    pack = release.pack_release(release_dir, out_path=tmp_path / PACK)
    github = FakeGitHub()

    if refused is None:
        publish.publish_release(pack, gh=github)
        assert github.index()["latest"] == RELEASE
    else:
        with pytest.raises(GateFailedError, match=refused):
            publish.publish_release(pack, gh=github)
        assert github.calls == []


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
    github = FakeGitHub()

    def reported(problems: list[str]) -> set[str]:
        return {problem.split(":")[0] for problem in problems}

    assert reported(release.unpublishable(release.packed_manifest(pack))) == missing
    with pytest.raises(GateFailedError) as refused:
        publish.publish_release(pack, gh=github)
    assert reported(refused.value.failures) == missing
    assert github.calls == []


@pytest.mark.parametrize(
    "settings",
    [
        {"key_repair": False},
        {"skip_eib_routes": ["bulk"]},
        {"review": False},
        {"years.crdf": [2021, 2022]},
        {"embed": True},
        {"gap_fill": True},
        {"jev_labels": True},
    ],
)
def test_a_pack_built_with_other_settings_than_a_release_is_not_published(
    tmp_path: Path, settings: dict[str, object]
) -> None:
    pack = release.pack_release(
        gated(built_release(tmp_path / RELEASE), settings=settings),
        out_path=tmp_path / PACK,
    )
    github = FakeGitHub()

    with pytest.raises(UnpublishedSettingsError):
        publish.publish_release(pack, gh=github)
    assert github.calls == []


def test_the_shipped_build_toml_holds_the_settings_of_a_release() -> None:
    config = load_config(Path(__file__).parents[1] / "build.toml")

    assert {
        name: setting_value(config, name) for name in PUBLISHED_SETTINGS
    } == PUBLISHED_SETTINGS
    assert config.sources.ledgers is not None


@pytest.mark.parametrize(
    "name", [release_module.SYNTHETIC_RELEASE, "2026a-rc", "2026A", "2026a_test"]
)
def test_only_a_published_name_is_cut_or_published(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    release_dir = gated(built_release(tmp_path / RELEASE))
    path = release_dir / "assemble" / "release.json"
    manifest = ReleaseManifest.model_validate_json(path.read_text(encoding="utf-8"))
    path.write_text(
        manifest.model_copy(update={"release": name}).model_dump_json(),
        encoding="utf-8",
    )
    pack = release.pack_release(
        release_dir, out_path=tmp_path / f"climate-finance-{name}.tar.zst"
    )
    monkeypatch.setattr(release.build_package, "build", pytest.fail)
    github = FakeGitHub()

    synthetic = name == release_module.SYNTHETIC_RELEASE
    refused = SyntheticReleaseError if synthetic else ReleaseNameError
    with pytest.raises(refused):
        publish.publish_release(pack, gh=github)
    if synthetic:
        with pytest.raises(SyntheticReleaseError):
            release.cut_release(name, config=BUILD_TOML, out_dir=tmp_path)
    assert github.calls == []


def test_publish_creates_the_data_release_then_uploads_the_pack_and_the_index(
    tmp_path: Path,
) -> None:
    pack = release.pack_release(
        gated(built_release(tmp_path / RELEASE)), out_path=tmp_path / PACK
    )
    github = FakeGitHub()

    published = publish.publish_release(pack, gh=github)

    create, pack_upload, index_upload, rename = github.writes
    assert create[:3] == ("release", "create", publish.DATA_TAG)
    # Published, so anyone can download its assets, and it is not GitHub's latest.
    assert "--latest=false" in create
    assert "--draft" not in create
    # The upload reads a private copy of the pack, not the path given.
    assert Path(pack_upload[3]).name == PACK
    assert Path(pack_upload[3]).parent != tmp_path
    assert Path(index_upload[3]).name == publish.STAGED_INDEX_ASSET
    assert rename[:3] == ("api", "--method", "PATCH")
    assert github.assets is not None
    assert set(github.assets) == {PACK, publish.INDEX_ASSET}
    assert not publish.recovery_index(pack).parent.exists()
    assert github.assets[PACK] == pack.read_bytes()
    entry = {
        "url": publish.asset_url(PACK),
        "sha256": sha256_file(pack),
        "data_format": DATA_FORMAT,
    }
    assert github.index() == {"latest": RELEASE, "releases": {RELEASE: entry}}
    assert published.index == tmp_path / publish.INDEX_ASSET
    assert json.loads(published.index.read_text(encoding="utf-8")) == github.index()
    assert (published.url, published.latest) == (entry["url"], RELEASE)


def test_publish_adds_to_the_published_index_keeping_fields_it_does_not_know(
    tmp_path: Path,
) -> None:
    pack = release.pack_release(
        gated(built_release(tmp_path / RELEASE), previous="2025a"),
        out_path=tmp_path / PACK,
    )
    before = json.loads(indexed("2025a")[publish.INDEX_ASSET])
    before["generated"] = "2026-01-01"
    before["releases"]["2025a"]["requires"] = ">=2.0"
    github = FakeGitHub({publish.INDEX_ASSET: json.dumps(before).encode()})

    publish.publish_release(pack, gh=github)

    after = github.index()
    # The new index replaces the old by a staged upload, a delete and a rename.
    assert [call[1] for call in github.writes] == [
        "upload",
        "upload",
        "delete-asset",
        "--method",
    ]
    assert after["generated"] == "2026-01-01"
    assert after["latest"] == RELEASE
    assert after["releases"] == before["releases"] | {
        RELEASE: {
            "url": publish.asset_url(PACK),
            "sha256": sha256_file(pack),
            "data_format": DATA_FORMAT,
        }
    }


def test_an_older_release_joins_the_index_and_leaves_latest_alone(
    tmp_path: Path,
) -> None:
    pack = release.pack_release(
        gated(built_release(tmp_path / RELEASE), previous="2025a"),
        out_path=tmp_path / PACK,
    )
    github = FakeGitHub(indexed("2025a", "2026a.10"))

    publish.publish_release(pack, gh=github)

    assert github.index()["latest"] == "2026a.10"
    assert set(github.index()["releases"]) == {"2025a", RELEASE, "2026a.10"}
    assert publish.release_order("2026a.10") > publish.release_order("2026a.9")
    assert publish.release_order("2026b") > publish.release_order("2026a.10")


@pytest.mark.parametrize(
    ("assets", "previous", "previous_sha256", "refused"),
    [
        (indexed(RELEASE), None, None, AlreadyPublishedError),
        (indexed("2025a"), None, None, DriftBaselineError),
        (indexed("2025a"), "2026a-rc", None, DriftBaselineError),
        (indexed("2024a", "2025a"), "2024a", "0" * 64, DriftBaselineError),
        (None, "2025a", "0" * 64, DriftBaselineError),
        (indexed("2025a", "2027a"), "2024a", "0" * 64, DriftBaselineError),
        # A published candidate cannot be latest, so it is no drift baseline.
        (
            indexed("2025a", "2026b-rc", latest="2026b-rc"),
            "2026b-rc",
            "0" * 64,
            PublishedIndexError,
        ),
        # An index whose latest it does not list.
        (indexed("2025a", latest="2025b"), "2025b", "0" * 64, PublishedIndexError),
        # A local copy named like the published release, from another pack.
        (indexed("2025a"), "2025a", "f" * 64, DriftBaselineError),
        (indexed("2025a"), "2025a", None, DriftBaselineError),
    ],
    ids=[
        "re-cut",
        "first-release",
        "own-rc",
        "older",
        "none-published",
        "unlisted",
        "rc-latest",
        "latest-unlisted",
        "other-pack",
        "no-pack",
    ],
)
def test_publish_refuses_a_name_already_published_or_drift_against_another_release(
    tmp_path: Path,
    assets: dict[str, bytes] | None,
    previous: str | None,
    previous_sha256: str | None,
    refused: type[BuildError],
) -> None:
    pack = release.pack_release(
        gated(
            built_release(tmp_path / RELEASE),
            previous=previous,
            previous_sha256=previous_sha256,
        ),
        out_path=tmp_path / PACK,
    )
    github = FakeGitHub(assets)

    with pytest.raises(refused) as raised:
        publish.publish_release(pack, gh=github)
    # The command prints the refusal as one line.
    assert "\n" not in str(raised.value)
    assert github.writes == []
    assert github.assets == assets


@pytest.mark.parametrize("left", ["recovery-index", "nothing"])
def test_packs_without_an_index_are_refused_naming_the_index_to_restore(
    tmp_path: Path, left: str
) -> None:
    pack = release.pack_release(
        gated(built_release(tmp_path / RELEASE)), out_path=tmp_path / PACK
    )

    def interrupted(github: FakeGitHub) -> None:
        # The first publish stops once its pack is up, before any index is.
        if github.calls[-1][3].endswith(PACK):
            raise BuildError("the connection dropped")

    github = FakeGitHub(after_write=interrupted)
    with pytest.raises(BuildError, match="connection dropped"):
        publish.publish_release(pack, gh=github)
    recovery = publish.recovery_index(pack)
    if left == "nothing":
        recovery.unlink()
    github.after_write = None

    with pytest.raises(MissingIndexError) as raised:
        publish.publish_release(pack, gh=github)

    message = str(raised.value)
    assert str(recovery) in message
    assert "--first-release" not in message
    assert not (tmp_path / publish.INDEX_ASSET).exists()
    assert [call[1] for call in github.writes] == ["create", "upload"]
    if left == "recovery-index":
        assert "No publish" not in message
        # Following the message restores the index the stopped publish was writing.
        upload = ("release", "upload", "data", str(recovery), "--clobber")
        github([*upload, "--repo", publish.PUBLISH_REPO])
        assert github.index()["releases"][RELEASE]["sha256"] == sha256_file(pack)
    else:
        assert "No publish of a pack in this directory" in message


def test_a_pack_replaced_while_it_publishes_is_not_the_one_uploaded(
    tmp_path: Path,
) -> None:
    pack = release.pack_release(
        gated(built_release(tmp_path / RELEASE)), out_path=tmp_path / PACK
    )
    original = pack.read_bytes()
    other = release.pack_release(
        gated(built_release(tmp_path / "other" / RELEASE), settings={"gap_fill": True}),
        out_path=tmp_path / "other" / PACK,
    )
    github = FakeGitHub()

    def replacing(args: Sequence[str]) -> str:
        # A concurrent build writes another pack to the path once checks begin.
        if tuple(args[:2]) == ("release", "list"):
            shutil.copyfile(other, pack)
        return github(args)

    published = publish.publish_release(pack, gh=replacing)

    assert pack.read_bytes() != original
    assert github.assets is not None
    assert github.assets[PACK] == original
    assert published.sha256 == hashlib.sha256(original).hexdigest()
    assert github.index()["releases"][RELEASE]["sha256"] == published.sha256


def test_an_index_another_publish_replaced_meanwhile_is_reported(
    tmp_path: Path,
) -> None:
    pack = release.pack_release(
        gated(built_release(tmp_path / RELEASE), previous="2025a"),
        out_path=tmp_path / PACK,
    )

    def concurrent(github: FakeGitHub) -> None:
        # Another publish replaces the index right after this one's rename.
        if github.calls[-1][0] == "api":
            assert github.assets is not None
            github.assets[publish.INDEX_ASSET] = indexed("2025a", "2025b")[
                publish.INDEX_ASSET
            ]

    github = FakeGitHub(indexed("2025a"), after_write=concurrent)

    with pytest.raises(IndexReplacedError):
        publish.publish_release(pack, gh=github)
    assert not (tmp_path / publish.INDEX_ASSET).exists()


def test_a_pack_renamed_for_another_release_is_refused(tmp_path: Path) -> None:
    # Uploaded under its file name, it would replace the 2025a pack.
    pack = release.pack_release(
        gated(built_release(tmp_path / RELEASE)),
        out_path=tmp_path / "climate-finance-2025a.tar.zst",
    )
    github = FakeGitHub()

    with pytest.raises(ReleaseNameError):
        publish.publish_release(pack, gh=github)
    assert github.calls == []


def test_a_repository_that_is_not_owner_and_name_is_refused(tmp_path: Path) -> None:
    pack = release.pack_release(
        gated(built_release(tmp_path / RELEASE)), out_path=tmp_path / PACK
    )
    github = FakeGitHub()

    with pytest.raises(RepositoryNameError):
        publish.publish_release(pack, repo="--help", gh=github)
    assert github.calls == []


def test_a_dry_run_changes_nothing_on_github_or_beside_the_pack(
    tmp_path: Path,
) -> None:
    pack = release.pack_release(
        gated(built_release(tmp_path / RELEASE), previous="2025a"),
        out_path=tmp_path / PACK,
    )
    github = FakeGitHub(indexed("2025a"))

    published = publish.publish_release(pack, gh=github, dry_run=True)

    assert github.writes == []
    assert github.assets == indexed("2025a")
    assert [command[1] for command in published.commands] == [
        "upload",
        "upload",
        "delete-asset",
        "--method",
    ]
    # The releases.json beside the pack is the one to ship, so a dry run leaves it.
    assert not (tmp_path / publish.INDEX_ASSET).exists()
    assert published.index.parent != tmp_path
    assert json.loads(published.index.read_text(encoding="utf-8"))["latest"] == RELEASE


def test_a_draft_data_release_is_refused(tmp_path: Path) -> None:
    pack = release.pack_release(
        gated(built_release(tmp_path / RELEASE)), out_path=tmp_path / PACK
    )
    github = FakeGitHub({}, draft=True)

    with pytest.raises(DraftReleaseError):
        publish.publish_release(pack, gh=github)
    assert github.writes == []


def test_the_publish_command_prints_each_gh_step_and_refuses_in_one_line(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pack = release.pack_release(
        gated(built_release(tmp_path / RELEASE)), out_path=tmp_path / PACK
    )
    github = FakeGitHub(indexed(RELEASE))
    monkeypatch.setattr(publish, "run_gh", github)
    runner = CliRunner()

    refused = runner.invoke(app, ["publish", str(pack)])
    github.assets = None
    dry = runner.invoke(app, ["publish", str(pack), "--dry-run"])

    assert refused.exit_code == 1
    assert "AlreadyPublishedError: " in refused.output
    assert f"[{AlreadyPublishedError.code}]" in refused.output
    assert "Traceback" not in refused.output
    assert dry.exit_code == 0, dry.output
    assert "gh release list" in dry.output
    assert "would run: gh release create data" in dry.output
    assert "would run: gh release upload data " in dry.output
    upload = next(line for line in dry.output.splitlines() if "release upload" in line)
    assert re.search(rf"[\\/]{re.escape(PACK)}[\"']? --clobber", upload)
    assert "new index previewed in" in dry.output
    assert github.writes == []


@pytest.mark.parametrize(
    ("platform", "path", "line"),
    [
        ("linux", "/tmp/a b/pack.tar.zst", "'/tmp/a b/pack.tar.zst' --clobber"),
        (
            "win32",
            r"C:\Users\a b\pack.tar.zst",
            r'"C:\Users\a b\pack.tar.zst" --clobber',
        ),
        ("win32", r"C:\Users\pack.tar.zst", r"C:\Users\pack.tar.zst --clobber"),
    ],
)
def test_a_printed_command_is_quoted_for_the_platforms_shell(
    monkeypatch: pytest.MonkeyPatch, platform: str, path: str, line: str
) -> None:
    monkeypatch.setattr(publish.sys, "platform", platform)

    assert publish.shell_join([path, "--clobber"]) == line


def test_a_refusal_prints_its_causes_and_the_traceback_on_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pack = release.pack_release(
        gated(built_release(tmp_path / RELEASE)), out_path=tmp_path / PACK
    )
    github = FakeGitHub({publish.INDEX_ASSET: b"<html>Not Found</html>"})
    monkeypatch.setattr(publish, "run_gh", github)
    runner = CliRunner()

    short = runner.invoke(app, ["publish", str(pack)])
    monkeypatch.setenv(TRACEBACK_VAR, "1")
    full = runner.invoke(app, ["publish", str(pack)])

    assert short.exit_code == 1
    assert "PublishedIndexError: " in short.output
    assert "caused by JSONDecodeError: Expecting value: line 1 column 1" in (
        short.output
    )
    assert isinstance(full.exception, PublishedIndexError)
    assert isinstance(full.exception.__cause__, json.JSONDecodeError)


def test_a_malformed_index_is_refused_in_one_line_without_pydantics_dump(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pack = release.pack_release(
        gated(built_release(tmp_path / RELEASE)), out_path=tmp_path / PACK
    )
    index = {"latest": "2025b", "releases": {}}
    github = FakeGitHub({publish.INDEX_ASSET: json.dumps(index).encode()})
    monkeypatch.setattr(publish, "run_gh", github)

    refused = CliRunner().invoke(app, ["publish", str(pack)])

    assert refused.exit_code == 1
    assert "caused by" not in refused.output
    assert "validation error" not in refused.output
    assert (
        sum("PublishedIndexError: " in line for line in refused.output.splitlines())
        == 1
    )


def test_gh_missing_is_a_build_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PATH", "")

    with pytest.raises(GhError):
        publish.run_gh(["--version"])


def test_gh_runs_from_an_argument_list_with_no_shell_and_a_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runs = []

    def hung(command: list[str], **options: object) -> None:
        runs.append((command, options))
        raise subprocess.TimeoutExpired(command, publish.GH_TIMEOUT)

    monkeypatch.setattr(publish.subprocess, "run", hung)

    with pytest.raises(GhError):
        publish.run_gh(["release", "list"])
    [(command, options)] = runs
    assert command == ["gh", "release", "list"]
    assert options["timeout"] == publish.GH_TIMEOUT
    assert not options.get("shell")


def test_the_index_url_is_the_data_release_asset() -> None:
    assert publish.asset_url(publish.INDEX_ASSET) == release_module.INDEX_URL


def _double_climate(release_dir: Path) -> None:
    """Rebuild `release_dir`'s `quality` with its climate totals doubled, with a
    `release.json` that matches, as a build rerun after its pack was cut."""
    assemble = release_dir / "assemble"
    path = assemble / "quality.parquet"
    pl.read_parquet(path).with_columns(pl.col("climate_usd") * 2).write_parquet(path)
    manifest = ReleaseManifest.model_validate_json(
        (assemble / "release.json").read_text(encoding="utf-8")
    )
    entry = manifest.tables["quality"].model_copy(update={"sha256": sha256_file(path)})
    (assemble / "release.json").write_text(
        manifest.model_copy(
            update={"tables": manifest.tables | {"quality": entry}}
        ).model_dump_json(),
        encoding="utf-8",
    )


def _climate(opened: Path | cf.Release) -> float:
    """The climate total of a release, given opened or as its directory."""
    if isinstance(opened, Path):
        opened = cf.load(path=opened)
    quality = opened.table("quality")
    return quality.select(pl.col("climate_usd").sum()).collect().item()


@pytest.mark.parametrize(
    "copy", ["pack", "build-beside-its-pack", "downloaded", "bare"]
)
def test_the_drift_baseline_holds_the_bytes_of_the_pack_it_records(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, copy: str
) -> None:
    previous = built_release(tmp_path / "old" / RELEASE)
    pack = release.pack_release(previous, out_path=tmp_path / "old" / PACK)
    packed = (sha256_file(pack), _climate(previous / "assemble"))
    _double_climate(previous)
    rebuilt = _climate(previous / "assemble")
    assert rebuilt == pytest.approx(2 * packed[1]) != packed[1]
    if copy == "pack":
        given, expected = pack, packed
    elif copy == "build-beside-its-pack":
        given, expected = previous, packed
    elif copy == "downloaded":
        (previous / "assemble" / release_module.PACK_SHA256).write_text(
            "a" * 64, encoding="utf-8"
        )
        given, expected = previous / "assemble", ("a" * 64, rebuilt)
    else:
        pack.unlink()
        given, expected = previous, (None, rebuilt)

    opened = _count_opens(monkeypatch)

    with release.previous_baseline(given) as baseline:
        assert baseline is not None
        if pack.exists():
            # The baseline was fixed on entry, so the pack may change now.
            pack.write_bytes(b"replaced")
        assert len(opened) == 1
        assert (baseline.pack_sha256, _climate(baseline.release)) == pytest.approx(
            expected
        )


def _count_opens(monkeypatch: pytest.MonkeyPatch) -> list[Path]:
    """The release directories `cf.load` opens and checks, in order."""
    opened: list[Path] = []
    real = release_module._open

    def counted(path: Path, **options: Any) -> cf.Release:
        opened.append(path)
        return real(path, **options)

    monkeypatch.setattr(release_module, "_open", counted)
    return opened


def test_gate_and_release_compare_with_the_bytes_of_the_previous_pack(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    previous = built_release(tmp_path / "old" / RELEASE)
    pack = release.pack_release(previous, out_path=tmp_path / "old" / PACK)
    packed = _climate(previous / "assemble")
    _double_climate(previous)
    seen: list[tuple[str, float]] = []

    def fake_gates(release_dir: Path, **options: Any) -> pl.DataFrame:
        before = options["previous"]
        seen.append((before.manifest.release, _climate(before)))
        return pl.DataFrame(
            {
                "gate": ["drift"],
                "subject": [None],
                "passed": [True],
                "waived": [False],
                "detail": ["compared"],
            }
        )

    monkeypatch.setattr(gate, "run_gates", fake_gates)
    monkeypatch.setattr(
        release.build_package,
        "build",
        lambda name, *, config, out_dir, reuse_sources: built_release(out_dir / name),
    )
    opened = _count_opens(monkeypatch)
    config = str(release_toml(tmp_path))

    for given in (pack, previous):
        gated_run = CliRunner().invoke(
            app,
            ["gate", str(previous), "--config", config, "--previous", str(given)],
        )
        assert gated_run.exit_code == 0, gated_run.output
    cut = CliRunner().invoke(
        app,
        [
            *("release", RELEASE, "--config", config),
            *("--out-dir", str(tmp_path / "new"), "--previous", str(pack)),
        ],
    )

    assert cut.exit_code == 0, cut.output
    assert seen == [(RELEASE, pytest.approx(packed))] * 3
    assert len(opened) == 3


@pytest.mark.parametrize("command", ["gate", "release"])
@pytest.mark.parametrize("given", ["missing", "not-a-pack"])
def test_a_previous_release_that_cannot_be_read_is_refused_in_one_line(
    tmp_path: Path, command: str, given: str
) -> None:
    previous = tmp_path / "previous.tar.zst"
    if given == "not-a-pack":
        previous.write_text("not a pack", encoding="utf-8")
    target = str(tmp_path / RELEASE) if command == "gate" else RELEASE
    options = ["--out-dir", str(tmp_path)] if command == "release" else []

    result = CliRunner().invoke(
        app,
        [
            *(command, target, "--config", str(release_toml(tmp_path))),
            *(*options, "--previous", str(previous)),
        ],
    )

    assert result.exit_code == 1
    assert "Traceback" not in result.output
    [line] = [line for line in result.output.splitlines() if str(previous) in line]
    if given == "missing":
        assert line.startswith(f"BuildError: --previous {previous} does not exist")
    else:
        assert line.startswith("ReleaseIntegrityError: ")


@pytest.fixture
def published(
    tmp_path: Path, server: tuple[str, Path], monkeypatch: pytest.MonkeyPatch
) -> tuple[ReleaseIndex, Path]:
    """The fixture release packed where `server` serves it, and listed in a shipped
    index and in the index online. Returns that index and the served directory."""
    url, served = server
    pack = release.pack_release(
        gated(built_release(tmp_path / RELEASE)), out_path=served / PACK
    )
    index = ReleaseIndex(
        latest=RELEASE,
        releases={
            RELEASE: IndexEntry(
                url=f"{url}/{PACK}", sha256=sha256_file(pack), data_format=DATA_FORMAT
            )
        },
    )
    shipped = tmp_path / "releases.json"
    shipped.write_text(index.model_dump_json(), encoding="utf-8")
    (served / "releases.json").write_text(index.model_dump_json(), encoding="utf-8")
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
    stamp.write_text("0" * 64, encoding="utf-8")
    downloads = []
    fetch = release_module._fetch

    def counted(url: str, *, out_path: Path) -> str:
        downloads.append(url)
        return fetch(url, out_path=out_path)

    monkeypatch.setattr(release_module, "_fetch", counted)

    cf.load(cache_dir=cache)

    assert downloads == [index.releases[RELEASE].url]
    assert stamp.read_text(encoding="utf-8") == index.releases[RELEASE].sha256


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
            (raced[0].path / release_module.PACK_SHA256).write_text(
                "f" * 64, encoding="utf-8"
            )
        return fetch(url, out_path=out_path)

    monkeypatch.setattr(release_module, "_fetch", racing)

    loaded = cf.load(cache_dir=cache)

    assert loaded.path == raced[0].path
    assert [path.name for path in cache.iterdir()] == [f"climate-finance-{RELEASE}"]
    stamp = loaded.path / release_module.PACK_SHA256
    assert stamp.read_text(encoding="utf-8") == index.releases[RELEASE].sha256


@pytest.mark.parametrize("other", ["moved-it-away", "replaced-it"])
def test_a_stale_copy_another_load_handles_first_is_left_to_it(
    tmp_path: Path,
    published: tuple[ReleaseIndex, Path],
    monkeypatch: pytest.MonkeyPatch,
    other: str,
) -> None:
    cache = tmp_path / "cache"
    target = cf.load(cache_dir=cache).path
    (target / release_module.PACK_SHA256).write_text("0" * 64, encoding="utf-8")
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


@pytest.mark.parametrize(
    "read", [release_module.PACK_SHA256, "release.json", "commitments.parquet"]
)
def test_a_cached_copy_moved_away_mid_check_downloads_again(
    tmp_path: Path,
    published: tuple[ReleaseIndex, Path],
    monkeypatch: pytest.MonkeyPatch,
    read: str,
) -> None:
    cache = tmp_path / "cache"
    target = cf.load(cache_dir=cache).path
    fetch = release_module._fetch
    downloads = []

    def counted(url: str, *, out_path: Path) -> str:
        downloads.append(url)
        return fetch(url, out_path=out_path)

    def moving(original):
        # Another load moves the copy aside after this one found the file.
        def opened(self: Path, *args, **kwargs):
            if self == target / read and not downloads:
                target.rename(tmp_path / "moved")
            return original(self, *args, **kwargs)

        return opened

    monkeypatch.setattr(release_module, "_fetch", counted)
    monkeypatch.setattr(Path, "read_text", moving(Path.read_text))
    monkeypatch.setattr(Path, "open", moving(Path.open))

    loaded = cf.load(cache_dir=cache)

    assert len(downloads) == 1
    assert loaded.spending().table.height > 0


def test_refresh_reads_an_index_with_fields_this_package_does_not_know(
    tmp_path: Path, published: tuple[ReleaseIndex, Path]
) -> None:
    index, served = published
    newer = json.loads(index.model_dump_json())
    newer["generated"] = "2027-01-01"
    newer["releases"][RELEASE]["requires"] = ">=2.1"
    (served / "releases.json").write_text(json.dumps(newer), encoding="utf-8")

    loaded = cf.load(cache_dir=tmp_path / "cache", refresh=True)

    assert loaded.manifest.release == RELEASE


@pytest.mark.parametrize(
    "body",
    [
        b"<html>Not Found</html>",
        b'{"releases": []}',
        b'{"latest": "2027a", "releases": {}}',
    ],
)
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
    (tmp_path / "releases.json").write_text(gone.model_dump_json(), encoding="utf-8")
    with pytest.raises(SourceUnavailableError, match="Could not download"):
        cf.load(cache_dir=tmp_path / "cache")


def test_the_shipped_index_parses() -> None:
    assert isinstance(release_module._shipped_index(), ReleaseIndex)

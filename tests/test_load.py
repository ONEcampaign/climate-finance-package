"""`load(path=)` opens a release only when every table matches `release.json`."""

import json
import shutil
from collections.abc import Callable
from pathlib import Path

import polars as pl
import pytest
from make_fixture import FIXTURE_DIR
from polars.testing import assert_frame_equal
from releases import copy_release

import climate_finance as cf
from climate_finance import query
from climate_finance import release as release_module
from climate_finance.errors import (
    FormatQueryError,
    FormatTableError,
    InvalidInputError,
    ReleaseFormatError,
    ReleaseIntegrityError,
)
from climate_finance.release import (
    PACK_SHA256,
    SYNTHETIC_RELEASE,
    IndexEntry,
    ReleaseIndex,
    check_release_name,
    release_dirname,
)
from climate_finance.schema import DATA_FORMAT, RELEASE_TABLES, ReleaseManifest


@pytest.mark.parametrize("as_path", [Path, str])
def test_load_opens_the_fixture_with_its_manifest(as_path) -> None:
    release = cf.load(path=as_path(FIXTURE_DIR))

    assert isinstance(release, cf.Release)
    assert release.manifest.release == SYNTHETIC_RELEASE
    assert release.table("commitments").collect().height == 14


def test_a_release_prints_its_name_flow_years_and_tables(tmp_path: Path) -> None:
    published = copy_release(tmp_path / "2026a")
    path = published / "release.json"
    manifest = ReleaseManifest.model_validate_json(path.read_text(encoding="utf-8"))
    withheld = {"btr": "built locally", "btr_links": "built locally"}
    path.write_text(
        manifest.model_copy(
            update={
                "release": "2026a",
                "tables": {
                    name: entry
                    for name, entry in manifest.tables.items()
                    if name not in withheld
                },
                "withheld_tables": withheld,
            }
        ).model_dump_json(),
        encoding="utf-8",
    )

    # The example release's name marks its figures as invented.
    assert repr(cf.load_example()) == (
        "Release('synthetic-example', synthetic figures): commitments 2021 to 2022, "
        "disbursements 2021 to 2024; 21 tables"
    )
    assert repr(cf.load(path=published)) == (
        "Release('2026a'): commitments 2021 to 2022, disbursements 2021 to 2024; "
        "19 tables, withholding btr, btr_links"
    )


def _change_bytes(release: Path) -> None:
    (release / "modelled.parquet").write_bytes(
        (release / "quality.parquet").read_bytes()
    )


def _drop_from_manifest(release: Path) -> None:
    manifest = json.loads((release / "release.json").read_text(encoding="utf-8"))
    del manifest["tables"]["objectives"]
    (release / "release.json").write_text(json.dumps(manifest), encoding="utf-8")


@pytest.mark.parametrize(
    ("damage", "message"),
    [
        (_change_bytes, "modelled: sha256"),
        (
            lambda release: (release / "btr.parquet").unlink(),
            r"btr: btr\.parquet is missing",
        ),
        (_drop_from_manifest, "objectives: not in release"),
        (lambda release: (release / "release.json").unlink(), r"no release\.json"),
    ],
    ids=["changed bytes", "missing file", "absent from manifest", "no release.json"],
)
def test_a_damaged_release_fails_its_check(
    tmp_path: Path, damage: Callable[[Path], None], message: str
) -> None:
    release = copy_release(tmp_path / "release")
    damage(release)

    with pytest.raises(ReleaseIntegrityError, match=message):
        cf.load(path=release)


def _fixture_manifest(**changes: object) -> bytes:
    """The fixture's `release.json` with `changes`, a None value dropping its key."""
    manifest = json.loads((FIXTURE_DIR / "release.json").read_text(encoding="utf-8"))
    for key, value in changes.items():
        if value is None:
            del manifest[key]
        else:
            manifest[key] = value
    return json.dumps(manifest).encode()


@pytest.mark.parametrize(
    "damaged",
    [
        b'{"release": "fixt',
        b"\xff\xfe",
        b'{"release": "synthetic-example"}',
        _fixture_manifest(data_format=None),
        _fixture_manifest(data_format=str(DATA_FORMAT)),
        _fixture_manifest(data_format=True),
        _fixture_manifest(data_format=0),
        _fixture_manifest(build=None),
    ],
    ids=[
        "truncated-json",
        "undecodable-bytes",
        "missing-fields",
        "no-data-format",
        "string-data-format",
        "boolean-data-format",
        "unknown-data-format",
        "no-build",
    ],
)
def test_a_malformed_manifest_is_an_integrity_error(
    tmp_path: Path, damaged: bytes
) -> None:
    release = copy_release(tmp_path / "release")
    (release / "release.json").write_bytes(damaged)

    with pytest.raises(ReleaseIntegrityError, match="malformed"):
        cf.load(path=release)


def test_a_cached_release_with_a_malformed_manifest_downloads_again(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache = tmp_path / "cache"
    name = release_dirname(SYNTHETIC_RELEASE)
    shutil.copytree(FIXTURE_DIR, cache / name)
    (cache / name / PACK_SHA256).write_text("0", encoding="utf-8")
    (cache / name / "release.json").write_text('{"release": "fixt', encoding="utf-8")
    index = ReleaseIndex(
        latest=SYNTHETIC_RELEASE,
        releases={
            SYNTHETIC_RELEASE: IndexEntry(
                url="https://example.org/pack", sha256="0", data_format=DATA_FORMAT
            )
        },
    )
    downloads = []

    def download(entry: IndexEntry, *, target: Path) -> None:
        downloads.append(entry.url)
        shutil.copytree(FIXTURE_DIR, target)

    monkeypatch.setattr(release_module, "_shipped_index", lambda: index)
    monkeypatch.setattr(release_module, "_download", download)

    release = cf.load(cache_dir=cache)

    assert downloads == ["https://example.org/pack"]
    assert release.manifest.release == SYNTHETIC_RELEASE


UNSAFE_NAMES = ["x/../../victim", "..", ".hidden", "a b", "", "x\\y", "x\n"]


@pytest.mark.parametrize("name", UNSAFE_NAMES)
def test_a_release_name_must_be_one_safe_path_segment(name: str) -> None:
    entry = IndexEntry(
        url="https://example.org/pack", sha256="0", data_format=DATA_FORMAT
    )

    with pytest.raises(InvalidInputError):
        check_release_name(name)
    with pytest.raises(ValueError, match="releases"):
        ReleaseIndex(releases={name: entry})
    with pytest.raises(ValueError, match="latest"):
        ReleaseIndex(latest=name)


@pytest.mark.parametrize("name", ["fixture", "2026a", "1.2.3-rc_1", "_x"])
def test_an_ordinary_release_name_is_accepted(name: str) -> None:
    entry = IndexEntry(
        url="https://example.org/pack", sha256="0", data_format=DATA_FORMAT
    )

    assert check_release_name(name) == name
    assert ReleaseIndex(latest=name, releases={name: entry}).latest == name


def test_an_index_whose_latest_it_does_not_list_is_refused() -> None:
    with pytest.raises(ValueError, match="latest"):
        ReleaseIndex(latest="2026a")


def test_load_leaves_a_directory_outside_the_cache_alone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache = tmp_path / "cache"
    (cache / release_dirname("x")).mkdir(parents=True)
    victim = tmp_path / "victim"
    victim.mkdir()
    (victim / "sentinel").write_text("keep", encoding="utf-8")
    name = "x/../../victim"
    # model_construct skips the name check `load` relies on, to test its own.
    index = ReleaseIndex.model_construct(
        latest=name,
        releases={
            name: IndexEntry(
                url="https://example.org/pack", sha256="0", data_format=DATA_FORMAT
            )
        },
    )
    downloads = []
    monkeypatch.setattr(release_module, "_shipped_index", lambda: index)
    monkeypatch.setattr(
        release_module, "_download", lambda entry, *, target: downloads.append(target)
    )

    with pytest.raises(InvalidInputError, match="outside the cache"):
        cf.load(cache_dir=cache)

    assert (victim / "sentinel").read_text(encoding="utf-8") == "keep"
    assert downloads == []


@pytest.mark.parametrize("extra", [{}, {"build": {"python": "3.14"}}])
def test_a_newer_data_format_asks_for_an_upgrade_and_stays_cached(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, extra: dict[str, object]
) -> None:
    cache = tmp_path / "cache"
    release = shutil.copytree(FIXTURE_DIR, cache / release_dirname(SYNTHETIC_RELEASE))
    (release / PACK_SHA256).write_text("0", encoding="utf-8")
    (release / "release.json").write_bytes(
        _fixture_manifest(data_format=DATA_FORMAT + 1, **extra)
    )
    index = ReleaseIndex(
        latest=SYNTHETIC_RELEASE,
        releases={
            SYNTHETIC_RELEASE: IndexEntry(
                url="https://example.org/pack", sha256="0", data_format=DATA_FORMAT
            )
        },
    )
    downloads = []
    monkeypatch.setattr(release_module, "_shipped_index", lambda: index)
    monkeypatch.setattr(
        release_module, "_download", lambda entry, *, target: downloads.append(target)
    )

    for load in (lambda: cf.load(path=release), lambda: cf.load(cache_dir=cache)):
        with pytest.raises(ReleaseFormatError) as raised:
            load()
        assert raised.value.data_format == DATA_FORMAT + 1
        assert raised.value.supported == DATA_FORMAT
    assert downloads == []
    assert (release / "commitments.parquet").is_file()


def test_an_index_entry_of_a_newer_data_format_is_refused_before_downloading(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    index = ReleaseIndex(
        latest="2026b",
        releases={
            "2026b": IndexEntry(
                url="https://example.org/pack", sha256="0", data_format=DATA_FORMAT + 1
            )
        },
    )
    downloads = []
    monkeypatch.setattr(release_module, "_shipped_index", lambda: index)
    monkeypatch.setattr(
        release_module, "_download", lambda entry, *, target: downloads.append(target)
    )

    with pytest.raises(ReleaseFormatError) as raised:
        cf.load(cache_dir=tmp_path / "cache")

    assert (raised.value.release, raised.value.data_format) == (
        "2026b",
        DATA_FORMAT + 1,
    )
    assert downloads == []
    assert not (tmp_path / "cache").exists()


def test_a_release_of_an_older_format_serves_what_it_holds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Format 0 is test-local: format 1 less `reflows`, with memorandum rows.
    monkeypatch.setattr(
        release_module,
        "FORMAT_TABLES",
        {0: frozenset(RELEASE_TABLES) - {"reflows"}, 1: frozenset(RELEASE_TABLES)},
    )
    monkeypatch.setattr(query, "MEMORANDUM_FORMAT", 0)
    path = copy_release(tmp_path / "release") / "release.json"
    manifest = ReleaseManifest.model_validate_json(path.read_text(encoding="utf-8"))
    tables = {name: e for name, e in manifest.tables.items() if name != "reflows"}
    path.write_text(
        manifest.model_copy(
            update={"data_format": 0, "tables": tables}
        ).model_dump_json(),
        encoding="utf-8",
    )
    (path.parent / "reflows.parquet").unlink()

    release = cf.load(path=path.parent)

    assert release.spending(flow="commitments").table.height > 0
    assert release.spending(untraced=False).table.height > 0
    with pytest.raises(FormatTableError) as by_table:
        release.table("reflows")
    with pytest.raises(FormatTableError) as by_query:
        release.spending(flow="net_disbursements", years=2022)
    for raised in (by_table, by_query):
        assert (raised.value.table, raised.value.data_format) == ("reflows", 0)


def test_a_release_before_memorandum_rows_reads_its_tables_and_refuses_queries(
    tmp_path: Path,
) -> None:
    # Its `total_flows` would hold the memorandum records and its rows no flag.
    no_flag: dict[str, Callable[[pl.DataFrame], pl.DataFrame]] = {
        name: lambda frame: frame.drop("psi_flag")
        for name in ("commitments", "disbursements", "untraced", "reflows")
    }
    path = copy_release(tmp_path / "release", edits=no_flag) / "release.json"
    manifest = ReleaseManifest.model_validate_json(path.read_text(encoding="utf-8"))
    path.write_text(
        manifest.model_copy(update={"data_format": 2}).model_dump_json(),
        encoding="utf-8",
    )
    release = cf.load(path=path.parent)

    assert release.table("commitments").collect().height == 14
    queries: dict[str, Callable[[], object]] = {
        "spending": release.spending,
        "quality": release.quality,
        "reconcile": release.reconcile,
        "jev_flags": release.jev_flags,
    }
    for name, run in queries.items():
        with pytest.raises(FormatQueryError) as raised:
            run()
        assert (raised.value.data_format, raised.value.needed) == (2, 3), name


def test_table_refuses_names_outside_the_release_contract() -> None:
    release = cf.load(path=FIXTURE_DIR)

    with pytest.raises(cf.UnknownNameError) as raised:
        release.table("btr_total")
    assert raised.value.suggestions[0] == "btr_totals"
    assert set(RELEASE_TABLES) <= set(release.manifest.tables)


@pytest.mark.parametrize("refresh", [False, True])
def test_an_empty_index_names_path_and_where_releases_come_from(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, refresh: bool
) -> None:
    monkeypatch.setattr(release_module, "_shipped_index", ReleaseIndex)
    monkeypatch.setattr(release_module, "_fetch_index", ReleaseIndex)

    with pytest.raises(cf.UnknownNameError) as raised:
        cf.load(cache_dir=tmp_path, refresh=refresh)

    # The shipped index predates any release published since, so only an empty
    # index online says none is published.
    message = str(raised.value)
    assert "cf.load(path=...)" in message
    assert "cf.load_example()" in message
    assert ("No release is published" in message) is refresh
    assert ("refresh=True" in message) is not refresh


def test_disbursement_profile_reads_the_release_table_by_group_and_year() -> None:
    profile = cf.load(path=FIXTURE_DIR).disbursement_profile()

    assert profile.columns == ["group", "years_since", "cumulative_share", "vintages"]
    assert profile.filter(group="all", years_since=0)["cumulative_share"].item() == 0.23
    assert profile["years_since"].is_sorted()


@pytest.mark.parametrize("lazy", [False, True], ids=["new query", "lazy result"])
def test_a_relative_path_reads_the_release_it_verified(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, lazy: bool
) -> None:
    copy_release(tmp_path / "a" / "release")
    # Another directory's release, with every commitment ten times larger.
    copy_release(
        tmp_path / "b" / "release",
        edits={
            "commitments": lambda frame: frame.with_columns(pl.col("face_usd") * 10)
        },
    )
    expected = cf.load(path=FIXTURE_DIR).spending(flow="commitments")
    monkeypatch.chdir(tmp_path / "a")
    release = cf.load(path="release")
    result = release.spending(flow="commitments")

    monkeypatch.chdir(tmp_path / "b")
    other = cf.load(path="release").spending(flow="commitments")
    assert not other.table.equals(expected.table)

    table = result.table if lazy else release.spending(flow="commitments").table
    assert_frame_equal(table, expected.table)

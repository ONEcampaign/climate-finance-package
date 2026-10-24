"""`load(path=)` opens a release only when every table matches `release.json`."""

import json
import shutil
from pathlib import Path

import pytest
from make_fixture import FIXTURE_DIR
from releases import copy_release

import climate_finance as cf
from climate_finance import snapshot
from climate_finance.errors import ReleaseIntegrityError
from climate_finance.schema import RELEASE_TABLES
from climate_finance.snapshot import IndexEntry, ReleaseIndex, release_dirname


@pytest.mark.parametrize("as_path", [Path, str])
def test_load_opens_the_fixture_with_its_manifest(as_path) -> None:
    snap = cf.load(path=as_path(FIXTURE_DIR))

    assert snap.manifest.release == "fixture"
    assert snap.table("commitments").collect().height == 14


def test_a_table_whose_bytes_changed_fails_its_hash(tmp_path: Path) -> None:
    release = copy_release(tmp_path / "release")
    (release / "modelled.parquet").write_bytes(
        (release / "quality.parquet").read_bytes()
    )

    with pytest.raises(ReleaseIntegrityError, match="modelled: sha256"):
        cf.load(path=release)


def test_a_missing_table_file_fails(tmp_path: Path) -> None:
    release = copy_release(tmp_path / "release")
    (release / "btr.parquet").unlink()

    with pytest.raises(ReleaseIntegrityError, match=r"btr: btr\.parquet is missing"):
        cf.load(path=release)


def test_a_release_table_absent_from_the_manifest_fails(tmp_path: Path) -> None:
    release = copy_release(tmp_path / "release")
    manifest = json.loads((release / "release.json").read_text())
    del manifest["tables"]["objectives"]
    (release / "release.json").write_text(json.dumps(manifest))

    with pytest.raises(ReleaseIntegrityError, match="objectives: not in release"):
        cf.load(path=release)


def test_a_directory_without_release_json_fails(tmp_path: Path) -> None:
    with pytest.raises(ReleaseIntegrityError, match=r"no release\.json"):
        cf.load(path=tmp_path)


@pytest.mark.parametrize(
    "damaged",
    [b'{"release": "fixt', b"\xff\xfe", b'{"release": "fixture"}'],
    ids=["truncated-json", "undecodable-bytes", "missing-fields"],
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
    name = release_dirname("fixture")
    shutil.copytree(FIXTURE_DIR, cache / name)
    (cache / name / "release.json").write_text('{"release": "fixt')
    index = ReleaseIndex(
        latest="fixture",
        releases={"fixture": IndexEntry(url="https://example.org/pack", sha256="0")},
    )
    downloads = []

    def download(entry: IndexEntry, *, target: Path) -> None:
        downloads.append(entry.url)
        shutil.copytree(FIXTURE_DIR, target)

    monkeypatch.setattr(snapshot, "_shipped_index", lambda: index)
    monkeypatch.setattr(snapshot, "_download", download)

    snap = cf.load(cache_dir=cache)

    assert downloads == ["https://example.org/pack"]
    assert snap.manifest.release == "fixture"


def test_table_refuses_names_outside_the_release_contract() -> None:
    snap = cf.load(path=FIXTURE_DIR)

    with pytest.raises(KeyError, match="links"):
        snap.table("links")
    assert set(RELEASE_TABLES) <= set(snap.manifest.tables)


def test_disbursement_profile_reads_the_release_table_by_group_and_year() -> None:
    profile = cf.load(path=FIXTURE_DIR).disbursement_profile()

    assert profile.columns == ["group", "years_since", "cumulative_share", "vintages"]
    assert profile.filter(group="all", years_since=0)["cumulative_share"].item() == 0.23
    assert profile["years_since"].is_sorted()

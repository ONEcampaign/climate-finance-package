"""`load(path=)` opens a release only when every table matches `release.json`."""

import json
from pathlib import Path

import pytest
from make_fixture import FIXTURE_DIR
from releases import copy_release

import climate_finance as cf
from climate_finance.errors import ReleaseIntegrityError
from climate_finance.schema import RELEASE_TABLES


def test_load_opens_the_fixture_with_its_manifest() -> None:
    snap = cf.load(path=FIXTURE_DIR)

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


def test_table_refuses_names_outside_the_release_contract() -> None:
    snap = cf.load(path=FIXTURE_DIR)

    with pytest.raises(KeyError, match="links"):
        snap.table("links")
    assert set(RELEASE_TABLES) <= set(snap.manifest.tables)

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
from climate_finance import release as release_module
from climate_finance.errors import ReleaseIntegrityError
from climate_finance.release import IndexEntry, ReleaseIndex, release_dirname
from climate_finance.schema import RELEASE_TABLES, ReleaseManifest


@pytest.mark.parametrize("as_path", [Path, str])
def test_load_opens_the_fixture_with_its_manifest(as_path) -> None:
    release = cf.load(path=as_path(FIXTURE_DIR))

    assert isinstance(release, cf.Release)
    assert release.manifest.release == "fixture"
    assert release.table("commitments").collect().height == 14


def test_a_release_prints_its_name_flow_years_and_tables(tmp_path: Path) -> None:
    published = copy_release(tmp_path / "2026a")
    path = published / "release.json"
    manifest = ReleaseManifest.model_validate_json(path.read_text())
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
        ).model_dump_json()
    )

    # The fixture's name marks its figures as invented.
    assert repr(cf.load(path=FIXTURE_DIR)) == (
        "Release('fixture', synthetic figures): commitments 2021 to 2022, "
        "disbursements 2021 to 2024; 17 tables"
    )
    assert repr(cf.load(path=published)) == (
        "Release('2026a'): commitments 2021 to 2022, disbursements 2021 to 2024; "
        "15 tables, withholding btr, btr_links"
    )


def _change_bytes(release: Path) -> None:
    (release / "modelled.parquet").write_bytes(
        (release / "quality.parquet").read_bytes()
    )


def _drop_from_manifest(release: Path) -> None:
    manifest = json.loads((release / "release.json").read_text())
    del manifest["tables"]["objectives"]
    (release / "release.json").write_text(json.dumps(manifest))


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

    monkeypatch.setattr(release_module, "_shipped_index", lambda: index)
    monkeypatch.setattr(release_module, "_download", download)

    release = cf.load(cache_dir=cache)

    assert downloads == ["https://example.org/pack"]
    assert release.manifest.release == "fixture"


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

    assert "cf.load(path=...)" in str(raised.value)
    assert release_module.RELEASES_PAGE in str(raised.value)


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

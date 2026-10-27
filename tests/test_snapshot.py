"""`load(path=)` opens a release only when every table matches `release.json`."""

import json
from collections.abc import Callable
from pathlib import Path

import polars as pl
import pytest
from make_fixture import FIXTURE_DIR
from polars.testing import assert_frame_equal
from releases import copy_release

import climate_finance as cf
from climate_finance.errors import ReleaseIntegrityError
from climate_finance.schema import RELEASE_TABLES


@pytest.mark.parametrize("as_path", [Path, str])
def test_load_opens_the_fixture_with_its_manifest(as_path) -> None:
    snap = cf.load(path=as_path(FIXTURE_DIR))

    assert snap.manifest.release == "fixture"
    assert snap.table("commitments").collect().height == 14


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
    snap = cf.load(path="release")
    result = snap.spending(flow="commitments")

    monkeypatch.chdir(tmp_path / "b")
    other = cf.load(path="release").spending(flow="commitments")
    assert not other.table.to_polars().equals(expected.table.to_polars())

    table = result.table if lazy else snap.spending(flow="commitments").table
    assert_frame_equal(table.to_polars(), expected.table.to_polars())

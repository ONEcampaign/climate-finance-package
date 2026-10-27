"""Copies of the fixture release with tables edited and `release.json` rehashed."""

import shutil
from collections.abc import Callable, Mapping
from pathlib import Path

import polars as pl
from make_fixture import FIXTURE_DIR

from climate_finance.schema import ReleaseManifest, TableEntry, sha256_file

type Edit = Callable[[pl.DataFrame], pl.DataFrame]


def copy_release(target: Path, *, edits: Mapping[str, Edit] | None = None) -> Path:
    """Copy the fixture release to `target`, apply `edits` per table, rehash them."""
    shutil.copytree(FIXTURE_DIR, target)
    manifest = ReleaseManifest.model_validate_json(
        (target / "release.json").read_text()
    )
    tables = dict(manifest.tables)
    for name, edit in (edits or {}).items():
        path = target / f"{name}.parquet"
        frame = edit(pl.read_parquet(path))
        frame.write_parquet(path)
        tables[name] = TableEntry(sha256=sha256_file(path), rows=frame.height)
    rehashed = manifest.model_copy(update={"tables": tables})
    (target / "release.json").write_text(rehashed.model_dump_json(indent=2))
    return target

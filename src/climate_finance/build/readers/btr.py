"""BTR1 Table III.1 workbooks, read by header role."""

from pathlib import Path

import polars as pl


def read_btr_workbook(path: Path, *, party: str) -> pl.DataFrame:
    """One party's Table III.1 rows in the `btr` schema."""
    raise NotImplementedError("slice S19a")


def ingest_unfccc(archive_dir: Path, *, out_path: Path) -> Path:
    """Parse every workbook listed in the archive manifest into one parquet."""
    raise NotImplementedError("slice S19a")


def write_party_weights(weights: pl.DataFrame, *, path: Path) -> Path:
    """Write estimated party weights with their evidence, `confirmed: false`."""
    raise NotImplementedError("slice S19a")

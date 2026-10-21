"""GCF disbursements from the API and the readiness list."""

from pathlib import Path

import polars as pl


def read_gcf_disbursements(
    *, archive: Path | None = None, offline: bool
) -> pl.DataFrame:
    """GCF project disbursements by project id and date."""
    raise NotImplementedError("slice S12a")


def read_gcf_readiness(path: Path) -> pl.DataFrame:
    """GCF readiness grants by id and year."""
    raise NotImplementedError("slice S12a")

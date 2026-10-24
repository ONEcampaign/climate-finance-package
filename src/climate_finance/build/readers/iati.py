"""IATI activity XML for GEF and other publishers."""

from pathlib import Path

import polars as pl


def read_iati_activities(path: Path) -> pl.DataFrame:
    """Commitment (type 2) and disbursement (type 3) transactions by activity."""
    raise NotImplementedError("slice S04b")

"""World Bank Finances One loan disbursements (DS00975 IBRD, DS00976 IDA)."""

from pathlib import Path
from typing import Literal

import polars as pl


def read_finances_one(
    *,
    dataset: Literal["DS00975", "DS00976"],
    years: range,
    archive: Path | None = None,
    offline: bool,
) -> pl.DataFrame:
    """Calendar-year loan disbursements from the cumulative monthly snapshots."""
    raise NotImplementedError("slice S12a")

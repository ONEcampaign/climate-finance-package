"""BTR Table III.1 rows to same-year CRS records, per party."""

import polars as pl


def link_btr(btr: pl.LazyFrame, *, crs: pl.LazyFrame) -> pl.LazyFrame:
    """Ids in titles first, then per-party calibrated Splink."""
    raise NotImplementedError("slice S19b")

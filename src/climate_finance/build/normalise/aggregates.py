"""Id-less bilateral aggregate rows rebuilt from bulk records."""

import polars as pl


def rebuild_aggregates(crdf: pl.LazyFrame, *, crs: pl.LazyFrame) -> pl.LazyFrame:
    """Add child rows with `parent_row_id` for aggregates that partition to 0.1%."""
    raise NotImplementedError("slice S05b")

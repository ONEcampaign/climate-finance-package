"""Rows repeated across CRDF vintages."""

import polars as pl


def mark_repeats(crdf: pl.LazyFrame, *, crs: pl.LazyFrame) -> pl.LazyFrame:
    """Set `repeat_of` on later rows that repeat an earlier vintage's row."""
    raise NotImplementedError("slice S05b")

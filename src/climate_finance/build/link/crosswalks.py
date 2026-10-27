"""Crosswalks: GEF to implementing-agency ids, CRS project number to USAID award."""

import polars as pl


def build_crosswalks(crs: pl.LazyFrame, *, external: pl.LazyFrame) -> pl.LazyFrame:
    """Id pairs that let GEF and US commitments link by key."""
    raise NotImplementedError("slice S16")

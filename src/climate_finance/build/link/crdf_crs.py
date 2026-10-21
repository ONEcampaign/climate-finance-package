"""CRDF rows to CRS records, within a year, with the recovery routes."""

import polars as pl


def link_within_year(crdf: pl.LazyFrame, *, crs: pl.LazyFrame) -> pl.LazyFrame:
    """Join on (donor, crs id, year, purpose)."""
    raise NotImplementedError("slice S06a")


def recover(
    crdf: pl.LazyFrame, *, crs: pl.LazyFrame, external: pl.LazyFrame
) -> pl.LazyFrame:
    """EIB project-list, IATI crosswalk, title-recipient-amount, GCF and GEF joins."""
    raise NotImplementedError("slice S07")

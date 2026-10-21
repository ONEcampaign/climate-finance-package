"""Bulk CRS records into the `crs` schema, in USD units."""

import polars as pl


def normalise_crs(raw: pl.LazyFrame) -> pl.LazyFrame:
    """Rename, retype and convert the bulk CRS; keep `bi_multi` and null markers."""
    raise NotImplementedError("slice S05a")

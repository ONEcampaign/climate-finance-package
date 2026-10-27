"""Each (donor, agency)'s id practice per period."""

import polars as pl


def classify_id_practice(crs: pl.LazyFrame) -> pl.LazyFrame:
    """Class each (donor, agency, period) by how open commitments continue."""
    raise NotImplementedError("slice S06a")

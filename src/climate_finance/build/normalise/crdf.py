"""CRDF provider and recipient rows into the `crdf` schema."""

import polars as pl


def normalise_crdf(raw: pl.LazyFrame) -> pl.LazyFrame:
    """Normalise CRDF rows and classify each EIB row's `id_scheme`."""
    raise NotImplementedError("slice S05a")

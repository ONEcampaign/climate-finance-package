"""The exact key tiers, first match wins."""

import polars as pl


def normalise_pn(project_number: pl.Expr) -> pl.Expr:
    """Strip whitespace and null placeholder project numbers."""
    raise NotImplementedError("slice S06a")


def link_by_keys(
    units: pl.LazyFrame, *, commitments: pl.LazyFrame, id_practice: pl.LazyFrame
) -> pl.LazyFrame:
    """Link each disbursement unit by the first key tier reaching a commitment."""
    raise NotImplementedError("slice S06a")

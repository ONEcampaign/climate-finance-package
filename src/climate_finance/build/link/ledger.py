"""The ledger check: allocated CRS disbursement against bank ledgers."""

import polars as pl


def check_ledgers(
    links: pl.LazyFrame, *, allocations: pl.LazyFrame, ledgers: pl.LazyFrame
) -> pl.LazyFrame:
    """Set `ledger_check` per link and emit `ledger` pair labels."""
    raise NotImplementedError("slice S12b")

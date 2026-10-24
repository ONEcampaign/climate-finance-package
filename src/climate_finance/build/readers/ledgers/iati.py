"""IATI disbursement and expenditure transactions as ledgers."""

from pathlib import Path

import polars as pl


def read_iati_ledger(path: Path, *, publisher: str) -> pl.DataFrame:
    """Disbursement and expenditure transactions by activity id and date."""
    raise NotImplementedError("read_iati_ledger is not implemented")

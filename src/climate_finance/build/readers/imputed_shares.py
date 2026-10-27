"""`Imputed_multilateral_shares.xlsx`: the OECD's imputed climate shares."""

from pathlib import Path

import polars as pl


def read_imputed_shares(path: Path) -> pl.DataFrame:
    """Climate shares by channel, year and objective, skipping the draft sheet."""
    raise NotImplementedError("slice S04b")

"""The OECD multilateral system (DSD_MULTI@DF_MULTI) over SDMX."""

import polars as pl


def read_multisystem(*, start_year: int, end_year: int, offline: bool) -> pl.DataFrame:
    """Core contributions to multilateral agencies, by provider, channel and year."""
    raise NotImplementedError("slice S04b")

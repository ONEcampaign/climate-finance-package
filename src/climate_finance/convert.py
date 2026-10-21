"""Currency and price conversion through pydeflate, applied last."""

from typing import Literal

import polars as pl


def convert(
    frame: pl.LazyFrame, *, currency: str, prices: int | Literal["current"]
) -> pl.LazyFrame:
    """Convert USD current-price amounts to `currency` at `prices`."""
    raise NotImplementedError("slice S09a")

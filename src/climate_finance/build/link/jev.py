"""Jev adjudication of pairs in the band."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from climate_finance.build.jev_client import JevClient


def adjudicate_band(pairs: pl.LazyFrame, *, client: JevClient) -> pl.LazyFrame:
    """Combine model and Jev probabilities for pairs in the configured band."""
    raise NotImplementedError("adjudicate_band is not implemented")

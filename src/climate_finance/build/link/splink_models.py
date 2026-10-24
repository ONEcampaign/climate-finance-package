"""Per-provider Splink models with Jev-labelled m and isotonic calibration."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from climate_finance.build.jev_client import JevClient


def score_with_splink(
    candidates: pl.LazyFrame,
    *,
    provider_profile: pl.LazyFrame,
    labels: pl.LazyFrame,
    client: JevClient,
) -> pl.LazyFrame:
    """Calibrated probabilities for providers outside the truth group."""
    raise NotImplementedError("slice S13b")

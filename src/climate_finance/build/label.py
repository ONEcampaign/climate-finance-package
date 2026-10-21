"""The label stage: inherited markers and Jev objective labels."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from climate_finance.build.jev_client import JevClient
    from climate_finance.build.stages import StageContext, StageOutput


def run_label(context: StageContext) -> StageOutput:
    """Write `objectives` and `jev_level_map`."""
    raise NotImplementedError("slice S08 (Jev labels: slice S18b)")


def inherit_markers(
    allocations: pl.LazyFrame, *, commitments: pl.LazyFrame, crs: pl.LazyFrame
) -> pl.LazyFrame:
    """Each disbursement's markers and climate share from the commitment it fills."""
    raise NotImplementedError("slice S08")


def classify_objectives(records: pl.LazyFrame, *, client: JevClient) -> pl.LazyFrame:
    """Jev adaptation and mitigation labels for CRS texts."""
    raise NotImplementedError("slice S18b")


def fit_jev_level_map(labels: pl.LazyFrame, *, human: pl.LazyFrame) -> pl.LazyFrame:
    """Isotonic map from Jev's expected coefficient to a calibrated level."""
    raise NotImplementedError("slice S18b")

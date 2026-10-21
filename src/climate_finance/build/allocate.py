"""The allocate stage: dated FIFO allocation and commitment status."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from climate_finance.build.stages import StageContext, StageOutput


def run_allocate(context: StageContext) -> StageOutput:
    """Allocate linked disbursement units to commitments and set statuses."""
    raise NotImplementedError("slice S06b")


def allocate(
    units: pl.LazyFrame, *, commitments: pl.LazyFrame, links: pl.LazyFrame
) -> pl.LazyFrame:
    """Split each unit across its group's commitments, oldest first, capped at face."""
    raise NotImplementedError("slice S06b")


def compute_status(
    *,
    commitments: pl.LazyFrame,
    allocations: pl.LazyFrame,
    pending: pl.LazyFrame | None = None,
) -> pl.LazyFrame:
    """Status, disbursed bounds and `project_disbursing` per commitment."""
    raise NotImplementedError("slice S06b")

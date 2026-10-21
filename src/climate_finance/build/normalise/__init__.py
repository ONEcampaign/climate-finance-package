"""The normalise stage: CRDF, CRS and side sources into the stage schemas."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from climate_finance.build.stages import StageContext, StageOutput


def run_normalise(context: StageContext) -> StageOutput:
    """Normalise the fetched sources into `crdf`, `crs` and the side tables."""
    raise NotImplementedError("slice S05a")

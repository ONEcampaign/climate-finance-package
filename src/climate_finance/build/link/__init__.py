"""The link stage: keys, id practice, candidates and scoring into `links`."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from climate_finance.build.stages import StageContext, StageOutput


def run_link(context: StageContext) -> StageOutput:
    """Link CRDF rows to CRS records and later disbursements to commitments."""
    raise NotImplementedError("slice S06a")

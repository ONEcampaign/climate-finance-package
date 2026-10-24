"""The fill stage: gap fills from bank ledgers."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from climate_finance.build.stages import StageContext, StageOutput


def run_fill(context: StageContext) -> StageOutput:
    """Fill eligible commitments from ledgers in bank-years that pass overlap."""
    raise NotImplementedError("slice S15")

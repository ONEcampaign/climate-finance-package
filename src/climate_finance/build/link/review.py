"""The review stage: ledger check, review queue, pending status and decisions."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from climate_finance.build.stages import StageContext, StageOutput


def run_review(context: StageContext) -> StageOutput:
    """Check links against ledgers, build the queue, mark pending commitments."""
    raise NotImplementedError("slice S14 (ledger step: slice S12b)")


def write_queue_csv(*, release_dir: Path, out_path: Path) -> Path:
    """Write the review queue as a CSV with empty decision columns."""
    raise NotImplementedError("slice S14")


def apply_decisions(*, csv_path: Path, decisions_path: Path) -> Path:
    """Append reviewed decisions to the review-decisions parquet."""
    raise NotImplementedError("slice S14")

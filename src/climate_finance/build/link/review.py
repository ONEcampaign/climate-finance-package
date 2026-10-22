"""The review stage: ledger check, review queue, pending status and decisions."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from climate_finance.build.link.ledger import run_ledger_check

if TYPE_CHECKING:
    from climate_finance.build.stages import StageContext, StageOutput


def run_review(context: StageContext) -> StageOutput:
    """Check links against ledgers, build the queue, mark pending commitments.

    With `review` off, pass the link stage's links and the fill stage's commitments
    through unchanged. With it on, the ledger check sets `ledger_check` on the links
    and gives the pairs it confirms as `pair_labels`.
    """
    if not context.config.review:
        return {
            "links": context.read("link", "links"),
            "commitments": context.read("fill", "commitments"),
        }
    links, pair_labels = run_ledger_check(context)
    return {
        "links": links,
        "pair_labels": pair_labels,
        "review_queue": _review_queue(links),
        "commitments": _mark_pending(context.read("fill", "commitments"), links=links),
    }


def _review_queue(links: pl.LazyFrame) -> pl.LazyFrame:
    """The links people review: model links in the band and contradicted links."""
    raise NotImplementedError("slice S14")


def _mark_pending(commitments: pl.LazyFrame, *, links: pl.LazyFrame) -> pl.LazyFrame:
    """Commitments whose status waits on queued links, marked pending."""
    raise NotImplementedError("slice S14")


def write_queue_csv(*, release_dir: Path, out_path: Path) -> Path:
    """Write the review queue as a CSV with empty decision columns."""
    raise NotImplementedError("slice S14")


def apply_decisions(*, csv_path: Path, decisions_path: Path) -> Path:
    """Append reviewed decisions to the review-decisions parquet."""
    raise NotImplementedError("slice S14")

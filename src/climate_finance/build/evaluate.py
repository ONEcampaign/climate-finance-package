"""The stratified blind sample and the precision estimate."""

from pathlib import Path

import polars as pl


def draw_blind_sample(*, release_dir: Path, out_path: Path) -> Path:
    """Draw pairs stratified by provider and band, scores hidden, weights stored."""
    raise NotImplementedError("drawing the blind sample")


def estimate_precision(labels: pl.DataFrame) -> pl.DataFrame:
    """Per-provider precision with its sequential 95% interval."""
    raise NotImplementedError("estimating precision")

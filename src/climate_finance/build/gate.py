"""Release gates from `gates.yaml`."""

from pathlib import Path

import polars as pl


def run_gates(
    release_dir: Path, *, report_only: bool = False, waive_precision: bool = False
) -> pl.DataFrame:
    """Evaluate every gate; raise unless `report_only` when any fails."""
    raise NotImplementedError("slice S17")

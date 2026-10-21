"""Release gates from `gates.yaml`."""

from pathlib import Path

import polars as pl


def run_gates(
    release_dir: Path, *, report_only: bool = False, waive_precision: bool = False
) -> pl.DataFrame:
    """Evaluate every gate. When any gate fails, raise unless `report_only` is set.

    `waive_precision` lets a failed precision gate pass.
    """
    raise NotImplementedError("slice S17")

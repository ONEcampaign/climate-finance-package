"""The GCF projects API (`api.gcfund.org/v1/projects`)."""

from pathlib import Path

import polars as pl


def read_gcf_projects(*, archive: Path | None = None, offline: bool) -> pl.DataFrame:
    """GCF projects with their ids, recipients and approved amounts."""
    raise NotImplementedError("slice S04b")

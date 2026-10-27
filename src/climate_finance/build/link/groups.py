"""Project groups: each later unit joins its highest-probability group."""

import polars as pl


def assign_groups(links: pl.LazyFrame) -> pl.LazyFrame:
    """Set `group_id`, ties going to the older commitment."""
    raise NotImplementedError("slice S14")

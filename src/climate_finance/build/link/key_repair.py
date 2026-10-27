"""Key-repair rules, each writing its own `link_method`."""

import polars as pl


def repair_keys(
    units: pl.LazyFrame, *, commitments: pl.LazyFrame, links: pl.LazyFrame
) -> pl.LazyFrame:
    """Links for units the exact tiers missed through a keying fault."""
    raise NotImplementedError("slice S06c")

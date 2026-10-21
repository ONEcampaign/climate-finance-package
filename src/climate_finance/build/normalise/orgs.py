"""Provider, agency and channel names resolved with resolvekit's exact tiers."""

import polars as pl


def resolve_orgs(names: pl.LazyFrame) -> pl.LazyFrame:
    """Resolve names to DAC codes into the `orgs` schema."""
    raise NotImplementedError("slice S07")

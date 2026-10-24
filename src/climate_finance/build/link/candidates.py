"""Candidate pairs by blocking within donor, and programme stems."""

import polars as pl


def learn_programme_stems(crs: pl.LazyFrame) -> pl.LazyFrame:
    """Programme stem rules per (donor, agency), relearned each release."""
    raise NotImplementedError("slice S11")


def generate_candidates(
    units: pl.LazyFrame,
    *,
    commitments: pl.LazyFrame,
    links: pl.LazyFrame,
    stems: pl.LazyFrame,
) -> pl.LazyFrame:
    """Eligible unlinked pairs from the blocking rules, with `programme_level`."""
    raise NotImplementedError("slice S11")

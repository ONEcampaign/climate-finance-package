"""The truth-group classifier and the provider profile."""

import polars as pl


def build_provider_profile(
    candidates: pl.LazyFrame, *, links: pl.LazyFrame, id_practice: pl.LazyFrame
) -> pl.LazyFrame:
    """Sort providers into scoring groups from data."""
    raise NotImplementedError("build_provider_profile is not implemented")


def score_truth_group(
    candidates: pl.LazyFrame, *, labels: pl.LazyFrame, provider_profile: pl.LazyFrame
) -> pl.LazyFrame:
    """Calibrated probabilities from gradient boosting for the truth group."""
    raise NotImplementedError("score_truth_group is not implemented")

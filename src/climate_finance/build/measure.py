"""The measure stage: agency shares, attribution inputs, the modelled line."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from climate_finance.build.stages import StageContext, StageOutput


def run_measure(context: StageContext) -> StageOutput:
    """Write `agency_shares` and `modelled`."""
    raise NotImplementedError("slice S08")


def agency_shares(crs: pl.LazyFrame, *, imputed_shares: pl.LazyFrame) -> pl.LazyFrame:
    """Each agency's climate share by year and objective, ONE and OECD."""
    raise NotImplementedError("slice S08")


def modelled_ranges(crdf: pl.LazyFrame, *, crs: pl.LazyFrame) -> pl.LazyFrame:
    """Low and high disbursement for providers that publish none."""
    raise NotImplementedError("slice S08")

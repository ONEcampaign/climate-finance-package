"""The label stage: inherited markers and objective labels.

A disbursement inherits the Rio markers and climate share of the CRDF commitment it
fills, and keeps the CRS record's own markers beside them. `marker_source` says how
the two relate: `components` where the commitment carries climate components (a
climate share, which the weighting reads in place of markers), `commitment` where
the record's own markers match the inherited ones, and `record_differs` where they
do not.

Each linked record's reported objectives enter `objectives`: the CRDF objective of
the climate-component commitments it fills (`crdf`, level 2), and the record's own
Rio markers (`marker`).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from climate_finance.schema import JEV_LEVEL_MAP, OBJECTIVE_LABELS, OBJECTIVES

if TYPE_CHECKING:
    from climate_finance.build.jev_client import JevClient
    from climate_finance.build.stages import StageContext, StageOutput

MARKER_SOURCES = ("components", "commitment", "record_differs")
# The level a climate-component commitment gives its CRDF objective.
COMPONENT_LEVEL = 2


def run_label(context: StageContext) -> StageOutput:
    """Write `objectives` and `jev_level_map`.

    The objectives hold the CRDF and marker labels of linked records. Jev labels and
    their level map are not built yet, so `jev_level_map` is written empty.
    """
    objectives = reported_objectives(
        context.read("fill", "allocations"),
        commitments=context.read("normalise", "crdf"),
        crs=context.read("normalise", "crs"),
    )
    return {
        "objectives": objectives,
        "jev_level_map": pl.DataFrame(
            schema={
                name: column.dtype for name, column in JEV_LEVEL_MAP.columns.items()
            }
        ),
    }


def inherit_markers(
    allocations: pl.LazyFrame, *, commitments: pl.LazyFrame, crs: pl.LazyFrame
) -> pl.LazyFrame:
    """Each disbursement's markers and climate share from the commitment it fills.

    Args:
        allocations: allocations; rows without a CRDF commitment are left out.
        commitments: CRDF rows, by `row_id`.
        crs: CRS records, by `record_id`.

    Returns:
        The allocations to CRDF commitments with `inherited_rio_adaptation`,
        `inherited_rio_mitigation`, `record_rio_adaptation`,
        `record_rio_mitigation`, `marker_source` and `climate_share`.
    """
    inherited = commitments.select(
        "row_id",
        "climate_share",
        inherited_rio_adaptation="rio_adaptation",
        inherited_rio_mitigation="rio_mitigation",
    )
    own = crs.select(
        "record_id",
        record_rio_adaptation="rio_adaptation",
        record_rio_mitigation="rio_mitigation",
    )
    agree = pl.col("record_rio_adaptation").eq_missing(
        pl.col("inherited_rio_adaptation")
    ) & pl.col("record_rio_mitigation").eq_missing(pl.col("inherited_rio_mitigation"))
    return (
        allocations.filter(pl.col("row_id").is_not_null())
        .join(inherited, on="row_id", how="left", validate="m:1")
        .join(own, on="record_id", how="left", validate="m:1")
        .with_columns(
            marker_source=pl.when(pl.col("climate_share").is_not_null())
            .then(pl.lit("components"))
            .when(agree)
            .then(pl.lit("commitment"))
            .otherwise(pl.lit("record_differs"))
        )
    )


def reported_objectives(
    allocations: pl.LazyFrame, *, commitments: pl.LazyFrame, crs: pl.LazyFrame
) -> pl.LazyFrame:
    """CRDF and marker objective labels for every record allocated to a CRDF
    commitment.

    A record filling climate-component commitments takes each one's CRDF objective
    at `COMPONENT_LEVEL`, and every linked record takes its own Rio markers, where
    set, as `marker` labels. A record holds one label per objective and source, at
    the highest level.
    """
    linked = allocations.filter(pl.col("row_id").is_not_null()).select(
        "row_id", "record_id"
    )
    components = commitments.filter(
        pl.col("climate_share").is_not_null(), pl.col("objective").is_in(OBJECTIVES)
    ).select("row_id", "objective")
    crdf = linked.join(components, on="row_id").select(
        "record_id",
        "objective",
        level=pl.lit(COMPONENT_LEVEL, pl.Int8),
        label_source=pl.lit("crdf"),
    )
    markers = (
        linked.select("record_id")
        .unique()
        .join(
            crs.select("record_id", "rio_adaptation", "rio_mitigation"), on="record_id"
        )
        .unpivot(
            index="record_id",
            on=["rio_adaptation", "rio_mitigation"],
            variable_name="objective",
            value_name="level",
        )
        .drop_nulls("level")
        .select(
            "record_id",
            pl.col("objective").str.strip_prefix("rio_"),
            "level",
            label_source=pl.lit("marker"),
        )
    )
    return (
        pl.concat([crdf, markers])
        .group_by("record_id", "objective", "label_source")
        .agg(pl.col("level").max())
        .with_columns(probability=pl.lit(None, pl.Float64))
        .select(list(OBJECTIVE_LABELS.columns))
    )


def classify_objectives(records: pl.LazyFrame, *, client: JevClient) -> pl.LazyFrame:
    """Jev adaptation and mitigation labels for CRS texts."""
    raise NotImplementedError("slice S18b")


def fit_jev_level_map(labels: pl.LazyFrame, *, human: pl.LazyFrame) -> pl.LazyFrame:
    """Isotonic map from Jev's expected coefficient to a calibrated level."""
    raise NotImplementedError("slice S18b")

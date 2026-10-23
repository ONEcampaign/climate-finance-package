"""The review stage: ledger check, review queue, pending status and decisions.

A trace link joins the queue when the ledger contradicts it, when a rule linked it
though its records disagree on recipient or project number, when a classifier gave
it a probability in the Jev band (`config.jev.band`), or when Jev gave a Splink link
a probability in `JEV_REVIEW_BAND`, its reason the first of these that holds. The
queue also holds the Splink calibration pairs and every commitment row allocated
more than `OVER_FACE` times its face. A pair someone has decided stays out, and an
over-face row returns in every build while it stays over face.

`write_queue_packet` renders the queue for people and agents to decide.

A queued link keeps its allocation, so money stays in totals. A commitment row is
`pending_review` when rejecting the queued links into its commitment group would
change its status (`pending_rows`).
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import polars as pl
from scipy.sparse import coo_array
from scipy.sparse.csgraph import connected_components

from climate_finance import schema
from climate_finance.build.allocate import (
    allocate,
    allocate_to_rows,
    compute_status,
    counted_rows,
    dated_donors,
    window_donors,
)
from climate_finance.build.link.decisions import PAIR, drop_pairs
from climate_finance.build.link.ledger import run_ledger_check
from climate_finance.build.link.packet import code_names, write_packet
from climate_finance.build.link.splink_models import read_decision_labels
from climate_finance.build.normalise.crs import trace_population

if TYPE_CHECKING:
    from climate_finance.build.stages import StageContext, StageOutput

# Links whose probability is a classifier's calibrated probability.
CLASSIFIER_METHODS = ("classifier", "classifier_jev", "weak_classifier")
JEV_REVIEW_BAND = (0.5, 0.9)
OVER_FACE = 1.1


def run_review(context: StageContext) -> StageOutput:
    """Check links against ledgers, build the queue, mark pending commitments.

    With `review` off, pass the link stage's links and the fill stage's commitments
    through unchanged. With it on, the ledger check sets `ledger_check` on the links
    and gives the pairs it confirms as `pair_labels`, queued links carry `review`
    and their `review_reason`, and status is recomputed with the pending rows.
    """
    if not context.config.review:
        return {
            "links": context.read("link", "links"),
            "commitments": context.read("fill", "commitments"),
        }
    checked, pair_labels = run_ledger_check(context)
    decided = read_decision_labels(context.config.sources.review_decisions)
    links = flag_review(checked.collect(), labels=decided, band=context.config.jev.band)
    commitments = context.read("fill", "commitments").collect()
    allocations = context.read("fill", "allocations").collect()
    pending = pending_rows(
        commitments,
        allocations=allocations,
        links=links,
        units=context.read("link", "units"),
        crs=trace_population(context.read("normalise", "crs")),
        crdf=counted_rows(context.read("normalise", "crdf")),
    )
    return {
        "links": links,
        "pair_labels": pair_labels,
        "review_queue": review_queue(
            links,
            commitments=commitments,
            pair_scores=context.read("link", "pair_scores").collect(),
            labels=decided,
        ),
        "commitments": compute_status(
            commitments=commitments, allocations=allocations, pending=pending
        ),
    }


def flag_review(
    links: pl.DataFrame, *, labels: pl.DataFrame, band: tuple[float, float]
) -> pl.DataFrame:
    """`links` with `review` and `review_reason` set on the trace links the queue
    holds (see the module docstring) and cleared on the others.

    Args:
        links: `links` rows, with the ledger check's `ledger_check`.
        labels: `read_decision_labels`; a pair people or agents decided is not
            queued.
        band: the classifier probabilities queued, low inclusive.
    """
    reason = (
        pl.when(pl.col("task") != "trace")
        .then(None)
        .when(pl.col("ledger_check") == "contradicted")
        .then(pl.lit("ledger_contradicted"))
        .when(pl.col("records_disagree"))
        .then(pl.lit("rule_disagreement"))
        .when(
            pl.col("link_method").is_in(CLASSIFIER_METHODS)
            & pl.col("probability").is_between(*band, closed="left")
        )
        .then(pl.lit("band"))
        .when(
            (pl.col("link_method") == "splink_jev")
            & pl.col("p_jev").is_between(*JEV_REVIEW_BAND, closed="left")
        )
        .then(pl.lit("jev_band"))
    )
    decided = labels.select(PAIR, decided=pl.lit(True))
    return (
        links.join(decided, on=PAIR, how="left", maintain_order="left")
        .with_columns(review_reason=pl.when(pl.col("decided").is_null()).then(reason))
        .with_columns(review=pl.col("review_reason").is_not_null())
        .select(links.columns)
    )


def review_queue(
    links: pl.DataFrame,
    *,
    commitments: pl.DataFrame,
    pair_scores: pl.DataFrame,
    labels: pl.DataFrame,
) -> pl.DataFrame:
    """The `review_queue` table: the links `flag_review` flagged, the Splink
    calibration pairs that are not links, and the commitment rows allocated over
    `OVER_FACE` times face, as `crdf_crs` rows with no right side, less the pairs
    `labels` (`read_decision_labels`) holds.

    `field_weights` holds a scored pair's comparison levels as JSON.
    """
    columns = {
        name: column.dtype for name, column in schema.REVIEW_QUEUE.columns.items()
    }
    field_weights = pl.col("levels").struct.json_encode()
    flagged = links.filter("review").select(
        *PAIR,
        "probability",
        "p_model",
        "p_jev",
        "review_reason",
        field_weights=field_weights,
    )
    calibration = (
        pair_scores.filter(review_reason="calibration")
        .select(
            "left_id",
            "right_id",
            "probability",
            "review_reason",
            task=pl.lit("trace"),
            field_weights=field_weights,
        )
        .join(links.select(PAIR), on=PAIR, how="anti")
    )
    face = pl.col("face_usd")
    over_face = commitments.filter(
        face > 0, pl.col("disbursed_usd") > OVER_FACE * face
    ).select(
        task=pl.lit("crdf_crs"),
        left_id="row_id",
        right_id=pl.lit(None, dtype=pl.String),
        review_reason=pl.lit("over_face"),
    )
    queue = pl.concat([flagged, calibration, over_face], how="diagonal_relaxed")
    return (
        queue.join(labels.select(PAIR), on=PAIR, how="anti")
        .with_columns(
            pl.lit(None).alias(name) for name in columns if name not in queue.columns
        )
        .select(pl.col(name).cast(dtype) for name, dtype in columns.items())
    )


def pending_rows(
    commitments: pl.DataFrame,
    *,
    allocations: pl.DataFrame,
    links: pl.DataFrame,
    units: pl.LazyFrame,
    crs: pl.LazyFrame,
    crdf: pl.LazyFrame,
) -> pl.DataFrame:
    """The `row_id`s of commitment rows whose status waits on queued links.

    A row's queued links are the flagged trace links into the units of its
    `crdf_crs` group. Accepting them all gives the row its status in
    `commitments`. Rejecting them all (`drop_pairs`) gives the status a new
    allocation gives it, with the fill stage's ledger and IATI allocations held.
    The row is pending when the two differ.

    Rejecting links changes allocation only within their connected component of
    trace links, `crdf_crs` links and repeat rows. Each round reallocates the
    components of at most one queued group each, so a component holding k queued
    groups takes k rounds and the others none.

    Args:
        commitments: the fill stage's `commitments`.
        allocations: the fill stage's `allocations`.
        links: the links with `review` set (`flag_review`).
        units: the link stage's `units`.
        crs: the CRS trace population.
        crdf: the counted CRDF rows (`counted_rows`).
    """
    no_rows = pl.DataFrame(schema={"row_id": pl.String})
    groups = links.filter(task="crdf_crs").select(
        "group_id", row_id="left_id", unit_id="right_id"
    )
    queued = (
        links.filter("review", task="trace")
        .select(*PAIR, unit_id="right_id")
        .join(groups.select("unit_id", "group_id").unique(), on="unit_id")
    )
    if queued.is_empty():
        return no_rows
    component = _components(links, rows=crdf.select("row_id", "repeat_of").collect())
    unit_component = component.filter(kind="unit").select(
        unit_id="id", component="component"
    )
    rounds = (
        queued.select("group_id", "unit_id")
        .join(unit_component, on="unit_id")
        .select("group_id", "component")
        .unique()
        .with_columns(
            round=pl.col("group_id").rank("ordinal").over("component").cast(pl.Int32)
        )
    )
    # Donor classes come from every unit, as the allocate stage reads them.
    dated = dated_donors(units)
    windowed = window_donors(units, crs=crs)

    # Narrow the inputs once to the components some round reallocates.
    touched = rounds.select("component").unique()
    near_units = (
        units.collect()
        .join(unit_component, on="unit_id")
        .join(touched, on="component", how="semi")
    )
    record_ids = near_units.select(pl.col("record_ids").explode().alias("record_id"))
    near_crs = crs.join(record_ids.lazy(), on="record_id", how="semi").collect().lazy()
    near_rows = (
        crdf.collect()
        .join(
            component.filter(kind="row").select(row_id="id", component="component"),
            on="row_id",
        )
        .join(touched, on="component", how="semi")
    )
    near_links = links.join(
        unit_component.rename({"unit_id": "right_id"}), on="right_id"
    ).join(touched, on="component", how="semi")

    held = allocations.filter(pl.col("disbursement_source") != "crs")
    accepted = commitments.select("row_id", accepted="status")
    pending = [no_rows]
    for these in rounds.sort("round").partition_by("round", maintain_order=True):
        own = these.select("component")
        rejected = drop_pairs(
            near_links.join(own, on="component", how="semi").drop("component"),
            pairs=queued.join(these, on="group_id"),
        ).lazy()
        round_units = (
            near_units.join(own, on="component", how="semi").drop("component").lazy()
        )
        pairs = allocate(
            round_units,
            crs=near_crs,
            links=rejected,
            dated=dated,
            windowed=windowed,
        )
        allocated = allocate_to_rows(
            pairs,
            crdf=near_rows.join(own, on="component", how="semi").lazy(),
            units=round_units,
            crs=near_crs,
            links=rejected,
        )
        rows = groups.join(these, on="group_id").select("row_id").unique()
        status = compute_status(
            commitments=commitments.join(rows, on="row_id"),
            allocations=pl.concat([allocated, held.join(rows, on="row_id")]),
        )
        pending.append(
            status.collect()
            .join(accepted, on="row_id")
            .filter(pl.col("status") != pl.col("accepted"))
            .select("row_id")
        )
    return pl.concat(pending).unique().sort("row_id")


def _components(links: pl.DataFrame, *, rows: pl.DataFrame) -> pl.DataFrame:
    """The connected component of each linked unit and CRDF row (`kind`, `id`,
    `component`) in the graph of trace links, `crdf_crs` links and repeat rows."""
    edges = pl.concat(
        [
            links.filter(task="trace").select(
                a_kind=pl.lit("unit"), a="left_id", b_kind=pl.lit("unit"), b="right_id"
            ),
            links.filter(task="crdf_crs").select(
                a_kind=pl.lit("row"), a="left_id", b_kind=pl.lit("unit"), b="right_id"
            ),
            rows.drop_nulls("repeat_of").select(
                a_kind=pl.lit("row"), a="row_id", b_kind=pl.lit("row"), b="repeat_of"
            ),
        ]
    )
    nodes = (
        pl.concat(
            [edges.select(kind="a_kind", id="a"), edges.select(kind="b_kind", id="b")]
        )
        .unique()
        .sort("kind", "id")
        .with_row_index("node")
    )
    ends = edges.join(
        nodes.rename({"kind": "a_kind", "id": "a", "node": "a_node"}),
        on=["a_kind", "a"],
    ).join(
        nodes.rename({"kind": "b_kind", "id": "b", "node": "b_node"}),
        on=["b_kind", "b"],
    )
    graph = coo_array(
        (
            np.ones(ends.height, dtype=np.int8),
            (ends["a_node"].to_numpy(), ends["b_node"].to_numpy()),
        ),
        shape=(nodes.height, nodes.height),
    )
    _, labels = connected_components(graph, directed=False)
    return nodes.select("kind", "id", component=pl.Series(labels, dtype=pl.Int64))


def write_queue_packet(*, release_dir: Path, out: Path, show_ids: bool = True) -> Path:
    """Write the review queue's pairs, the Splink calibration pairs among them, as a
    labelling packet (`packet.write_packet`), with the commitment rows allocated
    over `OVER_FACE` times face in a table after the pairs.

    Names of agencies, purposes, channels and flows come from the bulk CRS file
    fetch wrote, when the release still holds it.
    """
    # The stage registry imports this module, so read its paths at call time.
    from climate_finance.build.fetch import CRS_FILE
    from climate_finance.build.stages import raw_dir, table_path

    queue = pl.read_parquet(table_path(release_dir, "review", "review_queue"))
    over_face = (
        queue.filter(review_reason="over_face")
        .select(row_id="left_id")
        .join(
            pl.read_parquet(table_path(release_dir, "review", "commitments")),
            on="row_id",
        )
        .sort("row_id")
    )
    appendix = [
        f"## Commitments allocated over {OVER_FACE} times face",
        "",
        "| CRDF row | Face | Disbursed |",
        "| --- | --- | --- |",
        *(
            f"| {row_id} | USD {face:,.0f} | USD {disbursed:,.0f} |"
            for row_id, face, disbursed in over_face.select(
                "row_id", "face_usd", "disbursed_usd"
            ).iter_rows()
        ),
    ]
    raw_crs = raw_dir(release_dir) / CRS_FILE
    return write_packet(
        queue.filter(task="trace").drop_nulls("right_id").sort(PAIR),
        units=schema.scan_table(table_path(release_dir, "link", "units")),
        crs=schema.scan_table(table_path(release_dir, "normalise", "crs")),
        out=out,
        title="Review queue",
        show_ids=show_ids,
        names=code_names(raw_crs) if raw_crs.exists() else None,
        appendix="\n".join(appendix) if over_face.height else None,
    )

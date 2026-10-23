"""The review stage: ledger check, review queue, pending status and decisions.

A trace link joins the queue when the ledger contradicts it, when a rule linked it
though its records disagree on recipient or project number, when a classifier gave
it a probability in the Jev band (`config.jev.band`), or when Jev gave a Splink link
a probability in `JEV_REVIEW_BAND`, its reason the first of these that holds. The
queue also holds the Splink calibration pairs and every commitment row allocated
more than `OVER_FACE` times its face. A pair someone has decided stays out, and an
over-face row returns in every build while it stays over face.

A queued link keeps its allocation, so money stays in totals. A commitment row is
`pending_review` when rejecting the queued links into its commitment group would
change its status (`pending_rows`).
"""

from __future__ import annotations

from datetime import UTC, datetime
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
from climate_finance.build.errors import ReviewDecisionsError
from climate_finance.build.link.decisions import PAIR, drop_pairs
from climate_finance.build.link.ledger import run_ledger_check
from climate_finance.build.link.splink_models import read_human_labels
from climate_finance.build.normalise.crs import trace_population

if TYPE_CHECKING:
    from climate_finance.build.stages import StageContext, StageOutput

# Links whose probability is a classifier's calibrated probability.
CLASSIFIER_METHODS = ("classifier", "classifier_jev", "weak_classifier")
JEV_REVIEW_BAND = (0.5, 0.9)
OVER_FACE = 1.1
# What the queue CSV shows of each record of a pair.
SIDE_COLUMNS = (
    "donor_code",
    "crs_id",
    "project_number",
    "recipient_code",
    "purpose_code",
    "year",
    "usd",
    "title",
)
_DECISION_COLUMNS = ("decision", "reviewer", "note")


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
    human = read_human_labels(context.config.sources.review_decisions)
    links = flag_review(checked.collect(), labels=human, band=context.config.jev.band)
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
            labels=human,
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
        labels: `read_human_labels`; a pair people decided is not queued.
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
    `labels` (`read_human_labels`) holds.

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


def write_queue_csv(*, release_dir: Path, out_path: Path) -> Path:
    """Write the review queue as a CSV with empty decision columns.

    Each row sets its two records side by side, as `left_` and `right_` columns of
    `SIDE_COLUMNS`. A unit shows its keys, year and amount (its commitment for a
    commitment unit, else its disbursement) with its largest record's title, and a
    CRDF row its own fields and face.
    """
    # The stage registry imports this module, so read its paths at call time.
    from climate_finance.build.stages import table_path

    queue = pl.read_parquet(table_path(release_dir, "review", "review_queue"))
    units = _unit_sides(
        pl.scan_parquet(table_path(release_dir, "link", "units")),
        crs=pl.scan_parquet(table_path(release_dir, "normalise", "crs")),
        ids=pl.concat([queue["left_id"], queue["right_id"]]).drop_nulls().unique(),
    )
    rows = (
        pl.scan_parquet(table_path(release_dir, "normalise", "crdf"))
        .join(queue.lazy().select(row_id="left_id"), on="row_id", how="semi")
        .select(
            id="row_id",
            donor_code="provider_code",
            crs_id="crs_id",
            project_number="project_number",
            recipient_code="recipient_code",
            purpose_code="purpose_code",
            year="year",
            usd="face_usd",
            title="title",
        )
        .collect()
    )
    left = pl.concat(
        [
            queue.filter(task="trace").join(units, left_on="left_id", right_on="id"),
            queue.filter(task="crdf_crs").join(rows, left_on="left_id", right_on="id"),
        ],
        how="diagonal_relaxed",
    ).rename({name: f"left_{name}" for name in SIDE_COLUMNS})
    right = units.rename({name: f"right_{name}" for name in SIDE_COLUMNS})
    head = [
        name for name in schema.REVIEW_QUEUE.columns if name not in _DECISION_COLUMNS
    ]
    left.join(right, left_on="right_id", right_on="id", how="left").sort(
        PAIR, nulls_last=True
    ).select(
        *head,
        *(f"left_{name}" for name in SIDE_COLUMNS),
        *(f"right_{name}" for name in SIDE_COLUMNS),
        *_DECISION_COLUMNS,
    ).write_csv(out_path)
    return out_path


def _unit_sides(
    units: pl.LazyFrame, *, crs: pl.LazyFrame, ids: pl.Series
) -> pl.DataFrame:
    """`SIDE_COLUMNS` of the units `ids` names, under `id`."""
    wanted = units.join(ids.to_frame("unit_id").lazy(), on="unit_id", how="semi")
    size = pl.max_horizontal(
        pl.col("commitment_usd").abs(), pl.col("disbursement_usd").abs()
    ).fill_null(0.0)
    titles = (
        wanted.select("unit_id", record_id="record_ids")
        .explode("record_id")
        .join(
            crs.select("record_id", "title", "commitment_usd", "disbursement_usd"),
            on="record_id",
        )
        .sort("unit_id", size, "record_id", descending=[False, True, False])
        .group_by("unit_id", maintain_order=True)
        .agg(pl.col("title").first())
    )
    return (
        wanted.join(titles, on="unit_id", how="left")
        .select(
            id="unit_id",
            donor_code="donor_code",
            crs_id="crs_id",
            project_number="project_number",
            recipient_code="recipient_code",
            purpose_code="purpose_code",
            year="year",
            usd=pl.when(pl.col("commitment_usd") > 0)
            .then("commitment_usd")
            .otherwise("disbursement_usd"),
            title="title",
        )
        .collect()
    )


def apply_decisions(*, csv_path: Path, decisions_path: Path) -> Path:
    """Append the decided rows of a reviewed CSV to the review-decisions parquet.

    A row is decided when its `decision` is filled. It must name its `reviewer` and
    carry either the queue's `review_reason` or a blind sample's `sample_id`, which
    `read_human_labels` reads to keep blind-sample decisions out of the build.

    Raises:
        ReviewDecisionsError: naming decided rows whose decision is not `accept` or
            `reject`, that name no reviewer, or that carry neither or both of
            `review_reason` and `sample_id`.
    """
    columns = {
        name: column.dtype for name, column in schema.REVIEW_DECISIONS.columns.items()
    }
    given = [name for name in columns if name != "decided_at"]
    reviewed = pl.read_csv(csv_path, infer_schema=False)
    decided = (
        reviewed.with_columns(
            pl.lit(None, dtype=pl.String).alias(name)
            for name in given
            if name not in reviewed.columns
        )
        .select(pl.col(given).str.strip_chars().replace("", None))
        .filter(pl.col("decision").is_not_null())
    )
    tags = pl.sum_horizontal(
        pl.col("review_reason").is_not_null(), pl.col("sample_id").is_not_null()
    )
    bad = decided.filter(
        ~pl.col("decision").is_in(schema.DECISIONS)
        | pl.col("reviewer").is_null()
        | (tags != 1)
    )
    if bad.height:
        raise ReviewDecisionsError(
            f"{bad.height} decided rows of {csv_path} need a decision of accept or "
            "reject, a reviewer, and one of review_reason or sample_id, such as "
            f"{bad.select(PAIR).head(3).rows()}"
        )
    new = decided.with_columns(decided_at=pl.lit(datetime.now(UTC))).select(
        pl.col(name).cast(dtype) for name, dtype in columns.items()
    )
    schema.validate(schema.REVIEW_DECISIONS, new)
    if decisions_path.exists():
        new = pl.concat([pl.read_parquet(decisions_path), new])
    decisions_path.parent.mkdir(parents=True, exist_ok=True)
    new.write_parquet(decisions_path)
    return decisions_path

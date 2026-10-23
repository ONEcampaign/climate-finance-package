"""Decisions on review pairs: imported from labelling packets, applied as exact
link overrides.

People and agents decide the pairs of a packet (`packet.write_packet`) and return a
CSV that `import_labels` appends to the review-decisions table.
`splink_models.read_decision_labels` reads the queue decisions back as `human` and
`agent` pair labels, leaving out blind-sample decisions, which measure the links'
precision and so neither calibrate a model nor change a link. An accepted link takes
probability 1, and its records no longer count as disagreeing. A rejected link
leaves `links` (`drop_pairs`).
"""

from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from climate_finance import schema
from climate_finance.build.errors import ReviewDecisionsError
from climate_finance.build.link.packet import packet_digest, packet_pairs_path

PAIR = ["task", "left_id", "right_id"]
# A labels CSV's columns, and what its `label` says of a pair.
LABEL_COLUMNS = ("pair", "label", "confidence", "reason")
LABELS = {"match": "accept", "no_match": "reject"}
# The labels that decide pairs: people's and agents'.
DECIDING_SOURCES = ("human", "agent")


def override_links(links: pl.DataFrame, *, labels: pl.DataFrame) -> pl.DataFrame:
    """`links` with the `human` and `agent` pair labels of `labels` applied:
    accepted links at probability 1 with records that agree, rejected links dropped
    (`drop_pairs`). A label on a pair the build did not link changes nothing."""
    decided = labels.filter(pl.col("pair_label_source").is_in(DECIDING_SOURCES))
    accepted = decided.filter("is_match").select(PAIR, accepted=pl.lit(True))
    kept = drop_pairs(links, pairs=decided.filter(~pl.col("is_match")))
    is_accepted = pl.col("accepted").fill_null(False)
    return (
        kept.join(accepted, on=PAIR, how="left", maintain_order="left")
        .with_columns(
            probability=pl.when(is_accepted).then(1.0).otherwise("probability"),
            records_disagree=pl.when(is_accepted & pl.col("records_disagree"))
            .then(False)
            .otherwise("records_disagree"),
        )
        .drop("accepted")
    )


def drop_pairs(links: pl.DataFrame, *, pairs: pl.DataFrame) -> pl.DataFrame:
    """`links` less the links of `pairs` (`task`, `left_id`, `right_id`).

    Allocation pools a trace group's disbursement units over the commitments they
    all reach. A dropped trace link whose disbursement unit keeps another link in
    its group therefore takes its commitment out of the group for every unit of the
    group, so they still reach the same commitments. A unit whose only link in the
    group is dropped just leaves it.
    """
    dropped = links.join(pairs.select(PAIR), on=PAIR, how="semi")
    kept = links.join(dropped, on=PAIR, how="anti")
    trace = pl.col("task") == "trace"
    group = ["task", "group_id"]
    shared = (
        dropped.filter(trace)
        .join(kept.filter(trace).select("left_id", *group), on=["left_id", *group])
        .select(*group, "right_id")
        .unique()
    )
    return kept.join(shared, on=[*group, "right_id"], how="anti", maintain_order="left")


def import_labels(
    *,
    packet: Path,
    labels_csv: Path,
    decisions_path: Path,
    label_source: str,
    reviewer: str,
    model: str | None = None,
    label_round: str | None = None,
) -> Path:
    """Append the decisions of a labels CSV to the review-decisions table.

    The CSV holds `LABEL_COLUMNS`: the packet's pair number, `match` or `no_match`,
    the decider's confidence that the pair matches, from 0 to 1 and on the label's
    side of 0.5, and the reason. It
    must answer exactly the pairs of `packet`, and the packet must be the one its
    pairs file was written with.

    Args:
        packet: the packet the CSV answers (`packet.write_packet`).
        labels_csv: the CSV.
        decisions_path: the review-decisions parquet.
        label_source: `human` or `agent`.
        reviewer: the person, or the agent run, that decided.
        model: the agent's model; an agent's decisions need it, a person's take none.
        label_round: the agent's labelling round (`schema.LABEL_ROUNDS`), `label`
            when not given; a person's decisions take none.

    Returns:
        `decisions_path`.

    Raises:
        ReviewDecisionsError: when the packet changed since its pairs were written,
            the CSV's pairs differ from the packet's, a label or confidence is out of
            range or the two disagree, or the provenance arguments do not fit
            `label_source`.
    """
    if label_source not in schema.DECIDERS:
        raise ReviewDecisionsError(f"label_source must be one of {schema.DECIDERS}")
    agent = label_source == "agent"
    if agent and model is None:
        raise ReviewDecisionsError("an agent's decisions need its model")
    if not agent and (model is not None or label_round is not None):
        raise ReviewDecisionsError("a person's decisions take no model or round")
    label_round = (label_round or "label") if agent else None
    if label_round is not None and label_round not in schema.LABEL_ROUNDS:
        raise ReviewDecisionsError(f"round must be one of {schema.LABEL_ROUNDS}")
    pairs = pl.read_parquet(packet_pairs_path(packet))
    if pairs["packet_sha256"].unique().to_list() != [packet_digest(packet)]:
        raise ReviewDecisionsError(
            f"{packet} changed since its pairs were written; render it again"
        )
    given = pl.read_csv(labels_csv, infer_schema=False)
    if missing := sorted(set(LABEL_COLUMNS) - set(given.columns)):
        raise ReviewDecisionsError(f"{labels_csv} lacks columns {missing}")
    given = given.select(pl.col(LABEL_COLUMNS).str.strip_chars()).with_columns(
        pl.col("pair").cast(pl.Int32, strict=False),
        pl.col("confidence").cast(pl.Float64, strict=False),
    )
    expected, answered = set(pairs["pair"]), given["pair"].to_list()
    if (
        None in answered
        or len(answered) != len(set(answered))
        or set(answered) != expected
    ):
        raise ReviewDecisionsError(
            f"{labels_csv} must answer each of the {len(expected)} pairs of {packet} "
            f"once; it answers {len(set(answered) - {None})}, with "
            f"{len(answered) - len(set(answered))} repeats and "
            f"{len(set(answered) - expected - {None})} pairs the packet lacks"
        )
    confidence = pl.col("confidence")
    label = pl.col("label")
    contradicts = ((label == "match") & (confidence < 0.5)) | (
        (label == "no_match") & (confidence > 0.5)
    )
    bad = given.filter(
        ~label.is_in(list(LABELS))
        | (confidence.is_null() if agent else pl.lit(value=False))
        | ~confidence.is_between(0.0, 1.0).fill_null(value=not agent)
        | contradicts.fill_null(value=False)
    )
    if bad.height:
        raise ReviewDecisionsError(
            f"{bad.height} rows of {labels_csv} need a label of match or no_match and "
            "a confidence from 0 to 1 that agrees with it (match at 0.5 or more, "
            f"no_match at 0.5 or less), such as pairs {bad['pair'].head(3).to_list()}"
        )
    columns = {
        name: column.dtype for name, column in schema.REVIEW_DECISIONS.columns.items()
    }
    new = (
        given.join(pairs, on="pair")
        .select(
            *PAIR,
            decision=pl.col("label").replace_strict(LABELS),
            label_source=pl.lit(label_source),
            reviewer=pl.lit(reviewer),
            model=pl.lit(model, dtype=pl.String),
            confidence="confidence",
            round=pl.lit(label_round, dtype=pl.String),
            note=pl.col("reason").replace("", None),
            review_reason="review_reason",
            sample_id="sample_id",
            decided_at=pl.lit(datetime.now(UTC)),
        )
        .select(pl.col(name).cast(dtype) for name, dtype in columns.items())
    )
    tags = pl.sum_horizontal(
        pl.col("review_reason").is_not_null(), pl.col("sample_id").is_not_null()
    )
    if new.filter(tags != 1).height:
        raise ReviewDecisionsError(
            f"the pairs of {packet} need one of review_reason or sample_id each"
        )
    schema.validate(schema.REVIEW_DECISIONS, new)
    if decisions_path.exists():
        new = pl.concat([pl.read_parquet(decisions_path), new])
    decisions_path.parent.mkdir(parents=True, exist_ok=True)
    new.write_parquet(decisions_path)
    return decisions_path

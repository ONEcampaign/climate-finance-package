"""Decisions on review pairs: imported from labelling packets, applied as exact
link overrides.

People and agents decide the pairs of a packet (`packet.write_packet`) and return a CSV
that `import_labels` records in the review-decisions table.
`read_decision_labels` reads the queue decisions back as `human` and
`agent` pair labels, leaving out blind-sample decisions, which only measure the links'
precision. Decisions hold pair by pair: an accepted link stays at probability 1, and
its records count as agreeing, and a rejected link leaves `links`, splitting its
trace group where other units accepted the same commitment (`drop_pairs`). The link
stage applies the decisions to each key and recovery tier as it runs, so a rejected
link frees its disbursement for the tiers and blocking that follow, and no later
tier links it again.
"""

from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from climate_finance import schema
from climate_finance.build.errors import ReviewDecisionsError
from climate_finance.build.link.packet import packet_digest, packet_pairs_path

PAIR = ["task", "left_id", "right_id"]
# A person's decision replaces the same person's decision on the same pair and draw.
PERSON_KEY = [*PAIR, "sample_id", "reviewer", "label_source"]
# A labels CSV's columns, and what its `label` says of a pair.
LABEL_COLUMNS = ("pair", "label", "confidence", "reason")
LABELS = {"match": "accept", "no_match": "reject"}
# The `review_reason` of the pairs people label to calibrate a Splink model.
CALIBRATION_REASON = "calibration"
# What a split trace group's `group_id` ends in, before the part's rank.
SPLIT = "|split|"


def rejected_pairs(labels: pl.DataFrame) -> pl.DataFrame:
    """The pairs (`task`, `left_id`, `right_id`) that `human` and `agent` labels
    of `labels` reject."""
    return _decided(labels).filter(~pl.col("is_match")).select(PAIR)


def accepted_pairs(labels: pl.DataFrame) -> pl.DataFrame:
    """The pairs (`task`, `left_id`, `right_id`) that `human` and `agent` labels
    of `labels` accept."""
    return _decided(labels).filter("is_match").select(PAIR)


def _decided(labels: pl.DataFrame) -> pl.DataFrame:
    return labels.filter(pl.col("pair_label_source").is_in(schema.DECIDERS))


def read_decision_labels(path: Path) -> pl.DataFrame:
    """People's and agents' queue decisions at `path` as `pair_labels` rows with
    source `human` or `agent`, one per pair, and none when nothing has been decided yet.

    A pair's standing decision is the one `standing_decisions` keeps. A
    blind-sample decision (one with a `sample_id`) measures precision, so it is left
    out of the labels. `stratified` marks the decisions on calibration pairs, which
    `stratified_sample` drew, so only they take inverse-probability weights.
    """
    columns = schema.PAIR_LABELS.dtypes | {"stratified": pl.Boolean()}
    if not path.exists():
        return pl.DataFrame(schema=columns)
    decided = (
        pl.read_parquet(path)
        .filter(pl.col("sample_id").is_null())
        .drop_nulls("right_id")
    )
    return (
        standing_decisions(decided, key=PAIR)
        .sort(PAIR)
        .select(
            *PAIR,
            is_match=pl.col("decision") == "accept",
            pair_label_source="label_source",
            stratified=pl.col("review_reason") == CALIBRATION_REASON,
        )
        .cast(columns)  # ty: ignore[invalid-argument-type]
    )


def standing_decisions(decided: pl.DataFrame, *, key: str | list[str]) -> pl.DataFrame:
    """The standing decision of each `key` among the review-decision rows `decided`.

    A person's decision stands over any agent's, whenever either was imported, and
    the latest decision stands among people. Of an agent's decisions, the latest
    round stands (`schema.LABEL_ROUNDS` order), then the latest imported.
    `decided_at` is the import time, so ordering people and agents by it would let
    a re-imported agent round erase a person's verdict.
    """
    rounds = {name: rank for rank, name in enumerate(schema.LABEL_ROUNDS)}
    agent = (
        decided.filter(label_source="agent")
        .with_columns(
            rank=pl.col("round").replace_strict(rounds, default=0, return_dtype=pl.Int8)
        )
        .sort("rank", "decided_at", maintain_order=True)
        .unique(key, keep="last", maintain_order=True)
        .drop("rank")
    )
    human = (
        decided.filter(label_source="human")
        .sort("decided_at", maintain_order=True)
        .unique(key, keep="last", maintain_order=True)
    )
    return pl.concat([agent, human]).unique(key, keep="last", maintain_order=True)


def override_links(links: pl.DataFrame, *, labels: pl.DataFrame) -> pl.DataFrame:
    """`links` with the `human` and `agent` pair labels of `labels` applied:
    accepted links at probability 1 with records that agree, rejected links dropped
    (`drop_pairs`). A label on a pair the build did not link changes nothing."""
    accepted = accepted_pairs(labels)
    kept = drop_pairs(links, pairs=rejected_pairs(labels), accepted=accepted)
    is_accepted = pl.col("accepted").fill_null(False)
    return (
        kept.join(
            accepted.with_columns(accepted=pl.lit(True)),
            on=PAIR,
            how="left",
            maintain_order="left",
        )
        .with_columns(
            probability=pl.when(is_accepted).then(1.0).otherwise("probability"),
            records_disagree=pl.when(is_accepted & pl.col("records_disagree"))
            .then(False)
            .otherwise("records_disagree"),
        )
        .drop("accepted")
    )


def drop_pairs(
    links: pl.DataFrame, *, pairs: pl.DataFrame, accepted: pl.DataFrame
) -> pl.DataFrame:
    """`links` less the links of `pairs` (`task`, `left_id`, `right_id`), with the
    links of `accepted` kept.

    Decisions hold pair by pair: a link of `pairs` goes, and a link of `accepted`
    stays. Allocation pools a trace group's disbursement units over the commitments
    they all reach. A dropped trace link whose disbursement unit keeps another link
    in its group therefore takes its commitment out of the group for every unit
    that has no accepted link to it. A unit whose only link in the group is dropped
    leaves the group.

    Where some units keep such a commitment through an accepted link and others
    lose it, the group splits: the units keeping the same set of those commitments
    form one part. Parts holding accepted links rank first, in the order of the
    commitments they keep, and the part holding none ranks last. Each part's
    `group_id` takes the suffix `SPLIT` and its rank, and its `split_path` takes
    the rank after those of the splits before, so a part split again ranks its
    own parts inside its place among its group's parts. Allocation runs each part
    in a pass after the parts ranked before it (`allocate.allocate`), so parts of
    one group may share a commitment. Applying the same decisions again changes
    nothing, as their rejected links are gone.
    """
    if pairs.is_empty():
        return links
    accepted = accepted.select(PAIR)
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
    reached = kept.join(shared, on=[*group, "right_id"], how="semi")
    held = reached.join(accepted, on=PAIR, how="semi")
    kept = kept.join(
        reached.join(held, on=PAIR, how="anti"),
        on=PAIR,
        how="anti",
        maintain_order="left",
    )
    if held.is_empty():
        return kept
    # Each unit of a split group, with the shared commitments it keeps.
    units = (
        kept.join(held.select(group).unique(), on=group, how="semi")
        .select(*group, "left_id")
        .unique()
        .join(
            held.group_by(*group, "left_id").agg(
                keeps=pl.col("right_id").sort().str.join("|")
            ),
            on=[*group, "left_id"],
            how="left",
        )
    )
    # The part that keeps none sorts last.
    parts = units.select(*group, "keeps").unique()
    ranked = parts.with_columns(
        rank=pl.struct(none=pl.col("keeps").is_null(), keeps="keeps")
        .rank("dense")
        .over(group)
        .sub(1)
    )
    renamed = units.join(ranked, on=[*group, "keeps"], nulls_equal=True).select(
        *group, "left_id", "rank"
    )
    rank = pl.col("rank")
    split = rank.is_not_null()
    no_path = pl.lit([], dtype=pl.List(pl.Int16))
    return (
        kept.join(renamed, on=[*group, "left_id"], how="left", maintain_order="left")
        .with_columns(
            group_id=pl.when(split)
            .then(
                pl.concat_str([pl.col("group_id"), pl.lit(SPLIT), rank.cast(pl.String)])
            )
            .otherwise("group_id"),
            split_path=pl.when(split)
            .then(
                pl.concat_list(
                    pl.col("split_path").fill_null(no_path), rank.cast(pl.Int16)
                )
            )
            .otherwise("split_path"),
        )
        .drop("rank")
    )


def import_labels(
    *,
    packet: Path,
    labels_csv: Path,
    decisions_path: Path,
    label_source: str,
    reviewer: str,
    model: str | None = None,
    label_round: str | None = None,
    partial: bool = False,
) -> Path:
    """Record the decisions of a labels CSV in the review-decisions table.

    The CSV holds `LABEL_COLUMNS`: the packet's pair number, `match` or `no_match`,
    the decider's confidence that the pair matches, from 0 to 1 and on the label's
    side of 0.5, and the reason. It must answer exactly the pairs of `packet`, or
    some of them once each when `partial`, and the packet must be the one its pairs
    file was written with.

    A person's decision replaces that person's earlier decision on the same pair
    (and `sample_id`), so importing a changed or repeated answer leaves one row. An
    agent's decisions append, since each round stands as a decision of its own.

    Args:
        packet: the packet the CSV answers (`packet.write_packet`).
        labels_csv: the CSV.
        decisions_path: the review-decisions parquet.
        label_source: `human` or `agent`.
        reviewer: the person, or the agent run, that decided.
        model: the agent's model. An agent's decisions need it, a person's take none.
        label_round: the agent's labelling round (`schema.LABEL_ROUNDS`), `label`
            when not given. A person's decisions take none.
        partial: whether the CSV may answer only some of the packet's pairs.

    Returns:
        `decisions_path`.

    Raises:
        ReviewDecisionsError: when the packet changed since its pairs were written,
            the CSV's pairs differ from the packet's (when `partial`, the CSV repeats
            a pair or names one the packet lacks), a label or confidence is out of
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
    stray = set(answered) - expected - {None}
    if (
        None in answered
        or len(answered) != len(set(answered))
        or stray
        or (not partial and set(answered) != expected)
    ):
        raise ReviewDecisionsError(
            f"{labels_csv} must answer {'some of' if partial else 'each of'} the "
            f"{len(expected)} pairs of {packet} once; it answers "
            f"{len(set(answered) - {None})}, with "
            f"{len(answered) - len(set(answered))} repeats and {len(stray)} pairs "
            "the packet lacks"
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
        .select(
            pl.col(name).cast(dtype)
            for name, dtype in schema.REVIEW_DECISIONS.dtypes.items()
        )
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
        earlier = pl.read_parquet(decisions_path)
        if not agent:
            earlier = earlier.join(
                new.select(PERSON_KEY),
                on=PERSON_KEY,
                how="anti",
                nulls_equal=True,
                maintain_order="left",
            )
        new = pl.concat([earlier, new])
    decisions_path.parent.mkdir(parents=True, exist_ok=True)
    # A page saves after every answer, so a crash must leave the old table whole.
    staging = decisions_path.with_name(f".{decisions_path.name}.tmp")
    new.write_parquet(staging)
    staging.replace(decisions_path)
    return decisions_path

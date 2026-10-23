"""People's queue decisions as exact link overrides.

`splink_models.read_human_labels` reads the queue decisions as `human` pair labels,
leaving out blind-sample decisions, which measure the links' precision and so
neither calibrate a model nor change a link. An accepted link takes probability 1,
and its records no longer count as disagreeing. A rejected link leaves `links`
(`drop_pairs`).
"""

import polars as pl

PAIR = ["task", "left_id", "right_id"]


def override_links(links: pl.DataFrame, *, labels: pl.DataFrame) -> pl.DataFrame:
    """`links` with the `human` pair labels of `labels` applied: accepted links at
    probability 1 with records that agree, rejected links dropped (`drop_pairs`). A
    label on a pair the build did not link changes nothing."""
    human = labels.filter(pair_label_source="human")
    accepted = human.filter("is_match").select(PAIR, accepted=pl.lit(True))
    kept = drop_pairs(links, pairs=human.filter(~pl.col("is_match")))
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

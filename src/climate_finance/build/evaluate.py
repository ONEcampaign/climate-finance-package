"""The stratified blind sample and the precision estimate.

The sample measures precision on allocated pairs: the `trace` links that carried an
allocation to a published commitment row, each with the face value it carried.
Within a provider, pairs fall into four strata (`STRATA`): ledger-confirmed links,
key-tier links without a ledger confirmation, every other link method (models,
key-repair and recovery rules), and links people or agents accepted in the review
queue.

A provider with fewer than `census_below` pairs is labelled in full. Otherwise the
sample is an ordered sequence of `cap` draws: the first draw of each stratum comes
first, then each draw goes to the stratum furthest below its share of allocated
value. Within a stratum a draw picks a pair with probability proportional to its
allocated value, with replacement, so a pair can be drawn more than once and is
labelled once. Each draw's weight is its stratum's allocated value over the
stratum's draws, so a provider's weights sum to its allocated value.

People or agents decide the sample from labelling packets (`packet.write_packet`),
one per provider and batch, which show both units' ids and fields and no score,
link method or stratum. Their decisions carry a `sample_id`, so they measure
precision and never become pair labels or link overrides. The estimate counts an
agent's decision as it counts a person's and reports the share agents decided.

The estimate is the share of allocated value whose pair is right: per stratum the
weighted mean of the draws' decisions, combined by stratum value. Labelling runs in
batches, and a look reads the unbroken run of labelled draws from the first, cut to
whole batches. The bounds are a Wilson interval at the estimate's effective sample
size, at `interim_z` before the cap and `final_z` at it. A provider passes when the
lower bound reaches the threshold. It fails when the upper bound falls below the
threshold, when the lower bound could not reach it by the cap even with every later
draw right, or at the cap. With the thresholds in `gates.yaml`, a provider whose
true precision is 0.90 passes 2.6% of the time. On a fail, the weakest stratum is
dropped (reviewed pairs, then model links) and the rest is certified on a fresh
sample.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
from pydantic import BaseModel, ConfigDict

# `release` imports the gates, which import this module, so it is read at call time.
from climate_finance.build import release
from climate_finance.build.link.decisions import PAIR, standing_decisions
from climate_finance.build.link.packet import code_names, write_packet
from climate_finance.build.link.splink_models import read_decision_labels
from climate_finance.build.sums import ordered_sum
from climate_finance.schema import KEY_TIERS, scan_table

STRATA = ("ledger", "key", "model", "reviewed")
DESIGNS = ("census", "pps")
DECISIONS = ("pass", "fail", "continue")
# The ledger checks that confirm a link.
CONFIRMED = ("confirmed", "confirmed_undisbursed")
# One row per draw of a blind sample: the design file beside its packets.
DESIGN_SCHEMA = pl.Schema(
    {
        "sample_id": pl.String(),
        "task": pl.String(),
        "left_id": pl.String(),
        "right_id": pl.String(),
        "provider_code": pl.Int32(),
        "link_method": pl.String(),
        "stratum": pl.String(),
        "design": pl.String(),
        "draw": pl.Int32(),
        "weight_usd": pl.Float64(),
        "stratum_usd": pl.Float64(),
        "stratum_pairs": pl.Int32(),
    }
)
ESTIMATE_SCHEMA = pl.Schema(
    {
        "provider_code": pl.Int32(),
        "design": pl.String(),
        "labels": pl.Int32(),
        "estimate": pl.Float64(),
        "lower": pl.Float64(),
        "upper": pl.Float64(),
        "decision": pl.String(),
        "agent_share": pl.Float64(),
    }
)


class PrecisionRule(BaseModel):
    """The blind sample's design and the sequential rule that reads it."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    threshold: float
    batch: int
    cap: int
    interim_z: float
    final_z: float
    census_below: int
    seed: int


def design_path(out_dir: Path) -> Path:
    """Where `draw_blind_sample` writes the design of the sample in `out_dir`."""
    return out_dir / "design.parquet"


def batch_packet_path(out_dir: Path, *, provider: int, batch: int) -> Path:
    """Where `draw_blind_sample` writes one provider's packet for one batch."""
    return out_dir / str(provider) / f"batch-{batch:02d}.md"


def allocated_pairs(release_dir: Path, *, decisions: Path) -> pl.DataFrame:
    """The trace links that carried an allocation to a published commitment row.

    Columns: `provider_code` (the disbursing unit's donor), the pair (`task`,
    `left_id`, `right_id`), `link_method`, `allocated_usd` (face value summed over
    the pair's allocations) and `stratum`. A link people or agents accepted in the
    review queue, read from `decisions`, is `reviewed` whatever its method.
    """
    from climate_finance.build.stages import table_path

    def scan(stage: str, table: str) -> pl.LazyFrame:
        return scan_table(table_path(release_dir, stage, table))

    allocations = scan("fill", "allocations").filter(
        pl.col("row_id").is_not_null(),
        pl.col("rule") != "excess",
        pl.col("disbursement_source") == "crs",
    )
    units = scan("link", "units")
    carried = release.carrying_links(
        allocations,
        crdf=scan("normalise", "crdf"),
        units=units,
        links=scan("review", "links"),
    )
    accepted = (
        read_decision_labels(decisions)
        .filter("is_match")
        .select(*PAIR, reviewed=pl.lit(value=True))
        .lazy()
    )
    stratum = (
        pl.when(pl.col("reviewed").fill_null(value=False))
        .then(pl.lit("reviewed"))
        .when(pl.col("ledger_check").is_in(CONFIRMED))
        .then(pl.lit("ledger"))
        .when(pl.col("link_method").is_in(KEY_TIERS))
        .then(pl.lit("key"))
        .otherwise(pl.lit("model"))
    )
    return (
        allocations.join(carried, on=["row_id", "record_id"])
        .group_by("d_unit", "c_unit", "link_method", "ledger_check")
        .agg(pair_usd=ordered_sum(pl.col("allocated_usd")))
        .join(units.select(d_unit="unit_id", provider_code="donor_code"), on="d_unit")
        .select(
            "provider_code",
            "link_method",
            "ledger_check",
            task=pl.lit("trace"),
            left_id="d_unit",
            right_id="c_unit",
            allocated_usd="pair_usd",
        )
        .join(accepted, on=PAIR, how="left")
        .filter(pl.col("allocated_usd") > 0)
        .select("provider_code", *PAIR, "link_method", "allocated_usd", stratum=stratum)
        .sort("provider_code", *PAIR)
        .collect()
    )


def draw_blind_sample(
    *, release_dir: Path, out_dir: Path, decisions: Path, rule: PrecisionRule
) -> Path:
    """Draw the blind sample of `release_dir`'s allocated pairs into `out_dir`.

    Each provider's pairs go into one labelling packet per batch
    (`batch_packet_path`), each pair once, in the batch of its first draw and in
    draw order. A packet shows both units' ids and fields and no score, link
    method or stratum, and its pairs file carries each pair's `sample_id`, which
    `decisions.import_labels` records with the decisions. The design, one row per
    draw with its stratum and weight, goes to `design_path(out_dir)`, which the
    precision gate reads.

    Returns:
        `out_dir`.
    """
    from climate_finance.build.fetch import CRS_FILE
    from climate_finance.build.stages import raw_dir, table_path

    pairs = allocated_pairs(release_dir, decisions=decisions)
    design = sample_design(pairs, rule=rule, release=release_dir.name)
    out_dir.mkdir(parents=True, exist_ok=True)
    design.write_parquet(design_path(out_dir))

    shown = (
        design.sort("provider_code", "draw")
        .unique("sample_id", keep="first", maintain_order=True)
        .with_columns(batch=(pl.col("draw") - 1) // rule.batch + 1)
    )
    units = scan_table(table_path(release_dir, "link", "units"))
    crs = scan_table(table_path(release_dir, "normalise", "crs"))
    raw_crs = raw_dir(release_dir) / CRS_FILE
    names = code_names(raw_crs) if raw_crs.exists() else None
    for (provider, batch), batch_pairs in shown.partition_by(
        "provider_code", "batch", as_dict=True, maintain_order=True
    ).items():
        write_packet(
            batch_pairs.select(*PAIR, "sample_id"),
            units=units,
            crs=crs,
            out=batch_packet_path(out_dir, provider=int(provider), batch=int(batch)),
            title=(
                f"Blind sample {release_dir.name}: provider {provider}, batch {batch}"
            ),
            show_ids=True,
            names=names,
        )
    return out_dir


def sample_design(
    pairs: pl.DataFrame, *, rule: PrecisionRule, release: str
) -> pl.DataFrame:
    """One row per draw of the blind sample of `pairs` (`allocated_pairs`).

    A pair's `sample_id` is `<release>/<provider>/<first draw>`, shared by its
    draws. Each provider draws from its own random stream, seeded by `rule.seed`
    and its code, over its pairs in pair order, so the design depends neither on
    the order of `pairs` nor on the other providers.
    """
    parts = [
        _provider_draws(group, rule=rule, provider=int(provider))
        for (provider,), group in pairs.sort("provider_code", *PAIR)
        .partition_by("provider_code", as_dict=True, maintain_order=True)
        .items()
    ]
    if not parts:
        return pl.DataFrame(schema=DESIGN_SCHEMA)
    first = pl.col("draw").min().over("provider_code", *PAIR)
    return (
        pl.concat(parts, how="diagonal")
        .with_columns(
            sample_id=pl.format(
                "{}/{}/{}",
                pl.lit(release),
                "provider_code",
                first.cast(pl.String).str.zfill(3),
            )
        )
        .select(DESIGN_SCHEMA.names())
        .cast(DESIGN_SCHEMA)
    )


def _provider_draws(
    pairs: pl.DataFrame, *, rule: PrecisionRule, provider: int
) -> pl.DataFrame:
    """One provider's draws: every pair once below `rule.census_below` pairs, else
    `rule.cap` value-weighted draws spread over the strata by value share."""
    rng = np.random.default_rng([rule.seed, provider])
    strata = (
        pairs.group_by("stratum")
        .agg(stratum_usd=ordered_sum(pl.col("allocated_usd")), stratum_pairs=pl.len())
        .sort("stratum_usd", "stratum", descending=[True, False])
    )
    if pairs.height < rule.census_below:
        return (
            pairs[rng.permutation(pairs.height)]
            .join(strata, on="stratum", maintain_order="left")
            .with_columns(
                design=pl.lit("census"),
                draw=pl.int_range(1, pl.len() + 1),
                weight_usd="allocated_usd",
            )
        )
    shares = (strata["stratum_usd"] / strata["stratum_usd"].sum()).to_numpy()
    sequence = _stratum_sequence(shares, draws=rule.cap)
    picked = []
    for index, (stratum, stratum_usd) in enumerate(
        strata.select("stratum", "stratum_usd").iter_rows()
    ):
        members = pairs.filter(stratum=stratum)
        positions = np.flatnonzero(sequence == index) + 1
        value = members["allocated_usd"].to_numpy()
        chosen = rng.choice(members.height, size=positions.size, p=value / value.sum())
        picked.append(
            members[chosen].with_columns(
                draw=pl.Series(positions),
                weight_usd=pl.lit(stratum_usd / positions.size),
            )
        )
    return (
        pl.concat(picked)
        .join(strata, on="stratum")
        .with_columns(design=pl.lit("pps"))
        .sort("draw")
    )


def _stratum_sequence(shares: np.ndarray, *, draws: int) -> np.ndarray:
    """The stratum of each draw: one draw per stratum first, then each draw to the
    stratum furthest below its share of the draws so far."""
    counts = np.ones(shares.size)
    sequence = list(range(shares.size))
    for position in range(shares.size + 1, draws + 1):
        index = int(np.argmax(shares * position - counts))
        counts[index] += 1
        sequence.append(index)
    return np.array(sequence[:draws])


def read_sample_labels(design: Path, *, decisions: Path) -> pl.DataFrame:
    """The draws of the design at `design`, each with `correct`, whether its pair
    was accepted, and `label_source`, who decided it; both null while the pair is
    undecided.

    A draw's standing decision is the one `decisions.standing_decisions` keeps, as
    for the review queue's pairs.

    Raises:
        FileNotFoundError: when `decisions` does not exist, since the sample's
            labels can live nowhere else.
    """
    if not decisions.exists():
        raise FileNotFoundError(f"no review decisions at {decisions}")
    sampled = pl.read_parquet(decisions).filter(pl.col("sample_id").is_not_null())
    decided = standing_decisions(sampled, key="sample_id").select(
        "sample_id",
        correct=pl.col("decision") == "accept",
        label_source="label_source",
    )
    return pl.read_parquet(design).join(
        decided, on="sample_id", how="left", maintain_order="left"
    )


def estimate_precision(labels: pl.DataFrame, *, rule: PrecisionRule) -> pl.DataFrame:
    """Per provider, the precision estimate at its latest look and the rule's
    decision (`pass`, `fail` or `continue`).

    Args:
        labels: draws in `DESIGN_SCHEMA` with a Boolean `correct` and the
            `label_source` (`human` or `agent`) that decided it, both null while
            the pair is undecided (`read_sample_labels`).
        rule: the sequential rule.

    Returns:
        Rows in `ESTIMATE_SCHEMA`. `labels` counts the draws the look reads, and
        `estimate`, `lower` and `upper` are null before the first look. A census
        reads every decided pair and decides once all are decided, when its
        estimate is exact. `agent_share` is the share of the draws read that an
        agent decided.
    """
    census = pl.col("design") == "census"
    known = pl.col("correct").is_not_null()
    ordered = labels.sort("provider_code", "draw").with_columns(
        unbroken=known.cast(pl.Int32).cum_min().over("provider_code"),
        pairs=pl.len().over("provider_code"),
        decided=known.sum().over("provider_code"),
    )
    look = (pl.col("unbroken").sum() // rule.batch * rule.batch).clip(
        upper_bound=rule.cap
    )
    read = ordered.with_columns(look=look.over("provider_code")).filter(
        pl.when(census).then(known).otherwise(pl.col("draw") <= pl.col("look"))
    )

    weight, right = pl.col("weight_usd"), pl.col("correct").cast(pl.Float64)
    cells = (
        read.with_columns(
            cell=pl.when(census).then(pl.lit("all")).otherwise("stratum"),
            cell_usd=pl.when(census)
            .then(pl.col("weight_usd").sum().over("provider_code"))
            .otherwise("stratum_usd"),
        )
        .group_by("provider_code", "cell")
        .agg(
            pl.col("design", "pairs", "decided", "cell_usd").first(),
            draws=pl.len(),
            agent_draws=(pl.col("label_source") == "agent").sum(),
            weight_total=ordered_sum(weight),
            right_total=ordered_sum(weight * right),
            square_total=ordered_sum(weight * weight),
            square_right=ordered_sum(weight * weight * right),
        )
    )
    p_cell = pl.col("right_total") / pl.col("weight_total")
    # The sum of w^2 (y - p)^2 over 0/1 decisions, over the squared weight total.
    spread = (
        pl.col("square_right") * (1 - 2 * p_cell) + pl.col("square_total") * p_cell**2
    ) / pl.col("weight_total") ** 2
    draws = pl.col("draws")
    sampled = pl.when(census).then(draws / pl.col("pairs")).otherwise(0.0)
    # A cell read from one draw has no spread to measure, so it takes the largest.
    v_cell = (
        pl.when(draws > 1)
        .then((1 - sampled) * draws / (draws - 1) * spread)
        .when(sampled < 1)
        .then(0.25)
        .otherwise(0.0)
    )
    cell_usd = pl.col("cell_usd")
    providers = (
        cells.with_columns(p_cell=p_cell, v_cell=v_cell)
        .group_by("provider_code")
        .agg(
            pl.col("pairs", "decided").first(),
            labels=draws.sum(),
            agent_share=pl.col("agent_draws").sum() / draws.sum(),
            estimate=ordered_sum(cell_usd * pl.col("p_cell")) / ordered_sum(cell_usd),
            variance=ordered_sum(cell_usd**2 * pl.col("v_cell"))
            / ordered_sum(cell_usd) ** 2,
        )
    )
    return _decide(
        labels.select("provider_code", "design")
        .unique()
        .join(providers, on="provider_code", how="left"),
        rule=rule,
    )


def _decide(providers: pl.DataFrame, *, rule: PrecisionRule) -> pl.DataFrame:
    """Wilson bounds at each provider's effective sample size, and the decision."""
    p, n = pl.col("estimate"), pl.col("labels").cast(pl.Float64)
    pps = pl.col("design") == "pps"
    complete = ~pps & (pl.col("decided") == pl.col("pairs"))
    at_cap = pps & (pl.col("labels") >= rule.cap)
    z = pl.when(at_cap).then(rule.final_z).otherwise(rule.interim_z)
    variance = pl.col("variance")
    effective = (
        pl.when(variance > 0)
        .then(pl.min_horizontal(p * (1 - p) / variance, n))
        .otherwise(n)
    )
    lower, upper = _wilson(p, effective, z)
    # The best the cap can give: every draw after this look right.
    best = (n * p + (rule.cap - n)) / rule.cap
    best_lower, _ = _wilson(best, pl.lit(float(rule.cap)), pl.lit(rule.final_z))
    threshold = rule.threshold
    bounded = providers.with_columns(
        lower=pl.when(complete).then(p).otherwise(lower),
        upper=pl.when(complete).then(p).otherwise(upper),
        hopeless=pps & (best_lower < threshold),
    )
    lower, upper = pl.col("lower"), pl.col("upper")
    decision = (
        pl.when(p.is_null() | ~(pps | complete))
        .then(pl.lit("continue"))
        .when(lower >= threshold)
        .then(pl.lit("pass"))
        .when(complete | at_cap | (upper < threshold) | pl.col("hopeless"))
        .then(pl.lit("fail"))
        .otherwise(pl.lit("continue"))
    )
    return (
        bounded.with_columns(decision=decision, labels=pl.col("labels").fill_null(0))
        .select(ESTIMATE_SCHEMA.names())
        .cast(ESTIMATE_SCHEMA)
        .sort("provider_code")
    )


def _wilson(p: pl.Expr, n: pl.Expr, z: pl.Expr) -> tuple[pl.Expr, pl.Expr]:
    """The Wilson score interval for a proportion `p` over `n` trials."""
    shrink = 1 + z**2 / n
    centre = (p + z**2 / (2 * n)) / shrink
    half = z / shrink * (p * (1 - p) / n + z**2 / (4 * n**2)).sqrt()
    return centre - half, centre + half

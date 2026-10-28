"""The stratified blind sample and the precision estimate.

The sample measures precision on allocated pairs: the `trace` links that carried an
allocation to a published commitment row, each with the face value it carried.
Within a provider, pairs fall into four strata (`STRATA`): ledger-confirmed links,
key-tier links without a ledger confirmation, every other link method (models,
key-repair and recovery rules), and links people or agents accepted in the review
queue.

A pair that links a unit to itself (`self_linked`) stays out of the sample: the
unit's records committed and disbursed, so the link cannot be wrong. The sample
draws from the other pairs (`sample_frame`), and the estimate counts the
self-linked pairs' value as right.

A stratum of at most `min_draws` pairs is a census: each of its pairs is drawn
once, in random order, and weighs its own allocated value. Every stratum of a
provider with fewer than `census_below` pairs is a census too. A provider whose
strata are all census is a census and draws its pairs in one random order.
Otherwise the sample is an ordered sequence of `cap` draws: the first draw of each
stratum comes first, then each draw goes to the stratum furthest below its share of
the draws. A census stratum's share is its pairs, and the other strata (`pps`)
share the rest by allocated value. Within a pps stratum a draw picks a pair with
probability proportional to its allocated value, with replacement, so a pair can be
drawn more than once and is labelled once, and each draw weighs its stratum's
allocated value over the stratum's draws. A provider's weights therefore sum to its
allocated value. The design records each provider's population fingerprint
(`population_fingerprints`), and the precision gate refuses a sample whose
population the build no longer allocates.

People or agents decide the sample from labelling packets (`packet.write_packet`),
one per provider and batch, which show both units' ids and fields and no score,
link method or stratum. Their decisions carry a `sample_id`, so they feed the
precision estimate and create no pair labels or link overrides. A decision counts
for a draw only when its pair is the draw's pair. The estimate counts an agent's
decision as it counts a person's and reports the share agents decided.

The estimate is the share of allocated value whose pair is right: the self-linked
value, plus the sampled value times its estimate, over the provider's allocated
value. The sampled estimate is per stratum the weighted mean of the draws'
decisions, combined by stratum value, and the provider's bounds count the
self-linked value the same way. Labelling runs in batches, and a look reads the
unbroken run of labelled draws from the first, cut to whole batches. The sampled
bounds are a Wilson interval at the estimate's effective sample size, at
`interim_z` before the cap and `final_z` at it. A provider passes when the lower
bound reaches the threshold. A census provider decides once every pair is decided,
when its estimate is exact. A pps provider fails when the upper bound falls below
the threshold, when the lower bound could not reach it by the cap even with every
later draw right (the cap's sampled estimate is then one less the weight of the
wrong draws read over the sampled value), or at the cap. The rule decides at every
look the labels reach, and a fail at any look stands whatever later looks decide,
so labels read on past a fail cannot turn it into a pass. With the thresholds in
`gates.yaml`, a provider with no self-linked pairs whose true precision is 0.90
passes 2.6% of the time.
The rule reads the provider's estimate, which blends its strata by value, so each
stratum's estimate and interval at the same look are reported beside it: a
provider whose value is mostly key-tier links can pass while its model links are
far less precise. On a fail, the weakest stratum is dropped (reviewed pairs, then
model links) and the rest is certified on a fresh sample.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Self

import numpy as np
import polars as pl
from pydantic import BaseModel, ConfigDict, model_validator

# `release` imports the gates, which import this module, so it is read at call time.
from climate_finance.build import release
from climate_finance.build.errors import ReviewDecisionsError, SampleDesignError
from climate_finance.build.link.decisions import (
    PAIR,
    read_decision_labels,
    standing_decisions,
)
from climate_finance.build.link.packet import code_names, write_packet
from climate_finance.build.sums import ordered_sum
from climate_finance.schema import KEY_TIERS, scan_table

STRATA = ("ledger", "key", "model", "reviewed")
DESIGNS = ("census", "pps")
DECISIONS = ("pass", "fail", "continue")
# The ledger checks that confirm a link.
CONFIRMED = ("confirmed", "confirmed_undisbursed")
# Seeds every population fingerprint, so a sample drawn from another sampling frame
# than `sample_frame`'s is refused as stale. It changes when the frame does.
_FRAME = b"allocated pairs less self-linked pairs"
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
        "population": pl.String(),
    }
)
# Each stratum's estimate at its provider's look, in `ESTIMATE_SCHEMA`'s `strata`.
STRATUM_ESTIMATE = pl.Struct(
    {
        "stratum": pl.String(),
        "allocated_usd": pl.Float64(),
        "labels": pl.Int32(),
        "estimate": pl.Float64(),
        "lower": pl.Float64(),
        "upper": pl.Float64(),
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
        "failed_look": pl.Int32(),
        "agent_share": pl.Float64(),
        "self_linked_share": pl.Float64(),
        "strata": pl.List(STRATUM_ESTIMATE),
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
    min_draws: int
    seed: int

    @model_validator(mode="after")
    def _strata_minimums_fit_the_cap(self) -> Self:
        """Every stratum can take its minimum within the cap."""
        if self.min_draws * len(STRATA) > self.cap:
            raise ValueError(
                f"min_draws {self.min_draws} over {len(STRATA)} strata exceeds "
                f"the cap of {self.cap}"
            )
        return self

    @model_validator(mode="after")
    def _interim_bounds_are_wider(self) -> Self:
        """Interim looks read the sample several times, so their bounds are at
        least as wide as the final one's."""
        if self.interim_z < self.final_z:
            raise ValueError(
                f"interim_z {self.interim_z} is below final_z {self.final_z}"
            )
        return self


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
    links = scan("review", "links")
    pair = ["record_unit_id", "commitment_unit_id", "link_method"]
    # An allocation counts only while the review stage still holds its carrier.
    held = links.filter(pl.col("task") == "trace").select(
        "link_method", record_unit_id="left_id", commitment_unit_id="right_id"
    )
    carried = release.with_carrying_links(allocations, units=units, links=links).join(
        held, on=pair, how="semi"
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
        carried.group_by(*pair, "ledger_check")
        .agg(pair_usd=ordered_sum(pl.col("allocated_usd")))
        .join(
            units.select(record_unit_id="unit_id", provider_code="donor_code"),
            on="record_unit_id",
        )
        .select(
            "provider_code",
            "link_method",
            "ledger_check",
            task=pl.lit("trace"),
            left_id="record_unit_id",
            right_id="commitment_unit_id",
            allocated_usd="pair_usd",
        )
        .join(accepted, on=PAIR, how="left")
        .filter(pl.col("allocated_usd") > 0)
        .select("provider_code", *PAIR, "link_method", "allocated_usd", stratum=stratum)
        .sort("provider_code", *PAIR)
        .collect()
    )


def self_linked() -> pl.Expr:
    """Whether a pair links a unit to itself: the disbursing unit (`left_id`) is
    the commitment unit (`right_id`), whose records committed and disbursed, so
    the link cannot be wrong."""
    return pl.col("left_id") == pl.col("right_id")


def sample_frame(pairs: pl.DataFrame) -> pl.DataFrame:
    """The allocated pairs (`allocated_pairs`) the blind sample draws from: all but
    the `self_linked` ones."""
    return pairs.filter(~self_linked())


def self_linked_shares(pairs: pl.DataFrame) -> pl.DataFrame:
    """Per provider in `pairs` (`allocated_pairs`), `self_linked_share`: the share
    of its allocated value on `self_linked` pairs, exactly 1 when every pair is."""
    usd = pl.col("allocated_usd")
    return (
        pairs.sort("provider_code", *PAIR)
        .group_by("provider_code", maintain_order=True)
        .agg(
            self_linked_share=pl.when(self_linked().all())
            .then(1.0)
            .otherwise(ordered_sum(usd.filter(self_linked())) / ordered_sum(usd))
        )
    )


def population_fingerprints(pairs: pl.DataFrame) -> pl.DataFrame:
    """Per provider in the `sample_frame` of `pairs` (`allocated_pairs`), a sha256
    of the pairs the sample draws from: each pair's identity, link method, stratum
    and allocated value, after a tag of the frame's definition.

    The digest does not depend on the order of `pairs`, and any added, dropped or
    changed pair in the frame changes it, as does a change to the frame.

    Returns:
        A frame of `provider_code` and `population`, the hex digest.
    """
    columns = (*PAIR, "link_method", "stratum", "allocated_usd")
    providers = (
        sample_frame(pairs)
        .select("provider_code", *columns)
        .sort("provider_code", *columns)
    )
    digests = []
    for (provider,), group in providers.partition_by(
        "provider_code", as_dict=True, maintain_order=True
    ).items():
        digest = hashlib.sha256(_FRAME)
        for row in group.select(columns).iter_rows():
            digest.update(repr(row).encode())
        digests.append((provider, digest.hexdigest()))
    return pl.DataFrame(
        digests,
        schema={"provider_code": pl.Int32(), "population": pl.String()},
        orient="row",
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
    """One row per draw of the blind sample of `pairs` (`allocated_pairs`), drawn
    from their `sample_frame`.

    Each row carries its provider's `population` (`population_fingerprints`). A
    pair's `sample_id` is `<release>/<provider>/<population>/<first draw>`, with the
    population's first 12 hex digits, shared by its draws. Each provider draws
    from its own random stream, seeded by `rule.seed` and its code, over its pairs
    in pair order, so a provider's draws do not change with the order of `pairs`
    or with the other providers.
    """
    parts = [
        _provider_draws(group, rule=rule, provider=int(provider))
        for (provider,), group in sample_frame(pairs)
        .sort("provider_code", *PAIR)
        .partition_by("provider_code", as_dict=True, maintain_order=True)
        .items()
    ]
    if not parts:
        return pl.DataFrame(schema=DESIGN_SCHEMA)
    first = pl.col("draw").min().over("provider_code", *PAIR)
    return (
        pl.concat(parts, how="diagonal")
        .join(population_fingerprints(pairs), on="provider_code")
        .with_columns(
            sample_id=pl.format(
                "{}/{}/{}/{}",
                pl.lit(release),
                "provider_code",
                pl.col("population").str.head(12),
                first.cast(pl.String).str.zfill(3),
            )
        )
        .select(DESIGN_SCHEMA.names())
        .cast(DESIGN_SCHEMA)
    )


def _provider_draws(
    pairs: pl.DataFrame, *, rule: PrecisionRule, provider: int
) -> pl.DataFrame:
    """One provider's draws. A census stratum, of at most `rule.min_draws` pairs or
    in a provider of fewer than `rule.census_below`, draws each pair once at its
    own value. When every stratum is census the provider's pairs come in one random
    order. Otherwise the provider takes `rule.cap` draws: each census stratum its
    pairs, and the other strata value-weighted draws by value share, with at least
    `rule.min_draws` each (`_draw_shares`). A pps draw's weight is its stratum's
    value over the stratum's draws, so a stratum drawn above its value share counts
    at its share in the estimate."""
    rng = np.random.default_rng([rule.seed, provider])
    strata = (
        pairs.group_by("stratum")
        .agg(stratum_usd=ordered_sum(pl.col("allocated_usd")), stratum_pairs=pl.len())
        .sort("stratum_usd", "stratum", descending=[True, False])
        .with_columns(
            design=pl.when(
                (pl.col("stratum_pairs") <= rule.min_draws)
                | (pairs.height < rule.census_below)
            )
            .then(pl.lit("census"))
            .otherwise(pl.lit("pps"))
        )
    )
    census = (strata["design"] == "census").to_numpy()
    if census.all():
        return (
            pairs[rng.permutation(pairs.height)]
            .join(strata, on="stratum", maintain_order="left")
            .with_columns(
                draw=pl.int_range(1, pl.len() + 1), weight_usd="allocated_usd"
            )
        )
    stratum_pairs = strata["stratum_pairs"].to_numpy()
    shares = _draw_shares(
        strata["stratum_usd"].to_numpy(),
        floors=np.where(census, stratum_pairs, rule.min_draws),
        fixed=census,
        draws=rule.cap,
    )
    sequence = _stratum_sequence(shares, draws=rule.cap)
    picked = []
    for index, (stratum, stratum_usd, design) in enumerate(
        strata.select("stratum", "stratum_usd", "design").iter_rows()
    ):
        members = pairs.filter(stratum=stratum)
        positions = np.flatnonzero(sequence == index) + 1
        if design == "census":
            chosen = rng.permutation(members.height)
            weight = pl.col("allocated_usd")
        else:
            value = members["allocated_usd"].to_numpy()
            chosen = rng.choice(
                members.height, size=positions.size, p=value / value.sum()
            )
            weight = pl.lit(stratum_usd / positions.size)
        picked.append(
            members[chosen].with_columns(draw=pl.Series(positions), weight_usd=weight)
        )
    return pl.concat(picked).join(strata, on="stratum").sort("draw")


def _draw_shares(
    usd: np.ndarray, *, floors: np.ndarray, fixed: np.ndarray, draws: int
) -> np.ndarray:
    """Each stratum's share of `draws`: `floors` draws where `fixed`, else its share
    of `usd` among the unfixed strata, raised to `floors` draws where that gives
    fewer, with the other strata sharing the rest by value. A floored share times
    `draws` is a whole number, so `_stratum_sequence` gives the stratum exactly its
    floor."""
    floored = fixed.copy()
    while True:
        free = draws - floors[floored].sum()
        scaled = np.where(floored, floors, usd * free / usd[~floored].sum())
        below = ~floored & (scaled < floors)
        if not below.any():
            return scaled / draws
        floored |= below


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

    A decision counts for a draw only when its `sample_id` and its pair (`task`,
    `left_id`, `right_id`) are the draw's, so a pair drawn under a reused id stays
    undecided. A draw's standing decision is the one `decisions.standing_decisions`
    keeps among those, as for the review queue's pairs.

    Raises:
        ReviewDecisionsError: when `decisions` does not exist, since the sample's
            labels can live nowhere else.
        SampleDesignError: when a draw's `weight_usd` or `stratum_usd` is not a
            finite positive amount, since no estimate can read it.
    """
    if not decisions.exists():
        raise ReviewDecisionsError(
            f"No review decisions at {decisions}, so the blind sample has no labels; "
            "record them with `cf-build import-labels`, or point "
            "sources.review_decisions in build.toml at the file it wrote."
        )
    drawn = pl.read_parquet(design)
    weights = pl.col("weight_usd", "stratum_usd")
    if bad := drawn.filter(
        pl.any_horizontal(~(weights.is_finite() & (weights > 0)).fill_null(False))
    ).height:
        raise SampleDesignError(
            f"{design}: {bad} draws have a weight_usd or stratum_usd that is not "
            "a finite positive amount"
        )
    key = ["sample_id", *PAIR]
    sampled = pl.read_parquet(decisions).join(
        drawn.select(key).unique(), on=key, how="semi"
    )
    decided = standing_decisions(sampled, key=key).select(
        *key,
        correct=pl.col("decision") == "accept",
        label_source="label_source",
    )
    return drawn.join(decided, on=key, how="left", maintain_order="left")


def estimate_precision(
    labels: pl.DataFrame,
    *,
    rule: PrecisionRule,
    certain: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Per provider, the precision estimate at its latest look and the rule's
    decision (`pass`, `fail` or `continue`), with each stratum's estimate beside it.
    The decision is `fail` when any look up to the latest decides `fail`.

    Args:
        labels: draws in `DESIGN_SCHEMA` with a Boolean `correct` and the
            `label_source` (`human` or `agent`) that decided it, both null while
            the pair is undecided (`read_sample_labels`).
        rule: the sequential rule.
        certain: per provider, `self_linked_share` (`self_linked_shares`),
            the share of its allocated value outside the sample and counted
            right. A provider it does not list has none.

    Returns:
        Rows in `ESTIMATE_SCHEMA`. `labels` counts the draws the look reads.
        `estimate`, `lower` and `upper` count the `self_linked_share` right and
        the rest at the sample's estimate and bounds, the rule decides on them,
        and they are null before the first look. A census reads every decided
        pair and decides once all are decided, when its estimate is exact.
        `failed_look` is the draws the first look deciding `fail` read, null
        when none did. `agent_share` is the share of the draws read that an agent
        decided.
        `strata` lists the provider's sampled strata in `STRATUM_ESTIMATE`,
        largest allocated value first: the draws the look reads in each, their
        weighted share right and its Wilson bounds at the provider's `z`. A census
        stratum with every pair decided is exact, and a stratum with no draw read
        has null estimate and bounds. A provider is a census when every draw is
        (`DESIGN_SCHEMA`'s `design`), else pps.
    """
    census = pl.col("census")
    known = pl.col("correct").is_not_null()
    ordered = labels.sort("provider_code", "draw").with_columns(
        census=(pl.col("design") == "census").all().over("provider_code"),
        unbroken=known.cast(pl.Int32).cum_min().over("provider_code"),
        pairs=pl.len().over("provider_code"),
        stratum_pairs=pl.len().over("provider_code", "stratum"),
        decided=known.sum().over("provider_code"),
        provider_usd=pl.col("weight_usd").sum().over("provider_code"),
    )
    latest = (pl.col("unbroken").sum() // rule.batch * rule.batch).clip(
        upper_bound=rule.cap
    )

    # A pps provider reads its draws at every look up to its latest, each draw from
    # the first look that reaches it. A census reads every decided pair once.
    first = ((pl.col("draw") - 1) // rule.batch + 1) * rule.batch
    read = (
        ordered.with_columns(latest=latest.over("provider_code"))
        .filter(
            pl.when(census).then(known).otherwise(pl.col("draw") <= pl.col("latest"))
        )
        .with_columns(
            look=pl.int_ranges(
                pl.when(census).then(0).otherwise(first),
                pl.when(census).then(0).otherwise("latest") + 1,
                rule.batch,
            )
        )
        .explode("look")
    )
    cells = _cells(
        read.with_columns(
            cell="stratum", cell_usd="stratum_usd", cell_pairs="stratum_pairs"
        )
    )
    draws = pl.col("draws")
    cell_usd = pl.col("cell_usd")
    looks = cells.group_by("provider_code", "look").agg(
        pl.col("pairs", "decided", "provider_usd").first(),
        labels=draws.sum(),
        wrong_usd=ordered_sum(pl.col("wrong_total")),
        agent_share=pl.col("agent_draws").sum() / draws.sum(),
        estimate=ordered_sum(cell_usd * pl.col("p_cell")) / ordered_sum(cell_usd),
        variance=ordered_sum(cell_usd**2 * pl.col("v_cell"))
        / ordered_sum(cell_usd) ** 2,
    )
    looks = (
        labels.group_by("provider_code")
        .agg(
            design=pl.when((pl.col("design") == "census").all())
            .then(pl.lit("census"))
            .otherwise(pl.lit("pps"))
        )
        .join(looks, on="provider_code", how="left")
    )
    if certain is None:
        looks = looks.with_columns(self_linked_share=pl.lit(0.0))
    else:
        looks = looks.join(
            certain.select("provider_code", "self_linked_share"),
            on="provider_code",
            how="left",
        ).with_columns(pl.col("self_linked_share").fill_null(0.0))

    # The rule decides at every look, and a fail at any look stands: a later look
    # that passes read on past a stop.
    decided = _decide(looks, rule=rule)
    failed = (
        decided.filter(decision="fail")
        .group_by("provider_code")
        .agg(failed_look=pl.col("labels").min())
    )
    providers = (
        decided.sort("provider_code", "look")
        .unique("provider_code", keep="last", maintain_order=True)
        .join(failed, on="provider_code", how="left")
        .with_columns(
            decision=pl.when(pl.col("failed_look").is_not_null())
            .then(pl.lit("fail"))
            .otherwise("decision")
        )
    )
    strata = _stratum_estimates(
        labels,
        cells=cells.join(providers, on=["provider_code", "look"], how="semi"),
        providers=providers,
        rule=rule,
    )
    return (
        providers.join(strata, on="provider_code", how="left", maintain_order="left")
        .select(ESTIMATE_SCHEMA.names())
        .cast(ESTIMATE_SCHEMA)
    )


def _cells(read: pl.DataFrame) -> pl.DataFrame:
    """Per provider, look and `cell`, the draws `read` holds, their weighted share right
    (`p_cell`) and its variance (`v_cell`). A census cell of `cell_pairs` pairs,
    each drawn once, shrinks the variance by the share of them read."""
    weight, right = pl.col("weight_usd"), pl.col("correct").cast(pl.Float64)
    census = pl.col("design") == "census"
    cells = read.group_by("provider_code", "look", "cell").agg(
        pl.col(
            "design", "pairs", "decided", "cell_usd", "cell_pairs", "provider_usd"
        ).first(),
        draws=pl.len(),
        agent_draws=(pl.col("label_source") == "agent").sum(),
        weight_total=ordered_sum(weight),
        right_total=ordered_sum(weight * right),
        wrong_total=ordered_sum(weight * (1 - right)),
        square_total=ordered_sum(weight * weight),
        square_right=ordered_sum(weight * weight * right),
    )
    p_cell = pl.col("right_total") / pl.col("weight_total")
    # The sum of w^2 (y - p)^2 over 0/1 decisions, over the squared weight total.
    spread = (
        pl.col("square_right") * (1 - 2 * p_cell) + pl.col("square_total") * p_cell**2
    ) / pl.col("weight_total") ** 2
    draws = pl.col("draws")
    sampled = pl.when(census).then(draws / pl.col("cell_pairs")).otherwise(0.0)
    # A cell read from one draw has no spread to measure, so it takes the largest.
    v_cell = (
        pl.when(draws > 1)
        .then((1 - sampled) * draws / (draws - 1) * spread)
        .when(sampled < 1)
        .then(0.25)
        .otherwise(0.0)
    )
    return cells.with_columns(p_cell=p_cell, v_cell=v_cell)


def _stratum_estimates(
    labels: pl.DataFrame,
    *,
    cells: pl.DataFrame,
    providers: pl.DataFrame,
    rule: PrecisionRule,
) -> pl.DataFrame:
    """Per provider, `strata`: each stratum of the design `labels` with its
    allocated value and its estimate from `cells` (`_cells` by stratum), bounded at
    the `z` of its provider's look in `providers`."""
    p = pl.col("p_cell")
    exact = (pl.col("design") == "census") & (pl.col("draws") == pl.col("cell_pairs"))
    lower, upper = _interval(
        p, pl.col("draws").cast(pl.Float64), pl.col("v_cell"), _look_z(rule)
    )
    estimates = cells.join(
        providers.select("provider_code", "labels"), on="provider_code"
    ).select(
        "provider_code",
        stratum="cell",
        labels="draws",
        estimate=p,
        lower=pl.when(exact).then(p).otherwise(lower),
        upper=pl.when(exact).then(p).otherwise(upper),
    )
    return (
        labels.group_by("provider_code", "stratum")
        .agg(allocated_usd=pl.col("stratum_usd").first())
        .join(estimates, on=["provider_code", "stratum"], how="left")
        .with_columns(pl.col("labels").fill_null(0))
        .sort(
            "provider_code", "allocated_usd", "stratum", descending=[False, True, False]
        )
        .group_by("provider_code", maintain_order=True)
        .agg(strata=pl.struct(field.name for field in STRATUM_ESTIMATE.fields))
    )


def _at_cap(rule: PrecisionRule) -> pl.Expr:
    """Whether a provider's sample has read every draw to the cap."""
    return (pl.col("design") == "pps") & (pl.col("labels") >= rule.cap)


def _look_z(rule: PrecisionRule) -> pl.Expr:
    """The interval width at a provider's look: `final_z` at the cap."""
    return pl.when(_at_cap(rule)).then(rule.final_z).otherwise(rule.interim_z)


def non_finite() -> pl.Expr:
    """Whether a provider's `estimate`, `lower` or `upper` is NaN or infinite."""
    return pl.any_horizontal(
        ~pl.col(name).is_finite() for name in ("estimate", "lower", "upper")
    )


def _decide(providers: pl.DataFrame, *, rule: PrecisionRule) -> pl.DataFrame:
    """Wilson bounds at each provider's effective sample size, each figure with
    the provider's self-linked value counted right, and the decision on them."""
    p, n = pl.col("estimate"), pl.col("labels").cast(pl.Float64)
    pps = pl.col("design") == "pps"
    complete = ~pps & (pl.col("decided") == pl.col("pairs"))
    at_cap = _at_cap(rule)
    lower, upper = _interval(p, n, pl.col("variance"), _look_z(rule))
    # The best the cap can give: every draw after this look right. At the cap a
    # stratum's draws carry its whole value, so the estimate is the share of the
    # provider's value whose draws are right.
    best = 1 - pl.col("wrong_usd") / pl.col("provider_usd")
    best_lower, _ = _wilson(best, pl.lit(float(rule.cap)), pl.lit(rule.final_z))
    threshold = rule.threshold
    certain = pl.col("self_linked_share")

    def counted(share: pl.Expr) -> pl.Expr:
        return certain + (1 - certain) * share

    bounded = providers.with_columns(
        estimate=counted(p),
        lower=counted(pl.when(complete).then(p).otherwise(lower)),
        upper=counted(pl.when(complete).then(p).otherwise(upper)),
        hopeless=pps & (counted(best_lower) < threshold),
    )
    p, lower, upper = pl.col("estimate"), pl.col("lower"), pl.col("upper")
    # NaN sorts above every number, so a NaN bound would otherwise pass.
    decision = (
        pl.when(non_finite())
        .then(pl.lit("fail"))
        .when(p.is_null() | ~(pps | complete))
        .then(pl.lit("continue"))
        .when(lower >= threshold)
        .then(pl.lit("pass"))
        .when(complete | at_cap | (upper < threshold) | pl.col("hopeless"))
        .then(pl.lit("fail"))
        .otherwise(pl.lit("continue"))
    )
    return bounded.with_columns(
        decision=decision, labels=pl.col("labels").fill_null(0)
    ).sort("provider_code")


def _interval(
    p: pl.Expr, n: pl.Expr, variance: pl.Expr, z: pl.Expr
) -> tuple[pl.Expr, pl.Expr]:
    """Wilson bounds on `p` from `n` draws whose estimate has `variance`: at the
    effective sample size `p (1 - p) / variance`, at most `n`. At a `p` of 0 or 1
    that size is 0 whatever the variance, so the bounds take `n`."""
    effective = (
        pl.when((variance > 0) & (p * (1 - p) > 0))
        .then(pl.min_horizontal(p * (1 - p) / variance, n))
        .otherwise(n)
    )
    return _wilson(p, effective, z)


def _wilson(p: pl.Expr, n: pl.Expr, z: pl.Expr) -> tuple[pl.Expr, pl.Expr]:
    """The Wilson score interval for a proportion `p` over `n` trials."""
    shrink = 1 + z**2 / n
    centre = (p + z**2 / (2 * n)) / shrink
    half = z / shrink * (p * (1 - p) / n + z**2 / (4 * n**2)).sqrt()
    return centre - half, centre + half

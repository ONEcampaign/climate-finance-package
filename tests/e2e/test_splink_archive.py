"""Per-provider Splink on the archive build. Needs CF_E2E_DATA pointing at the
scoping `data/`.

With no recorded decisions, no Splink model has calibration labels, so the build
gives no Splink pair a probability and each model offers only its calibration sample
to the review queue.

The simulation calibrates models for truth-group providers whose ids the models never
read. Id truth stands in for people's labels on each model's calibration sample, and
precision and recall are measured against it on the pairs left unlabelled. Jev's
answers replay from `SIMULATION_CACHE` under CF_E2E_DATA, which one online run of
`simulate` wrote.
"""

import os
from dataclasses import dataclass
from pathlib import Path

import polars as pl
import pytest

from climate_finance import schema
from climate_finance.build.jev_client import JevClient
from climate_finance.build.link.decisions import CALIBRATION_REASON
from climate_finance.build.link.score import ELIGIBLE, scoring_groups
from climate_finance.build.link.splink_models import (
    CALIBRATION_LABELS,
    SplinkResult,
    score_with_splink,
)
from climate_finance.build.stages import table_path

pytestmark = pytest.mark.slow

MODEL = "jev-1.14.0"
SIMULATION_CACHE = Path("jev-cache/splink-simulation")
# Truth-group providers of several sizes: four with their own model (New Zealand,
# Austria, France, Denmark) and three small enough to share the pooled model.
SIMULATED = (820, 1, 4, 3, 68, 1624, 76)
# Splink pairs on the archive build with commitments from 2015 and the key tiers
# before the rules in `PAIRS_MOVED`.
SPLINK_PAIRS = 1_640_847
# Pairs each change adds, each measured with the changes above it in place. The
# lettered-parent rule, the EU parent floor and the floor naming no commitment move
# none.
PAIRS_MOVED = {
    # A missing project number counts as one more project in `crs_key`.
    "crs_key on a missing project number": 2,
    # A component award keys each component by purpose (`split_components`), so the
    # disbursements the project-number tiers linked across components, US awards
    # among them, go to scoring.
    "component awards": 27_826,
    # The build takes every commitment year the CRS holds, from 2000, so the
    # providers' disbursements also pair with earlier commitments: 804,454 pairs
    # have a commitment from before 2015, 795,740 of them the US's.
    "commitments from 2000": 804_639,
    # A line filed under several purposes is one unit (`build_units`), so a
    # disbursement pairs with it once.
    "one unit per line": -4_100,
}
_LABELS = {
    name: column.dtype for name, column in schema.PAIR_LABELS.columns.items()
} | {"stratified": pl.Boolean()}


@dataclass(frozen=True)
class LinkTables:
    candidates: pl.DataFrame
    units: pl.DataFrame
    stems: pl.DataFrame
    crs: pl.LazyFrame
    profile: pl.DataFrame
    scores: pl.DataFrame


@pytest.fixture(scope="module")
def tables(release: Path) -> LinkTables:
    release_dir = release.parent

    def read(stage: str, table: str) -> pl.DataFrame:
        return pl.read_parquet(table_path(release_dir, stage, table))

    return LinkTables(
        candidates=read("link", "candidates"),
        units=read("link", "units"),
        stems=read("link", "programme_stems"),
        crs=pl.scan_parquet(table_path(release_dir, "normalise", "crs")),
        profile=read("link", "provider_profile"),
        scores=read("link", "pair_scores"),
    )


def _offline(cache_dir: Path) -> JevClient:
    return JevClient(model=MODEL, cost_cap_usd=10.0, cache_dir=cache_dir, offline=True)


def test_without_decisions_splink_only_asks_for_calibration_labels(
    tables: LinkTables,
) -> None:
    scores = tables.scores
    providers = tables.profile.filter(
        pl.col("scoring_group").is_in(["splink", "pooled_splink"])
    )["provider_code"]
    assert set(scores["donor_code"]) <= set(providers)
    # No probability, so no Splink pair can link.
    assert scores["probability"].null_count() == scores.height
    assert scores["label_set"].null_count() == scores.height
    queued = scores.filter(review_reason=CALIBRATION_REASON).group_by("model").len()
    assert set(queued["len"]) == {CALIBRATION_LABELS}
    assert set(scores["review_reason"].drop_nulls()) == {CALIBRATION_REASON}
    # The archive build: 40 providers outside the truth group and five usable models
    # (USA, three more providers of their own and the pooled model). Portugal (76) has
    # 26 true training pairs, under `MIN_TRUE_PAIRS`, and leaves the truth group. The
    # pairs are those providers' eligible candidates, blocked on trimmed ids and on
    # project numbers with rounded ones null, less the disbursements the key tiers link.
    # The GEF reuses a crs id across projects, so `crs_key` leaves most of its
    # disbursements to scoring. Its own model then completes one EM session, its project
    # numbers block one pair and EM drives a recipient-and-purpose m to zero, so it
    # joins the pooled model. `template_lines` takes every pair of the Development Bank
    # of Latin America and the New Development Bank. Commitments from 2000 give the EBRD
    # (990) 527 commitments, over `SPLINK_MIN_COMMITMENTS`, but its title session ends
    # with an m of zero and its recipient-and-purpose session with an m of one, so it
    # joins the pooled model.
    assert len(providers) == 40
    assert scores.height == SPLINK_PAIRS + sum(PAIRS_MOVED.values())
    assert queued.height == 5


def test_two_runs_give_the_same_scores(tables: LinkTables, tmp_path: Path) -> None:
    # Slow: refits every Splink model, about 10 minutes.
    again = score_with_splink(
        tables.candidates,
        units=tables.units,
        crs=tables.crs,
        stems=tables.stems,
        provider_profile=tables.profile,
        labels=pl.DataFrame(schema=_LABELS),
        client=_offline(tmp_path),
    )
    assert again.scores.equals(tables.scores)


def simulate(
    *,
    candidates: pl.DataFrame,
    units: pl.DataFrame,
    stems: pl.DataFrame,
    crs: pl.LazyFrame,
    client: JevClient,
) -> tuple[SplinkResult, pl.DataFrame]:
    """Calibrate the `SIMULATED` providers' models on id truth.

    The pool is every blocked pair of the providers' id-persistent commitments,
    labelled by `same_crs_id`. Id truth answers each model's calibration sample in
    the build (`score_with_splink`'s `answer`), standing in for people's labels.
    Returns the run and its scores with `y` and `labelled`.
    """
    pool = candidates.filter(
        "id_persistent", pl.col("donor_code").is_in(SIMULATED)
    ).with_columns(eligible=pl.lit(value=True))
    truth = pool.select("left_id", "right_id", y="same_crs_id")
    asked: list[pl.DataFrame] = []

    def answer(pairs: pl.DataFrame, attributes: pl.DataFrame) -> pl.DataFrame:
        asked.append(pairs.select("left_id", "right_id"))
        return pairs.join(truth, on=["left_id", "right_id"]).select(
            "left_id",
            "right_id",
            y=pl.col("y").cast(pl.Float64),
            pair_label_source=pl.lit("human"),
        )

    result = score_with_splink(
        pool,
        units=units,
        crs=crs,
        stems=stems,
        # With their ids hidden, the providers fall outside the truth group.
        provider_profile=scoring_groups(
            pool.with_columns(split=pl.lit(ELIGIBLE), true_pair=pl.lit(value=False))
        ),
        labels=pl.DataFrame(schema=_LABELS),
        client=client,
        answer=answer,
    )
    scores = result.scores.join(truth, on=["left_id", "right_id"]).join(
        pl.concat(asked).with_columns(labelled=pl.lit(value=True)),
        on=["left_id", "right_id"],
        how="left",
    )
    return result, scores.with_columns(pl.col("labelled").fill_null(value=False))


def precision_recall(scores: pl.DataFrame) -> pl.DataFrame:
    """Per provider, on its unlabelled pairs: pairs at each cut of the calibrated
    probability and of EM's, their precision against id truth, and their recall
    of the true pairs."""
    unlabelled = scores.filter(~pl.col("labelled"))
    cuts = {
        "em_0.5": pl.col("p_raw") >= 0.5,
        "p_0.5": pl.col("probability") >= 0.5,
        "p_0.9": pl.col("probability") >= 0.9,
    }
    return (
        unlabelled.group_by("donor_code")
        .agg(
            pairs=pl.len(),
            true_pairs=pl.col("y").sum(),
            band=pl.col("probability").is_between(0.3, 0.9, closed="left").sum(),
            **{f"{name}_links": cut.sum() for name, cut in cuts.items()},
            **{
                f"{name}_precision": (cut & pl.col("y")).sum() / cut.sum()
                for name, cut in cuts.items()
            },
            **{
                f"{name}_recall": (cut & pl.col("y")).sum() / pl.col("y").sum()
                for name, cut in cuts.items()
            },
        )
        .sort("donor_code")
    )


def test_calibrated_splink_against_id_truth(tables: LinkTables) -> None:
    cache = Path(os.environ["CF_E2E_DATA"]) / SIMULATION_CACHE
    client = _offline(cache)
    result, scores = simulate(
        candidates=tables.candidates,
        units=tables.units,
        stems=tables.stems,
        crs=tables.crs,
        client=client,
    )
    assert client.live_calls == 0
    assert all(report.calibrated for report in result.models if report.usable)
    measured = precision_recall(scores)
    # Calibration lifts precision above raw EM's for every provider.
    assert (
        measured["p_0.5_precision"].fill_nan(1.0) >= measured["em_0.5_precision"]
    ).all()
    at_half = {
        row["donor_code"]: (
            row["p_0.5_links"],
            round(row["p_0.5_precision"], 3),
            round(row["p_0.5_recall"], 3),
        )
        for row in measured.filter(pl.col("p_0.5_links") > 0).iter_rows(named=True)
    }
    # Of the pooled model's providers (68, 1624, 76), only the Hewlett Foundation (1624)
    # links at 0.5. A disbursement pairs with a purpose-split line once
    # (`purpose_units`), which moves New Zealand (820) most. Over commitments from 2015,
    # the `short_description` fallback adds 165 New Zealand pairs (21 true) and 15
    # Danish ones to the pool, and `template_lines` takes 2,908 French and 548 Danish
    # ones out. Commitments from 2000 give the pool 132,177 more pairs, all Austrian,
    # Danish, French and pooled; New Zealand has none before 2015. `SIMULATION_CACHE`
    # holds Jev's answers for every pair the calibration samples draw.
    assert at_half == {
        1: (2251, 0.681, 0.833),
        3: (1736, 0.641, 0.472),
        4: (2655, 0.901, 0.760),
        820: (1302, 0.637, 0.839),
        1624: (7, 1.0, 0.104),
    }

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
from climate_finance.build.link.score import ELIGIBLE, scoring_groups
from climate_finance.build.link.splink_models import (
    CALIBRATION_LABELS,
    CALIBRATION_REASON,
    SplinkResult,
    score_with_splink,
)
from climate_finance.build.stages import table_path

pytestmark = pytest.mark.slow

MODEL = "jev-1.14.0"
SIMULATION_CACHE = Path("jev-cache/splink-simulation")
# Truth-group providers of several sizes: four with their own model (Belgium,
# Austria, France, Denmark) and three small enough to share the pooled model.
SIMULATED = (820, 1, 4, 3, 68, 1624, 76)
_LABELS = {name: column.dtype for name, column in schema.PAIR_LABELS.columns.items()}


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
    # The archive build: 41 providers outside the truth group and six usable models
    # (USA, four more providers of their own and the pooled model). The pairs are
    # those providers' eligible candidates, blocked on trimmed ids and on project
    # numbers with rounded ones null, less the disbursements the key tiers link. The
    # GEF reuses a crs id across projects, so `crs_key` leaves most of its
    # disbursements to scoring; its own model then completes one EM session, its
    # project numbers block one pair and EM drives a recipient-and-purpose m to zero,
    # so it joins the pooled model.
    assert len(providers) == 41
    assert scores.height == 1_645_421
    assert queued.height == 6


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
    labelled by `same_crs_id`. A first run draws each model's calibration sample;
    its truth becomes people's labels for the second, calibrated run. Returns the
    second run and its scores with `y` and `labelled`.
    """
    pool = candidates.filter(
        "id_persistent", pl.col("donor_code").is_in(SIMULATED)
    ).with_columns(eligible=pl.lit(value=True))
    truth = pool.select("left_id", "right_id", y="same_crs_id")

    def run(labels: pl.DataFrame) -> SplinkResult:
        return score_with_splink(
            pool,
            units=units,
            crs=crs,
            stems=stems,
            # With their ids hidden, the providers fall outside the truth group.
            provider_profile=scoring_groups(
                pool.with_columns(split=pl.lit(ELIGIBLE), true_pair=pl.lit(value=False))
            ),
            labels=labels,
            client=client,
        )

    first = run(pl.DataFrame(schema=_LABELS))
    sample = first.scores.filter(review_reason=CALIBRATION_REASON).join(
        truth, on=["left_id", "right_id"]
    )
    labels = sample.select(
        task=pl.lit("trace"),
        left_id="left_id",
        right_id="right_id",
        is_match="y",
        pair_label_source=pl.lit("human"),
    ).cast(_LABELS)  # ty: ignore[invalid-argument-type]
    second = run(labels)
    scores = second.scores.join(truth, on=["left_id", "right_id"]).join(
        sample.select("left_id", "right_id", labelled=pl.lit(value=True)),
        on=["left_id", "right_id"],
        how="left",
    )
    return second, scores.with_columns(pl.col("labelled").fill_null(value=False))


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
    # The pooled model's providers (68, 1624, 76) link nothing at 0.5. France's
    # calibration labels include Jev's answers for 22 pairs that entered its sample
    # after `SIMULATION_CACHE` was first written; they set its figures.
    assert at_half == {
        1: (1633, 0.662, 0.791),
        3: (842, 0.632, 0.447),
        4: (1964, 0.954, 0.854),
        820: (1170, 0.740, 0.665),
    }

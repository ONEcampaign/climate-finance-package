"""Jev's adaptation labels on held-out Weikmans et al. projects.

The re-evaluated projects whose text is in Toetzke et al.'s training file are left
out. The rest split by distinct text: 20% of the sorted distinct texts, drawn with
the prototype's seed, fit the threshold, and no held-out project shares a text with
them, since the build asks Jev once per text. Jev's answers replay offline from the
prototype's cached responses. The labels' dollar total at fixed weights is reported
with a bootstrap interval, as a figure: 4,000 projects cannot resolve it to 5%.
Needs CF_E2E_DATA pointing at the scoping `data/`.
"""

import os
from collections.abc import Callable
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from climate_finance.build.jev_client import (
    JevClient,
    PrototypeCache,
    import_prototype_cache,
)
from climate_finance.build.label import (
    ADAPTATION_QUESTIONS,
    WEIKMANS_LEVEL,
    ask_jev,
    fit_adaptation_threshold,
    jev_objectives,
    read_adaptation_labels,
)
from climate_finance.methods import ONE

pytestmark = pytest.mark.slow

MODEL = "jev-1.14.0"
SEED = 20241020
FITTING_SHARE = 0.2
BOOTSTRAP_DRAWS = 2000
BOOTSTRAP_SEED = 1
COEFFICIENT = {0: 0.0, 1: ONE.significant, 2: ONE.principal}


def _data() -> Path:
    return Path(os.environ["CF_E2E_DATA"])


def _projects(path: Path, *, training: Path) -> pl.DataFrame:
    """Each labelled project's `label_id`, human level and commitment, without the
    projects in Toetzke et al.'s training file."""

    def normalised(text: pl.Expr) -> pl.Expr:
        return text.str.replace_all(r"\s+", " ").str.strip_chars().str.to_lowercase()

    seen = pl.read_csv(training).select(normalised(pl.col("text").fill_null("nan")))
    return (
        pl.read_csv(path, infer_schema_length=0)
        .with_row_index("label_id")
        .filter(pl.col(WEIKMANS_LEVEL).is_not_null())
        .filter(
            ~normalised(pl.col("text_preprocessed").fill_null("nan")).is_in(
                seen.to_series().implode()
            )
        )
        .select(
            pl.col("label_id").cast(pl.Int64),
            commitment_usd=pl.col("usd_commitment").cast(pl.Float64).fill_null(0.0),
        )
    )


def _split_by_text(projects: pl.DataFrame) -> pl.DataFrame:
    """`projects` with `fitting` set on 20% of their distinct texts."""
    texts = np.array(sorted(projects["text"].unique()), dtype=object)
    size = round(FITTING_SHARE * len(texts))
    drawn = np.random.default_rng(SEED).choice(texts, size=size, replace=False)
    return projects.with_columns(
        fitting=pl.col("text").is_in(
            pl.Series(drawn.tolist(), dtype=pl.String).implode()
        )
    )


@pytest.fixture(scope="module")
def split(tmp_path_factory: pytest.TempPathFactory) -> pl.DataFrame:
    """Every project with its human `level`, Jev's answers and `fitting` flag."""
    labels_path = _data() / "raw" / "classifiers" / "toetzke" / "wk.csv"
    human = read_adaptation_labels(labels_path)
    cache_dir = tmp_path_factory.mktemp("jev-cache")
    states = {
        label_id: {"project": text}
        for label_id, text in human.select("label_id", "text").iter_rows()
    }
    import_prototype_cache(
        [
            PrototypeCache(
                responses=_data() / "jev-cache" / f"{split_name}_{MODEL}_shuf.jsonl",
                key="rid",
                states=states,
                questions=ADAPTATION_QUESTIONS,
            )
            for split_name in ("dev", "test")
        ],
        cache_dir=cache_dir,
    )
    client = JevClient(
        model=MODEL, cost_cap_usd=10.0, cache_dir=cache_dir, offline=True
    )
    projects = human.join(
        _projects(labels_path, training=labels_path.with_name("train_set.csv")),
        on="label_id",
        validate="1:1",
    )
    answered = projects.join(
        ask_jev(projects.select("label_id", "text"), client=client), on="label_id"
    )
    assert client.live_calls == 0
    return _split_by_text(answered)


@pytest.fixture(scope="module")
def evaluated(split: pl.DataFrame) -> pl.DataFrame:
    """Held-out projects with their human `level` and Jev's level as `jev`."""
    held_out = split.filter(~pl.col("fitting")).with_columns(
        record_id=pl.col("label_id").cast(pl.String)
    )
    threshold = fit_adaptation_threshold(split.filter(pl.col("fitting")))
    labels = jev_objectives(held_out, threshold=threshold)
    return held_out.join(
        labels.select("record_id", jev=pl.col("level")), on="record_id", validate="1:1"
    )


def test_no_held_out_text_is_in_the_fitting_set(split: pl.DataFrame) -> None:
    fitting = split.filter(pl.col("fitting"))
    held_out = split.filter(~pl.col("fitting"))

    assert (fitting.height, held_out.height) == (959, 4211)
    assert not set(fitting["text"]) & set(held_out["text"])


def test_adaptation_f1_on_unseen_texts_reaches_0_72(
    evaluated: pl.DataFrame, record_property: Callable[[str, object], None]
) -> None:
    human, jev = pl.col("level") > 0, pl.col("jev") > 0
    f1 = evaluated.select(2 * (human & jev).sum() / (human.sum() + jev.sum())).item()
    record_property("adaptation_f1", f1)
    print(f"Adaptation F1 on {evaluated.height} held-out projects: {f1:.3f}")

    assert f1 >= 0.72


def test_the_fixed_weight_total_is_reported_with_its_interval(
    evaluated: pl.DataFrame, record_property: Callable[[str, object], None]
) -> None:
    """The held-out dollar total at ONE's fixed weights, Jev's levels against the
    human ones, with a 95% interval from resampling distinct texts."""
    amount = pl.col("commitment_usd")
    by_text = (
        evaluated.group_by("text")
        .agg(
            human=(amount * pl.col("level").replace_strict(COEFFICIENT)).sum(),
            jev=(amount * pl.col("jev").replace_strict(COEFFICIENT)).sum(),
        )
        .sort("text")
    )
    totals = by_text.select("human", "jev").to_numpy()
    draws = np.random.default_rng(BOOTSTRAP_SEED).integers(
        0, len(totals), (BOOTSTRAP_DRAWS, len(totals))
    )
    sums = totals[draws].sum(axis=1)
    low, high = np.percentile(sums[:, 1] / sums[:, 0] - 1, [2.5, 97.5])
    human_total, jev_total = totals.sum(axis=0)
    difference = jev_total / human_total - 1
    record_property("human_total_usd_m", human_total)
    record_property("jev_total_usd_m", jev_total)
    record_property("difference_95_interval", (difference, low, high))
    print(
        f"Jev at fixed weights USD {jev_total:,.1f}m against human USD "
        f"{human_total:,.1f}m: {difference:+.1%} (95% interval {low:+.1%} to "
        f"{high:+.1%})"
    )

    assert low <= difference <= high

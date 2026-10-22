"""Jev's adaptation labels and level map on held-out Weikmans et al. projects.

The fitting set and the held-out set are the prototype's split of the re-evaluated
projects: projects whose text is in Toetzke et al.'s training file are left out, and
20% of each human level, drawn with the prototype's seed, fit the threshold and the
level map. Jev's answers replay offline from the prototype's cached responses.
Needs CF_E2E_DATA pointing at the scoping `data/`.
"""

import os
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
    fit_jev_level_map,
    jev_objectives,
    read_adaptation_labels,
)
from climate_finance.methods import ONE
from climate_finance.query import jev_levels

pytestmark = pytest.mark.slow

MODEL = "jev-1.14.0"
SEED = 20241020
FITTING_SHARE = 0.2


def _data() -> Path:
    return Path(os.environ["CF_E2E_DATA"])


def _split(path: Path, *, training: Path) -> pl.DataFrame:
    """Each labelled project's `label_id`, commitment and split, as the prototype
    drew them, without the projects in Toetzke et al.'s training file."""

    def normalised(text: pl.Expr) -> pl.Expr:
        return text.str.replace_all(r"\s+", " ").str.strip_chars().str.to_lowercase()

    seen = pl.read_csv(training).select(normalised(pl.col("text").fill_null("nan")))
    projects = (
        pl.read_csv(path, infer_schema_length=0)
        .with_row_index("label_id")
        .filter(pl.col(WEIKMANS_LEVEL).is_not_null())
        .with_columns(
            seen=normalised(pl.col("text_preprocessed").fill_null("nan")).is_in(
                seen.to_series().implode()
            )
        )
        .filter(~pl.col("seen"))
        .select(
            pl.col("label_id").cast(pl.Int64),
            human=pl.col(WEIKMANS_LEVEL).cast(pl.Int16),
            commitment_usd=pl.col("usd_commitment").cast(pl.Float64).fill_null(0.0),
        )
    )
    rng = np.random.default_rng(SEED)
    fitting: list[int] = []
    for human in sorted(projects["human"].unique()):
        ids = np.array(sorted(projects.filter(pl.col("human") == human)["label_id"]))
        size = round(FITTING_SHARE * len(ids))
        fitting.extend(rng.choice(ids, size=size, replace=False).tolist())
    return projects.with_columns(fitting=pl.col("label_id").is_in(fitting))


@pytest.fixture(scope="module")
def evaluated(tmp_path_factory: pytest.TempPathFactory) -> pl.DataFrame:
    """Held-out projects with human and Jev levels, calibrated level and amount."""
    labels_path = _data() / "raw" / "classifiers" / "toetzke" / "wk.csv"
    human = read_adaptation_labels(labels_path)
    split = _split(labels_path, training=labels_path.with_name("train_set.csv"))
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
    projects = human.join(split, on="label_id", validate="1:1")
    answered = projects.join(
        ask_jev(projects.select("label_id", "text"), client=client), on="label_id"
    )
    assert client.live_calls == 0
    fitting = answered.filter(pl.col("fitting"))
    held_out = answered.filter(~pl.col("fitting")).with_columns(
        record_id=pl.col("label_id").cast(pl.String)
    )
    labels = jev_objectives(held_out, threshold=fit_adaptation_threshold(fitting))
    levels = jev_levels(
        labels.lazy(), level_map=fit_jev_level_map(fitting, model=MODEL).lazy()
    ).collect()
    return held_out.join(
        labels.select("record_id", jev=pl.col("level")), on="record_id"
    ).join(levels, on="record_id", validate="1:1")


def test_the_split_is_the_prototypes() -> None:
    labels_path = _data() / "raw" / "classifiers" / "toetzke" / "wk.csv"
    split = _split(labels_path, training=labels_path.with_name("train_set.csv"))

    for name, fitting in (("dev", True), ("test", False)):
        cached = pl.read_ndjson(
            _data() / "jev-cache" / f"{name}_{MODEL}_shuf.jsonl"
        ).select(label_id=pl.col("rid").cast(pl.Int64))
        ids = split.filter(pl.col("fitting") == fitting).select("label_id")
        assert sorted(ids["label_id"]) == sorted(cached["label_id"])


def test_adaptation_f1_on_held_out_projects_reaches_0_72(
    evaluated: pl.DataFrame,
) -> None:
    human, jev = pl.col("level") > 0, pl.col("jev") > 0
    f1 = evaluated.select(2 * (human & jev).sum() / (human.sum() + jev.sum())).item()

    assert f1 >= 0.72


def test_the_calibrated_total_is_within_5_percent_of_the_human_total(
    evaluated: pl.DataFrame,
) -> None:
    human_coefficient = pl.col("level").replace_strict(
        {0: 0.0, 1: ONE.significant, 2: ONE.principal}, return_dtype=pl.Float64
    )
    amount = pl.col("commitment_usd")
    human_total, calibrated_total = evaluated.select(
        human=(amount * human_coefficient).sum(),
        calibrated=(amount * pl.col("jev_level")).sum(),
    ).row(0)

    assert calibrated_total == pytest.approx(human_total, rel=0.05)

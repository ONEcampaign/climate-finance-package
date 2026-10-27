"""The blind sample's design and the sequential precision rule, on invented pairs and
simulated labels."""

import numpy as np
import polars as pl
import pytest
from pydantic import ValidationError

from climate_finance.build.evaluate import (
    PrecisionRule,
    _stratum_sequence,
    estimate_precision,
    sample_design,
)
from climate_finance.build.gate import load_gates

RULE = load_gates().precision
SMALL, LARGE = 4, 8


def _pairs() -> pl.DataFrame:
    """Provider 4 holds 20 pairs, under the census size; provider 8 holds 300 over
    three strata with skewed values."""
    rng = np.random.default_rng(7)
    strata = ["key"] * 200 + ["ledger"] * 80 + ["model"] * 20
    return pl.DataFrame(
        {
            "provider_code": [SMALL] * 20 + [LARGE] * 300,
            "task": "trace",
            "left_id": [f"d{i}" for i in range(320)],
            "right_id": [f"c{i}" for i in range(320)],
            "link_method": "crs_id",
            "allocated_usd": rng.lognormal(mean=12, sigma=2, size=320),
            "stratum": ["key"] * 20 + strata,
        },
        schema_overrides={"provider_code": pl.Int32},
    )


def test_the_weights_sum_to_the_population_in_every_stratum() -> None:
    pairs = _pairs()

    design = sample_design(pairs, rule=RULE, release="r")

    population = pairs.group_by("provider_code", "stratum").agg(
        pl.col("allocated_usd").sum(), pairs=pl.len()
    )
    weights = design.group_by("provider_code", "stratum").agg(
        pl.col("weight_usd").sum(), pl.col("stratum_pairs").first()
    )
    joined = population.join(weights, on=["provider_code", "stratum"])
    assert joined.height == population.height == 4
    assert joined["weight_usd"].to_list() == pytest.approx(
        joined["allocated_usd"].to_list(), rel=1e-12
    )
    assert joined["stratum_pairs"].to_list() == joined["pairs"].to_list()


def test_a_small_provider_is_a_census_and_a_large_one_draws_to_the_cap() -> None:
    design = sample_design(_pairs(), rule=RULE, release="r").sort("draw")

    small = design.filter(provider_code=SMALL)
    assert small["design"].unique().to_list() == ["census"]
    assert small.select("left_id").is_unique().all()
    assert small.height == 20
    large = design.filter(provider_code=LARGE)
    assert large["design"].unique().to_list() == ["pps"]
    assert large["draw"].to_list() == list(range(1, RULE.cap + 1))
    # Every stratum is drawn in the first batch, the largest by value first.
    assert large.head(3)["stratum"].n_unique() == 3
    # A pair drawn twice keeps one sample id.
    ids = large.group_by("left_id").agg(pl.col("sample_id").n_unique())
    assert ids["sample_id"].max() == 1


def test_a_small_stratum_draws_its_minimum_and_counts_at_its_value_share() -> None:
    # Of provider 8's 1,000,090 in value, the ledger's 70 pairs hold 70 and the
    # model's 20 pairs 20, so by value share neither would draw once in 500.
    strata = ["key"] * 200 + ["ledger"] * 70 + ["model"] * 20
    pairs = pl.DataFrame(
        {
            "provider_code": LARGE,
            "task": "trace",
            "left_id": [f"d{i}" for i in range(290)],
            "right_id": [f"c{i}" for i in range(290)],
            "link_method": "crs_id",
            "allocated_usd": [5000.0] * 200 + [1.0] * 90,
            "stratum": strata,
        },
        schema_overrides={"provider_code": pl.Int32},
    )

    design = sample_design(pairs, rule=RULE, release="r")
    # Every model link is wrong and every other link right.
    labels = design.with_columns(
        correct=pl.col("stratum") != "model", label_source=pl.lit("human")
    )
    row = estimate_precision(labels, rule=RULE).row(0, named=True)

    counts = dict(design.group_by("stratum").len().iter_rows())
    assert counts == {"key": RULE.cap - 50 - 20, "ledger": 50, "model": 20}
    assert row["labels"] == RULE.cap
    # The weights count each stratum at its value, so the 20 wrong draws of the
    # model stratum cost it 20 of 1,000,090.
    assert row["estimate"] == pytest.approx(1 - 20 / 1_000_090)


def test_the_design_is_the_same_on_every_draw() -> None:
    pairs = _pairs()

    first = sample_design(pairs, rule=RULE, release="r")
    again = sample_design(
        pairs.sample(fraction=1.0, shuffle=True, seed=3), rule=RULE, release="r"
    )

    assert first.sort("provider_code", "draw").equals(
        again.sort("provider_code", "draw")
    )


def test_a_changed_population_draws_under_new_sample_ids() -> None:
    pairs = _pairs()
    changed = pairs.with_columns(
        allocated_usd=pl.when(pl.col("left_id") == "d319")
        .then(pl.col("allocated_usd") * 2)
        .otherwise("allocated_usd")
    )

    first = sample_design(pairs, rule=RULE, release="r")
    again = sample_design(changed, rule=RULE, release="r")

    def ids(design: pl.DataFrame, provider: int) -> set[str]:
        return set(design.filter(provider_code=provider)["sample_id"])

    # d319 is provider 8's, so provider 4's population and ids stay.
    assert ids(first, SMALL) == ids(again, SMALL)
    assert not ids(first, LARGE) & ids(again, LARGE)


def _draws(
    correct: np.ndarray, *, stratum: str = "model", weight: float = 1.0
) -> pl.DataFrame:
    """One pps provider per row of `correct`, a draw per column."""
    runs, draws = correct.shape
    return pl.DataFrame(
        {
            "provider_code": np.repeat(np.arange(runs), draws).astype(np.int32),
            "draw": np.tile(np.arange(1, draws + 1), runs).astype(np.int32),
            "correct": correct.ravel(),
            "design": "pps",
            "label_source": "human",
            "stratum": stratum,
            "weight_usd": weight,
            "stratum_usd": weight * draws,
        }
    )


def _pass_rate(precision: float, *, runs: int = 2000) -> float:
    """The share of simulated providers the rule passes, labelling batch by batch
    until it decides."""
    correct = np.random.default_rng(11).random((runs, RULE.cap)) < precision
    draws = _draws(correct)
    decided: dict[int, str] = {}
    for look in range(RULE.batch, RULE.cap + 1, RULE.batch):
        labelled = draws.with_columns(
            correct=pl.when(pl.col("draw") <= look).then("correct")
        )
        estimates = estimate_precision(labelled, rule=RULE)
        for provider, decision in estimates.select("provider_code", "decision").rows():
            if decision != "continue":
                decided.setdefault(provider, decision)
    assert len(decided) == runs
    return sum(decision == "pass" for decision in decided.values()) / runs


def test_the_sequential_rule_rarely_passes_a_provider_at_the_threshold() -> None:
    assert _pass_rate(0.90) < 0.03


def test_the_sequential_rule_passes_a_precise_provider() -> None:
    assert _pass_rate(0.96) > 0.99


def test_a_look_reads_whole_batches_of_unbroken_labels() -> None:
    correct = np.ones((1, RULE.cap), dtype=bool)
    labels = _draws(correct).with_columns(
        # Draw 70 is undecided, so the look stops at the batch before it, and the
        # rejections after it are not read.
        correct=pl.when(pl.col("draw") == 70)
        .then(None)
        .when(pl.col("draw") > 70)
        .then(pl.lit(value=False))
        .otherwise("correct")
    )

    row = estimate_precision(labels, rule=RULE).row(0, named=True)

    assert row["labels"] == RULE.batch
    assert row["estimate"] == 1.0
    assert row["decision"] == "continue"


def test_strata_combine_by_allocated_value() -> None:
    rule = PrecisionRule(
        threshold=0.9,
        batch=4,
        cap=8,
        interim_z=2.5,
        final_z=1.96,
        census_below=0,
        min_draws=0,
        seed=0,
    )
    # Stratum a holds 900 of value and is right 3 times in 4; b holds 100 and is
    # right every time.
    labels = pl.DataFrame(
        {
            "provider_code": [1] * 8,
            "draw": list(range(1, 9)),
            "correct": [True, True, True, False, True, True, True, True],
            "design": "pps",
            "label_source": "human",
            "stratum": ["a"] * 4 + ["b"] * 4,
            "weight_usd": [225.0] * 4 + [25.0] * 4,
            "stratum_usd": [900.0] * 4 + [100.0] * 4,
        },
        schema_overrides={"provider_code": pl.Int32, "draw": pl.Int32},
    )

    row = estimate_precision(labels, rule=rule).row(0, named=True)

    assert row["estimate"] == pytest.approx(0.9 * 0.75 + 0.1 * 1.0)
    assert row["lower"] < row["estimate"] < row["upper"]
    assert row["decision"] == "fail"


def test_each_stratum_reports_its_own_precision_beside_the_providers() -> None:
    # Key draws hold 900 of value and are all right; model draws hold 100 and are
    # right every other time.
    correct = np.ones(RULE.cap, dtype=bool)
    correct[450::2] = False
    key_draw = pl.col("draw") <= 450
    labels = _draws(correct[np.newaxis], weight=2.0).with_columns(
        stratum=pl.when(key_draw).then(pl.lit("key")).otherwise("stratum"),
        stratum_usd=pl.when(key_draw).then(900.0).otherwise(100.0),
    )

    row = estimate_precision(labels, rule=RULE).row(0, named=True)
    key, model = row["strata"]

    assert row["estimate"] == pytest.approx(0.95)
    assert (key["stratum"], key["labels"], key["estimate"]) == ("key", 450, 1.0)
    assert (model["stratum"], model["allocated_usd"], model["labels"]) == (
        "model",
        100.0,
        50,
    )
    assert model["estimate"] == pytest.approx(0.5)
    assert model["lower"] < 0.5 < model["upper"] < row["lower"]


def test_a_stratum_with_no_draw_read_is_listed_unestimated() -> None:
    labels = _draws(np.ones((1, RULE.batch), dtype=bool)).vstack(
        _draws(np.ones((1, 1), dtype=bool), stratum="reviewed").with_columns(
            draw=pl.lit(RULE.batch + 1, dtype=pl.Int32),
            correct=pl.lit(None, dtype=pl.Boolean),
        )
    )

    strata = estimate_precision(labels, rule=RULE).row(0, named=True)["strata"]

    assert [(row["stratum"], row["labels"], row["estimate"]) for row in strata] == [
        ("model", RULE.batch, 1.0),
        ("reviewed", 0, None),
    ]


def test_a_stratum_read_from_one_draw_keeps_the_bounds_finite() -> None:
    # Every draw is right, and the first look reads one draw of the small stratum,
    # whose spread cannot be measured.
    labels = _draws(np.ones((1, RULE.batch), dtype=bool)).with_columns(
        stratum=pl.when(pl.col("draw") == 1).then(pl.lit("key")).otherwise("stratum"),
        stratum_usd=pl.when(pl.col("draw") == 1).then(1.0).otherwise(49.0),
    )

    row = estimate_precision(labels, rule=RULE).row(0, named=True)
    bounds = [row["lower"]] + [stratum["lower"] for stratum in row["strata"]]

    assert row["estimate"] == 1.0
    assert all(0 < bound < RULE.threshold for bound in bounds)
    assert row["decision"] == "continue"


def test_interim_bounds_are_at_least_as_wide_as_the_final_ones() -> None:
    with pytest.raises(ValidationError, match="interim_z"):
        RULE.model_validate(RULE.model_dump() | {"interim_z": 1.5})


def test_a_census_decides_only_once_complete_and_then_exactly() -> None:
    rule = RULE.model_copy(update={"census_below": 100})
    labels = pl.DataFrame(
        {
            "provider_code": [1] * 3,
            "draw": [1, 2, 3],
            "correct": [True, True, None],
            "design": "census",
            "label_source": "human",
            "stratum": "key",
            "weight_usd": [90.0, 5.0, 5.0],
            "stratum_usd": 100.0,
        },
        schema_overrides={"provider_code": pl.Int32, "draw": pl.Int32},
    )

    partial = estimate_precision(labels, rule=rule).row(0, named=True)
    complete = estimate_precision(
        labels.with_columns(correct=pl.col("correct").fill_null(value=False)), rule=rule
    ).row(0, named=True)

    assert partial["decision"] == "continue"
    assert (complete["estimate"], complete["lower"], complete["upper"]) == (
        pytest.approx(0.95),
    ) * 3
    assert complete["decision"] == "pass"


def test_futility_reads_the_strata_the_cap_will_draw() -> None:
    # Four strata by value. Rounding gives the smallest-but-one 53 of the first 400
    # draws and 67 of the 500, a larger share at the cap than at the look.
    usd = np.array([4366.0, 3642.0, 659.0, 1333.0])
    strata = _stratum_sequence(usd / usd.sum(), draws=RULE.cap)
    counts = np.bincount(strata)
    draw = np.arange(1, RULE.cap + 1)
    # Its first 37 draws are wrong and every other draw is right.
    wrong = np.flatnonzero(strata == 3)[:37] + 1
    labels = pl.DataFrame(
        {
            "provider_code": np.zeros(RULE.cap, dtype=np.int32),
            "draw": draw.astype(np.int32),
            "correct": ~np.isin(draw, wrong),
            "design": "pps",
            "label_source": "human",
            "stratum": strata.astype(str),
            "weight_usd": usd[strata] / counts[strata],
            "stratum_usd": usd[strata],
        }
    )

    look = estimate_precision(
        labels.with_columns(correct=pl.when(pl.col("draw") <= 400).then("correct")),
        rule=RULE,
    ).row(0, named=True)
    cap = estimate_precision(labels, rule=RULE).row(0, named=True)

    assert (np.bincount(strata[:400])[3], counts[3]) == (53, 67)
    assert look["labels"] == 400
    assert look["upper"] > RULE.threshold
    assert look["decision"] == "continue"
    assert cap["decision"] == "pass"

"""The truth-group classifier and the provider profile.

Where an agency's ids persist, a later record continues a commitment exactly when it
carries the commitment's crs id, so those agencies' candidate pairs are labelled
truth. A provider with at least `MIN_TRUE_PAIRS` true pairs among its training
pairs is in the truth group: one gradient-boosted classifier, trained on the truth
group's labelled pairs, scores the eligible pairs of its other agencies. Providers
without that truth are scored by Splink, alone or, below `SPLINK_MIN_COMMITMENTS`
open commitments or `SPLINK_MIN_PAIRS` eligible pairs, pooled.

Labelled pairs split into halves by the md5 of their commitment's donor and crs id
(the unit id where it has none). The classifier trains on one half, with five folds
grouped by the same key, and the other half measures it. The out-of-fold scores fit
an isotonic map: the provider's own where it has at least `MIN_CALIBRATION_PAIRS`
true and as many false training pairs, else the pooled one. The link threshold
maximises F1 on out-of-fold scores calibrated by maps fitted on the other folds, so
no map scores the pairs it was fitted on. F1 counts as misses the true pairs
blocking did not produce.

An eligible pair links when its probability reaches the threshold and it is not a
sibling (`programme_level` 1). Each disbursement unit joins the one commitment it
scores highest, ties to the earlier placed year, then the smaller unit id. A
commitment's classifier links form one group.
"""

import hashlib
from dataclasses import dataclass

import numpy as np
import polars as pl
import sklearn
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.isotonic import IsotonicRegression
from sklearn.model_selection import GroupKFold

from climate_finance import schema
from climate_finance.build.link.id_practice import period_of
from climate_finance.build.link.levels import (
    LEVELS,
    comparison_levels,
    levels_struct,
    unit_attributes,
)

MIN_TRUE_PAIRS = 30
MIN_CALIBRATION_PAIRS = 30
SPLINK_MIN_COMMITMENTS = 300
SPLINK_MIN_PAIRS = 2_000
FOLDS = 5
THRESHOLDS = np.round(np.arange(0.01, 1.0, 0.01), 2)
FEATURES = (*LEVELS, "year_gap", "log_ratio", "later_commit", "programme_level")
PARAMS = {
    "max_iter": 400,
    "learning_rate": 0.05,
    "max_leaf_nodes": 31,
    "min_samples_leaf": 40,
    "l2_regularization": 1.0,
    "early_stopping": False,
    "random_state": 0,
}
METHOD = "classifier"
MODEL_VERSION = f"truth_hgb/scikit-learn-{sklearn.__version__}"
# The values of a pair's `split`.
TRAIN, HELDOUT, ELIGIBLE = "train", "heldout", "eligible"
_SIBLING = 1
_PAIR = ("left_id", "right_id")


@dataclass(frozen=True)
class TruthScores:
    """The classifier's calibrated scores and what it chose.

    `pairs` holds the truth group's candidate pairs with their features, `split`
    and `p_model`: cross-fitted out-of-fold probabilities for training pairs, the
    final model's for the others. `calibration` names, per truth-group provider,
    the isotonic map its pairs took (`own` or `pooled`).
    """

    pairs: pl.DataFrame
    threshold: float
    calibration: pl.DataFrame


@dataclass(frozen=True)
class Scored:
    """The scoring step's output: the classifier's links, the scoring columns of
    the provider profile, and the truth group's scores (None when the group is
    empty)."""

    links: pl.DataFrame
    profile: pl.DataFrame
    scores: TruthScores | None


def score_candidates(
    candidates: pl.DataFrame,
    *,
    units: pl.DataFrame,
    crs: pl.LazyFrame,
    links: pl.DataFrame,
    id_practice: pl.DataFrame,
    first_year: int,
) -> Scored:
    """Sort providers into scoring groups and link what the truth group scores.

    Args:
        candidates: the `candidates` table.
        units: the link stage's units.
        crs: the CRS records the units sum.
        links: the `trace` and `crdf_crs` links of the steps before scoring.
        id_practice: each agency's class per period.
        first_year: the first CRS year the build loads.
    """
    pairs = with_split(candidates, units=units)
    groups = scoring_groups(pairs)
    if not (groups["scoring_group"] == "truth").any():
        columns = schema.LINKS.columns.items()
        empty = pl.DataFrame(schema={name: column.dtype for name, column in columns})
        return Scored(
            links=empty, profile=scoring_profile(groups, scores=None), scores=None
        )
    members = groups.filter(scoring_group="truth")["provider_code"].implode()
    pairs = pairs.filter(pl.col("donor_code").is_in(members))
    compared = units.join(
        pl.concat([pairs["left_id"], pairs["right_id"]]).to_frame("unit_id"),
        on="unit_id",
        how="semi",
    )
    features = comparison_levels(pairs, attributes=unit_attributes(compared, crs=crs))
    truth = id_truth(units, links=links, id_practice=id_practice, first_year=first_year)
    scores = score_truth_group(features, groups=groups, truth=truth)
    return Scored(
        links=classifier_links(scores),
        profile=scoring_profile(groups, scores=scores),
        scores=scores,
    )


def split_key(donor_code: pl.Expr, crs_id: pl.Expr, unit_id: pl.Expr) -> pl.Expr:
    """The key labelled pairs are halved and folded by: donor and crs id, else the
    unit id."""
    return pl.concat_str(
        donor_code.cast(pl.String), pl.coalesce(crs_id, unit_id), separator="|"
    )


def is_heldout(keys: pl.Series) -> pl.Series:
    """Whether each split key falls in the held-out half: the md5 of the key, read
    as a number from its first eight hex digits, is odd."""
    distinct = keys.unique().sort()
    odd = [
        int(hashlib.md5(key.encode()).hexdigest()[:8], 16) % 2 == 1 for key in distinct
    ]
    lookup = pl.DataFrame(
        {"key": distinct, "heldout": pl.Series(odd, dtype=pl.Boolean)}
    )
    return keys.to_frame("key").join(
        lookup, on="key", how="left", maintain_order="left"
    )["heldout"]


def id_truth(
    units: pl.DataFrame,
    *,
    links: pl.DataFrame,
    id_practice: pl.DataFrame,
    first_year: int,
) -> pl.DataFrame:
    """Every (disbursement, commitment) unit pair the crs id says continues, blocked
    or not.

    A commitment counts when a `crdf_crs` link reaches it, it has a crs id, and its
    agency is `id_persistent` in its record year's period. It continues in every
    disbursement unit of its donor and crs id from a later record year whose floor
    year is not before `first_year`.
    """
    persistent = id_practice.filter(id_class="id_persistent").select(
        "donor_code", "agency_code", "period"
    )
    commitments = (
        units.join(
            links.filter(task="crdf_crs").select(unit_id="right_id"),
            on="unit_id",
            how="semi",
        )
        .drop_nulls("crs_id")
        .with_columns(period=period_of(pl.col("year")))
        .join(persistent, on=["donor_code", "agency_code", "period"], how="semi")
    )
    disbursements = units.filter(
        pl.col("disbursement_usd") > 0,
        pl.col("floor_year").is_null() | (pl.col("floor_year") >= first_year),
    )
    return (
        commitments.select("unit_id", "donor_code", "crs_id", "year")
        .join(
            disbursements.select("unit_id", "donor_code", "crs_id", "year"),
            on=["donor_code", "crs_id"],
            suffix="_left",
        )
        .filter(pl.col("year_left") > pl.col("year"))
        .select(left_id="unit_id_left", right_id="unit_id", donor_code="donor_code")
    )


def with_split(candidates: pl.DataFrame, *, units: pl.DataFrame) -> pl.DataFrame:
    """`candidates` with `split`, and the commitment's `split_key` and
    `placed_year`.

    A labelled pair (`id_persistent`) is `train` or `heldout` by its split key; any
    other is `eligible`.
    """
    commitments = units.select(
        right_id="unit_id",
        split_key=split_key(pl.col("donor_code"), pl.col("crs_id"), pl.col("unit_id")),
        placed_year="placed_year",
    )
    pairs = candidates.join(
        commitments, on="right_id", how="left", maintain_order="left"
    )
    heldout = is_heldout(pairs["split_key"])
    return pairs.with_columns(
        split=pl.when(~pl.col("id_persistent"))
        .then(pl.lit(ELIGIBLE))
        .when(heldout)
        .then(pl.lit(HELDOUT))
        .otherwise(pl.lit(TRAIN))
    )


def scoring_groups(pairs: pl.DataFrame) -> pl.DataFrame:
    """Each provider's scoring group and the counts that set it.

    Args:
        pairs: `with_split` candidates.

    Returns:
        provider_code, scoring_group, true_pairs (true training pairs), commitments
        (commitments in eligible pairs) and pairs (eligible pairs), one row per
        provider with candidate pairs.
    """
    eligible = pl.col("split") == ELIGIBLE
    counts = pairs.group_by(provider_code="donor_code").agg(
        true_pairs=((pl.col("split") == TRAIN) & pl.col("same_crs_id")).sum(),
        commitments=pl.col("right_id").filter(eligible).n_unique(),
        pairs=eligible.sum(),
    )
    return counts.select(
        "provider_code",
        scoring_group=pl.when(pl.col("true_pairs") >= MIN_TRUE_PAIRS)
        .then(pl.lit("truth"))
        .when(
            (pl.col("commitments") >= SPLINK_MIN_COMMITMENTS)
            & (pl.col("pairs") >= SPLINK_MIN_PAIRS)
        )
        .then(pl.lit("splink"))
        .otherwise(pl.lit("pooled_splink")),
        true_pairs=pl.col("true_pairs").cast(pl.Int32),
        commitments=pl.col("commitments").cast(pl.Int32),
        pairs=pl.col("pairs").cast(pl.Int32),
    ).sort("provider_code")


def score_truth_group(
    features: pl.DataFrame, *, groups: pl.DataFrame, truth: pl.DataFrame
) -> TruthScores:
    """Train, calibrate and apply the classifier for the truth group.

    Args:
        features: `with_split` candidates with `comparison_levels`.
        groups: `scoring_groups`.
        truth: `id_truth`, for the true pairs blocking missed.
    """
    members = groups.filter(scoring_group="truth")
    pairs = features.filter(
        pl.col("donor_code").is_in(members["provider_code"].implode())
    ).sort(*_PAIR)
    train = pairs.filter(split=TRAIN)
    x, y = _matrix(train), train["same_crs_id"].to_numpy()
    donors = train["donor_code"].to_numpy()
    folds = list(GroupKFold(n_splits=FOLDS).split(x, y, train["split_key"].to_numpy()))
    raw = np.empty(len(y))
    for fit_rows, score_rows in folds:
        raw[score_rows] = _predict(_fit(x[fit_rows], y[fit_rows]), x[score_rows])
    crossed = np.empty(len(y))
    for fit_rows, score_rows in folds:
        fold_maps = _isotonic_maps(raw[fit_rows], y[fit_rows], donors[fit_rows])
        crossed[score_rows] = _calibrate(fold_maps, raw[score_rows], donors[score_rows])
    n_true = truth.join(train.select("right_id").unique(), on="right_id", how="semi")
    threshold = _best_threshold(crossed, y, n_true=n_true.height)

    model = _fit(x, y)
    maps = _isotonic_maps(raw, y, donors)
    rest = pairs.filter(pl.col("split") != TRAIN)
    p_rest = _calibrate(
        maps, _predict(model, _matrix(rest)), rest["donor_code"].to_numpy()
    )
    scored = pl.concat(
        [
            train.with_columns(p_model=pl.Series(crossed)),
            rest.with_columns(p_model=pl.Series(p_rest)),
        ]
    ).sort(*_PAIR)
    own = pl.col("provider_code").is_in([donor for donor in maps if donor is not None])
    calibration = members.select(
        "provider_code",
        calibration=pl.when(own).then(pl.lit("own")).otherwise(pl.lit("pooled")),
    )
    return TruthScores(pairs=scored, threshold=threshold, calibration=calibration)


def classifier_links(scores: TruthScores) -> pl.DataFrame:
    """The classifier's `trace` links, in the `links` schema."""
    taken = scores.pairs.filter(
        pl.col("split") == ELIGIBLE,
        pl.col("programme_level") != _SIBLING,
        pl.col("p_model") >= scores.threshold,
    )
    best = taken.sort(
        "left_id",
        "p_model",
        "placed_year",
        "right_id",
        descending=[False, True, False, False],
    ).unique("left_id", keep="first", maintain_order=True)
    given = {
        "task": pl.lit("trace"),
        "left_id": pl.col("left_id"),
        "right_id": pl.col("right_id"),
        "group_id": pl.concat_str(pl.lit(METHOD), "right_id", separator="|"),
        "probability": pl.col("p_model"),
        "link_method": pl.lit(METHOD),
        "p_model": pl.col("p_model"),
        "programme_level": pl.col("programme_level"),
        "levels": levels_struct(),
        "model_version": pl.lit(MODEL_VERSION),
        "review": pl.lit(value=False),
    }
    return best.select(
        given.get(name, pl.lit(None)).cast(column.dtype).alias(name)
        for name, column in schema.LINKS.columns.items()
    ).sort(*_PAIR)


def scoring_profile(
    groups: pl.DataFrame, *, scores: TruthScores | None
) -> pl.DataFrame:
    """The scoring columns of `provider_profile`: `groups` with each truth-group
    provider's calibration and threshold; `scores` is None when the truth group is
    empty."""
    if scores is None:
        profile = groups.with_columns(
            calibration=pl.lit(None, dtype=pl.String),
            threshold=pl.lit(None, dtype=pl.Float64),
        )
    else:
        truth = pl.col("scoring_group") == "truth"
        profile = groups.join(scores.calibration, on="provider_code", how="left")
        profile = profile.with_columns(
            threshold=pl.when(truth).then(pl.lit(scores.threshold))
        )
    return profile.select(
        "provider_code",
        "scoring_group",
        "true_pairs",
        "commitments",
        "pairs",
        "calibration",
        "threshold",
    ).sort("provider_code")


def provider_profile(crosswalk: pl.DataFrame, *, scoring: pl.DataFrame) -> pl.DataFrame:
    """`provider_profile` from the crosswalk columns (`crosswalk_profile`) and the
    scoring columns (`scoring_profile`), one row per provider in either."""
    profile = crosswalk.join(scoring, on="provider_code", how="full", coalesce=True)
    return profile.select(list(schema.PROVIDER_PROFILE.columns)).sort("provider_code")


def _matrix(pairs: pl.DataFrame) -> np.ndarray:
    """The features as floats: levels shifted by one so missing is category 0, and
    a missing log ratio NaN."""
    shifted = [pl.col(level) + 1 for level in LEVELS]
    others = FEATURES[len(LEVELS) :]
    return pairs.select(*shifted, *others).to_numpy().astype(np.float64)


def _fit(x: np.ndarray, y: np.ndarray) -> HistGradientBoostingClassifier:
    model = HistGradientBoostingClassifier(
        categorical_features=list(range(len(LEVELS))), **PARAMS
    )
    return model.fit(x, y)


def _predict(model: HistGradientBoostingClassifier, x: np.ndarray) -> np.ndarray:
    return model.predict_proba(x)[:, 1]


def _isotonic_maps(
    raw: np.ndarray, y: np.ndarray, donors: np.ndarray
) -> dict[int | None, IsotonicRegression]:
    """The pooled map (key None) and each donor's own where its pairs allow."""
    maps = {None: _isotonic(raw, y)}
    for donor in np.unique(donors):
        own = donors == donor
        true = int(y[own].sum())
        if min(true, int(own.sum()) - true) >= MIN_CALIBRATION_PAIRS:
            maps[int(donor)] = _isotonic(raw[own], y[own])
    return maps


def _isotonic(x: np.ndarray, y: np.ndarray) -> IsotonicRegression:
    return IsotonicRegression(out_of_bounds="clip", y_min=0, y_max=1).fit(x, y)


def _calibrate(
    maps: dict[int | None, IsotonicRegression], raw: np.ndarray, donors: np.ndarray
) -> np.ndarray:
    """`raw` through each donor's own map, else the pooled one."""
    out = np.empty(len(raw))
    for donor in np.unique(donors):
        rows = donors == donor
        out[rows] = maps.get(int(donor), maps[None]).predict(raw[rows])
    return out


def _best_threshold(p: np.ndarray, y: np.ndarray, *, n_true: int) -> float:
    """The grid threshold of highest F1, the lowest on ties, against `n_true` true
    pairs in all."""
    true = y.astype(bool)
    f1 = [
        2 * int((p[true] >= t).sum()) / (int((p >= t).sum()) + n_true)
        for t in THRESHOLDS
    ]
    return float(THRESHOLDS[int(np.argmax(f1))])

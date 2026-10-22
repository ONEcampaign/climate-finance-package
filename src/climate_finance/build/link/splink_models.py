"""Per-provider Splink models, calibrated through people's labels and Jev's.

Providers outside the truth group (`score.scoring_groups`) have no ids that say which
later record continues which commitment. A provider in the `splink` group gets its
own Fellegi-Sunter model over the comparisons of `levels.comparisons`, and the
`pooled_splink` group shares a pooled model with a provider indicator. A model's u
comes from random sampling and its prior from `PRIOR_RULES`; its m comes from three
EM sessions, each blocked on one of `EM_RULES` and each leaving out the comparisons
its blocking rule reads. A model needs two completed sessions to estimate every
comparison. A provider whose own model completes fewer joins the pooled model, and a
pooled model that completes fewer scores nothing. u and EM run on one DuckDB thread,
since a parallel sum adds in whatever order threads finish, and two builds must give
the same model.

Raw EM probabilities never link: on the prototype's id truth they reached precision
0.141 at p of 0.5 or more. A model is calibrated once people have labelled
`CALIBRATION_LABELS` of its pairs. Until then it gives no probability, and its pairs
enter the review queue only as its calibration sample: `CALIBRATION_LABELS` pairs
spread evenly over the bands of EM match weight in `WEIGHT_BANDS`, less those already
labelled.

Once calibrated, Jev answers a sample of `JEV_SAMPLE` pairs drawn the same way.
People's labels set Jev's scale for the model, a logistic of their label on the logit
of Jev's p(same activity), because that scale shifts between tasks and wording. m is
re-estimated from the labelled pairs with inverse-probability weights and fixed in the
model, which then rescores every pair. An isotonic map of label on the new match
weight gives the probability: the model's own map when its labelled pairs hold
`MIN_ISOTONIC_OUTCOMES` matches and as many non-matches, else a map pooled over the
calibrated models. When people's labels hold under `MIN_JEV_SCALE_OUTCOMES` of either
outcome, Jev's scale cannot be fitted, and their labels alone set m and the map.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb
import numpy as np
import polars as pl
import splink
import splink.comparison_level_library as cll
import splink.comparison_library as cl
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from splink import DuckDBAPI, Linker, SettingsCreator

# Splink's own reading of a comparison gives each level's number and the m it
# scores with; splink is pinned below 6.
from splink.internals.comparison import Comparison

from climate_finance import schema
from climate_finance.build.link.levels import (
    LEVELS,
    comparison_levels,
    comparisons,
    levels_struct,
    unit_attributes,
)
from climate_finance.build.normalise.ids import content_id

if TYPE_CHECKING:
    from climate_finance.build.jev_client import JevClient

# Splink puts the input with the smaller table name on the left.
LEFT, RIGHT = "a_commit", "b_later"
_LATER = "l.donor_code = r.donor_code and l.year < r.year"
EM_RULES = (
    f"{_LATER} and l.pn_norm = r.pn_norm and l.pn_freq <= 300",
    f"{_LATER} and l.title_norm = r.title_norm and l.title_freq <= 100",
    f"{_LATER} and l.recipient_code = r.recipient_code "
    "and l.purpose_code = r.purpose_code",
)
# Deterministic rules for the prior, and the share of id-truth continuations they
# found on the prototype's 2021 to 2022 training half (7,759 pairs). The prior moves
# only the raw EM probability: the isotonic map reads the match weight, and a
# constant shift in it leaves the map's output unchanged.
PRIOR_RULES = (
    f"{_LATER} and l.pn_norm = r.pn_norm and l.pn_freq <= 300",
    f"{_LATER} and l.title_norm = r.title_norm and l.title_freq <= 100 "
    "and l.recipient_code = r.recipient_code",
)
PRIOR_RULE_RECALL = 0.963
SEED = 42
# u: random pairs, at most U_MAX_PAIRS, stopping early once every level has
# U_MIN_COUNT_PER_LEVEL observations, over U_NUM_CHUNKS chunks. Term frequencies
# replace u on the exact project number and title levels, the rarest at random.
U_MAX_PAIRS = 2e6
U_MIN_COUNT_PER_LEVEL = 100
U_NUM_CHUNKS = 10
# EM: at most EM_MAX_PAIRS blocked pairs per session, the count taken over every
# record (the models are small enough), and no session on fewer than MIN_EM_PAIRS.
# EM runs on one thread, and the prototype's largest session held 133,532 pairs.
EM_MAX_PAIRS = 2e5
EM_RECORD_SAMPLE_PROPORTION = 1.0
MIN_EM_PAIRS = 100
# Bands of EM match weight that the calibration and Jev samples spread over.
WEIGHT_BANDS = (-10.0, -5.0, 0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0)
CALIBRATION_LABELS = 100
JEV_SAMPLE = 500
MIN_JEV_SCALE_OUTCOMES = 5
MIN_ISOTONIC_OUTCOMES = 30
# Weight of EM's m in the labelled estimate, as in Splink's estimate from labels.
M_PRIOR_WEIGHT = 1.0
CALIBRATION_REASON = "calibration"
# The ten comparisons labels re-estimate; `donor` and `provider` keep EM's m.
LEVEL_NAMES = tuple(name.removeprefix("gamma_") for name in LEVELS)

# Jev's pair questions, wording v1, as the prototypes asked them.
RELATION_QUESTION = (
    "Each record is one yearly line of a donor's aid database (OECD CRS). The earlier "
    "record carries a commitment; the later record is from a later year. How do the "
    "two records relate as aid activities? Judge from the titles, descriptions, "
    "project numbers, recipients and purposes; use derived_facts for dates and "
    "amounts."
)
RELATION_LEVELS = (
    "Different activities: the later record belongs to another project, programme or "
    "component, even if the donor, country or sector is the same.",
    "Related, and possibly the same: the records share a programme or theme, but the "
    "later record may be a separate component, phase, new grant or sibling of the "
    "earlier one.",
    "Same activity: the later record is the same project or grant as the earlier "
    "record, reported again in a later year.",
)
DISBURSEMENT_QUESTION = (
    "Is the later record a disbursement of the same activity that the earlier record "
    "committed funds to? Answer yes when it is the same project or grant continuing "
    "in a later year (a later payment or a repeat of the same record). Answer no when "
    "it is a different project, a new commitment for a new phase, or a separate "
    "component."
)
SIBLING_QUESTION = (
    "Is the later record a sibling sub-activity of the same umbrella programme as the "
    "earlier record, that is, a separate numbered component, phase or grant under one "
    "programme, rather than the same activity itself?"
)
PAIR_QUESTIONS: dict[str, object] = {
    "relation": {
        "type": "score",
        "instructions": RELATION_QUESTION,
        "criteria": list(RELATION_LEVELS),
    },
    "disbursement": {"type": "noul", "instructions": DISBURSEMENT_QUESTION},
    "sibling": {"type": "noul", "instructions": SIBLING_QUESTION},
}
# Characters of each text field Jev reads.
JEV_TEXT_CHARS = 450

SPLINK_SCORES = {
    name: column.dtype for name, column in schema.PAIR_SCORES.columns.items()
}

_log = logging.getLogger(__name__)


@dataclass
class ModelReport:
    """What one model did: its providers, EM sessions and calibration."""

    name: str
    donors: list[int]
    pairs: int = 0
    commitments: int = 0
    later_records: int = 0
    sessions: dict[str, int] = field(default_factory=dict)
    failed: dict[str, str] = field(default_factory=dict)
    usable: bool = False
    prior: float | None = None
    human_labels: int = 0
    jev_labels: int = 0
    calibrated: bool = False
    own_map: bool = False


@dataclass(frozen=True)
class SplinkResult:
    """Scores in `SPLINK_SCORES`, sorted by pair, and a report per model."""

    scores: pl.DataFrame
    models: list[ModelReport]


@dataclass
class _Model:
    """A model's report, scored pairs and, once calibrated, its labelled pairs."""

    report: ModelReport
    scores: pl.DataFrame | None = None
    labelled: pl.DataFrame | None = None
    label_set: str | None = None


def score_with_splink(
    candidates: pl.DataFrame,
    *,
    units: pl.DataFrame,
    crs: pl.LazyFrame,
    stems: pl.DataFrame,
    provider_profile: pl.DataFrame,
    labels: pl.DataFrame,
    client: JevClient,
) -> SplinkResult:
    """Splink scores for the eligible candidate pairs of the providers in the
    `splink` and `pooled_splink` scoring groups.

    Args:
        candidates: the link stage's candidates.
        units: the link stage's units.
        crs: the CRS records the units sum.
        stems: the programme stem rules.
        provider_profile: each provider's `scoring_group`. A `splink` provider
            gets its own model and a `pooled_splink` provider the pooled model.
        labels: pair labels in the `pair_labels` schema; the `human` labels of task
            `trace` calibrate.
        client: the build's Jev client.

    Returns:
        One row per eligible pair that a usable model scores.
        `probability` is null for a model without calibration labels, and
        `review_reason` is `calibration` on the pairs people should label to
        calibrate it.
    """
    groups = provider_profile.filter(
        pl.col("scoring_group").is_in(["splink", "pooled_splink"])
    ).sort("provider_code")
    pairs = candidates.filter(
        pl.col("eligible"),
        pl.col("donor_code").is_in(groups["provider_code"].implode()),
    ).select("left_id", "right_id", "donor_code")
    unit_ids = pl.concat([pairs["left_id"], pairs["right_id"]]).unique()
    attributes = unit_attributes(
        units.filter(pl.col("unit_id").is_in(unit_ids.implode())), crs=crs, stems=stems
    )
    human = (
        labels.filter(task="trace", pair_label_source="human")
        .select("left_id", "right_id", y=pl.col("is_match").cast(pl.Float64))
        .unique(["left_id", "right_id"], keep="last", maintain_order=True)
    )
    own = groups.filter(scoring_group="splink")["provider_code"].to_list()
    pooled = groups.filter(scoring_group="pooled_splink")["provider_code"].to_list()
    models: list[_Model] = []
    con = duckdb.connect()
    try:
        for donor in own:
            model = _model(
                con,
                name=f"own_{donor}",
                donors=[donor],
                pairs=pairs,
                attributes=attributes,
                human=human,
                client=client,
            )
            models.append(model)
            if not model.report.usable:
                pooled.append(donor)
        if pooled:
            models.append(
                _model(
                    con,
                    name="pooled",
                    donors=sorted(pooled),
                    pairs=pairs,
                    attributes=attributes,
                    human=human,
                    client=client,
                    pooled=True,
                )
            )
    finally:
        con.close()
    return SplinkResult(
        scores=_apply_maps(models).sort("left_id", "right_id"),
        models=[model.report for model in models],
    )


def read_human_labels(path: Path) -> pl.DataFrame:
    """People's decisions at `path` as `pair_labels` rows with source `human`, the
    latest decision on each pair; none when no decision has been recorded yet."""
    columns = {
        name: column.dtype for name, column in schema.PAIR_LABELS.columns.items()
    }
    if not path.exists():
        return pl.DataFrame(schema=columns)
    return (
        pl.read_parquet(path)
        .drop_nulls("right_id")
        .sort("decided_at")
        .unique(["task", "left_id", "right_id"], keep="last")
        .sort("task", "left_id", "right_id")
        .select(
            "task",
            "left_id",
            "right_id",
            is_match=pl.col("decision") == "accept",
            pair_label_source=pl.lit("human"),
        )
        .cast(columns)  # ty: ignore[invalid-argument-type]
    )


def _settings(*, provider_u: Mapping[int, float] | None) -> SettingsCreator:
    """A model's settings. With `provider_u`, the first comparison is a provider
    indicator whose u values are fixed at it (the pooled model); without, a donor
    comparison that every pair passes."""
    if provider_u:
        first = cl.CustomComparison(
            output_column_name="provider",
            comparison_levels=[
                *(
                    cll.CustomLevel(
                        f"provider_l = {code} and provider_r = {code}",
                        label_for_charts=f"provider {code}",
                    ).configure(u_probability=max(u, 1e-12), fix_u_probability=True)
                    for code, u in provider_u.items()
                ),
                cll.ElseLevel().configure(
                    u_probability=1 - sum(provider_u.values()), fix_u_probability=True
                ),
            ],
        )
    else:
        first = cl.CustomComparison(
            output_column_name="donor",
            comparison_levels=[
                cll.ExactMatchLevel("donor_code").configure(
                    m_probability=0.999999, fix_m_probability=True
                ),
                cll.ElseLevel().configure(m_probability=1e-6, fix_m_probability=True),
            ],
        )
    return SettingsCreator(
        link_type="link_only",
        unique_id_column_name="unique_id",
        probability_two_random_records_match=1e-6,
        comparisons=[first, *comparisons()],
        retain_matching_columns=False,
        retain_intermediate_calculation_columns=False,
    )


def _model(
    con: duckdb.DuckDBPyConnection,
    *,
    name: str,
    donors: list[int],
    pairs: pl.DataFrame,
    attributes: pl.DataFrame,
    human: pl.DataFrame,
    client: JevClient,
    pooled: bool = False,
) -> _Model:
    """Fit the model for `donors`, score their pairs and calibrate it."""
    own_pairs = pairs.filter(pl.col("donor_code").is_in(donors))
    report = ModelReport(name=name, donors=donors, pairs=own_pairs.height)
    inputs = _register_inputs(con, pairs=own_pairs, attributes=attributes)
    report.commitments, report.later_records = (
        _count(con, f"select count(*) from {table}") for table in (LEFT, RIGHT)
    )
    # A pooled model of one provider needs no provider indicator.
    provider_u = _provider_u(con, donors) if pooled and len(donors) > 1 else None
    started = time.perf_counter()
    model = _fit(con, inputs, report=report, provider_u=provider_u)
    if model is None:
        _log.info("Splink model %s is not usable: %s", name, report)
        return _Model(report=report)
    fitted = time.perf_counter()
    scores = _predict(con, inputs, model=model, pairs=own_pairs).join(
        comparison_levels(
            own_pairs.select("left_id", "right_id"), attributes=attributes
        ).select("left_id", "right_id", levels=levels_struct()),
        on=["left_id", "right_id"],
        maintain_order="left",
    )
    _log.info(
        "Splink model %s: %d pairs, fitted in %.0f s, scored in %.0f s",
        name,
        own_pairs.height,
        fitted - started,
        time.perf_counter() - fitted,
    )
    labelled = scores.join(human, on=["left_id", "right_id"])
    report.human_labels = labelled.height
    if labelled.height < CALIBRATION_LABELS:
        sample = stratified_sample(
            scores.join(labelled, on=["left_id", "right_id"], how="anti"),
            n=CALIBRATION_LABELS,
        ).select("left_id", "right_id", review_reason=pl.lit(CALIBRATION_REASON))
        scores = scores.join(sample, on=["left_id", "right_id"], how="left")
        return _Model(report=report, scores=scores)

    report.calibrated = True
    labels, label_set = _labels(
        scores, human=labelled, attributes=attributes, client=client, report=report
    )
    weighted = _band_weights(scores, labels)
    rescored = _predict(
        con, inputs, model=with_labelled_m(model, weighted), pairs=own_pairs
    ).with_columns(p_raw=scores["p_raw"], levels=scores["levels"])
    return _Model(
        report=report,
        scores=rescored,
        labelled=weighted.join(
            rescored.select("left_id", "right_id", "match_weight"),
            on=["left_id", "right_id"],
        ),
        label_set=label_set,
    )


def _count(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    row = con.sql(sql).fetchone()
    assert row is not None
    return int(row[0])


def _register_inputs(
    con: duckdb.DuckDBPyConnection, *, pairs: pl.DataFrame, attributes: pl.DataFrame
) -> list[Any]:
    """The model's two Splink inputs: the commitments and the later records its
    pairs name, each with the frequencies the blocking rules cap."""
    sides = {
        LEFT: attributes.join(
            pairs.select(unique_id="right_id").unique(), on="unique_id", how="semi"
        ),
        RIGHT: attributes.join(
            pairs.select(unique_id="left_id").unique(), on="unique_id", how="semi"
        ),
    }
    frequencies = [
        sides[RIGHT]
        .drop_nulls(column)
        .group_by("donor_code", column)
        .agg(pl.len().alias(f"{short}_freq"))
        for column, short in (("pn_norm", "pn"), ("title_norm", "title"))
    ]
    db_api = DuckDBAPI(connection=con)
    inputs = []
    for table, frame in sides.items():
        for counts in frequencies:
            frame = frame.join(counts, on=counts.columns[:2], how="left")
        frame = frame.with_columns(pl.col("pn_freq", "title_freq").fill_null(0))
        con.register("_frame", frame)
        con.sql(f"create or replace table {table} as select * from _frame")
        con.unregister("_frame")
        inputs.append(db_api.register(table, dataset_display_name=table))
    return inputs


def _provider_u(con: duckdb.DuckDBPyConnection, donors: list[int]) -> dict[int, float]:
    """Each provider's u in the pooled model: the chance that a random pair of a
    commitment and a later record is that provider's on both sides."""
    shares = [
        dict(
            con.sql(
                f"select provider, count(*) / sum(count(*)) over () from {table} "
                "group by provider"
            ).fetchall()
        )
        for table in (LEFT, RIGHT)
    ]
    return {
        code: shares[0].get(code, 0.0) * shares[1].get(code, 0.0) for code in donors
    }


def _fit(
    con: duckdb.DuckDBPyConnection,
    inputs: list[Any],
    *,
    report: ModelReport,
    provider_u: Mapping[int, float] | None,
) -> dict[str, Any] | None:
    """The fitted model as Splink's settings dict, or None when it is not usable:
    the prior rules find no match, or as many as every pair, or fewer than two EM
    sessions complete."""
    linker = _linker(inputs, _settings(provider_u=provider_u))
    try:
        linker.training.estimate_probability_two_random_records_match(
            list(PRIOR_RULES), recall=PRIOR_RULE_RECALL
        )
    except ValueError as error:
        # More rule matches than the recall allows: nearly every pair matches.
        report.failed["prior"] = str(error).split(".")[0]
        return None
    report.prior = linker.misc.save_model_to_json()[
        "probability_two_random_records_match"
    ]
    if not 0 < report.prior < 1:
        return None
    # One thread: u and EM sum floats, and only a fixed order keeps two builds
    # equal. Scoring sums nothing across pairs and runs on every thread.
    con.sql("set threads = 1")
    linker.training.estimate_u_using_random_sampling(
        max_pairs=min(report.commitments * report.later_records, U_MAX_PAIRS),
        seed=SEED,
        min_count_per_level=U_MIN_COUNT_PER_LEVEL,
        num_chunks=U_NUM_CHUNKS,
    )
    model = linker.misc.save_model_to_json()
    for rule in EM_RULES:
        blocked = _count(con, f"select count(*) from {LEFT} l join {RIGHT} r on {rule}")
        report.sessions[rule] = blocked
        if blocked < MIN_EM_PAIRS:
            continue
        try:
            linker.training.estimate_parameters_using_expectation_maximisation(
                rule,
                max_pairs=EM_MAX_PAIRS,
                record_sample_proportion=EM_RECORD_SAMPLE_PROPORTION,
            )
        except ValueError as error:
            # Splink refuses an m that EM drives to zero after it has overwritten
            # part of the model, so the session is dropped and the model rolled back.
            report.failed[rule] = str(error).split(".")[0]
            linker = _linker(inputs, deepcopy(model))
            continue
        model = linker.misc.save_model_to_json()
    completed = sum(
        blocked >= MIN_EM_PAIRS and rule not in report.failed
        for rule, blocked in report.sessions.items()
    )
    con.sql("reset threads")
    report.usable = completed >= 2
    return model if report.usable else None


def _linker(inputs: list[Any], settings: SettingsCreator | dict[str, Any]) -> Linker:
    return Linker(inputs, settings, log_level=logging.ERROR)


def _predict(
    con: duckdb.DuckDBPyConnection,
    inputs: list[Any],
    *,
    model: dict[str, Any],
    pairs: pl.DataFrame,
) -> pl.DataFrame:
    """Score exactly `pairs` with `model`: each pair's match weight and
    probability, sorted by pair."""
    con.register(
        "_pairs",
        pairs.select(
            match_key=pl.lit("0"),
            join_key_l=pl.lit(f"{LEFT}-__-") + pl.col("right_id"),
            join_key_r=pl.lit(f"{RIGHT}-__-") + pl.col("left_id"),
        ),
    )
    con.sql("create or replace table _blocked_pairs as select * from _pairs")
    con.unregister("_pairs")
    linker = _linker(inputs, deepcopy(model))
    db_api = DuckDBAPI(connection=con)
    linker.table_management.register_blocked_pairs_for_predict(
        db_api.register("_blocked_pairs")
    )
    predictions = linker.inference.predict()
    scored = con.sql(
        "select unique_id_r as left_id, unique_id_l as right_id, match_weight, "
        f"match_probability as p_raw from {predictions.physical_name}"
    ).pl()
    predictions.drop_table_from_database_and_remove_from_cache()
    return pairs.join(scored, on=["left_id", "right_id"], validate="1:1").sort(
        "left_id", "right_id"
    )


def _comparisons(model: Mapping[str, Any]) -> list[tuple[str, Comparison]]:
    """The ten comparisons of `model` as Splink reads them, so each level carries
    the comparison vector value and the m that Splink scores with, its default
    where EM left the level untrained."""
    return [
        (
            spec["output_column_name"],
            Comparison(
                comparison_levels=spec["comparison_levels"],
                sqlglot_dialect="duckdb",
                output_column_name=spec["output_column_name"],
            ),
        )
        for spec in model["comparisons"]
        if spec["output_column_name"] in LEVEL_NAMES
    ]


def stratified_sample(scores: pl.DataFrame, *, n: int) -> pl.DataFrame:
    """`n` pairs of `scores` spread evenly over the bands of `WEIGHT_BANDS`: the
    first pair of every band, then the second, and so on, so a band smaller than
    its share gives all its pairs and the others make up the rest.

    Within a band, pairs are taken in the order of a digest of their ids, so every
    build draws the same pairs from the same scores.
    """
    order = content_id(["left_id", "right_id"], order_by=["left_id", "right_id"])
    return (
        scores.with_columns(band=_band(), order=order)
        .with_columns(rank=pl.col("order").rank("ordinal").over("band"))
        .sort("rank", "band")
        .head(n)
        .drop("band", "order", "rank")
    )


def _band() -> pl.Expr:
    """The band of `WEIGHT_BANDS` a pair's match weight falls in, from 0."""
    return pl.sum_horizontal(pl.col("match_weight") >= edge for edge in WEIGHT_BANDS)


def _labels(
    scores: pl.DataFrame,
    *,
    human: pl.DataFrame,
    attributes: pl.DataFrame,
    client: JevClient,
    report: ModelReport,
) -> tuple[pl.DataFrame, str]:
    """The labels that calibrate a model, `y` per pair, and what they hold.

    People's labels stand. With both outcomes among them, Jev answers the
    stratified sample and every labelled pair, and its answers on pairs people did
    not label become labels through Jev's scale.
    """
    outcomes = human["y"].sum()
    if min(outcomes, human.height - outcomes) < MIN_JEV_SCALE_OUTCOMES:
        return human.select("left_id", "right_id", "y"), f"human:{human.height}"
    asked = pl.concat(
        [
            stratified_sample(scores, n=JEV_SAMPLE).select("left_id", "right_id"),
            human.select("left_id", "right_id"),
        ]
    ).unique(maintain_order=True)
    answers = asked.with_columns(
        jev=ask_jev(asked, attributes=attributes, client=client)
    )
    scale = jev_scale(human.join(answers, on=["left_id", "right_id"]))
    jev = answers.join(human, on=["left_id", "right_id"], how="anti").select(
        "left_id", "right_id", y=scale(pl.col("jev"))
    )
    report.jev_labels = jev.height
    labels = pl.concat([human.select("left_id", "right_id", "y"), jev])
    return labels, f"human:{human.height},jev:{jev.height}"


def _band_weights(scores: pl.DataFrame, labels: pl.DataFrame) -> pl.DataFrame:
    """`labels` with their comparison levels and inverse-probability weights: the
    pairs of their EM-weight band over the labelled pairs of that band."""
    return (
        scores.with_columns(band=_band())
        .with_columns(band_pairs=pl.len().over("band"))
        .join(labels, on=["left_id", "right_id"])
        .with_columns(weight=pl.col("band_pairs") / pl.len().over("band"))
        .select("left_id", "right_id", "levels", "y", "weight")
        .sort("left_id", "right_id")
    )


def with_labelled_m(model: dict[str, Any], labelled: pl.DataFrame) -> dict[str, Any]:
    """`model` with each of the ten comparisons' m re-estimated from `labelled`
    and fixed.

    For each level k, m_k = (sum of weight x label over pairs at k + a x m_EM_k) /
    (sum of weight x label over pairs at a non-null level + a), with a =
    `M_PRIOR_WEIGHT`: Splink's estimate from a label column, with sample weights
    and EM's m as a prior. u, the prior and the first comparison keep EM's values.
    """
    out = deepcopy(model)
    mass = labelled["weight"].to_numpy() * labelled["y"].to_numpy()
    specs = {spec["output_column_name"]: spec for spec in out["comparisons"]}
    for name, comparison in _comparisons(model):
        gamma = labelled["levels"].struct.field(f"gamma_{name}").to_numpy()
        levels = [
            (spec, level)
            for spec, level in zip(
                specs[name]["comparison_levels"],
                comparison.comparison_levels,
                strict=True,
            )
            if not level.is_null_level
        ]
        em = np.array([level.m_probability for _, level in levels], dtype=float)
        counts = np.array(
            [mass[gamma == level.comparison_vector_value].sum() for _, level in levels]
        )
        m = (counts + M_PRIOR_WEIGHT * em / em.sum()) / (counts.sum() + M_PRIOR_WEIGHT)
        for (spec, _), probability in zip(levels, m, strict=True):
            spec["m_probability"] = float(probability)
            spec["fix_m_probability"] = True
    return out


def jev_scale(labelled: pl.DataFrame) -> Callable[[pl.Expr], pl.Expr]:
    """The chance a pair matches given Jev's p(same activity) `jev`, fitted on
    people's labels `y`: a logistic on the logit of Jev's answer."""
    fit = LogisticRegression(C=1e6).fit(
        _logit(labelled["jev"].to_numpy())[:, None], labelled["y"].to_numpy()
    )
    slope, intercept = float(fit.coef_[0][0]), float(fit.intercept_[0])

    def scale(jev: pl.Expr) -> pl.Expr:
        clipped = jev.clip(1e-3, 1 - 1e-3)
        z = slope * (clipped / (1 - clipped)).log() + intercept
        return 1 / (1 + (-z).exp())

    return scale


def _logit(p: np.ndarray) -> np.ndarray:
    clipped = np.clip(p, 1e-3, 1 - 1e-3)
    return np.log(clipped / (1 - clipped))


def isotonic_map(labelled: pl.DataFrame) -> IsotonicRegression:
    """A non-decreasing map from match weight to the labels' match rate, each label
    weighted by its inverse inclusion probability."""
    ordered = labelled.sort("match_weight", "y", "left_id", "right_id")
    return IsotonicRegression(out_of_bounds="clip", y_min=0, y_max=1).fit(
        ordered["match_weight"].to_numpy(),
        ordered["y"].to_numpy(),
        sample_weight=ordered["weight"].to_numpy(),
    )


def _apply_maps(models: list[_Model]) -> pl.DataFrame:
    """Every scored pair with its calibrated probability, null where its model has
    no calibration labels."""
    labelled = [model.labelled for model in models if model.labelled is not None]
    pooled = isotonic_map(pl.concat(labelled)) if labelled else None
    frames = [pl.DataFrame(schema=SPLINK_SCORES)]
    for model in models:
        if model.scores is None:
            continue
        probability = pl.lit(None, dtype=pl.Float64)
        if model.labelled is not None and pooled is not None:
            outcomes = model.labelled["y"].sum()
            model.report.own_map = (
                min(outcomes, model.labelled.height - outcomes) >= MIN_ISOTONIC_OUTCOMES
            )
            mapping = isotonic_map(model.labelled) if model.report.own_map else pooled
            probability = pl.Series(
                mapping.predict(model.scores["match_weight"].to_numpy()),
                dtype=pl.Float64,
            )
        frames.append(
            model.scores.with_columns(
                model=pl.lit(f"splink-{splink.__version__}:{model.report.name}"),
                probability=probability,
                label_set=pl.lit(model.label_set, dtype=pl.String),
                review_reason=pl.col("review_reason")
                if "review_reason" in model.scores.columns
                else pl.lit(None, dtype=pl.String),
            ).select(list(SPLINK_SCORES))
        )
    return pl.concat(frames)


def pair_request(
    earlier: Mapping[str, Any], later: Mapping[str, Any]
) -> dict[str, object]:
    """Jev's request about a commitment unit and a later unit, wording v1.

    Each record gives its unit's fields, texts cut to `JEV_TEXT_CHARS`, and
    `derived_facts` states the comparisons of dates and amounts.
    """
    first, second = _jev_record(earlier), _jev_record(later)
    commitment = first["commitment_usd"]
    share = (
        None
        if commitment <= 0
        else float(f"{second['disbursement_usd'] / commitment:.3g}")
    )
    facts = {
        "years_between_records": second["year"] - first["year"],
        "same_provider": first["provider"] == second["provider"],
        "same_project_number": bool(
            first["project_number"]
            and first["project_number"] == second["project_number"]
        ),
        "same_title": first["title"].lower() == second["title"].lower(),
        "same_recipient": first["recipient"] == second["recipient"],
        "same_purpose_code": first["purpose_code"] == second["purpose_code"],
        "later_disbursement_as_share_of_earlier_commitment": share,
        "later_record_also_has_its_own_commitment": second["commitment_usd"] > 0,
    }
    return {
        "state": {
            "earlier_record": first,
            "later_record": second,
            "derived_facts": facts,
        },
        "questions": PAIR_QUESTIONS,
    }


def _jev_record(unit: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "year": unit["year"],
        "provider": unit["donor_code"],
        "recipient": unit["recipient_code"],
        "purpose_code": unit["purpose_code"],
        "channel_code": unit["channel_code"],
        "flow_code": unit["flow_code"],
        "finance_type_code": unit["finance_type_code"],
        "project_number": _jev_text(unit["project_number"]) or None,
        "title": _jev_text(unit["title"]),
        "short_description": _jev_text(unit["short_description"]),
        "long_description": _jev_text(unit["long_description"]),
        "commitment_usd": round(unit["commitment_usd"] or 0),
        "disbursement_usd": round(unit["disbursement_usd"] or 0),
    }


def _jev_text(text: str | None) -> str:
    cleaned = " ".join((text or "").split())
    return cleaned[:JEV_TEXT_CHARS] + ("..." if len(cleaned) > JEV_TEXT_CHARS else "")


def ask_jev(
    pairs: pl.DataFrame, *, attributes: pl.DataFrame, client: JevClient
) -> pl.Series:
    """Jev's p(same activity) for each (`left_id`, `right_id`) pair, in order: the
    commitment (right) is the earlier record."""
    by_id = {row["unique_id"]: row for row in attributes.iter_rows(named=True)}
    responses = client.score(
        [
            pair_request(by_id[right], by_id[left])
            for left, right in pairs.select("left_id", "right_id").iter_rows()
        ]
    )
    return pl.Series(
        [_same_activity(response) for response in responses], dtype=pl.Float64
    )


def _same_activity(response: Mapping[str, Any]) -> float:
    return float(response["answers"]["relation"]["probabilities"]["2"])

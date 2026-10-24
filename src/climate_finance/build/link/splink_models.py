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

Raw EM probabilities do not link. On the prototype's id truth they reached precision
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
Without a Jev client, which the link stage passes only with `jev_links` on, people's
labels alone set m and the map, and Jev is never asked.

With `jev_links` on, a model with fewer than `CALIBRATION_LABELS` people's labels
has its calibration sample answered instead (`jev.jev_answers`): Jev reads each pair,
and a scale fitted on id truth turns its answer into a label. Those labels, with any
of people's, set m and the map directly, since there are not enough people's labels
to set Jev's scale for the model. Only labels of source `human` or `agent` are
read from `labels`. Ledger-check labels do not calibrate, since they sit on key-linked
pairs, whose disbursements no model scores, and they hold no negatives.

`splink_links` links each disbursement unit to its best calibrated pair at or above
`LINK_THRESHOLD`, as `splink_jev`.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Mapping
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
from splink import DuckDBAPI, Linker, SettingsCreator

# Splink's own reading of a comparison gives each level's number and the m it
# scores with. Splink is pinned below 6.
from splink.internals.comparison import Comparison
from splink.internals.settings import Settings

from climate_finance import schema
from climate_finance.build.link.jev import (
    CalibrationAnswers,
    ask_jev,
    fit_logistic,
    spread_sample,
)
from climate_finance.build.link.levels import (
    LEVELS,
    comparison_levels,
    comparisons,
    levels_struct,
    unit_attributes,
)
from climate_finance.build.link.score import linked_pairs

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
# only the raw EM probability, since the isotonic map reads the match weight and a
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
# A calibrated pair links at this probability or more. The rest go to the review
# queue. With each truth-group provider's ids hidden in turn on the archive, the two
# models that reach 0.7 linked at precision 0.954 (France, 1,789 links) and 0.698
# (Denmark, 620) against id truth; at 0.5 they fell to 0.739 and 0.391, and the
# pooled model linked at 0.442. Denmark's cut sits on one step of an isotonic map
# fitted on 100 Jev labels. A draw that put the step at 0.69 gave 243 links at 0.905.
LINK_THRESHOLD = 0.7
LINK_METHOD = "splink_jev"
_PAIR = ["left_id", "right_id"]
# The ten comparisons labels re-estimate; `donor` and `provider` keep EM's m.
LEVEL_NAMES = tuple(name.removeprefix("gamma_") for name in LEVELS)

# Splink 5 collects the term-frequency columns into a set, so the order of their
# joins, and with it the order of EM's float sums, follows the process's hash seed.
# Every Splink `Settings` returns them sorted by name instead. Splink builds and
# copies its `Settings` itself, so the fix sits on the class, and the lookup raises
# at import if Splink renames the property. ty reads the class attribute as the
# property's value, so it refuses the assignment.
_term_frequency_columns: property = Settings.__dict__["_term_frequency_columns"]
Settings._term_frequency_columns = property(  # ty: ignore[invalid-assignment]
    lambda settings: sorted(
        _term_frequency_columns.__get__(settings), key=lambda column: column.name
    )
)

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
    client: JevClient | None,
    answer: CalibrationAnswers | None = None,
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
        labels: pair labels in the `pair_labels` schema with `stratified`, as
            `read_decision_labels` gives them; the `human` and `agent` labels of
            task `trace` calibrate.
        client: the build's Jev client, which answers the stratified sample of a
            calibrated model; None leaves calibration to people's labels alone.
        answer: what answers the calibration sample of a model with fewer than
            `CALIBRATION_LABELS` people's labels (`jev.jev_answers`); None leaves
            the sample to people.

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
        labels.filter(
            pl.col("pair_label_source").is_in(["human", "agent"]), task="trace"
        )
        .select(
            "left_id", "right_id", "stratified", y=pl.col("is_match").cast(pl.Float64)
        )
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
                answer=answer,
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
                    answer=answer,
                    pooled=True,
                )
            )
    finally:
        con.close()
    return SplinkResult(
        scores=_apply_maps(models).sort("left_id", "right_id"),
        models=[model.report for model in models],
    )


def read_decision_labels(path: Path) -> pl.DataFrame:
    """People's and agents' queue decisions at `path` as `pair_labels` rows with
    source `human` or `agent`, one per pair, and none when nothing has been decided yet.

    A person's decision on a pair stands over any agent's, whenever either was
    imported, and the latest decision stands among people. Of an agent's decisions,
    the latest round stands (`LABEL_ROUNDS` order), then the latest imported.
    `decided_at` is the import time, so ordering people and agents by it would let
    a re-imported agent round erase a person's verdict.
    A blind-sample decision (one with a `sample_id`) measures precision, so it is not
    used as a label. `stratified` marks the decisions on calibration pairs, which
    `stratified_sample` drew, so only they take inverse-probability weights.
    """
    columns = {
        name: column.dtype for name, column in schema.PAIR_LABELS.columns.items()
    } | {"stratified": pl.Boolean()}
    if not path.exists():
        return pl.DataFrame(schema=columns)
    pair = ["task", "left_id", "right_id"]
    rounds = {name: rank for rank, name in enumerate(schema.LABEL_ROUNDS)}
    decided = (
        pl.read_parquet(path)
        .filter(pl.col("sample_id").is_null())
        .drop_nulls("right_id")
        .with_columns(
            rank=pl.col("round").replace_strict(rounds, default=0, return_dtype=pl.Int8)
        )
    )
    agent = (
        decided.filter(label_source="agent")
        .sort("rank", "decided_at", maintain_order=True)
        .unique(pair, keep="last", maintain_order=True)
    )
    human = (
        decided.filter(label_source="human")
        .sort("decided_at", maintain_order=True)
        .unique(pair, keep="last", maintain_order=True)
    )
    return (
        pl.concat([agent, human])
        .unique(pair, keep="last", maintain_order=True)
        .sort(pair)
        .select(
            *pair,
            is_match=pl.col("decision") == "accept",
            pair_label_source="label_source",
            stratified=pl.col("review_reason") == CALIBRATION_REASON,
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
    client: JevClient | None,
    answer: CalibrationAnswers | None,
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
    labelled = scores.join(human, on=_PAIR)
    answered = pl.DataFrame(
        schema={
            "left_id": pl.String,
            "right_id": pl.String,
            "y": pl.Float64,
            "stratified": pl.Boolean,
        }
    )
    if labelled.height < CALIBRATION_LABELS:
        sample = stratified_sample(
            scores.join(labelled, on=_PAIR, how="anti"), n=CALIBRATION_LABELS
        ).select(*_PAIR)
        if answer is None:
            report.human_labels = labelled.height
            queued = sample.with_columns(review_reason=pl.lit(CALIBRATION_REASON))
            scores = scores.join(queued, on=_PAIR, how="left")
            return _Model(report=report, scores=scores)
        # People's and agents' answers join the labels; Jev's stay apart.
        # `stratified_sample` drew both.
        given = answer(sample.sort(_PAIR), attributes).with_columns(
            stratified=pl.lit(True)
        )
        by_people = given.filter(
            pl.col("pair_label_source").is_in(["human", "agent"])
        ).select(*_PAIR, "y", "stratified")
        labelled = pl.concat(
            [labelled.select(*_PAIR, "y", "stratified"), by_people],
            how="vertical_relaxed",
        ).sort(_PAIR)
        answered = given.filter(pair_label_source="jev").select(
            *_PAIR, "y", "stratified"
        )
    report.human_labels = labelled.height
    report.calibrated = True
    labels, label_set = _labels(
        scores,
        human=labelled,
        answered=answered,
        attributes=attributes,
        client=client,
        report=report,
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
        # The rules match more pairs than the recall allows, so nearly every pair
        # matches.
        report.failed["prior"] = str(error).split(".")[0]
        return None
    report.prior = linker.misc.save_model_to_json()[
        "probability_two_random_records_match"
    ]
    if not 0 < report.prior < 1:
        return None
    # One thread, since u and EM sum floats and only a fixed order keeps two builds
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
    build draws the same pairs from the same scores (`jev.spread_sample`).
    """
    return spread_sample(scores, band=_band(), n=n)


def _band() -> pl.Expr:
    """The band of `WEIGHT_BANDS` a pair's match weight falls in, from 0."""
    return pl.sum_horizontal(pl.col("match_weight") >= edge for edge in WEIGHT_BANDS)


def _labels(
    scores: pl.DataFrame,
    *,
    human: pl.DataFrame,
    answered: pl.DataFrame,
    attributes: pl.DataFrame,
    client: JevClient | None,
    report: ModelReport,
) -> tuple[pl.DataFrame, str]:
    """The labels that calibrate a model, `y` per pair, and what they hold.

    People's labels stand. Where Jev `answered` the calibration sample, its labels
    join them as they are. Otherwise, with a `client` and both outcomes among
    people's labels, Jev answers the stratified sample and every labelled pair, and
    its answers on pairs people did not label become labels through Jev's scale.
    `stratified` marks the labels `stratified_sample` drew: people's on calibration
    pairs and all of Jev's.
    """
    columns = ["left_id", "right_id", "y", "stratified"]
    if answered.height:
        report.jev_labels = answered.height
        labels = pl.concat([human.select(columns), answered])
        return labels, f"human:{human.height},jev:{answered.height}"
    outcomes = human["y"].sum()
    if (
        client is None
        or min(outcomes, human.height - outcomes) < MIN_JEV_SCALE_OUTCOMES
    ):
        return human.select(columns), f"human:{human.height}"
    asked = pl.concat(
        [
            stratified_sample(scores, n=JEV_SAMPLE).select("left_id", "right_id"),
            human.select("left_id", "right_id"),
        ]
    ).unique(maintain_order=True)
    answers = asked.with_columns(
        jev=ask_jev(asked, attributes=attributes, client=client)
    )
    scale = fit_logistic(
        human.join(answers, on=["left_id", "right_id"]), inputs=["jev"], y="y"
    )
    jev = answers.join(human, on=["left_id", "right_id"], how="anti").select(
        "left_id", "right_id", y=scale(pl.col("jev")), stratified=pl.lit(True)
    )
    report.jev_labels = jev.height
    labels = pl.concat([human.select(columns), jev])
    return labels, f"human:{human.height},jev:{jev.height}"


def _band_weights(scores: pl.DataFrame, labels: pl.DataFrame) -> pl.DataFrame:
    """`labels` with their comparison levels and weights. A `stratified` label's
    inverse-probability weight is the pairs of its EM-weight band over the
    stratified labels of that band. Any other label, which review chose and
    `stratified_sample` did not draw, weighs 1."""
    stratified = pl.col("stratified")
    return (
        scores.with_columns(band=_band())
        .with_columns(band_pairs=pl.len().over("band"))
        .join(labels, on=["left_id", "right_id"])
        .with_columns(
            weight=pl.when(stratified)
            .then(pl.col("band_pairs") / stratified.sum().over("band"))
            .otherwise(1.0)
        )
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


def splink_links(
    scores: pl.DataFrame,
    *,
    candidates: pl.DataFrame,
    units: pl.DataFrame,
    jev_model: str,
    rejected: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """The calibrated Splink models' `trace` links, in the `links` schema.

    Each disbursement unit joins the commitment of its highest-probability pair at
    or above `LINK_THRESHOLD` that is not a sibling and not in `rejected`, ties to
    the earlier placed year, then the smaller unit id (`score.linked_pairs`). A
    commitment's links form one group.

    Args:
        scores: `pair_scores` (`score_with_splink`).
        candidates: the link stage's candidates, for each pair's programme level.
        units: the link stage's units, for each commitment's placed year.
        jev_model: the Jev model behind the calibration labels, recorded on the
            links of models whose labels Jev gave some of.
        rejected: the trace pairs (`left_id`, `right_id`) review decisions
            rejected.
    """
    taken = (
        scores.drop_nulls("probability")
        .join(candidates.select(*_PAIR, "programme_level"), on=_PAIR)
        .join(units.select("placed_year", right_id="unit_id"), on="right_id")
    )
    best = linked_pairs(
        taken.with_columns(p_model="probability"),
        threshold=LINK_THRESHOLD,
        rejected=rejected,
    )
    given = {
        "task": pl.lit("trace"),
        "left_id": pl.col("left_id"),
        "right_id": pl.col("right_id"),
        "group_id": pl.concat_str(pl.lit(LINK_METHOD), "right_id", separator="|"),
        "probability": pl.col("probability"),
        "link_method": pl.lit(LINK_METHOD),
        "p_model": pl.col("probability"),
        "programme_level": pl.col("programme_level"),
        "levels": pl.col("levels"),
        "model_version": pl.col("model"),
        "jev_model": pl.when(pl.col("label_set").str.contains(r"jev:[1-9]")).then(
            pl.lit(jev_model)
        ),
        "label_set": pl.col("label_set"),
        "review": pl.lit(value=False),
    }
    return best.select(
        given.get(name, pl.lit(None)).cast(column.dtype).alias(name)
        for name, column in schema.LINKS.columns.items()
    ).sort(*_PAIR)

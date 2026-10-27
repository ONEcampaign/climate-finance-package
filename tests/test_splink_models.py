"""Per-provider Splink models on a hand-built donor whose continuations are known.

Jev answers through a mock transport: p(same activity) is high when the two records
share a project number.
"""

import json
import os
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import httpx
import numpy as np
import polars as pl
import pytest
import splink
from stage_runs import JEV_MODEL

from climate_finance import schema
from climate_finance.build.jev_client import JevClient
from climate_finance.build.link import splink_models
from climate_finance.build.link.decisions import (
    CALIBRATION_REASON,
    read_decision_labels,
)
from climate_finance.build.link.jev import CalibrationAnswers, Logistic, jev_answers
from climate_finance.build.link.score import ELIGIBLE, scoring_groups
from climate_finance.build.link.splink_models import (
    CALIBRATION_LABELS,
    JEV_SAMPLE,
    LINK_THRESHOLD,
    SplinkResult,
    isotonic_map,
    score_with_splink,
    splink_links,
    stratified_sample,
    with_labelled_m,
)

DONOR = 901
OTHER_DONOR = 902
_WORDS = (
    "water", "roads", "health", "schools", "energy", "solar", "irrigation", "forest",
    "coastal", "urban", "rural", "grid", "housing", "sanitation", "transport", "ports",
    "fisheries", "drought", "flood", "climate", "nutrition", "markets", "finance",
    "power", "wind",
)  # fmt: skip
# Each donor's (code, seed, projects, noise) in `world`.
WORLD = ((DONOR, 7, 160, 260), (OTHER_DONOR, 11, 90, 150))
_UNIT_SCHEMA = schema.UNITS.dtypes
_LABEL_SCHEMA = schema.PAIR_LABELS.dtypes | {"stratified": pl.Boolean()}
_STEMS = pl.DataFrame(schema=schema.PROGRAMME_STEMS.dtypes)


def _donor(donor: int, *, seed: int, projects: int, noise: int) -> list[dict]:
    """Units of one donor: `projects` commitments, each continued by one to three
    later disbursements under its own number and title, and `noise` disbursements
    of other projects in the same recipients and purposes."""
    rng = np.random.default_rng(seed)
    units = []

    def unit(kind: str, i: int, j: int, **fields: object) -> None:
        units.append(
            {
                "unit_id": f"{donor}-{kind}{i:04d}-{j}",
                "donor_code": donor,
                "agency_code": 1,
                "crs_id": None,
                "recipient_code": int(fields.pop("recipient")),
                "purpose_code": int(fields.pop("purpose")),
                "commitment_year": None,
                "commitment_date": None,
                "floor_year": None,
                **fields,
            }
        )

    for i in range(projects):
        words = rng.choice(len(_WORDS), size=3, replace=False)
        title = " ".join(_WORDS[w] for w in words)
        common = {
            "project_number": f"P{donor}{i:04d}",
            "title": f"{title} programme",
            "recipient": rng.integers(1, 6),
            "purpose": 23210 + 10 * int(rng.integers(0, 3)),
        }
        year = int(rng.integers(2015, 2018))
        face = float(rng.integers(1, 50)) * 1e6
        unit("c", i, 0, year=year, commitment_usd=face, disbursement_usd=0.0, **common)
        for j in range(1, int(rng.integers(2, 5))):
            unit(
                "c",
                i,
                j,
                year=year + j,
                commitment_usd=0.0,
                disbursement_usd=face * float(rng.uniform(0.1, 0.6)),
                **common,
            )
    for i in range(noise):
        words = rng.choice(len(_WORDS), size=3, replace=False)
        unit(
            "n",
            i,
            0,
            project_number=f"N{donor}{i:04d}",
            title=" ".join(_WORDS[w] for w in words) + " project",
            recipient=rng.integers(1, 6),
            purpose=23210 + 10 * int(rng.integers(0, 3)),
            year=int(rng.integers(2016, 2021)),
            commitment_usd=float(rng.integers(1, 30)) * 1e6,
            disbursement_usd=float(rng.integers(1, 30)) * 1e5,
        )
    return units


def _world(*donors: tuple[int, int, int, int]) -> dict[str, pl.DataFrame]:
    """Units, CRS records and candidates for `donors`, each (code, seed, projects,
    noise). Candidates pair each commitment with every later disbursement of its
    donor in the same recipient and purpose; `y` marks true continuations."""
    rows = [
        row
        for code, seed, projects, noise in donors
        for row in _donor(code, seed=seed, projects=projects, noise=noise)
    ]
    frame = pl.DataFrame(rows).with_columns(
        record_ids=pl.concat_list(pl.col("unit_id")),
        purpose_codes=pl.concat_list(pl.col("purpose_code")),
        placed_year=pl.col("year"),
        crs_id=pl.lit(None, dtype=pl.String),
    )
    units = frame.select(
        pl.col(name).cast(dtype) for name, dtype in _UNIT_SCHEMA.items()
    )
    crs = frame.select(
        record_id="unit_id",
        project_number="project_number",
        title="title",
        short_description=pl.lit(None, dtype=pl.String),
        long_description=pl.lit(None, dtype=pl.String),
        channel_code=pl.lit(None, dtype=pl.Int32),
        flow_code=pl.lit(11, dtype=pl.Int32),
        finance_t=pl.lit(110, dtype=pl.Int32),
        expected_start_date=pl.lit(None, dtype=pl.Date),
        completion_date=pl.lit(None, dtype=pl.Date),
        commitment_usd="commitment_usd",
        disbursement_usd="disbursement_usd",
    )
    commitments = units.filter(pl.col("commitment_usd") > 0)
    later = units.filter(pl.col("disbursement_usd") > 0)
    candidates = (
        later.join(
            commitments,
            on=["donor_code", "recipient_code", "purpose_code"],
            suffix="_c",
        )
        .filter(pl.col("year") > pl.col("year_c"))
        .select(
            left_id="unit_id",
            right_id="unit_id_c",
            donor_code="donor_code",
            eligible=pl.lit(value=True),
            id_persistent=pl.lit(value=False),
            same_crs_id=pl.lit(value=False),
            # `with_split` columns: every pair is open to scoring.
            split=pl.lit(ELIGIBLE),
            true_pair=pl.lit(value=False),
            y=pl.col("project_number") == pl.col("project_number_c"),
        )
        .sort("left_id", "right_id")
    )
    return {"units": units, "crs": crs, "candidates": candidates}


@pytest.fixture(scope="module")
def world() -> dict[str, pl.DataFrame]:
    """A donor large enough for its own model and one that joins the pooled model."""
    return _world(*WORLD)


def _responses(request: httpx.Request) -> httpx.Response:
    """Jev's answer: p(same activity) 0.95 when the project numbers agree, else 0.05."""
    body = json.loads(request.content)
    p = 0.95 if body["state"]["derived_facts"]["same_project_number"] else 0.05
    return httpx.Response(
        200,
        json={
            "model": body["model"],
            "answers": {
                "relation": {"probabilities": {"0": 1 - p, "1": 0.0, "2": p}},
                "disbursement": {"noul": p},
                "sibling": {"noul": 0.0},
            },
            "usage": {"input_tokens": 1000, "output_tokens": 10},
        },
    )


def _labels(pairs: pl.DataFrame) -> pl.DataFrame:
    """Human labels from the truth `y` of `pairs`, drawn by `stratified_sample`."""
    return pairs.select(
        task=pl.lit("trace"),
        left_id="left_id",
        right_id="right_id",
        is_match="y",
        pair_label_source=pl.lit("human"),
        stratified=pl.lit(True),
    ).cast(_LABEL_SCHEMA)  # ty: ignore[invalid-argument-type]


@dataclass
class Run:
    result: SplinkResult
    scores: pl.DataFrame
    client: JevClient
    # The labels each calibrated model weighed (`_band_weights`), concatenated.
    weighed: pl.DataFrame


def _run(
    world: dict[str, pl.DataFrame],
    cache_dir: Path,
    labels: pl.DataFrame | None = None,
    providers: tuple[int, ...] = (DONOR, OTHER_DONOR),
    answer: Callable[[JevClient], CalibrationAnswers] | None = None,
    *,
    jev: bool = True,
) -> Run:
    """Score both donors, with Jev answering through `_responses` and `answer`,
    given the client, answering calibration samples. Without `jev`, scoring gets
    no client. `scores` adds each pair's truth `y`."""
    weighed = []
    band_weights = splink_models._band_weights

    def record(scores: pl.DataFrame, labels: pl.DataFrame) -> pl.DataFrame:
        weighed.append(labels)
        return band_weights(scores, labels)

    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("TYPESAFE_API_KEY", "test")
        patch.setattr(splink_models, "_band_weights", record)
        client = JevClient(
            model=JEV_MODEL,
            cost_cap_usd=10.0,
            cache_dir=cache_dir,
            offline=False,
            transport=httpx.MockTransport(_responses),
        )
        result = score_with_splink(
            world["candidates"],
            units=world["units"],
            crs=world["crs"].lazy(),
            stems=_STEMS,
            provider_profile=scoring_groups(
                world["candidates"].filter(pl.col("donor_code").is_in(providers))
            ),
            labels=pl.DataFrame(schema=_LABEL_SCHEMA) if labels is None else labels,
            client=client if jev else None,
            answer=None if answer is None else answer(client),
        )
    scores = result.scores.join(
        world["candidates"].select("left_id", "right_id", "y"),
        on=["left_id", "right_id"],
    )
    return Run(
        result=result,
        scores=scores,
        client=client,
        weighed=pl.concat(weighed) if weighed else pl.DataFrame(),
    )


# Slow: each run fits two Splink models, about 5 s on an idle machine.
@pytest.fixture(scope="module")
def uncalibrated(
    world: dict[str, pl.DataFrame], tmp_path_factory: pytest.TempPathFactory
) -> Run:
    return _run(world, tmp_path_factory.mktemp("jev"))


@pytest.fixture(scope="module")
def calibration_labels(uncalibrated: Run) -> pl.DataFrame:
    """Labels on 200 of the large donor's pairs, spread over its bands, and on 10
    of the small donor's calibration sample."""
    scores = uncalibrated.scores
    large = stratified_sample(scores.filter(donor_code=DONOR), n=2 * CALIBRATION_LABELS)
    small = scores.filter(donor_code=OTHER_DONOR, review_reason=CALIBRATION_REASON)
    return pl.concat([large, small.head(10)])


@pytest.fixture(scope="module")
def calibrated(
    world: dict[str, pl.DataFrame],
    calibration_labels: pl.DataFrame,
    tmp_path_factory: pytest.TempPathFactory,
) -> Run:
    return _run(world, tmp_path_factory.mktemp("jev"), _labels(calibration_labels))


def test_a_model_without_calibration_labels_only_asks_for_them(
    uncalibrated: Run, world: dict[str, pl.DataFrame]
) -> None:
    scores = uncalibrated.scores
    assert scores.height == world["candidates"].height
    assert scores["probability"].null_count() == scores.height
    # Raw EM would link pairs, and none of them carries a probability.
    assert (scores["p_raw"] >= 0.5).sum() > 0
    queued = scores.filter(review_reason=CALIBRATION_REASON).group_by("model").len()
    assert set(queued["len"]) == {CALIBRATION_LABELS}
    assert set(scores["review_reason"].drop_nulls()) == {CALIBRATION_REASON}
    assert uncalibrated.client.live_calls == 0
    assert all(r.usable and not r.calibrated for r in uncalibrated.result.models)


def test_a_small_provider_shares_the_pooled_model(uncalibrated: Run) -> None:
    names = {report.name: report.donors for report in uncalibrated.result.models}
    assert names == {f"own_{DONOR}": [DONOR], "pooled": [OTHER_DONOR]}
    models = uncalibrated.scores.group_by("donor_code").agg(pl.col("model").first())
    assert dict(models.iter_rows()) == {
        DONOR: f"splink-{splink.__version__}:own_{DONOR}",
        OTHER_DONOR: f"splink-{splink.__version__}:pooled",
    }


def test_calibration_labels_give_monotone_probabilities(
    calibrated: Run, calibration_labels: pl.DataFrame
) -> None:
    reports = {report.name: report for report in calibrated.result.models}
    own = reports[f"own_{DONOR}"]
    assert own.calibrated
    assert own.human_labels == 2 * CALIBRATION_LABELS
    scores = calibrated.scores.filter(donor_code=DONOR)
    assert scores["probability"].null_count() == 0
    assert scores["review_reason"].null_count() == scores.height
    ordered = scores.sort("match_weight")["probability"].to_numpy()
    assert (np.diff(ordered) >= 0).all()
    # Jev answered the stratified sample and the labelled pairs, nothing else.
    assert own.jev_labels > 0
    assert calibrated.client.live_calls <= JEV_SAMPLE + calibration_labels.height
    linked = scores.filter(pl.col("probability") >= 0.9)
    assert linked.height
    assert linked["y"].mean() > 0.9


def test_without_a_client_people_s_labels_alone_calibrate(
    world: dict[str, pl.DataFrame],
    calibration_labels: pl.DataFrame,
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    # Slow: one more run, about 5 s. The labels that let Jev set its scale in
    # `calibrated` calibrate alone.
    run = _run(
        world, tmp_path_factory.mktemp("jev"), _labels(calibration_labels), jev=False
    )
    reports = {report.name: report for report in run.result.models}
    own = reports[f"own_{DONOR}"]
    assert own.calibrated
    assert own.jev_labels == 0
    assert run.client.live_calls == 0
    scores = run.scores.filter(donor_code=DONOR)
    assert scores["label_set"].unique().to_list() == [f"human:{2 * CALIBRATION_LABELS}"]
    assert scores["probability"].null_count() == 0


def test_a_provider_without_calibration_labels_stays_in_the_queue(
    calibrated: Run, calibration_labels: pl.DataFrame
) -> None:
    small = calibrated.scores.filter(donor_code=OTHER_DONOR)
    assert small["probability"].null_count() == small.height
    queued = small.filter(review_reason=CALIBRATION_REASON)
    # Its ten labelled pairs leave the calibration sample, and others take their place.
    assert queued.join(calibration_labels, on=["left_id", "right_id"]).is_empty()
    assert queued.height == CALIBRATION_LABELS


def test_jev_answers_the_calibration_samples_and_the_models_link(
    world: dict[str, pl.DataFrame], tmp_path_factory: pytest.TempPathFactory
) -> None:
    # Slow: one more run, about 5 s. Jev's answer is its label, unscaled.
    scale = Logistic(coefs=(1.0,), intercept=0.0)
    run = _run(
        world,
        tmp_path_factory.mktemp("jev"),
        answer=lambda client: jev_answers(client=client, scale=scale),
    )
    reports = run.result.models
    assert all(report.calibrated for report in reports)
    assert {report.jev_labels for report in reports} == {CALIBRATION_LABELS}
    assert {report.human_labels for report in reports} == {0}
    # Jev read each model's calibration sample and nothing else.
    assert run.client.live_calls == CALIBRATION_LABELS * len(reports)
    scores = run.scores
    assert scores["probability"].null_count() == 0
    assert scores["review_reason"].null_count() == scores.height
    assert set(scores["label_set"]) == {f"human:0,jev:{CALIBRATION_LABELS}"}
    # `stratified_sample` drew every label Jev gave, so each takes its band's
    # inverse-probability weight.
    assert run.weighed.height == CALIBRATION_LABELS * len(reports)
    assert run.weighed["stratified"].all()

    candidates = world["candidates"].with_columns(
        programme_level=pl.lit(0, dtype=pl.Int8)
    )
    links = splink_links(
        run.result.scores,
        candidates=candidates,
        units=world["units"],
        jev_model=JEV_MODEL,
    )
    schema.validate(schema.LINKS, links)
    assert set(links["jev_model"]) == {JEV_MODEL}
    # A model calibrated on people's labels alone links with no Jev model.
    people = splink_links(
        run.result.scores.with_columns(label_set=pl.lit(f"human:{CALIBRATION_LABELS}")),
        candidates=candidates,
        units=world["units"],
        jev_model=JEV_MODEL,
    )
    assert people.height == links.height
    assert people["jev_model"].null_count() == people.height
    assert links["left_id"].is_unique().all()
    assert (links["probability"] >= LINK_THRESHOLD).all()
    assert set(links["link_method"]) == {"splink_jev"}
    assert (links["group_id"] == "splink_jev|" + links["right_id"]).all()
    right = links.join(world["candidates"], on=["left_id", "right_id"])["y"]
    assert links.height > 100
    assert right.mean() > 0.9


def test_a_rejected_splink_pair_gives_way_to_the_next_best() -> None:
    scores = pl.DataFrame(
        {
            "left_id": ["d1", "d1", "d1"],
            "right_id": ["c1", "c2", "c3"],
            "probability": [0.99, 0.95, LINK_THRESHOLD - 0.01],
            "model": ["own_1"] * 3,
            "label_set": ["human:100"] * 3,
        }
    ).with_columns(levels=pl.lit(None, dtype=schema.LINKS.columns["levels"].dtype))
    candidates = scores.select("left_id", "right_id", programme_level=pl.lit(0))
    units = pl.DataFrame(
        {"unit_id": ["c1", "c2", "c3"], "placed_year": [2019, 2019, 2019]}
    )

    def linked(rejected: pl.DataFrame | None = None) -> list[str]:
        links = splink_links(
            scores,
            candidates=candidates,
            units=units,
            jev_model=JEV_MODEL,
            rejected=rejected,
        )
        return links["right_id"].to_list()

    assert linked() == ["c1"]
    c1 = pl.DataFrame({"left_id": ["d1"], "right_id": ["c1"]})
    assert linked(c1) == ["c2"]
    # Nothing else reaches the threshold.
    assert linked(pl.concat([c1, c1.with_columns(right_id=pl.lit("c2"))])) == []


def test_an_answer_for_people_calibrates_as_their_recorded_labels(
    uncalibrated: Run,
    world: dict[str, pl.DataFrame],
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    # Slow: two more runs, about 10 s. Id truth answers each calibration sample in
    # the build, as `simulate` in the Splink e2e test does, and gives what people's
    # decisions on the same pairs give.
    def truth(client: JevClient) -> CalibrationAnswers:
        def answer(pairs: pl.DataFrame, attributes: pl.DataFrame) -> pl.DataFrame:
            return pairs.join(world["candidates"], on=["left_id", "right_id"]).select(
                "left_id",
                "right_id",
                y=pl.col("y").cast(pl.Float64),
                pair_label_source=pl.lit("human"),
            )

        return answer

    answered = _run(world, tmp_path_factory.mktemp("jev"), answer=truth)
    sample = uncalibrated.scores.filter(review_reason=CALIBRATION_REASON)
    recorded = _run(world, tmp_path_factory.mktemp("jev"), _labels(sample))
    assert all(report.calibrated for report in answered.result.models)
    assert answered.scores.equals(recorded.scores)


def test_without_both_outcomes_people_s_labels_alone_calibrate(
    uncalibrated: Run,
    world: dict[str, pl.DataFrame],
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    no_matches = uncalibrated.scores.filter(donor_code=DONOR, y=False).head(
        CALIBRATION_LABELS
    )
    run = _run(world, tmp_path_factory.mktemp("jev"), _labels(no_matches))
    assert run.client.live_calls == 0
    scores = run.scores.filter(donor_code=DONOR)
    assert scores["label_set"].unique().to_list() == [f"human:{CALIBRATION_LABELS}"]
    assert scores["probability"].max() == 0


def test_two_runs_give_the_same_scores(
    uncalibrated: Run,
    world: dict[str, pl.DataFrame],
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    again = _run(world, tmp_path_factory.mktemp("jev"))
    assert again.scores.equals(uncalibrated.scores)


# Scores `world` without Jev, writes the scores to argv[1] and prints the
# term-frequency columns in the order Splink joins them.
_SCORE_WORLD = """
import sys
from pathlib import Path

from test_splink_models import WORLD, _run, _world

from climate_finance.build.link import splink_models

run = _run(_world(*WORLD), Path(sys.argv[2]), jev=False)
run.scores.write_parquet(sys.argv[1])
settings = splink_models._settings(provider_u=None).get_settings("duckdb")
print(*(column.name for column in settings._term_frequency_columns))
"""


# Slow: two fresh interpreters each import the package and fit two models, about
# 10 s in all on an idle machine.
def test_the_hash_seed_leaves_the_models_unchanged(tmp_path: Path) -> None:
    # Hash seeds 0 and 1 put the term-frequency columns in a set in opposite orders.
    runs = []
    for seed in ("0", "1"):
        scores = tmp_path / f"scores_{seed}.parquet"
        done = subprocess.run(
            [sys.executable, "-c", _SCORE_WORLD, str(scores), str(tmp_path / seed)],
            capture_output=True,
            encoding="utf-8",
            check=True,
            env=os.environ
            | {
                "PYTHONHASHSEED": seed,
                "PYTHONPATH": os.pathsep.join(
                    [str(Path(__file__).parent), os.environ.get("PYTHONPATH", "")]
                ),
            },
        )
        runs.append((done.stdout.split(), pl.read_parquet(scores)))

    (columns, scores), (other_columns, other_scores) = runs
    assert columns == other_columns == ['"pn_norm"', '"title_norm"']
    assert scores.equals(other_scores)


def test_a_model_too_small_to_fit_scores_nothing(tmp_path: Path) -> None:
    run = _run(_world((DONOR, 5, 4, 6)), tmp_path / "jev", providers=(DONOR,))
    assert run.scores.is_empty()
    [report] = run.result.models
    assert not report.usable


def test_people_s_decisions_are_human_labels(tmp_path: Path) -> None:
    path = tmp_path / "review_decisions.parquet"
    assert read_decision_labels(path).is_empty()
    pl.DataFrame(
        {
            "task": ["trace", "trace", "trace", "crdf_crs", "trace"],
            "left_id": ["b", "a", "b", "c", "a"],
            "right_id": ["y", "x", "y", None, "x"],
            "decision": ["reject", "accept", "accept", "reject", "reject"],
            "label_source": "human",
            "round": pl.Series([None] * 5, dtype=pl.String),
            "review_reason": ["band", "calibration", "band", "over_face", None],
            "sample_id": [None, None, None, None, "s1"],
            "decided_at": [datetime(2026, 11, d, tzinfo=UTC) for d in (2, 1, 1, 1, 3)],
        }
    ).write_parquet(path)
    labels = read_decision_labels(path)
    schema.validate(schema.PAIR_LABELS, labels.drop("stratified"))
    # The later decision on b stands; c names no commitment; the blind-sample
    # decision on a, though later, is no label. Only a's calibration pair was drawn.
    assert labels.select("left_id", "is_match", "stratified").rows() == [
        ("a", True, True),
        ("b", False, False),
    ]
    assert set(labels["pair_label_source"]) == {"human"}


def test_levels_follow_splink_numbering(uncalibrated: Run) -> None:
    scores = uncalibrated.scores.unnest("levels")
    # Equal project numbers: the exact level, the third of five.
    assert set(scores.filter("y")["gamma_project_number"]) == {3}
    # The same recipient on every candidate, and a gap of at least one year.
    assert set(scores["gamma_recipient_code"]) == {1}
    assert set(scores["gamma_year_gap"]) <= {1, 2, 3}


def test_stratified_sample_spreads_over_the_bands() -> None:
    weights = np.repeat(np.array([-20.0, -7.0, 2.0, 40.0]), [500, 3, 500, 500])
    scores = pl.DataFrame(
        {
            "left_id": [f"l{i}" for i in range(weights.size)],
            "right_id": "r",
            "match_weight": weights,
        }
    )
    sample = stratified_sample(scores, n=100)
    per_band = sample.group_by("match_weight").len()
    # The band of three gives all three; the others share the rest.
    assert dict(per_band.iter_rows()) == {-20.0: 33, -7.0: 3, 2.0: 32, 40.0: 32}
    # The same pairs whatever order they arrive in.
    assert sample.sort("left_id").equals(
        stratified_sample(scores.reverse(), n=100).sort("left_id")
    )


def test_only_drawn_labels_take_inverse_probability_weights() -> None:
    # One band of ten pairs: two labels the stratified sample drew stand for all
    # ten, and a label chosen for review stands for itself.
    scores = pl.DataFrame(
        {
            "left_id": [f"l{i}" for i in range(10)],
            "right_id": "r",
            "match_weight": 1.0,
            "levels": None,
        }
    )
    labels = pl.DataFrame(
        {
            "left_id": ["l0", "l1", "l2"],
            "right_id": "r",
            "y": [1.0, 0.0, 1.0],
            "stratified": [True, True, False],
        }
    )
    weighted = splink_models._band_weights(scores, labels)
    assert dict(weighted.select("left_id", "weight").iter_rows()) == {
        "l0": 5.0,
        "l1": 5.0,
        "l2": 1.0,
    }


def test_labelled_m_weights_each_label_by_its_band() -> None:
    model = {
        "comparisons": [
            {
                "output_column_name": "recipient_code",
                "comparison_levels": [
                    {"sql_condition": "x is null", "is_null_level": True},
                    {"sql_condition": "x_l = x_r", "m_probability": 0.6},
                    {"sql_condition": "ELSE", "m_probability": 0.4},
                ],
            }
        ]
    }
    levels = pl.DataFrame(
        {
            f"gamma_{name}": [1, 0, -1, 1] if name == "recipient_code" else [0] * 4
            for name in splink_models.LEVEL_NAMES
        }
    ).select(levels=pl.struct(pl.all().cast(pl.Int8)))
    labelled = levels.with_columns(
        y=pl.Series([1.0, 1.0, 1.0, 0.0]), weight=pl.Series([3.0, 1.0, 5.0, 2.0])
    )
    out = with_labelled_m(model, labelled)
    m = [lv.get("m_probability") for lv in out["comparisons"][0]["comparison_levels"]]
    # Matches at level 1 weigh 3, at level 0 weigh 1; the null level counts nothing.
    assert m[1] == pytest.approx((3 + 0.6) / (4 + 1))
    assert m[2] == pytest.approx((1 + 0.4) / (4 + 1))
    assert all(
        lv["fix_m_probability"] for lv in out["comparisons"][0]["comparison_levels"][1:]
    )
    assert model["comparisons"][0]["comparison_levels"][1]["m_probability"] == 0.6


def test_isotonic_map_is_monotone() -> None:
    rng = np.random.default_rng(3)
    weights = rng.normal(0, 10, 400)
    labelled = pl.DataFrame(
        {
            "left_id": [str(i) for i in range(400)],
            "right_id": "r",
            "match_weight": weights,
            "y": (rng.uniform(size=400) < 1 / (1 + np.exp(-weights / 3))).astype(float),
            "weight": rng.uniform(1, 5, 400),
        }
    )
    grid = np.linspace(-40, 40, 200)
    mapped = isotonic_map(labelled).predict(grid)
    assert (np.diff(mapped) >= 0).all()
    assert mapped.min() >= 0
    assert mapped.max() <= 1

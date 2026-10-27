"""Jev's adjudication of the classifier's band, on invented pairs.

Jev answers through a mock transport: p(same activity) 0.9 when the two records
share a project number, else 0.1.
"""

import json
from pathlib import Path

import httpx
import numpy as np
import polars as pl
import pytest
from stage_runs import JEV_MODEL

from climate_finance import schema
from climate_finance.build.errors import CostCapExceededError, LinkThresholdError
from climate_finance.build.jev_client import JevCacheMissError, JevClient
from climate_finance.build.link.jev import (
    JEV_TEXT_CHARS,
    JevBand,
    JevFitError,
    Logistic,
    adjudicate_band,
    best_link_threshold,
    pair_request,
)
from climate_finance.build.link.levels import LEVELS
from climate_finance.build.link.score import (
    ELIGIBLE,
    HELDOUT,
    TRAIN,
    TruthScores,
    classifier_links,
)

BAND = (0.3, 0.9)
_UNITS = schema.UNITS.dtypes
_STEMS = pl.DataFrame(schema=schema.PROGRAMME_STEMS.dtypes)


def _pair(
    n: int, split: str, p: float, *, same_pn: bool, true: bool, **fields: object
) -> dict[str, object]:
    return {
        "left_id": f"d{n:03d}",
        "right_id": f"c{n:03d}",
        "donor_code": 1,
        "split": split,
        "p_model": p,
        "same_pn": same_pn,
        "true_pair": true,
        "programme_level": 0,
        "placed_year": 2018,
    } | fields


def _rows() -> list[dict[str, object]]:
    """Training band pairs of donor 1, mostly true under one project number and
    spread alike over the band whether true or not; an out-of-band training pair;
    donor 2's training pairs, not single-id; and eligible pairs in and out of the
    band."""
    rows = []
    for i in range(20):
        rows.append(_pair(i, TRAIN, 0.35 + i / 40, same_pn=True, true=True))
        rows.append(_pair(20 + i, TRAIN, 0.35 + i / 40, same_pn=False, true=False))
    for i in range(3):
        rows.append(_pair(40 + i, TRAIN, 0.5, same_pn=False, true=True))
        rows.append(_pair(43 + i, TRAIN, 0.5, same_pn=True, true=False))
    rows.append(_pair(46, TRAIN, 0.95, same_pn=True, true=True))
    rows += [
        *(
            _pair(50 + i, TRAIN, 0.5, same_pn=True, true=False, donor_code=2)
            for i in range(4)
        ),
        _pair(60, HELDOUT, 0.5, same_pn=True, true=True),
        # Under the classifier's threshold of 0.5 and above it.
        _pair(61, ELIGIBLE, 0.45, same_pn=True, true=False),
        _pair(62, ELIGIBLE, 0.6, same_pn=False, true=False),
        _pair(63, ELIGIBLE, 0.95, same_pn=False, true=False),
        _pair(64, ELIGIBLE, 0.25, same_pn=True, true=False),
        _pair(65, ELIGIBLE, 0.6, same_pn=True, true=False, programme_level=1),
    ]
    return rows


def _scores(rows: list[dict[str, object]]) -> TruthScores:
    pairs = (
        pl.DataFrame(rows)
        .with_columns(
            donor_code=pl.col("donor_code").cast(pl.Int32),
            placed_year=pl.col("placed_year").cast(pl.Int16),
            programme_level=pl.col("programme_level").cast(pl.Int8),
            **{level: pl.lit(0, dtype=pl.Int8) for level in LEVELS},
        )
        .with_columns(
            gamma_project_number=pl.when("same_pn").then(3).otherwise(0).cast(pl.Int8),
        )
    )
    calibration = pl.DataFrame(
        {"provider_code": [1, 2], "calibration": ["own", "pooled"]},
        schema={"provider_code": pl.Int32, "calibration": pl.String},
    )
    return TruthScores(
        pairs=pairs, threshold=0.5, best_threshold=0.5, calibration=calibration
    )


def _world(rows: list[dict[str, object]]) -> tuple[pl.DataFrame, pl.LazyFrame]:
    """Each pair's commitment (2017) and disbursement (2019) units and records."""
    units = []
    for row in rows:
        n = int(str(row["right_id"])[1:])
        number = f"P{n:03d}"
        for unit, year, pn, committed in (
            (row["right_id"], 2017, number, True),
            (row["left_id"], 2019, number if row["same_pn"] else f"Q{n:03d}", False),
        ):
            units.append(
                {
                    "unit_id": unit,
                    "donor_code": row["donor_code"],
                    "project_number": pn,
                    "title": f"project {pn}",
                    "recipient_code": 1,
                    "purpose_code": 23210,
                    "year": year,
                    "commitment_usd": 1e6 if committed else 0.0,
                    "disbursement_usd": 0.0 if committed else 3e5,
                }
            )
    frame = pl.DataFrame(units).with_columns(
        agency_code=1,
        crs_id=pl.lit(None, dtype=pl.String),
        commitment_year=None,
        commitment_date=None,
        placed_year="year",
        floor_year=None,
        record_ids=pl.concat_list("unit_id"),
        purpose_codes=pl.concat_list("purpose_code"),
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
    return frame.select(pl.col(n).cast(t) for n, t in _UNITS.items()), crs.lazy()


def _responses(request: httpx.Request) -> httpx.Response:
    body = json.loads(request.content)
    p = 0.9 if body["state"]["derived_facts"]["same_project_number"] else 0.1
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


def _refuse(request: httpx.Request) -> httpx.Response:
    raise AssertionError("no live call expected")


def _client(
    cache_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    offline: bool = False,
    cost_cap_usd: float = 10.0,
    respond=_responses,
) -> JevClient:
    monkeypatch.setenv("TYPESAFE_API_KEY", "test")
    return JevClient(
        model=JEV_MODEL,
        cost_cap_usd=cost_cap_usd,
        cache_dir=cache_dir,
        offline=offline,
        transport=httpx.MockTransport(respond),
    )


def _adjudicate(rows: list[dict[str, object]], client: JevClient) -> JevBand:
    units, crs = _world(rows)
    return adjudicate_band(
        _scores(rows), units=units, crs=crs, stems=_STEMS, band=BAND, client=client
    )


def test_a_logistic_combines_known_logits() -> None:
    def sigmoid(z: float) -> float:
        return 1 / (1 + np.exp(-z))

    combined = Logistic(coefs=(2.0, 1.0), intercept=-0.5)
    frame = pl.DataFrame({"model": [sigmoid(0.25), 0.0], "jev": [sigmoid(1.0), 1.0]})
    p = frame.select(combined(pl.col("model"), pl.col("jev"))).to_series()
    # -0.5 + 2 x 0.25 + 1 x 1; at 0 and 1 the logits are clipped at 1e-3.
    clipped = np.log(1e-3 / (1 - 1e-3))
    assert p.to_list() == pytest.approx(
        [sigmoid(1.0), sigmoid(-0.5 + 2 * clipped - clipped)]
    )


def test_jev_adjudicates_the_band_and_the_combination_links(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rows = _rows()
    client = _client(tmp_path, monkeypatch)
    band = _adjudicate(rows, client)
    # Donor 1's 47 training pairs (the band's 46 and, for the Splink scale, the
    # out-of-band one) and the two eligible band pairs; not the eligible pairs out
    # of the band, the held-out or sibling pairs, or donor 2's, which is not
    # single-id.
    assert client.live_calls == 49
    assert band.answers["left_id"].to_list() == ["d061", "d062"]
    assert band.answers["p_jev"].to_list() == [0.9, 0.1]
    expected = band.answers.join(
        _scores(rows).pairs.select("left_id", "p_model"), on="left_id"
    ).select(band.combined(pl.col("p_model"), pl.col("p_jev")))
    assert band.answers["p_combined"].to_list() == expected.to_series().to_list()
    # Jev carries the training labels, so both fits lean on it.
    assert band.combined.coefs[1] > 0
    assert band.scale.coefs[0] > 0
    assert band.model == JEV_MODEL

    # Jev lifts d061 over the classifier's threshold and drops d062 under it.
    links = classifier_links(_scores(rows), jev=band)
    schema.validate(schema.LINKS, links)
    assert links.select("left_id", "link_method", "jev_model").rows() == [
        ("d061", "classifier_jev", JEV_MODEL),
        ("d063", "classifier", None),
    ]
    assert links["probability"][0] == band.answers["p_combined"][0]
    assert links["p_jev"].to_list() == [0.9, None]

    # A second build replays the cache offline and gives the same answer.
    again = _adjudicate(
        rows, _client(tmp_path, monkeypatch, offline=True, respond=_refuse)
    )
    assert again.answers.equals(band.answers)
    assert (again.combined, again.scale, again.best_threshold) == (
        band.combined,
        band.scale,
        band.best_threshold,
    )


def test_a_band_pair_links_at_the_classifier_threshold_not_the_best_cut(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Jev reads d066 as 0.4 likely, which combines to between the band's best grid
    # threshold and the classifier's.
    def respond(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        if body["state"]["later_record"]["project_number"] != "Q066":
            return _responses(request)
        relation = {"probabilities": {"0": 0.6, "1": 0.0, "2": 0.4}}
        return httpx.Response(
            200,
            json={
                "model": body["model"],
                "answers": {
                    "relation": relation,
                    "disbursement": {"noul": 0.4},
                    "sibling": {"noul": 0.0},
                },
                "usage": {"input_tokens": 1000, "output_tokens": 10},
            },
        )

    rows = [*_rows(), _pair(66, ELIGIBLE, 0.6, same_pn=False, true=False)]
    band = _adjudicate(rows, _client(tmp_path, monkeypatch, respond=respond))
    scores = _scores(rows)
    p = band.answers.filter(left_id="d066")["p_combined"].item()
    assert band.best_threshold < p < scores.threshold

    links = classifier_links(scores, jev=band)
    assert "d066" not in links["left_id"].to_list()


def _labelled(strong: int) -> pl.DataFrame:
    """`strong` disbursement units whose true pair scores 0.9, and one whose true
    pair scores 0.3 over a false pair at 0.2."""
    rows = [(f"s{i}", f"c{i}", 0.9, True) for i in range(strong)]
    rows += [("w", "cw", 0.3, True), ("w", "cf", 0.2, False)]
    return pl.DataFrame(
        rows, schema=["left_id", "right_id", "probability", "true_pair"], orient="row"
    ).with_columns(programme_level=0, placed_year=2018)


@pytest.mark.parametrize(
    ("strong", "refuses"),
    [
        # F1 is 1 at the best threshold and 400/401 at 0.5.
        (200, False),
        # 4/5 at 0.5.
        (2, True),
    ],
)
def test_the_band_refuses_a_cut_far_below_its_best_f1(
    strong: int, *, refuses: bool
) -> None:
    # Every grid threshold to 0.30 links the weak unit's true pair, so the best is
    # the lowest. At 0.5 it does not link.
    pairs = _labelled(strong)
    if not refuses:
        assert best_link_threshold(pairs, threshold=0.5) == 0.01
        return
    with pytest.raises(LinkThresholdError) as raised:
        best_link_threshold(pairs, threshold=0.5)
    assert (raised.value.threshold, raised.value.best_threshold) == (0.5, 0.01)
    assert raised.value.best_f1 == 1.0


def test_an_offline_build_refuses_a_cache_miss(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = _client(tmp_path, monkeypatch, offline=True, respond=_refuse)
    with pytest.raises(JevCacheMissError) as error:
        _adjudicate(_rows(), client)
    assert error.value.misses == 49


def test_the_cost_cap_refuses_the_whole_band(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = _client(tmp_path, monkeypatch, cost_cap_usd=1e-4, respond=_refuse)
    with pytest.raises(CostCapExceededError):
        _adjudicate(_rows(), client)
    assert client.live_calls == 0
    assert not (tmp_path / f"{JEV_MODEL}.jsonl").exists()


def test_too_few_training_outcomes_refuse_the_fit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rows = [row for row in _rows() if not row["true_pair"] or row["split"] != TRAIN]
    with pytest.raises(JevFitError):
        _adjudicate(rows, _client(tmp_path, monkeypatch, respond=_refuse))


def test_pair_request_states_the_derived_facts() -> None:
    earlier = {
        "year": 2018, "donor_code": 901, "recipient_code": 1, "purpose_code": 23210,
        "channel_code": None, "flow_code": 11, "finance_type_code": 110,
        "project_number": "P1", "title": "Solar Grid", "short_description": None,
        "long_description": "x " * 400, "commitment_usd": 2e6, "disbursement_usd": 0.0,
    }  # fmt: skip
    later = earlier | {
        "year": 2021, "title": "solar grid", "commitment_usd": None,
        "disbursement_usd": 5e5, "purpose_code": 23220,
    }  # fmt: skip
    request = pair_request(earlier, later)
    facts = request["state"]["derived_facts"]  # ty: ignore[not-subscriptable]
    assert facts == {
        "years_between_records": 3,
        "same_provider": True,
        "same_project_number": True,
        "same_title": True,
        "same_recipient": True,
        "same_purpose_code": False,
        "later_disbursement_as_share_of_earlier_commitment": 0.25,
        "later_record_also_has_its_own_commitment": False,
    }
    record = request["state"]["earlier_record"]  # ty: ignore[not-subscriptable]
    assert len(record["long_description"]) == JEV_TEXT_CHARS + 3
    assert set(request["questions"]) == {"relation", "disbursement", "sibling"}  # ty: ignore[invalid-argument-type]

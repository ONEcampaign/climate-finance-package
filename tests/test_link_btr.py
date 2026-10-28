"""The BTR-to-CRS link: id tier, recipient keys, calibration and the Splink tier.

Codes are real DAC codes; ids, titles and amounts are invented.
"""

import logging

import duckdb
import polars as pl
import pytest
from normalised import crs_records as crs_frame

from climate_finance import schema
from climate_finance.build.link.btr import (
    AGGREGATE,
    CALIBRATION_DRAWS,
    CALIBRATION_STRATA,
    LINK_PROBABILITY,
    MIN_CHECK_LABELS,
    MIN_MAP_OUTCOMES,
    POOLED_CHECK_SHARE,
    POOLED_FLOOR,
    btr_rows,
    calibrate,
    calibration_draw,
    comparisons,
    crs_records,
    current_decisions,
    link_btr,
    link_by_ids,
    linked_pairs,
    score_party,
)
from climate_finance.build.link.packet import btr_digests

_BTR_DEFAULTS: dict[str, object] = {
    "party": "NOR",
    "year": 2021,
    "recipient": "Kenya",
    "recipient_code": None,
    "title": None,
    "amount_domestic": None,
    "currency": "NOK",
    "face_usd": 1e6,
    "grant_equiv_usd": None,
    "status": "disbursed",
    "channel": None,
    "funding_source": "ODA",
    "instrument": "Grant",
    "support_type": "Mitigation",
    "sector": "Energy",
    "additional_information": None,
    "is_aggregate": False,
    "source_file": "NOR.xlsx",
}


# The parties' DAC provider codes.
_PROVIDERS = {"NOR": 8, "DEU": 5, "GBR": 12}


def _btr(*rows: dict[str, object]) -> pl.DataFrame:
    full = [_BTR_DEFAULTS | row for row in rows]
    frame = pl.DataFrame(
        [row | {"provider_code": _PROVIDERS[row["party"]]} for row in full],
        schema={name: column.dtype for name, column in schema.BTR.columns.items()},
    )
    schema.validate(schema.BTR, frame)
    return frame


def _id_links(btr: pl.DataFrame, crs: pl.DataFrame) -> set[tuple[str, str]]:
    rows = btr_rows(btr.lazy())
    pairs = link_by_ids(rows, records=crs_records(crs.lazy(), rows=rows))
    return set(pairs.iter_rows())


@pytest.mark.parametrize(
    ("row", "records", "linked"),
    [
        pytest.param(
            {"party": "DEU", "title": "Solar Kenya, BMZ-Nr. 2021.6543.2"},
            [{"donor_code": 5, "project_number": "202165432"}],
            {"r1"},
            id="bmz-number-is-the-project-number",
        ),
        pytest.param(
            {"title": "Solar Kenya (2021000123)"},
            [{"crs_id": "2021000123"}, {"crs_id": "2021000124"}],
            {"r1"},
            id="norwegian-crs-id",
        ),
        pytest.param(
            {"party": "GBR", "title": "300123 Solar Kenya"},
            [
                {"donor_code": 12, "project_number": "300123-101"},
                {"donor_code": 12, "project_number": "300123-105"},
                {"donor_code": 12, "project_number": "300124-101"},
            ],
            {"r1", "r2"},
            id="programme-number-reaches-every-component",
        ),
        pytest.param(
            {"party": "GBR", "title": "300123 Solar Kenya"},
            [
                {"donor_code": 12, "project_number": "300123-101"},
                # Ghana's component is another country's line; Africa, regional.
                {
                    "donor_code": 12,
                    "project_number": "300123-105",
                    "recipient_code": 241,
                },
                {
                    "donor_code": 12,
                    "project_number": "300123-106",
                    "recipient_code": 298,
                },
            ],
            {"r1", "r3"},
            id="programme-number-keeps-to-the-rows-recipient",
        ),
        pytest.param(
            {"party": "GBR", "title": "300123 Solar Africa", "recipient": "Africa"},
            [
                {"donor_code": 12, "project_number": "300123-101"},
                {
                    "donor_code": 12,
                    "project_number": "300123-105",
                    "recipient_code": 241,
                },
            ],
            {"r1", "r2"},
            id="regional-row-reaches-every-country",
        ),
        pytest.param(
            {"title": "Finnfund total (2021000123)", "is_aggregate": True},
            [{"crs_id": "2021000123"}],
            set(),
            id="aggregate-row-not-linked",
        ),
        pytest.param(
            {"additional_information": "CRS-ID: 2021000123"},
            [{"crs_id": "2021000123"}],
            {"r1"},
            id="id-in-additional-information",
        ),
        pytest.param(
            {"title": "Solar Kenya (2021000123)"},
            [{"crs_id": "2021000123", "year": 2022}],
            set(),
            id="other-year-not-linked",
        ),
        pytest.param(
            {"title": "Solar Kenya (2021000123)"},
            [{"crs_id": "2021000123", "donor_code": 10}],
            set(),
            id="other-donor-not-linked",
        ),
        pytest.param(
            {"title": "Solar Kenya (2021000123)"},
            [{"crs_id": "2021000123", "bi_multi": 2}],
            set(),
            id="core-contribution-not-linked",
        ),
        pytest.param(
            {"party": "GBR", "title": "300123 Solar Kenya"},
            [
                {
                    "donor_code": 12,
                    "project_number": "300123-101",
                    "commitment_usd": None,
                }
            ],
            {"r1"},
            id="record-without-amounts-still-linked",
        ),
    ],
)
def test_ids_written_by_the_party_link_same_year_records(
    row: dict[str, object], records: list[dict[str, object]], linked: set[str]
) -> None:
    btr = _btr({"row_id": "b1", **row})
    crs = crs_frame(
        *(
            {"record_id": f"r{i}", "year": 2021, "commitment_usd": 1e6} | record
            for i, record in enumerate(records, start=1)
        )
    )

    assert _id_links(btr, crs) == {("b1", record) for record in linked}


def test_recipients_key_on_the_resolver_and_non_countries_are_aggregate() -> None:
    btr = _btr(
        *(
            {"row_id": f"b{i}", "recipient": name}
            for i, name in enumerate(
                [
                    "Vietnam",
                    "Turkey",
                    "Africa",
                    "Multiple countries in Asia",
                    "Global",
                    None,
                ]
            )
        )
    )

    keys = btr_rows(btr.lazy()).sort("row_id")["rec_key"].to_list()

    assert keys == ["769", "55", AGGREGATE, AGGREGATE, AGGREGATE, None]


def _amount_level(
    btr_usd: float, *, status: str, commitment: float, disbursement: float
) -> str:
    """The first amount level whose condition a pair meets."""
    amount = next(
        comparison.get_comparison("duckdb")
        for comparison in comparisons()
        if comparison.get_comparison("duckdb").output_column_name == "amount"
    )
    pair = (
        f"select {btr_usd} amt_l, '{status}' status_l, {commitment} amt_c_r, "
        f"{disbursement} amt_d_r"
    )
    for level in amount.comparison_levels:
        if level.sql_condition == "ELSE":
            return level.label_for_charts
        if duckdb.sql(f"select ({level.sql_condition}) from ({pair})").fetchone()[0]:
            return level.label_for_charts
    raise AssertionError("no level")


@pytest.mark.parametrize(
    ("btr_usd", "status", "level"),
    [
        (40.0, "disbursed", "at a party weight"),
        (50.0, "disbursed", "at a party weight"),
        (60.0, "disbursed", "20-85%"),
        (100.0, "disbursed", "2%"),
        # A committed row reads against the commitment.
        (200.0, "committed", "2%"),
    ],
)
def test_an_amount_at_a_party_weight_has_its_own_level(
    btr_usd: float, status: str, level: str
) -> None:
    found = _amount_level(btr_usd, status=status, commitment=200.0, disbursement=100.0)

    assert found == level


def _best(party: str, rows: list[tuple[str, str, float]]) -> pl.DataFrame:
    return pl.DataFrame(
        [
            (party, row_id, record_id, weight, 0.5, "m")
            for row_id, record_id, weight in rows
        ],
        schema=[
            "party",
            "row_id",
            "record_id",
            "match_weight",
            "p_raw",
            "model_version",
        ],
        orient="row",
    )


def _labels(party: str, *, right: int, wrong: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    """`party`'s best candidates, `right` of them its id records at weight 10 and
    `wrong` not at weight -10, and the id truth behind them."""
    best = _best(
        party,
        [(f"{party}{i}", f"r{party}{i}", 10.0) for i in range(right)]
        + [(f"{party}{right + i}", "wrong", -10.0) for i in range(wrong)],
    )
    truth = best.select("row_id", record_id=pl.format("r{}", "row_id"))
    return best, truth


def test_a_party_without_enough_labels_takes_the_pooled_map(
    caplog: pytest.LogCaptureFixture,
) -> None:
    n = MIN_MAP_OUTCOMES
    # Party A: high weights are its id records, low weights are not.
    a, truth = _labels("a", right=n, wrong=n)
    # Party B's one label says weight 10 is wrong; on its own map it would read 0.
    # It is too few to check the pool, so B reads the pool unchecked and the pool
    # leaves it out. Below `POOLED_FLOOR` its rows read nothing.
    b = _best(
        "B",
        [
            ("b0", "wrong", 10.0),
            ("b1", "rb1", 10.0),
            ("b2", "rb2", POOLED_FLOOR - 0.1),
            ("b3", "rb3", POOLED_FLOOR),
        ],
    )
    truth = pl.concat([truth, pl.DataFrame({"row_id": ["b0"], "record_id": ["rb0"]})])

    with caplog.at_level(logging.INFO):
        probability = dict(
            calibrate(pl.concat([a, b]), truth=truth)
            .select("row_id", "probability")
            .iter_rows()
        )

    assert probability["a0"] == 1.0
    assert probability[f"a{n}"] == 0.0
    assert probability["b1"] == 1.0
    # The pooled map runs straight from 0 at -10 to 1 at 10.
    assert probability["b3"] == pytest.approx(0.75)
    assert probability["b2"] is None
    logged = [(r.levelno, r.args) for r in caplog.records]
    assert logged == [
        (logging.INFO, ("a", 0)),
        (logging.WARNING, ("B", 2, POOLED_FLOOR, 1, MIN_CHECK_LABELS)),
    ]


@pytest.mark.parametrize(
    ("right", "kept"),
    [
        (round(MIN_CHECK_LABELS * POOLED_CHECK_SHARE), True),
        (round(MIN_CHECK_LABELS * POOLED_CHECK_SHARE) - 1, False),
    ],
)
def test_a_partys_labels_from_the_floor_up_can_reject_the_pooled_map(
    *, right: int, kept: bool, caplog: pytest.LogCaptureFixture
) -> None:
    n = MIN_MAP_OUTCOMES
    a, truth = _labels("a", right=n, wrong=n)
    # Party B's `MIN_CHECK_LABELS` id labels at weight 10, `right` of them right:
    # too few wrong for a map of its own, enough to check the pooled map.
    b = _best(
        "B",
        [
            (f"b{i}", f"rb{i}" if i < right else "wrong", 10.0)
            for i in range(MIN_CHECK_LABELS)
        ]
        + [("bx", "rbx", 10.0)],
    )
    # Party C has no labels, so it reads the pool unchecked.
    c = _best("C", [("cx", "rcx", 10.0)])
    truth = pl.concat(
        [
            truth,
            b.head(MIN_CHECK_LABELS).select(
                "row_id", record_id=pl.format("r{}", "row_id")
            ),
        ]
    )

    with caplog.at_level(logging.INFO):
        found = dict(
            calibrate(pl.concat([a, b, c]), truth=truth)
            .select("row_id", "probability")
            .iter_rows()
        )

    logged = {r.args[0]: (r.levelno, r.args[1:]) for r in caplog.records}
    if kept:
        # B's labels join the pool: at weight 10, 54 of 60 right.
        assert found["bx"] == found["cx"] == pytest.approx(0.9)
        assert logged["B"] == (
            logging.INFO,
            (1, POOLED_FLOOR, MIN_CHECK_LABELS, right, POOLED_CHECK_SHARE),
        )
    else:
        # B links nothing, and C reads a pool without B's labels.
        assert found["bx"] is None
        assert found["cx"] == 1.0
        level, args = logged["B"]
        assert (level, args[:3]) == (logging.WARNING, (right, MIN_CHECK_LABELS, 5.0))


@pytest.mark.parametrize(
    ("party", "right", "wrong", "weight", "probability"),
    [
        # Half the right labels sit at weight 0 beside the wrong ones at 1, so the
        # lowest block of a's own map reads 0.5; a clipped map would give that to
        # weight -40.
        ("a", 2 * MIN_MAP_OUTCOMES, MIN_MAP_OUTCOMES, -40.0, 0.0),
        # One right label must not lift every row to 1.
        ("B", 1, 0, -30.0, None),
        ("B", 1, 0, 10.0, None),
        ("B", MIN_MAP_OUTCOMES, MIN_MAP_OUTCOMES - 1, 10.0, None),
    ],
)
def test_calibration_fails_closed_outside_its_labels(
    *,
    party: str,
    right: int,
    wrong: int,
    weight: float,
    probability: float | None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    a, truth = _labels("a", right=right, wrong=wrong)
    low = pl.int_range(pl.len()) < MIN_MAP_OUTCOMES
    a = a.with_columns(
        match_weight=pl.when(pl.col("record_id") == "wrong")
        .then(1.0)
        .when(low)
        .then(0.0)
        .otherwise(10.0)
    )
    b = _best(party, [("b0", "rb0", weight)])

    with caplog.at_level(logging.WARNING):
        calibrated = calibrate(pl.concat([a, b]), truth=truth).filter(row_id="b0")

    found = calibrated["probability"].item()
    if probability is None:
        assert found is None
        warned = [r.args for r in caplog.records if r.levelno == logging.WARNING]
        assert ("B", MIN_MAP_OUTCOMES, 1) in warned
    else:
        assert found == probability
        assert found < LINK_PROBABILITY


def test_no_labels_anywhere_gives_no_probability() -> None:
    best = _best("A", [("a0", "r0", 10.0)])

    calibrated = calibrate(
        best, truth=pl.DataFrame(schema={"row_id": pl.String, "record_id": pl.String})
    )

    assert calibrated["probability"].to_list() == [None]


def _decisions(
    pairs: pl.DataFrame,
    *,
    is_match: bool,
    stratified: bool = True,
    task: str = "btr_crs",
    source: str = "human",
) -> pl.DataFrame:
    """Decisions on `pairs` (`row_id`, `record_id`) as `read_decision_labels` gives
    them."""
    return pairs.select(
        task=pl.lit(task),
        left_id="row_id",
        right_id="record_id",
        is_match=pl.lit(is_match),
        pair_label_source=pl.lit(source),
        stratified=pl.lit(stratified),
    )


def test_a_decision_holds_only_while_its_row_and_record_are_unchanged() -> None:
    n = MIN_MAP_OUTCOMES
    ids, truth = _labels("a", right=n, wrong=n)
    idless = _best("a", [(f"x{i}", f"rx{i}", -5.0) for i in range(n)])
    btr = _btr(*({"row_id": f"x{i}", "title": "Solar grid"} for i in range(n)))
    crs = crs_frame(*({"record_id": f"rx{i}", "year": 2021} for i in range(n)))
    pairs = idless.select(left_id="row_id", right_id="record_id")
    seen = btr_digests(pairs, btr=btr.lazy(), crs=crs.lazy())
    accepted = _decisions(idless, is_match=True).join(seen, on=["left_id", "right_id"])

    def probability(
        btr: pl.DataFrame, crs: pl.DataFrame, labels: pl.DataFrame
    ) -> float:
        current = current_decisions(labels, btr=btr.lazy(), crs=crs.lazy())
        found = calibrate(pl.concat([ids, idless]), truth=truth, labels=current).filter(
            row_id="x0"
        )
        return found["probability"].item()

    # A revised workbook puts another activity at the same row ids, or the CRS
    # record changes: the decisions no longer hold, and the map reads 0.25 as with
    # none. A decision with no fingerprint never holds.
    revised = btr.with_columns(title=pl.lit("Coastal flood barriers"))
    changed = crs.with_columns(commitment_usd=pl.lit(5e6))
    assert probability(btr, crs, accepted) == 1.0
    assert probability(revised, crs, accepted) == pytest.approx(0.25)
    assert probability(btr, changed, accepted) == pytest.approx(0.25)
    assert probability(
        btr, crs, accepted.with_columns(left_digest=pl.lit(None, pl.String))
    ) == pytest.approx(0.25)


def test_a_block_of_decisions_on_id_less_rows_moves_the_map() -> None:
    n = MIN_MAP_OUTCOMES
    ids, truth = _labels("a", right=n, wrong=n)
    # Id-less rows at weight -5, between the id labels at -10 (wrong) and 10
    # (right), where the map reads 0.25.
    idless = _best("a", [(f"x{i}", f"rx{i}", -5.0) for i in range(n)])
    best = pl.concat([ids, idless])
    accepted = _decisions(idless, is_match=True)
    # Decisions on a trace pair, on an agent's rejected id row, on a Jev label and
    # on a record that is not the row's best candidate change nothing.
    ignored = pl.concat(
        [
            _decisions(idless, is_match=False, task="trace"),
            _decisions(ids.head(1), is_match=False, source="agent"),
            _decisions(idless, is_match=False, source="jev"),
            _decisions(idless.with_columns(record_id=pl.lit("other")), is_match=False),
        ]
    )

    def probability(labels: pl.DataFrame | None) -> float:
        found = calibrate(best, truth=truth, labels=labels).filter(row_id="x0")
        return found["probability"].item()

    assert probability(None) == pytest.approx(0.25)
    assert probability(ignored.clear()) == probability(None)
    assert probability(ignored) == probability(None)
    assert probability(accepted) == 1.0
    assert probability(pl.concat([accepted, ignored])) == 1.0


@pytest.mark.parametrize(
    ("stratified", "weight", "id_wrong", "probability"),
    [
        (True, -5.0, MIN_MAP_OUTCOMES, 30 / 130),
        (False, -5.0, MIN_MAP_OUTCOMES, 0.5),
        # The stratum from `POOLED_FLOOR` up is open-ended, and drawn alike for a
        # party whose ids give it no map of its own.
        (True, POOLED_FLOOR + 1, MIN_MAP_OUTCOMES - 1, 30 / 130),
        # The draw never takes it for a party with its own map, so a pair drawn
        # lower that now scores there weighs 1.
        (True, POOLED_FLOOR + 1, MIN_MAP_OUTCOMES, 0.5),
        # A drawn pair that now scores between the strata, where the draw took no
        # share, weighs 1.
        (True, -2.0, MIN_MAP_OUTCOMES, 0.5),
    ],
)
def test_drawn_decisions_weigh_the_id_less_rows_of_their_stratum(
    *, stratified: bool, weight: float, id_wrong: int, probability: float
) -> None:
    n = MIN_MAP_OUTCOMES
    ids, truth = _labels("a", right=n, wrong=id_wrong)
    # Thirty right id labels at `weight`, `id_wrong` wrong ones at -10, and a
    # hundred id-less rows at `weight`, thirty of them decided wrong. Drawn, each
    # decision stands for 100 / 30 rows.
    ids = ids.with_columns(
        match_weight=pl.when(pl.col("match_weight") > 0).then(weight).otherwise(-10.0)
    )
    idless = _best("a", [(f"x{i}", f"rx{i}", weight) for i in range(100)])
    rejected = _decisions(idless.head(n), is_match=False, stratified=stratified)

    calibrated = calibrate(
        pl.concat([ids, idless]), truth=truth, labels=rejected
    ).filter(row_id="x99")

    assert calibrated["probability"].item() == pytest.approx(probability)


def test_the_draw_spreads_each_partys_id_less_rows_over_the_strata() -> None:
    (low, _), *_, (floor, _) = CALIBRATION_STRATA
    # Party A: fifty id-less rows in each of the first two strata, ten in the third
    # and forty from the floor up, and rows outside every stratum and with ids,
    # which are never drawn.
    rows = [
        (f"s{stratum}-{i}", f"r{stratum}-{i}", CALIBRATION_STRATA[stratum][0] + 0.5)
        for stratum, size in enumerate((50, 50, 10, 40))
        for i in range(size)
    ]
    rows += [(f"out{i}", "r", weight) for i, weight in enumerate((low - 1, -3.0))]
    a = _best("A", [*rows, ("id0", "rid0", low)])
    # Party B has three rows, all drawn.
    b = _best("B", [(f"b{i}", "r", -5.0) for i in range(3)])
    # Party C's id labels give it a map of its own, so its rows from the floor up,
    # which never read the pooled map, are not drawn.
    c, c_truth = _labels("c", right=MIN_MAP_OUTCOMES, wrong=MIN_MAP_OUTCOMES)
    c = pl.concat([c, _best("c", [("c-low", "r", -5.0), ("c-top", "r", floor)])])
    truth = pl.concat(
        [pl.DataFrame({"row_id": ["id0"], "record_id": ["other"]}), c_truth]
    )
    best = pl.concat([a, b, c])

    drawn = calibration_draw(best, truth=truth)
    by_stratum = (
        drawn.filter(party="A")
        .group_by(pl.col("row_id").str.extract(r"^s(\d)"))
        .len()
        .sort("row_id")
    )

    in_order = drawn.filter(party="A").sort("draw_rank")
    strata = in_order["row_id"].str.extract(r"^s(\d)")

    # Ninety below -3, the third stratum's shortfall made up by the other two, and
    # thirty from the floor up.
    size = CALIBRATION_DRAWS + MIN_CHECK_LABELS
    assert drawn.filter(party="A").height == size
    assert dict(by_stratum.iter_rows()) == {"0": 40, "1": 40, "2": 10, "3": 30}
    # The draw takes the strata in turn, so any first part of it holds each stratum
    # alike, and within a stratum it follows the pairs' digests, not their ids.
    assert in_order["draw_rank"].to_list() == list(range(1, size + 1))
    assert strata.head(40).to_list() == ["0", "1", "2", "3"] * 10
    first = in_order.filter(strata == "0")["row_id"]
    assert first.to_list() != first.sort().to_list()
    assert drawn.filter(party="B")["row_id"].to_list() == ["b0", "b1", "b2"]
    assert drawn.filter(party="c")["row_id"].to_list() == ["c-low"]
    assert drawn.equals(
        calibration_draw(best.sample(fraction=1.0, shuffle=True, seed=1), truth=truth)
    )


_COUNTRIES = ["Kenya", "Ghana", "Uganda", "Malawi", "Zambia", "Nepal"]
_CODES = [248, 241, 285, 253, 288, 660]
# Three title words per activity, picked apart by these steps and shifts.
_MIX = ((1, 0), (7, 3), (3, 11))
_WORDS = [
    "solar", "grid", "wind", "hydro", "forest", "water", "drought", "coastal",
    "flood", "soil", "seed", "river", "cooking", "biogas", "mangrove",
    "irrigation", "storage", "transit", "cities", "wetland",
]  # fmt: skip


def _activities(n: int) -> list[tuple[str, int, str, float]]:
    """n invented activities: recipient name, code, a distinct title, an amount."""
    return [
        (
            _COUNTRIES[i % 6],
            _CODES[i % 6],
            " ".join(_WORDS[(i * step + shift) % 20] for step, shift in _MIX) + f" {i}",
            1e5 * (1 + i),
        )
        for i in range(n)
    ]


def _synthetic(*, ids: int, misnamed: int = 0) -> tuple[pl.DataFrame, pl.DataFrame]:
    """150 BTR rows of Norway and their CRS records, beside 150 other records. The
    first `ids` rows carry a crs id: the last `misnamed` of them that of the other
    record 150 on, which their text does not match, the rest their own record's."""
    activities = _activities(300)
    btr = _btr(
        *(
            {
                "row_id": f"b{i:03}",
                "recipient": name,
                "title": title,
                "face_usd": amount,
                "additional_information": (
                    f"CRS-ID: 2021{i + 150 * (i >= ids - misnamed):06}"
                    if i < ids
                    else None
                ),
            }
            for i, (name, _, title, amount) in enumerate(activities[:150])
        )
    )
    crs = crs_frame(
        *(
            {
                "record_id": f"r{i:03}",
                "year": 2021,
                "crs_id": f"2021{i:06}",
                "recipient_code": code,
                "title": title,
                "disbursement_usd": amount,
                "purpose_code": 23210,
                "finance_t": 110,
            }
            for i, (_, code, title, amount) in enumerate(activities)
        )
    )
    return btr, crs


def test_the_model_links_only_once_ids_calibrate_it() -> None:
    # The misnamed ids give the map the wrong labels it needs.
    btr, crs = _synthetic(ids=100, misnamed=MIN_MAP_OUTCOMES)

    links, draw, _ = link_btr(btr.lazy(), crs=crs.lazy())

    schema.validate(schema.LINKS, links)
    schema.validate(schema.BTR_CALIBRATION, draw)
    methods = dict(links.group_by("link_method").len().iter_rows())
    assert methods["btr_id"] == 100
    by_model = links.filter(link_method="btr_splink")
    assert by_model.height > 0
    # Each model link joins a row to its own activity's record.
    assert (by_model["left_id"].str.slice(1) == by_model["right_id"].str.slice(1)).all()


def test_raw_em_probabilities_never_link() -> None:
    btr, crs = _synthetic(ids=0)

    links = link_btr(btr.lazy(), crs=crs.lazy()).links

    assert links.is_empty()


def test_a_party_whose_model_does_not_fit_is_a_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Two rows against two records block too few pairs for any EM session.
    btr, crs = _synthetic(ids=0)
    rows = btr_rows(btr.head(2).lazy())

    with caplog.at_level(logging.WARNING):
        best = score_party(rows, records=crs_records(crs.head(2).lazy(), rows=rows))

    assert best.is_empty()
    assert [(r.levelno, r.args) for r in caplog.records] == [
        (logging.WARNING, ("NOR", 2, 2))
    ]


def test_linked_pairs_sum_a_programme_and_follow_the_row_status() -> None:
    btr = _btr(
        {"row_id": "b1", "party": "GBR", "status": "committed", "face_usd": 40.0},
        {"row_id": "b2", "status": "disbursed", "face_usd": 10.0},
    )
    crs = crs_frame(
        {"record_id": "r1", "year": 2021, "commitment_usd": 60.0, "rio_mitigation": 1},
        {"record_id": "r2", "year": 2021, "commitment_usd": 40.0, "rio_mitigation": 2},
        {
            "record_id": "r3",
            "year": 2021,
            "commitment_usd": 99.0,
            "disbursement_usd": 25.0,
            "rio_adaptation": 1,
            "rio_mitigation": 1,
        },
    )
    links = pl.DataFrame(
        {
            "task": "btr_crs",
            "left_id": ["b1", "b1", "b2"],
            "right_id": ["r1", "r2", "r3"],
        }
    )

    pairs = linked_pairs(links, btr=btr.lazy(), crs=crs.lazy())

    assert pairs.rows() == [
        ("GBR", 2021, 40.0, 100.0, 2, False),
        ("NOR", 2021, 10.0, 25.0, 1, True),
    ]

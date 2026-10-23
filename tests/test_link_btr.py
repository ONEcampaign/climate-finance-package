"""The BTR-to-CRS link: id tier, recipient keys, calibration and the Splink tier.

Codes are real DAC codes; ids, titles and amounts are invented.
"""

import duckdb
import polars as pl
import pytest
from normalised import crs_records as crs_frame

from climate_finance import schema
from climate_finance.build.link.btr import (
    AGGREGATE,
    MIN_ISOTONIC_OUTCOMES,
    btr_rows,
    calibrate,
    comparisons,
    crs_records,
    link_btr,
    link_by_ids,
    linked_pairs,
)

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


def test_recipients_key_on_the_resolver_and_regions_are_aggregate() -> None:
    btr = _btr(
        *(
            {"row_id": f"b{i}", "recipient": name}
            for i, name in enumerate(
                ["Vietnam", "Turkey", "Africa", "Multiple countries in Asia", None]
            )
        )
    )

    keys = btr_rows(btr.lazy()).sort("row_id")["rec_key"].to_list()

    assert keys == ["769", "55", AGGREGATE, AGGREGATE, None]


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


def test_a_party_without_enough_labels_takes_the_pooled_map() -> None:
    n = MIN_ISOTONIC_OUTCOMES
    # Party A: high weights are its id records, low weights are not.
    a = _best(
        "A",
        [(f"a{i}", f"ra{i}", 10.0) for i in range(n)]
        + [(f"a{n + i}", f"wrong{i}", -10.0) for i in range(n)],
    )
    # Party B's one label says weight 10 is wrong; on its own map it would read 0.
    b = _best("B", [("b0", "wrong", 10.0), ("b1", "rb1", 10.0)])
    truth = pl.DataFrame(
        {
            "row_id": [f"a{i}" for i in range(2 * n)] + ["b0"],
            "record_id": [f"ra{i}" for i in range(2 * n)] + ["rb0"],
        }
    )

    probability = dict(
        calibrate(pl.concat([a, b]), truth=truth)
        .select("row_id", "probability")
        .iter_rows()
    )

    assert probability["a0"] == 1.0
    assert probability[f"a{n}"] == 0.0
    assert probability["b1"] == pytest.approx(30 / 31)


def test_no_labels_anywhere_gives_no_probability() -> None:
    best = _best("A", [("a0", "r0", 10.0)])

    calibrated = calibrate(
        best, truth=pl.DataFrame(schema={"row_id": pl.String, "record_id": pl.String})
    )

    assert calibrated["probability"].to_list() == [None]


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


def _synthetic(*, ids: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    """150 BTR rows of Norway and their CRS records, beside 150 other records. The
    first `ids` rows carry their record's crs id."""
    activities = _activities(300)
    btr = _btr(
        *(
            {
                "row_id": f"b{i:03}",
                "recipient": name,
                "title": title,
                "face_usd": amount,
                "additional_information": f"CRS-ID: 2021{i:06}" if i < ids else None,
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
    btr, crs = _synthetic(ids=100)

    links = link_btr(btr.lazy(), crs=crs.lazy())

    schema.validate(schema.LINKS, links)
    methods = dict(links.group_by("link_method").len().iter_rows())
    assert methods["btr_id"] == 100
    by_model = links.filter(link_method="btr_splink")
    assert by_model.height > 0
    # Each model link joins a row to its own activity's record.
    assert (by_model["left_id"].str.slice(1) == by_model["right_id"].str.slice(1)).all()


def test_raw_em_probabilities_never_link() -> None:
    btr, crs = _synthetic(ids=0)

    links = link_btr(btr.lazy(), crs=crs.lazy())

    assert links.is_empty()


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

    assert pairs.rows() == [("GBR", 40.0, 100.0, 2), ("NOR", 10.0, 25.0, 1)]

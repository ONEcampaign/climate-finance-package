"""Bilateral aggregate rows rebuilt from bulk records, on hand-built rows."""

import polars as pl
import pytest
from normalised import crdf_rows, crs_records

from climate_finance import schema
from climate_finance.build.normalise.aggregates import continent, rebuild_aggregates

KENYA, GHANA, AFRICA, UNSPECIFIED = 248, 238, 298, 998


def _aggregate(row_id: str, recipient: int, climate: float, **fields: object) -> dict:
    """A Norfund-style row: OOF, mitigation principal, no id."""
    return {
        "row_id": row_id,
        "year": 2021,
        "recipient_code": recipient,
        "flow_code": 21,
        "climate_usd": climate,
    } | fields


def _record(
    record_id: str, recipient: int, commitment: float, **fields: object
) -> dict:
    return {
        "record_id": record_id,
        "year": 2021,
        "agency_code": 6,
        "crs_id": f"2021{record_id}",
        "recipient_code": recipient,
        "flow_code": 14,
        "commitment_usd": commitment,
    } | fields


NORFUND = [
    _record("000001", KENYA, 4e6),
    _record("000002", GHANA, 6e6),
    # Unmarked records stay out of the partition.
    _record("000003", KENYA, 2e6, rio_mitigation=0),
    # Core contributions are not the provider's own outflows.
    _record("000004", GHANA, 1e6, bi_multi=2),
]


def _children(crdf: pl.DataFrame, crs: pl.DataFrame) -> pl.DataFrame:
    rebuilt = rebuild_aggregates(crdf.lazy(), crs=crs.lazy()).collect()
    schema.validate(schema.CRDF, rebuilt)
    assert rebuilt["row_id"].is_unique().all()
    assert rebuilt.filter(pl.col("parent_row_id").is_null()).equals(crdf)
    return rebuilt.filter(pl.col("parent_row_id").is_not_null()).sort(
        "parent_row_id", "crs_id"
    )


def _parts(children: pl.DataFrame) -> list[tuple[str, int, str, float]]:
    return [
        (parent, recipient, crs_id, pytest.approx(climate))
        for parent, recipient, crs_id, climate in children.select(
            "parent_row_id", "recipient_code", "crs_id", "climate_usd"
        ).iter_rows()
    ]


def test_records_roll_up_to_their_continents_row() -> None:
    crdf = crdf_rows(_aggregate("africa", AFRICA, 10e6))

    children = _children(crdf, crs_records(*NORFUND))

    assert _parts(children) == [
        ("africa", KENYA, "2021000001", 4e6),
        ("africa", GHANA, "2021000002", 6e6),
    ]
    child = children.row(0, named=True)
    assert child["agency_code"] == 6
    assert child["face_usd"] == pytest.approx(4e6)
    assert (child["flow_code"], child["rio_mitigation"]) == (21, 2)


def test_a_record_goes_to_its_own_recipients_row_first() -> None:
    crdf = crdf_rows(_aggregate("africa", AFRICA, 6e6), _aggregate("kenya", KENYA, 4e6))

    assert _parts(_children(crdf, crs_records(*NORFUND))) == [
        ("africa", GHANA, "2021000002", 6e6),
        ("kenya", KENYA, "2021000001", 4e6),
    ]


def test_records_a_crdf_row_carries_stay_out_of_the_pool() -> None:
    listed = {"row_id": "listed", "year": 2021, "crs_id": "2021000005"}
    crdf = crdf_rows(_aggregate("africa", AFRICA, 10e6), listed)
    crs = crs_records(*NORFUND, _record("000005", KENYA, 5e6))

    assert _children(crdf, crs)["parent_row_id"].unique().to_list() == ["africa"]


def test_two_rows_of_one_recipient_split_direct_and_rolled_up_records() -> None:
    crdf = crdf_rows(_aggregate("p", AFRICA, 3e6), _aggregate("q", AFRICA, 7e6))
    crs = crs_records(_record("000001", KENYA, 7e6), _record("000002", AFRICA, 3e6))

    assert _parts(_children(crdf, crs)) == [
        ("p", AFRICA, "2021000002", 3e6),
        ("q", KENYA, "2021000001", 7e6),
    ]


@pytest.mark.parametrize(("climate", "matches"), [(10.005e6, True), (10.5e6, False)])
def test_a_partition_must_agree_within_tolerance(climate: float, matches: bool) -> None:
    # 0.05% off matches; 5% off matches neither the partition nor the cell.
    crdf = crdf_rows(_aggregate("africa", AFRICA, climate))

    assert _children(crdf, crs_records(*NORFUND)).height == (2 if matches else 0)


def test_rows_a_partition_misses_share_the_cells_records() -> None:
    # 298 gets 4m against 5m and 998 gets 6m against 5m, but the cell's 10m agrees.
    crdf = crdf_rows(
        _aggregate("africa", AFRICA, 5e6),
        _aggregate("unspecified", UNSPECIFIED, 5e6),
    )
    crs = crs_records(
        _record("000001", KENYA, 4e6), _record("000002", UNSPECIFIED, 6e6)
    )

    assert _parts(_children(crdf, crs)) == [
        ("africa", KENYA, "2021000001", 2e6),
        ("africa", UNSPECIFIED, "2021000002", 3e6),
        ("unspecified", KENYA, "2021000001", 2e6),
        ("unspecified", UNSPECIFIED, "2021000002", 3e6),
    ]


def test_a_row_left_over_pairs_with_its_closest_record() -> None:
    # A significant-marked row has no marked records in its cell.
    crdf = crdf_rows(
        _aggregate("ghana", GHANA, 6.03e6, rio_mitigation=1),
        _aggregate("ghana_2", GHANA, 9e6, rio_mitigation=1),
    )
    crs = crs_records(_record("000002", GHANA, 6e6))

    assert _parts(_children(crdf, crs)) == [("ghana", GHANA, "2021000002", 6.03e6)]


@pytest.mark.parametrize(
    "fields",
    [
        {"id_scheme": "none", "provider_code": 918, "agency_code": 3},
        {"bi_multi": 4},
        {"crs_id": "2019000777"},
        {"climate_usd": 0.0},
    ],
    ids=["eib", "multilateral", "known_id", "no_value"],
)
def test_rows_outside_the_population_are_not_rebuilt(fields: dict) -> None:
    crdf = crdf_rows(_aggregate("africa", AFRICA, 10e6, **fields))
    crs = crs_records(
        *NORFUND,
        *(
            _record(
                r["record_id"],
                r["recipient_code"],
                r["commitment_usd"],
                donor_code=918,
                agency_code=3,
            )
            for r in NORFUND[:2]
        ),
        _record("000777", KENYA, 1e6, year=2019, crs_id="2019000777"),
    )

    assert _children(crdf, crs).height == 0


@pytest.mark.parametrize(
    ("recipient", "region"),
    [
        (55, 89),
        (130, 298),
        (1030, 298),
        (248, 298),
        (425, 498),
        (1032, 498),
        (645, 798),
        (589, 798),
        (832, 889),
        (1035, 889),
        (998, 998),
    ],
)
def test_continent_of_a_recipient(recipient: int, region: int) -> None:
    frame = pl.DataFrame({"code": [recipient]}, schema={"code": pl.Int32})

    assert frame.select(continent(pl.col("code"))).item() == region


def test_unmarked_records_do_not_partition() -> None:
    unmarked = {"rio_mitigation": 0}
    crdf = crdf_rows(_aggregate("kenya", KENYA, 2e6, **unmarked))
    crs = crs_records(
        _record("000008", KENYA, 1e6, **unmarked),
        _record("000009", KENYA, 1e6, **unmarked),
    )

    assert _children(crdf, crs).height == 0

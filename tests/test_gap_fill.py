"""The fill stage on hand-built rows, units and ledgers with invented values."""

import shutil
from pathlib import Path

import polars as pl
import pytest
from stage_runs import BUILD_TOML, stage_context, write_fetched_years, write_inputs

from climate_finance import schema
from climate_finance.build.allocate import compute_status
from climate_finance.build.config import BuildConfig, Ledgers, load_config
from climate_finance.build.errors import ConfigError
from climate_finance.build.fetch import IATI_DIR
from climate_finance.build.link.gap_fill import (
    bank_year_overlap,
    fill_from_ledgers,
    keyed_units,
    ledger_flows,
    route_funds,
    routed_records,
    run_fill,
)
from climate_finance.build.stages import raw_dir, table_path

GCF, GEF, IDA, IBRD, ADB, AFDB, GERMANY = 1016, 811, 905, 901, 915, 913, 5
YEARS = (2021, 2022)
LEDGERS = Ledgers(years=YEARS, publishers=("ida",))
FIXTURE_IATI = Path(__file__).parent / "fixtures" / "raw" / "iati"

CRDF_SCHEMA = {
    "row_id": pl.String,
    "provider_code": pl.Int32,
    "year": pl.Int16,
    "project_number": pl.String,
    "title": pl.String,
    "face_usd": pl.Float64,
    "repeat_of": pl.String,
}
LEDGER_SCHEMA = {
    "publisher": pl.String,
    "key": pl.String,
    "activity_id": pl.String,
    "transaction_type": pl.String,
    "year": pl.Int16,
    "currency": pl.String,
    "amount_usd": pl.Float64,
}
UNIT_SCHEMA = {
    "unit_id": pl.String,
    "donor_code": pl.Int32,
    "project_number": pl.String,
    "year": pl.Int16,
    "disbursement_usd": pl.Float64,
}


def _frame(rows: list[tuple], frame_schema: dict) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=frame_schema, orient="row")


def _crdf(rows: list[tuple]) -> pl.DataFrame:
    """Rows of (row_id, provider_code, year, project_number, title, face_usd,
    repeat_of)."""
    return _frame(rows, CRDF_SCHEMA)


def _ledger(rows: list[tuple]) -> pl.DataFrame:
    """Rows of (publisher, key, activity_id, transaction_type, year, currency,
    amount_usd)."""
    return _frame(rows, LEDGER_SCHEMA)


def _status(statuses: dict[str, str], disbursed: dict[str, float] | None = None):
    disbursed = disbursed or {}
    return pl.DataFrame(
        {
            "row_id": list(statuses),
            "status": list(statuses.values()),
            "disbursed_usd": [disbursed.get(row, 0.0) for row in statuses],
        }
    )


# --- GCF and GEF IATI routes

# (row_id, provider_code, year, project_number, title, face_usd, repeat_of)
FUND_ROWS = [
    ("g_partly", GCF, 2021, "FP001 (Grants)", "A project", 100.0, None),
    ("g_full", GCF, 2021, None, "Resilience FP002 programme", 100.0, None),
    ("g_falls", GCF, 2021, "FP003", "A project", 100.0, None),
    ("g_none", GCF, 2021, "FP004", "A project", 100.0, None),
    ("g_unmatched", GCF, 2021, "FP999", "A project", 100.0, None),
    ("g_traced", GCF, 2021, "FP001", "A project", 100.0, None),
    ("g_repeat", GCF, 2022, "FP001", "A project", 100.0, "g_partly"),
    ("e_partly", GEF, 2021, "9001.0", "A project", 200.0, None),
    ("e_none", GEF, 2021, "9002", "A project", 50.0, None),
    ("d_other", GERMANY, 2021, "FP001", "A project", 100.0, None),
]
FUND_STATUS = {row[0]: "not_traceable" for row in FUND_ROWS if row[0] != "g_traced"} | {
    "g_traced": "partly_disbursed"
}
# (publisher, key, activity_id, transaction_type, year, currency, amount_usd)
FUND_ACTIVITIES = [
    ("gcf", "FP001", "a1", "commitment", 2019, "USD", 1000.0),
    ("gcf", "FP001", "a1", "disbursement", 2021, "USD", 300.0),
    ("gcf", "FP001", "a1", "disbursement", 2022, "USD", 200.0),
    ("gcf", "FP002", "a2", "commitment", 2019, "USD", 100.0),
    ("gcf", "FP002", "a2", "disbursement", 2021, "USD", 60.0),
    ("gcf", "FP002", "a2", "disbursement", 2022, "USD", 60.0),
    ("gcf", "FP003", "a3", "commitment", 2019, "USD", 100.0),
    ("gcf", "FP003", "a3", "disbursement", 2021, "USD", 50.0),
    ("gcf", "FP003", "a3", "disbursement", 2022, "USD", -20.0),
    ("gcf", "FP003", "a3", "disbursement", 2023, "USD", 30.0),
    ("gcf", "FP004", "a4", "commitment", 2019, "USD", 100.0),
    # After the last year read.
    ("gcf", "FP001", "a1", "disbursement", 2026, "USD", 400.0),
    ("gef", "9001", "e1", "commitment", 2019, "USD", 1000.0),
    ("gef", "9001", "e1", "disbursement", 2023, "USD", 100.0),
    ("gef", "9002", "e2", "commitment", 2019, "USD", 1000.0),
    ("gef", "9003", "e3", "commitment", 2019, "USD", 1000.0),
]


def _route() -> tuple[pl.DataFrame, pl.DataFrame]:
    return route_funds(
        _crdf(FUND_ROWS),
        commitments=_status(FUND_STATUS),
        activities=_ledger(FUND_ACTIVITIES),
        last_year=2025,
        committed_before=2024,
    )


def test_fund_rows_take_the_rise_in_their_disbursed_share_each_year() -> None:
    allocations, _ = _route()

    by_row_year = {
        (row["row_id"], row["year"]): row["allocated_usd"]
        for row in allocations.iter_rows(named=True)
    }
    assert by_row_year == pytest.approx(
        {
            # 300 then 200 of a 1,000 commitment, on a face of 100.
            ("g_partly", 2021): 30.0,
            ("g_partly", 2022): 20.0,
            # Found by the number in its title; the share caps at 1.
            ("g_full", 2021): 60.0,
            ("g_full", 2022): 40.0,
            # A negative year never lowers the share: 0.5, 0.3, 0.6 reads as 0.5,
            # 0.5, 0.6.
            ("g_falls", 2021): 50.0,
            ("g_falls", 2023): 10.0,
            ("e_partly", 2023): 20.0,
        }
    )
    assert set(allocations["rule"]) == {"iati"}
    assert set(allocations["disbursement_source"]) == {"iati"}
    assert allocations.filter(row_id="g_partly")["record_id"].sort().to_list() == [
        "iati:gcf:FP001:2021",
        "iati:gcf:FP001:2022",
    ]


def test_routed_records_are_the_fund_records_on_a_key_the_route_filled() -> None:
    allocations, _ = _route()
    crs = pl.LazyFrame(
        [
            # FP001 and FP002 (by title) are GCF keys the route filled, and 9001 the
            # GEF's, written without the CRDF's float artefact.
            ("gcf_number", GCF, "FP001", "A project"),
            ("gcf_title", GCF, None, "Phase 2 of FP002"),
            ("gef_number", GEF, "9001", "A project"),
            # FP004 matches an activity with no disbursement, FP999 none, and the
            # readiness grant carries no funded activity number.
            ("gcf_undisbursed", GCF, "FP004", "A project"),
            ("gcf_unmatched", GCF, "FP999", "A project"),
            ("gcf_unkeyed", GCF, "MLI-RS-001", "Readiness"),
            ("gef_undisbursed", GEF, "9002", "A project"),
            # FP001 is a GCF key, and Germany is no fund.
            ("germany", GERMANY, "FP001", "A project"),
        ],
        schema={
            "record_id": pl.String,
            "donor_code": pl.Int32,
            "project_number": pl.String,
            "title": pl.String,
        },
        orient="row",
    )

    routed = routed_records(
        crs, allocations=allocations.lazy(), crdf=_crdf(FUND_ROWS).lazy()
    ).collect()

    assert sorted(routed["record_id"]) == ["gcf_number", "gcf_title", "gef_number"]


def test_fund_rows_take_their_status_from_the_iati_share() -> None:
    allocations, basis = _route()
    commitments = (
        _crdf(FUND_ROWS)
        .join(_status(FUND_STATUS), on="row_id")
        .select(
            "row_id",
            "face_usd",
            climate_usd="face_usd",
            traceable=pl.lit(False),
            id_disbursing=pl.lit(False),
            # A CRS face the route replaces with the row's own.
            crs_face_usd=pl.col("face_usd") * 3,
        )
        .update(basis, on="row_id")
    )

    status = dict(
        compute_status(commitments=commitments, allocations=allocations)
        .select("row_id", "status")
        .collect()
        .iter_rows()
    )

    assert set(basis["row_id"]) == {
        "g_partly", "g_full", "g_falls", "g_none", "g_unmatched", "e_partly", "e_none",
    }  # fmt: skip
    assert status == {
        "g_partly": "partly_disbursed",
        "g_full": "disbursed",
        "g_falls": "partly_disbursed",
        # The GCF shows disbursement on three of its four activities.
        "g_none": "no_disbursement_record",
        "g_unmatched": "not_traceable",
        "e_partly": "partly_disbursed",
        # The GEF shows disbursement on one of its three: it does not report.
        "e_none": "not_traceable",
        # Rows the route leaves alone keep what allocation gave them.
        "g_traced": "not_traceable",
        "g_repeat": "not_traceable",
        "d_other": "not_traceable",
    }


# --- The overlap test

KEYS = 30


def _overlap_inputs(
    *, keys: int = KEYS, ledger_amounts: dict[int, float] | None = None
) -> tuple[list[tuple], list[tuple]]:
    """IDA credits disbursing 100 each in 2021 in the CRS, and in the ledger by
    default."""
    ledger_amounts = ledger_amounts or {}
    units = [
        (f"u{i}", IDA, f"P{i:06d}.IDA{i:05d}.CRS1", 2021, 100.0) for i in range(keys)
    ]
    ledger = [
        ("ida", f"IDA{i:05d}", f"IDA{i:05d}", "disbursement", 2021, "USD",
         ledger_amounts.get(i, 100.0))
        for i in range(keys)
    ]  # fmt: skip
    return units, ledger


def _overlap(units: list[tuple], ledger: list[tuple]) -> dict:
    result = bank_year_overlap(
        _frame(units, UNIT_SCHEMA).lazy(),
        ledger=_ledger(ledger).lazy(),
        years=YEARS,
    ).collect()
    schema.validate(schema.BANK_YEAR_OVERLAP, result)
    return result.filter(provider_code=IDA, year=2021).row(0, named=True)


def test_a_bank_year_whose_ledger_agrees_passes() -> None:
    row = _overlap(*_overlap_inputs())

    assert row["keys"] == KEYS
    assert row["crs_usd"] == row["ledger_usd"] == 3000.0
    assert row["share_keys_within_10pct"] == 1.0
    assert row["passed"]


def test_keys_the_ledger_shows_no_disbursement_on_stay_out_of_the_test() -> None:
    units, ledger = _overlap_inputs()
    # Three credits the ledger lists by their commitment alone.
    for i in range(KEYS, KEYS + 3):
        units.append((f"u{i}", IDA, f"P{i:06d}.IDA{i:05d}.CRS1", 2021, 100.0))
        ledger.append(
            ("ida", f"IDA{i:05d}", f"IDA{i:05d}", "commitment", 2019, "USD", 500.0)
        )

    row = _overlap(units, ledger)

    assert row["keys"] == KEYS
    assert row["passed"]


def _too_few_keys() -> tuple[list[tuple], list[tuple]]:
    return _overlap_inputs(keys=KEYS - 1)


def _total_off() -> tuple[list[tuple], list[tuple]]:
    return _overlap_inputs(ledger_amounts=dict.fromkeys(range(KEYS), 106.0))


def _keys_off() -> tuple[list[tuple], list[tuple]]:
    # Seven of 30 keys 20% high: the total is 4.7% high, 77% of keys agree.
    return _overlap_inputs(ledger_amounts=dict.fromkeys(range(7), 120.0))


def _unkeyed() -> tuple[list[tuple], list[tuple]]:
    units, ledger = _overlap_inputs()
    # 6% of the year's CRS disbursement under a project number with no credit.
    units.append(("u_bare", IDA, "P999999", 2021, 192.0))
    return units, ledger


@pytest.mark.parametrize("inputs", [_too_few_keys, _total_off, _keys_off, _unkeyed])
def test_a_bank_year_fails_on_each_threshold(inputs) -> None:
    assert not _overlap(*inputs())["passed"]


def test_adb_keys_with_a_local_currency_leg_and_unconverted_amounts_do_not_count() -> (
    None
):
    ledger = _ledger(
        [
            ("asdb", "L1", "a", "disbursement", 2021, "USD", 10.0),
            ("asdb", "L1", "a", "disbursement", 2022, "INR", 5.0),
            ("asdb", "L2", "b", "disbursement", 2021, "USD", 7.0),
            ("asdb", "L2", "b", "disbursement", 2021, "USD", -2.0),
            ("ebrd", "E1", "c", "disbursement", 2021, "EUR", None),
            ("ebrd", "E1", "c", "disbursement", 2021, "EUR", 3.0),
        ]
    ).lazy()

    flows = ledger_flows(ledger, years=YEARS).collect()

    assert sorted(flows.select("key", "ledger_usd").iter_rows()) == [
        ("E1", 3.0),
        ("L2", 7.0),
    ]


def test_keys_are_usable_when_they_can_name_a_ledger_activity() -> None:
    units = _frame(
        [
            ("afdb_id", AFDB, "2100150000006", 2021, 1.0),
            ("afdb_truncated", AFDB, "2100150000000", 2021, 1.0),
            ("afdb_other", AFDB, "P-Z1-AAA-001", 2021, 1.0),
            ("gcf_project", GCF, "FP001 (Grants)", 2021, 1.0),
            ("gcf_readiness", GCF, "MLI-RS-001", 2021, 1.0),
            ("gcf_other", GCF, "PWC", 2021, 1.0),
        ],
        UNIT_SCHEMA,
    ).lazy()
    ledger = _ledger(
        [
            ("afdb", "x", "x", "disbursement", 2021, "XDR", None),
            ("gcf", "x", "x", "disbursement", 2021, "USD", 1.0),
        ]
    ).lazy()

    keyed = keyed_units(units, ledger=ledger).select("unit_id", "key", "usable")

    assert sorted(keyed.collect().rows()) == [
        ("afdb_id", "2100150000006", True),
        ("afdb_other", "P-Z1-AAA-001", False),
        ("afdb_truncated", "2100150000000", False),
        ("gcf_other", None, False),
        ("gcf_project", "FP001", True),
        ("gcf_readiness", "MLI-RS-001", True),
    ]


# --- The bank-ledger fill

# (row_id, provider_code, year, project_number, title, face_usd, repeat_of)
GAP_ROWS = [
    # Two gaps and a partly disbursed row on one credit.
    ("a1", IDA, 2020, "P000100.IDA00100.CRS1", None, 60.0, None),
    ("a2", IDA, 2020, "P000100.IDA00100.CRS2", None, 40.0, None),
    ("a3", IDA, 2019, "P000100.IDA00100.CRS3", None, 80.0, None),
    # The ledger gives more than the face value.
    ("b", IDA, 2020, "P000200.IDA00200.CRS1", None, 10.0, None),
    # The CRS records disbursement on the credit in the window.
    ("c", IDA, 2020, "P000300.IDA00300.CRS1", None, 10.0, None),
    # Two ledger activities carry the key.
    ("d", IDA, 2020, "P000400.IDA00400.CRS1", None, 10.0, None),
    # The bank-year fails the overlap test.
    ("e", IBRD, 2020, "P000500.IBRD00500.CRS1", None, 10.0, None),
    # Committed after the ledger's disbursement.
    ("f", IDA, 2022, "P000700.IDA00700.CRS1", None, 10.0, None),
    # A repeat, and a provider with no ledger.
    ("h", IDA, 2021, "P000100.IDA00100.CRS1", None, 60.0, "a1"),
    ("g", GERMANY, 2020, "DE1", None, 10.0, None),
]
GAP_STATUS = {row[0]: "no_disbursement_record" for row in GAP_ROWS} | {
    "a3": "partly_disbursed",
    "c": "not_traceable",
}
GAP_LEDGER = [
    ("ida", "IDA00100", "x100", "disbursement", 2021, "USD", 30.0),
    ("ida", "IDA00100", "x100", "disbursement", 2022, "USD", 20.0),
    ("ida", "IDA00200", "x200", "disbursement", 2021, "USD", 50.0),
    ("ida", "IDA00300", "x300", "disbursement", 2021, "USD", 5.0),
    ("ida", "IDA00400", "x400", "disbursement", 2021, "USD", 5.0),
    ("ida", "IDA00400", "x401", "disbursement", 2021, "USD", 5.0),
    ("ibrd", "IBRD00500", "x500", "disbursement", 2021, "USD", 5.0),
    ("ida", "IDA00700", "x700", "disbursement", 2021, "USD", 5.0),
    ("ida", "DE1", "x800", "disbursement", 2021, "USD", 5.0),
]
GAP_UNITS = [
    ("u300", IDA, "P000300.IDA00300.CRS1", 2022, 4.0),
    # Before the window, so it does not block the credit.
    ("u100", IDA, "P000100.IDA00100.CRS3", 2019, 10.0),
]
GAP_OVERLAP = pl.DataFrame(
    {
        "provider_code": [IDA, IDA, IBRD, IBRD],
        "year": [2021, 2022, 2021, 2022],
        "passed": [True, True, False, False],
    },
    schema_overrides={"provider_code": pl.Int32, "year": pl.Int16},
)


def _fill() -> pl.DataFrame:
    return fill_from_ledgers(
        _crdf(GAP_ROWS),
        commitments=_status(GAP_STATUS, disbursed={"a3": 10.0}),
        units=_frame(GAP_UNITS, UNIT_SCHEMA).lazy(),
        ledger=_ledger(GAP_LEDGER).lazy(),
        overlap=GAP_OVERLAP,
        years=YEARS,
    )


def test_gaps_take_the_ledger_after_their_siblings_share_by_face_and_year() -> None:
    fills = _fill()

    by_row_year = {
        (row["row_id"], row["year"]): row["allocated_usd"]
        for row in fills.iter_rows(named=True)
    }
    # IDA00100: 30 + 20 in the ledger less the 10 a3 took, 40 over faces 60 and 40.
    assert by_row_year == pytest.approx(
        {
            ("a1", 2021): 24 * 30 / 50,
            ("a1", 2022): 24 * 20 / 50,
            ("a2", 2021): 16 * 30 / 50,
            ("a2", 2022): 16 * 20 / 50,
            ("b", 2021): 10.0,
        }
    )
    assert set(fills["rule"]) == set(fills["disbursement_source"]) == {"bank_ledger"}
    assert fills.filter(row_id="b")["record_id"].to_list() == [
        "ledger:ida:IDA00200:2021"
    ]


def test_a_failed_bank_year_gets_no_fill() -> None:
    passing = GAP_OVERLAP.with_columns(passed=pl.lit(True))

    fills = fill_from_ledgers(
        _crdf(GAP_ROWS),
        commitments=_status(GAP_STATUS, disbursed={"a3": 10.0}),
        units=_frame(GAP_UNITS, UNIT_SCHEMA).lazy(),
        ledger=_ledger(GAP_LEDGER).lazy(),
        overlap=passing,
        years=YEARS,
    )

    assert "e" not in _fill()["row_id"].to_list()
    assert "e" in fills["row_id"].to_list()


# --- The stage


def _stage_release(tmp_path: Path) -> Path:
    """A release with the allocate stage's tables for a GCF row, a GEF row and two
    IDA rows, the IDA ledger and the fixture GCF and GEF IATI files."""
    rows = _crdf(
        [
            ("gcf_row", GCF, 2021, "FP049 (Grants)", "A project", 100.0, None),
            ("gef_row", GEF, 2021, "9734", "A project", 100.0, None),
            ("ida_gap", IDA, 2020, "P000100.IDA00100.CRS1", None, 50.0, None),
            ("ida_crs", IDA, 2020, "P000200.IDA00200.CRS1", None, 50.0, None),
        ]
    ).with_columns(
        perspective=pl.lit("recipient"), parent_row_id=pl.lit(None, pl.String)
    )
    statuses = {
        "gcf_row": "not_traceable",
        "gef_row": "not_traceable",
        "ida_gap": "no_disbursement_record",
        "ida_crs": "partly_disbursed",
    }
    allocations = pl.DataFrame(
        {
            "row_id": ["ida_crs", None],
            "record_id": ["r1", "r1"],
            "year": [2021, 2021],
            "allocated_usd": [20.0, 5.0],
            "allocated_low_usd": [20.0, 5.0],
            "rule": ["fifo", "excess"],
            "disbursement_source": ["crs", "crs"],
            "link_method": ["crs_id", None],
            "commitment_unit_id": ["u-commitment", None],
        },
        schema_overrides={"year": pl.Int16},
    )
    basis = rows.select(
        "row_id",
        "face_usd",
        climate_usd="face_usd",
        traceable=pl.col("row_id").is_in(["ida_gap", "ida_crs"]),
        id_disbursing=pl.lit(False),
        crs_face_usd="face_usd",
    )
    commitments = compute_status(commitments=basis, allocations=allocations).collect()
    assert dict(commitments.select("row_id", "status").iter_rows()) == statuses
    units = [
        (f"u{i}", IDA, f"P{i:06d}.IDA{i:05d}.CRS1", year, 100.0)
        for i in range(300, 300 + KEYS)
        for year in YEARS
    ] + [("u200", IDA, "P000200.IDA00200.CRS1", 2021, 25.0)]
    ledger = [
        ("ida", f"IDA{i:05d}", f"IDA{i:05d}", "disbursement", year, "USD", 100.0)
        for i in range(300, 300 + KEYS)
        for year in YEARS
    ] + [("ida", "IDA00100", "x100", "disbursement", 2022, "USD", 30.0)]
    write_inputs(tmp_path, "normalise", {"crdf": rows, "ledger": _ledger(ledger)})
    write_inputs(tmp_path, "link", {"units": _frame(units, UNIT_SCHEMA)})
    write_inputs(
        tmp_path, "allocate", {"allocations": allocations, "commitments": commitments}
    )
    for publisher in ("gcf", "gef"):
        shutil.copytree(
            FIXTURE_IATI / publisher, raw_dir(tmp_path) / IATI_DIR / publisher
        )
    write_fetched_years(tmp_path, ledgers=LEDGERS)
    return tmp_path


def _config(**sources: object) -> BuildConfig:
    base = load_config(BUILD_TOML)
    return base.model_copy(
        update={
            "gap_fill": True,
            "sources": base.sources.model_copy(update={"ledgers": LEDGERS} | sources),
        }
    )


def _run(release_dir: Path, config: BuildConfig) -> dict[str, pl.DataFrame]:
    context = stage_context(release_dir, config=config)
    return {table: frame.lazy().collect() for table, frame in run_fill(context).items()}


def test_the_stage_routes_fills_and_conserves_every_amount(tmp_path: Path) -> None:
    release_dir = _stage_release(tmp_path)
    before = pl.read_parquet(table_path(release_dir, "allocate", "allocations"))

    output = _run(release_dir, _config())

    for table, frame in output.items():
        schema.validate(schema.STAGE_TABLES["fill"][table], frame)
    allocations, commitments = output["allocations"], output["commitments"]
    # CRS allocations are untouched.
    assert allocations.filter(disbursement_source="crs").equals(before)
    sources = dict(
        allocations.group_by("row_id").agg(pl.col("disbursement_source").first()).rows()
    )
    assert sources == {
        "gcf_row": "iati",
        "gef_row": "iati",
        "ida_gap": "bank_ledger",
        "ida_crs": "crs",
        None: "crs",
    }
    # Each commitment's disbursed amount is its allocations, within its face value.
    totals = commitments.join(
        allocations.filter(pl.col("rule") != "excess")
        .group_by("row_id")
        .agg(allocated=pl.col("allocated_usd").sum()),
        on="row_id",
    )
    assert (totals["disbursed_usd"] - totals["allocated"]).abs().max() < 1e-6
    assert (totals["disbursed_usd"] <= totals["face_usd"]).all()
    assert output["bank_year_overlap"]["passed"].all()
    assert commitments.filter(row_id="ida_gap")["status"].item() == "partly_disbursed"


@pytest.mark.parametrize(
    "sources",
    [{"ledgers": None}, {"iati_publishers": ("gcf",)}],
    ids=["no ledgers", "no GEF IATI files"],
)
def test_the_stage_refuses_a_config_it_cannot_fill_from(
    tmp_path: Path, sources: dict[str, object]
) -> None:
    with pytest.raises(ConfigError):
        _run(_stage_release(tmp_path), _config(**sources))

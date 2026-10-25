"""The assemble stage on hand-built normalised tables, run through the stage runner
from link to assemble, and the fill and review pass-through."""

import json
import re
import shutil
import tarfile
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path

import numpy as np
import polars as pl
import pyarrow as pa
import pytest
import yaml
from normalised import crdf_rows, crs_records
from stage_runs import (
    BUILD_TOML,
    registered,
    stage_context,
    write_recorded,
    year_rows,
)

import climate_finance as cf
from climate_finance import schema
from climate_finance.build import (
    BuildConfig,
    load_config,
    provenance,
    release,
    run_stages,
    write_table,
)
from climate_finance.build.allocate import (
    allocate,
    allocate_to_rows,
    counted_rows,
    marked_records,
    untraced_records,
)
from climate_finance.build.errors import (
    ConfigError,
    ReleaseInvariantError,
    StaleStageError,
)
from climate_finance.build.fetch import YEAR_SOURCES
from climate_finance.build.link.gap_fill import fill_allocations, routed_records
from climate_finance.build.link.keys import build_units, link_by_keys
from climate_finance.build.normalise.crs import trace_population
from climate_finance.build.provenance import StageRecord, record_path
from climate_finance.build.stages import REGISTRY, Stage, StageContext, table_path
from climate_finance.errors import SchemaError
from climate_finance.schema import STAGE_TABLES

NOR, GEF, IDA_CHANNEL = 8, 811, 44002
# Norway commits 100 in 2019 under crs id X1 and disburses 60 in 2020 and 70 in 2021,
# so the commitment fills and 30 of 2021 is excess. Y1 commits 50 with no CRDF row
# and disburses 20 to it; Z9 disburses 15 with no commitment, and Z8 disburses 5
# with no Rio marker. The GEF, a fund the fill stage can route through its IATI
# files, disburses 7 under G1 with no commitment.
CRS = crs_records(
    {"record_id": "c19", "year": 2019, "crs_id": "X1", "commitment_usd": 100.0},
    {"record_id": "d20", "year": 2020, "crs_id": "X1", "disbursement_usd": 60.0},
    {
        "record_id": "d21",
        "year": 2021,
        "crs_id": "X1",
        "disbursement_usd": 70.0,
        "grant_equiv_usd": 35.0,
    },
    {
        "record_id": "core",
        "year": 2021,
        "bi_multi": 2,
        "disbursement_usd": 9.0,
        "rio_mitigation": 0,
    },
    {"record_id": "y19", "year": 2019, "crs_id": "Y1", "commitment_usd": 50.0},
    {"record_id": "y20", "year": 2020, "crs_id": "Y1", "disbursement_usd": 20.0},
    {"record_id": "z20", "year": 2020, "crs_id": "Z9", "disbursement_usd": 15.0},
    {
        "record_id": "g20",
        "year": 2020,
        "donor_code": GEF,
        "crs_id": "G1",
        "disbursement_usd": 7.0,
    },
    {
        "record_id": "u20",
        "year": 2020,
        "crs_id": "Z8",
        "disbursement_usd": 5.0,
        "rio_mitigation": 0,
    },
)
# R1 is the commitment; R2 repeats it in a later vintage; P is an aggregate row
# rebuilt into child C; N is a published downward revision.
CRDF = crdf_rows(
    {
        "row_id": "R1",
        "year": 2019,
        "crs_id": "X1",
        "climate_usd": 40.0,
        "face_usd": 1e2,
    },
    {
        "row_id": "R2",
        "year": 2019,
        "vintage": 2020,
        "climate_usd": 40.0,
        "repeat_of": "R1",
    },
    {"row_id": "P", "year": 2019, "climate_usd": 25.0},
    {"row_id": "C", "year": 2019, "climate_usd": 25.0, "parent_row_id": "P"},
    {"row_id": "N", "year": 2019, "climate_usd": -5.0},
)
CORE = pl.DataFrame(
    {
        "provider_code": [NOR, NOR],
        "channel_code": [IDA_CHANNEL, IDA_CHANNEL],
        "year": [2021, 2021],
        "flow": ["commitments", "disbursements"],
        "contribution_usd": [12.0, 10.0],
    },
    schema=schema.CORE_CONTRIBUTIONS.dtypes,
)
LATER_STAGES = ("link", "allocate", "fill", "review", "label", "assemble")
_DISBURSED = ("disbursed_usd", "disbursed_low_usd", "disbursed_high_usd")
SOURCES = release.SOURCES_FILE


def _empty(table: schema.TableSchema) -> pl.DataFrame:
    return pl.DataFrame(schema=table.dtypes)


def _write(release_dir: Path, stage: str, frames: dict[str, pl.DataFrame]) -> None:
    for name, frame in frames.items():
        table = STAGE_TABLES[stage][name]
        write_table(frame, table, table_path(release_dir, stage, name))


def _raw_manifest(version: str) -> pl.DataFrame:
    crs = pl.DataFrame(
        {
            "source": ["crs"],
            "url": [None],
            "path": ["oecd/CRS.parquet"],
            "sha256": ["0" * 64],
            "etag": [None],
            "version": [version],
            "fetched_at": [datetime(2026, 10, 21, tzinfo=UTC)],
            "method": ["manual"],
            "first_year": [2010],
            "last_year": [2025],
            "held_first_year": [2010],
            "held_last_year": [2025],
        },
        schema=schema.RAW_MANIFEST.dtypes,
    )
    return pl.concat([crs, year_rows().filter(pl.col("source") != "crs")])


@pytest.fixture
def config(tmp_path: Path) -> BuildConfig:
    # The hand-written tables hold no truth group to set Jev's scale. No pair has
    # been decided yet.
    decisions = tmp_path / "decisions" / "review_decisions.parquet"
    decisions.parent.mkdir()
    _empty(schema.REVIEW_DECISIONS).write_parquet(decisions)
    config = load_config(BUILD_TOML)
    sources = config.sources.model_copy(
        update={"review_decisions": decisions, "btr_archive": None}
    )
    return config.model_copy(
        update={
            "key_repair": False,
            "recoveries": False,
            "jev_links": False,
            "sources": sources,
        }
    )


def _measure() -> dict[str, pl.DataFrame]:
    return {
        "agency_shares": _empty(schema.AGENCY_SHARES),
        # One share year, so release.json records agency_spending's years.
        "agency_spending": pl.DataFrame(
            [
                {
                    "channel_code": IDA_CHANNEL,
                    "year": 2022,
                    "rio_adaptation": 0,
                    "rio_mitigation": 2,
                    "published": 1.0,
                    "face_share": 0.5,
                    "adaptation_measured": True,
                    "share_source": "computed",
                },
            ],
            schema=schema.AGENCY_SPENDING.dtypes,
        ),
        "modelled": _empty(schema.MODELLED),
    }


@pytest.fixture
def release_dir(tmp_path: Path, config: BuildConfig) -> Path:
    """A release built from hand-written fetch, normalise and measure tables."""
    write_recorded(
        tmp_path,
        "fetch",
        {"raw_manifest": _raw_manifest("v20261006")},
        config=config,
    )
    write_recorded(
        tmp_path,
        "normalise",
        {
            "crdf": CRDF,
            "crdf_span": _empty(schema.CRDF_SPAN),
            "crs": CRS,
            "core_contributions": CORE,
            "imputed_shares": _empty(schema.IMPUTED_SHARES),
            "external": _empty(schema.EXTERNAL),
        },
        config=config,
        reads=("fetch",),
    )
    write_recorded(tmp_path, "measure", _measure(), config=config)
    run_stages(
        REGISTRY, release="r", release_dir=tmp_path, config=config, stages=LATER_STAGES
    )
    return tmp_path


def _read(release_dir: Path, table: str) -> pl.DataFrame:
    return pl.read_parquet(table_path(release_dir, "assemble", table))


def test_the_release_opens_and_matches_release_json(release_dir: Path) -> None:
    snapshot = cf.load(path=release_dir / "assemble")

    assert snapshot.manifest.source_versions == {
        source: "v20261006" if source == "crs" else "0" * 64 for source in YEAR_SOURCES
    }
    assert snapshot.manifest.years == {
        "crs": (2010, 2025),
        "crdf_recipient": (2015, 2024),
        "crdf_provider": (2015, 2024),
        "multisystem": (2015, 2024),
        # The years core_contributions and agency_spending hold; agency_shares
        # holds none.
        "core_contributions": (2021, 2021),
        "agency_spending": (2022, 2022),
    }
    assert set(snapshot.manifest.tables) == set(schema.RELEASE_TABLES)
    # The config fixture turns key repair and recoveries off.
    assert snapshot.manifest.settings == {
        "offline": False,
        "key_repair": False,
        "recoveries": False,
        "gap_fill": False,
        "review": False,
        "jev_labels": False,
        "jev_links": False,
        "embed": False,
        "skip_eib_routes": [],
        # The settings as configured; `years` above holds what they resolved to.
        "years.crdf": "all",
        "years.crs": None,
        "years.crs_last_complete": 2024,
    }


def test_commitments_leave_out_parents_and_keep_repeats_and_revisions(
    release_dir: Path,
) -> None:
    commitments = _read(release_dir, "commitments")

    assert sorted(commitments["row_id"]) == ["C", "N", "R1", "R2"]
    r1 = commitments.row(by_predicate=pl.col("row_id") == "R1", named=True)
    assert (r1["status"], r1["disbursed_usd"]) == ("disbursed", 100.0)


def test_commitments_carry_the_ledgers_undisbursed_confirmation() -> None:
    # R1 reaches one unit the ledger confirms undisbursed; C reaches two, of which
    # the ledger confirms one.
    links = pl.LazyFrame(
        {
            "task": ["crdf_crs"] * 3 + ["trace"],
            "left_id": ["R1", "C", "C", "u-d20"],
            "right_id": ["u1", "u2", "u3", "u1"],
            "ledger_check": ["confirmed_undisbursed"] * 2 + [None, "confirmed"],
        }
    )
    status = _empty(STAGE_TABLES["review"]["commitments"]).lazy()

    withheld = _empty(schema.DISBURSEMENTS).lazy()

    rows = release.release_commitments(
        CRDF.lazy(), status=status, links=links, withheld=withheld
    )

    assert dict(rows.select("row_id", "ledger_undisbursed").collect().rows()) == {
        "C": None,
        "N": None,
        "R1": True,
        "R2": None,
    }


def test_disbursements_carry_the_record_and_its_link_and_leave_out_excess(
    release_dir: Path,
) -> None:
    rows = _read(release_dir, "disbursements").sort("record_id")

    assert rows.select("record_id", "allocated_usd").rows() == [
        ("d20", 60.0),
        ("d21", 40.0),
    ]
    assert rows["record_disbursement_usd"].to_list() == [60.0, 70.0]
    assert rows["record_grant_equiv_usd"].to_list() == [None, 35.0]
    assert rows["link_method"].to_list() == ["crs_id_purpose"] * 2
    assert rows["link_probability"].to_list() == [1.0, 1.0]
    assert rows["provider_code"].to_list() == [NOR, NOR]


def test_a_fill_takes_its_commitments_facts_and_its_records_total() -> None:
    # One ledger key and year fills R1 and C, which have no CRS record behind them.
    fills = pl.LazyFrame(
        {
            "row_id": ["R1", "C"],
            "record_id": ["ledger:ida:IDA1:2021"] * 2,
            "year": [2021, 2021],
            "allocated_usd": [6.0, 4.0],
            "allocated_low_usd": [6.0, 4.0],
            "allocated_high_usd": [6.0, 4.0],
            "rule": ["bank_ledger"] * 2,
            "disbursement_source": ["bank_ledger"] * 2,
        },
        schema_overrides={"year": pl.Int16},
    )

    rows = release.fill_disbursements(fills, crdf=CRDF.lazy()).collect().sort("row_id")

    expected = CRDF.filter(pl.col("row_id").is_in(["C", "R1"])).sort("row_id")
    for column in ("provider_code", "recipient_code", "bi_multi"):
        assert rows[column].to_list() == expected[column].to_list(), column
    assert rows["flow_code"].is_null().all()
    assert rows["record_disbursement_usd"].to_list() == [10.0, 10.0]
    assert rows["record_grant_equiv_usd"].is_null().all()
    assert rows["link_method"].is_null().all()
    assert rows.columns == list(schema.DISBURSEMENTS.columns)


def test_a_disbursement_carries_the_ledger_check_of_the_link_that_filled_it() -> None:
    # One CRDF row covers m1 and m2. d22 repeats m2's date and title, so its own
    # commitment m2 takes all 60, and the row carries the ledger's check of that
    # link, though m1's unit sorts first.
    crs = crs_records(
        {
            "record_id": "m1",
            "year": 2021,
            "crs_id": "X2",
            "commitment_usd": 100.0,
            "commitment_date": date(2021, 6, 30),
            "title": "Module one: rural water",
        },
        {
            "record_id": "m2",
            "year": 2022,
            "crs_id": "X2",
            "commitment_usd": 100.0,
            "commitment_date": date(2022, 6, 30),
            "title": "Module two: feeder roads",
        },
        {
            "record_id": "d22",
            "year": 2022,
            "crs_id": "X2",
            "disbursement_usd": 60.0,
            "commitment_date": date(2022, 6, 30),
            "title": "Feeder roads, module two",
        },
    )
    crdf = counted_rows(
        crdf_rows(
            {"row_id": "R", "year": 2021, "crs_id": "X2", "face_usd": 200.0}
        ).lazy()
    )
    units = build_units(crs.lazy()).collect()
    m1, m2 = (
        units.filter(pl.col("record_ids").list.contains(record))["unit_id"].item()
        for record in ("m1", "m2")
    )
    assert m1 < m2
    trace = link_by_keys(units, first_year=2010).with_columns(
        ledger_check=pl.when(pl.col("right_id") == m2).then(pl.lit("confirmed"))
    )
    links = pl.concat(
        [
            trace,
            trace.select(
                task=pl.lit("crdf_crs"),
                left_id=pl.lit("R"),
                right_id="right_id",
                group_id=pl.lit("g"),
                probability=pl.lit(1.0),
                link_method=pl.lit("within_year"),
            ),
        ],
        how="diagonal",
    ).lazy()
    population = trace_population(crs.lazy())
    pairs = allocate(units.lazy(), crs=population, links=links, crdf=crdf)
    allocations = allocate_to_rows(
        pairs, crdf=crdf, units=units.lazy(), crs=population, links=links
    )

    rows = release.release_disbursements(
        allocations.lazy(), crdf=crdf, crs=crs.lazy(), units=units.lazy(), links=links
    ).collect()

    assert rows.select("row_id", "allocated_usd", "ledger_check").rows() == [
        ("R", 60.0, "confirmed")
    ]


def test_quality_counts_repeats_on_their_own_line(release_dir: Path) -> None:
    quality = _read(release_dir, "quality").row(0, named=True)

    # R1, C and N count; R2 repeats R1.
    assert quality["commitments"] == 3
    assert quality["climate_usd"] == 40.0 + 25.0 - 5.0
    assert quality["linked_usd"] == 40.0
    assert quality["repeat_climate_usd"] == 40.0
    # R1 reaches its CRS commitment in its own year.
    assert quality["certain_route_usd"] == 40.0
    # Of R1's 100 allocated, d21's 40 has a grant equivalent.
    assert quality["grant_equiv_coverage"] == 0.4
    # R1's 100 disbursed came through certain links, at its 40% climate share.
    assert quality["disbursed_climate_usd"] == 40.0
    assert quality["certain_disbursed_climate_usd"] == 40.0
    # With d21's 40 estimated, only d20's 60 counts.
    estimated = _read(release_dir, "disbursements").with_columns(
        allocated_low_usd=pl.when(pl.col("record_id") == "d21")
        .then(0.0)
        .otherwise(pl.col("allocated_low_usd"))
    )
    rebuilt = release.quality(
        _read(release_dir, "commitments").lazy(),
        disbursements=estimated.lazy(),
        objectives=_read(release_dir, "objectives").lazy(),
        links=pl.scan_parquet(table_path(release_dir, "review", "links")),
    ).collect()
    assert rebuilt.select("disbursed_climate_usd", "certain_disbursed_climate_usd").row(
        0
    ) == pytest.approx((40.0, 24.0))


def test_the_release_leaves_out_gcf_rows_and_keeps_what_they_disbursed(
    release_dir: Path, config: BuildConfig, tmp_path: Path
) -> None:
    # In 2021 the GCF's ledger fills C with 10 and its CC0 IATI files with 5.
    fills = pl.DataFrame(
        {
            "row_id": ["C", "C"],
            "record_id": ["ledger:gcf:FP001:2021", "iati:gcf:FP001:2021"],
            "year": [2021, 2021],
            "allocated_usd": [10.0, 5.0],
            "allocated_low_usd": [10.0, 5.0],
            "allocated_high_usd": [10.0, 5.0],
            "rule": ["bank_ledger", "iati"],
            "disbursement_source": ["bank_ledger", "iati"],
            "link_method": [None, None],
            "commitment_unit_id": [None, None],
        },
        schema_overrides={
            "year": pl.Int16,
            "link_method": pl.String,
            "commitment_unit_id": pl.String,
        },
    )
    allocations = pl.read_parquet(table_path(release_dir, "fill", "allocations"))
    _write(release_dir, "fill", {"allocations": pl.concat([allocations, fills])})
    # The label stage labels a ledger fill of a commitment with a climate component.
    objectives = pl.read_parquet(table_path(release_dir, "label", "objectives"))
    gcf_label = objectives.head(1).with_columns(
        record_id=pl.lit("ledger:gcf:FP001:2021")
    )
    _write(release_dir, "label", {"objectives": pl.concat([objectives, gcf_label])})
    is_c = pl.col("row_id") == "C"
    status = pl.read_parquet(table_path(release_dir, "review", "commitments"))
    _write(
        release_dir,
        "review",
        {
            "commitments": status.with_columns(
                status=pl.when(is_c)
                .then(pl.lit("partly_disbursed"))
                .otherwise("status"),
                **{
                    column: pl.when(is_c).then(15.0).otherwise(column)
                    for column in _DISBURSED
                },
            )
        },
    )
    manifest = pl.read_parquet(table_path(release_dir, "fetch", "raw_manifest"))
    _write(
        release_dir,
        "fetch",
        {
            "raw_manifest": pl.concat(
                [
                    manifest,
                    *(
                        manifest.with_columns(source=pl.lit(source))
                        for source in ("gcf_projects", "iati_gcf")
                    ),
                ]
            )
        },
    )

    run_stages(
        REGISTRY, release="r", release_dir=release_dir, config=config,
        stages=["assemble"],
    )  # fmt: skip
    pack = release.pack_release(release_dir, out_path=tmp_path / "r.tar.zst")

    with (
        pa.input_stream(str(pack), compression="zstd") as stream,
        tarfile.open(fileobj=stream, mode="r|") as tar,
    ):
        packed = {Path(m.name).name: tar.extractfile(m).read() for m in tar}
    disbursements = pl.read_parquet(packed["disbursements.parquet"])
    assert disbursements.filter(is_c)["record_id"].to_list() == ["iati:gcf:FP001:2021"]
    labelled = pl.read_parquet(packed["objectives.parquet"])["record_id"]
    assert not labelled.str.starts_with("ledger:gcf:").any()
    c = pl.read_parquet(packed["commitments.parquet"]).row(
        by_predicate=is_c, named=True
    )
    assert (c["status"], c["disbursed_usd"], c["withheld_usd"]) == (
        "partly_disbursed",
        15.0,
        10.0,
    )
    # Disbursements rows conserve against the commitment once its withheld part adds.
    shipped = disbursements.filter(is_c)["allocated_usd"].sum()
    assert shipped + c["withheld_usd"] == c["disbursed_usd"]
    # Quality keeps the GCF fill: C's 15 of 25 is all climate, beside R1's 40.
    quality = pl.read_parquet(packed["quality.parquet"])
    assert quality["disbursed_climate_usd"].sum() == 55.0
    notes = {row["source"]: row.get("note") for row in json.loads(packed[SOURCES])}
    assert notes == dict.fromkeys(YEAR_SOURCES) | {
        "gcf_projects": release.WITHHELD_NOTE,
        "iati_gcf": None,
    }


def test_quality_counts_certain_routes_only_where_every_link_takes_one() -> None:
    commitments = pl.LazyFrame(
        {
            "row_id": ["A", "B", "C"],
            "provider_code": [NOR] * 3,
            "year": [2019] * 3,
            "face_usd": [1.0, 2.0, 4.0],
            "climate_usd": [1.0, 2.0, 4.0],
            "repeat_of": [None, None, None],
        },
        schema_overrides={"repeat_of": pl.String},
    )
    links = pl.LazyFrame(
        {
            "task": ["crdf_crs"] * 4,
            "left_id": ["A", "B", "B", "C"],
            "probability": [1.0] * 4,
            "link_method": ["within_year", "crosswalk", "crs_id_purpose", "iati"],
        }
    )
    empty = pl.LazyFrame(
        schema={
            "row_id": pl.String,
            "record_id": pl.String,
            "allocated_usd": pl.Float64,
            "allocated_low_usd": pl.Float64,
            "allocated_high_usd": pl.Float64,
            "record_grant_equiv_usd": pl.Float64,
        }
    )

    quality = release.quality(
        commitments,
        disbursements=empty,
        objectives=pl.LazyFrame(
            schema={"record_id": pl.String, "label_source": pl.String}
        ),
        links=links,
    ).collect()

    assert quality.select("linked_usd", "certain_route_usd").row(0) == (7.0, 1.0)
    assert quality["grant_equiv_coverage"].to_list() == [None]


def test_untraced_unallocated_and_release_rows_sum_to_the_marked_disbursement(
    release_dir: Path,
) -> None:
    unallocated = _read(release_dir, "unallocated").sort("provider_code", "year")
    untraced = _read(release_dir, "untraced").sort("record_id")
    released = (
        _read(release_dir, "disbursements")
        .group_by("year")
        .agg(pl.col("allocated_usd").sum())
    )
    marked = {2020: 60.0 + 20.0 + 15.0 + 7.0, 2021: 70.0}

    assert unallocated.rows() == [
        (NOR, 2020, 95.0, 0.0, 20.0, 15.0, 0.0),
        (NOR, 2021, 70.0, 30.0, 0.0, 0.0, 0.0),
        (GEF, 2020, 7.0, 0.0, 0.0, 7.0, 0.0),
    ]
    # The excess and the unlinked records count as untraced, with the record's own
    # disbursement and grant equivalent. The build runs no gap fill, so no IATI
    # route counts the GEF's record.
    assert untraced.select(
        "record_id",
        "reason",
        "untraced_usd",
        "record_disbursement_usd",
        "record_grant_equiv_usd",
    ).rows() == [
        ("d21", "excess", 30.0, 70.0, 35.0),
        ("g20", "unlinked", 7.0, 7.0, None),
        ("z20", "unlinked", 15.0, 15.0, None),
    ]
    totals = (
        unallocated.group_by("year")
        .agg(pl.sum_horizontal(pl.exclude("provider_code", "year", "marked_usd")).sum())
        .join(released, on="year")
        .select("year", pl.sum_horizontal(pl.exclude("year")))
    )
    assert dict(totals.rows()) == marked
    assert (
        dict(unallocated.group_by("year").agg(pl.col("marked_usd").sum()).rows())
        == marked
    )


@pytest.mark.parametrize(
    ("filled", "untraced", "fund_unlinked"),
    [(False, ["e20", "f20"], 0.0), (True, ["f20"], 7.0)],
)
def test_a_fund_record_stays_out_of_untraced_only_where_the_route_filled_its_key(
    filled: bool, untraced: list[str], fund_unlinked: float
) -> None:
    # Two unlinked GEF records: e20 under GEF id 9001, the key of commitment E1,
    # and f20 under 9002, which no commitment carries. With gap fill on, the IATI
    # route fills E1, so it counts e20's money; with it off, nothing fills E1.
    crs = crs_records(
        {"record_id": "e20", "year": 2020, "donor_code": GEF, "project_number": "9001",
         "disbursement_usd": 7.0},
        {"record_id": "f20", "year": 2020, "donor_code": GEF, "project_number": "9002",
         "disbursement_usd": 3.0},
    ).lazy()  # fmt: skip
    crdf = crdf_rows(
        {"row_id": "E1", "year": 2018, "provider_code": GEF, "project_number": "9001"}
    ).lazy()
    route = pl.DataFrame(
        {"row_id": ["E1"], "publisher": ["gef"], "key": ["9001"], "year": [2020]},
        schema_overrides={"year": pl.Int16},
    )
    allocations = fill_allocations(
        route.head(1 if filled else 0), source="iati", prefix="iati", amount=pl.lit(5.0)
    ).lazy()

    marked = marked_records(
        allocations,
        crs=crs,
        last_year=2024,
        routed=routed_records(crs, allocations=allocations, crdf=crdf),
    )
    records = untraced_records(marked).collect()
    ledger = release.unallocated(marked).collect()

    assert sorted(records["record_id"]) == untraced
    assert ledger.select("marked_usd", "unlinked_marked_usd", "fund_unlinked_usd").row(
        0
    ) == (10.0, 10.0 - fund_unlinked, fund_unlinked)


def test_untraced_records_before_the_adaptation_marker_count_mitigation_alone() -> None:
    # Two unlinked 2009 records marked adaptation principal: one alone, which no
    # measured marker selects, and one with mitigation significant.
    crs = crs_records(
        {"record_id": "a09", "year": 2009, "rio_adaptation": 2, "rio_mitigation": 0,
         "disbursement_usd": 5.0},
        {"record_id": "b09", "year": 2009, "rio_adaptation": 2, "rio_mitigation": 1,
         "disbursement_usd": 4.0},
    ).lazy()  # fmt: skip
    allocations = _empty(schema.ALLOCATIONS).lazy()

    marked = marked_records(
        allocations,
        crs=crs,
        last_year=2024,
        routed=pl.LazyFrame(schema={"record_id": pl.String}),
    )
    untraced = release.release_untraced(untraced_records(marked), crs=crs).collect()

    assert untraced.select("record_id", "rio_adaptation", "rio_mitigation").rows() == [
        ("b09", None, 1)
    ]
    assert release.unallocated(marked).collect()["unlinked_marked_usd"].sum() == 4.0


def test_core_contributions_count_disbursements_as_grant_equivalent(
    release_dir: Path,
) -> None:
    core = _read(release_dir, "core_contributions")

    assert dict(core.select("flow", "contribution_usd").rows()) == {
        "commitments": 12.0,
        "disbursements": 10.0,
        "grant_equivalent": 10.0,
    }


def test_core_contributions_keep_leaf_channels_only() -> None:
    # MULTI's 40000 and 44000 aggregate IDA's 44002; 47066 has no aggregate here.
    stage = pl.LazyFrame(
        {
            "provider_code": [NOR] * 4,
            "channel_code": [40000, 44000, IDA_CHANNEL, 47066],
            "year": [2021] * 4,
            "flow": ["disbursements"] * 4,
            "contribution_usd": [15.0, 10.0, 10.0, 5.0],
        },
        schema_overrides={"channel_code": pl.Int32, "year": pl.Int16},
    )

    core = release.core_contributions(stage).collect()

    assert set(core["channel_code"]) == {IDA_CHANNEL, 47066}
    assert core.filter(flow="disbursements")["contribution_usd"].sum() == 15.0


def _core(rows: list[tuple[int, int, float]]) -> pl.LazyFrame:
    """Disbursement core contributions in 2021 as (provider, channel, amount)."""
    return pl.LazyFrame(
        [
            (provider, channel, 2021, "disbursements", usd)
            for provider, channel, usd in rows
        ],
        schema={n: c.dtype for n, c in schema.CORE_CONTRIBUTIONS.columns.items()},
        orient="row",
    )


def test_core_contributions_keep_the_unitemised_remainder_on_the_top_channel() -> None:
    # Norway's 40000 total of 20 holds 15 in leaves; 41000 and 44000 leave whatever
    # they hold, as MULTI's intermediate aggregates do not follow the code prefixes.
    stage = _core(
        [
            (NOR, 40000, 20.0),
            (NOR, 41000, 3.0),
            (NOR, 44000, 1.0),
            (NOR, IDA_CHANNEL, 10.0),
            (NOR, 47066, 5.0),
        ]
    )

    core = release.core_contributions(stage).collect()

    for flow in ("disbursements", "grant_equivalent"):
        rows = core.filter(flow=flow).select("channel_code", "contribution_usd")
        assert dict(rows.iter_rows()) == {40000: 5.0, IDA_CHANNEL: 10.0, 47066: 5.0}


@pytest.mark.parametrize(
    "rows",
    [
        # The leaves hold 15, above the 40000 total of 10.
        [(NOR, 40000, 10.0), (NOR, IDA_CHANNEL, 10.0), (NOR, 47066, 5.0)],
        # Norway reports the World Bank Group with no 40000 total to check it by.
        [(NOR, 44000, 10.0), (NOR, IDA_CHANNEL, 10.0)],
    ],
)
def test_core_contributions_refuse_leaves_the_top_channel_does_not_cover(
    rows: list[tuple[int, int, float]],
) -> None:
    with pytest.raises(ReleaseInvariantError, match=f"{NOR} 2021 disbursements"):
        release.core_contributions(_core(rows)).collect()


def test_core_contributions_read_an_empty_stage_table(
    tmp_path: Path, config: BuildConfig
) -> None:
    _write(
        tmp_path, "normalise", {"core_contributions": _empty(schema.CORE_CONTRIBUTIONS)}
    )
    context = StageContext(
        release="r", release_dir=tmp_path, config=config, offline=True
    )

    stage = context.read("normalise", "core_contributions")

    assert release.core_contributions(stage).collect().is_empty()


def test_total_flows_sum_every_crs_record_by_its_markers(release_dir: Path) -> None:
    totals = _read(release_dir, "total_flows")

    columns = ("provider_code", "year", "bi_multi", "flow", "rio_mitigation")
    assert sorted(totals.select(*columns, "amount_usd").rows()) == [
        (NOR, 2019, 1, "commitments", 2, 150.0),
        (NOR, 2020, 1, "disbursements", 0, 5.0),
        (NOR, 2020, 1, "disbursements", 2, 95.0),
        (NOR, 2021, 1, "disbursements", 2, 70.0),
        (NOR, 2021, 1, "grant_equivalent", 2, 35.0),
        (NOR, 2021, 2, "disbursements", 0, 9.0),
        (GEF, 2020, 1, "disbursements", 2, 7.0),
    ]
    assert totals["rio_adaptation"].unique().to_list() == [0]


def test_btr_links_carry_their_crs_records_facts() -> None:
    links = pl.DataFrame(
        {
            "task": ["btr_crs", "btr_crs", "trace"],
            "left_id": ["BTR-NOR-2021-00001", "BTR-NOR-2021-00002", "d21"],
            "right_id": ["d21", "gone", "c19"],
            "link_method": ["btr_id", "btr_splink", "crs_id"],
            "probability": [1.0, 0.8, 1.0],
        }
    )

    built = release.btr_links(links.lazy(), crs=CRS.lazy()).collect()

    schema.validate(schema.BTR_LINKS, built)
    # Only BTR links, and only to records the build holds.
    columns = ("row_id", "record_id", "year", "rio_adaptation", "rio_mitigation")
    assert built.select(columns).rows() == [("BTR-NOR-2021-00001", "d21", 2021, 0, 2)]
    assert built["disbursement_usd"].to_list() == [70.0]


def test_btr_totals_sum_the_rows_by_status_funding_currency_and_link() -> None:
    # Norway: two linked ODA disbursements (one reached by two links), an unlinked
    # one, an OOF commitment, and a row of no status with a grant equivalent only.
    rows = (
        ("N1", "disbursed", "ODA", 10.0, 9.0, 100.0),
        ("N2", "disbursed", "ODA (ETS)", 5.0, None, 50.0),
        ("N3", "disbursed", "ODA", 1.0, 1.0, 10.0),
        ("N4", "committed", "OOF (ICF) OOF (ICF*)", 7.0, 3.0, 70.0),
        ("N5", None, "ODA/OOF", None, 2.0, None),
    )
    btr = pl.DataFrame(
        {
            "row_id": [row[0] for row in rows],
            "party": "NOR",
            "provider_code": NOR,
            "year": 2021,
            "status": [row[1] for row in rows],
            "funding_source": [row[2] for row in rows],
            "currency": "NOK",
            "face_usd": [row[3] for row in rows],
            "grant_equiv_usd": [row[4] for row in rows],
            "amount_domestic": [row[5] for row in rows],
            # Norway's own rate is 10 crowns a dollar, the DAC rate 12.5.
            "dac_rate": 12.5,
        },
        schema_overrides={"year": pl.Int16, "provider_code": pl.Int32},
    ).with_columns(
        face_usd_at_dac=pl.col("amount_domestic") / 12.5,
        grant_equiv_usd_at_dac=pl.col("grant_equiv_usd") * 0.8,
    )
    links = pl.DataFrame({"row_id": ["N1", "N1", "N2"], "record_id": ["a", "b", "c"]})

    totals = release.btr_totals(btr.lazy(), links=links.lazy()).collect()

    schema.validate(schema.BTR_TOTALS, totals)
    key = ["status", "funding", "linked"]
    assert totals.sort(key, nulls_last=True).select(
        *key, "rows", "face_usd", "grant_equiv_usd", "amount_domestic"
    ).rows() == [
        ("committed", "oof", False, 1, 7.0, 3.0, 70.0),
        ("disbursed", "oda", False, 1, 1.0, 1.0, 10.0),
        ("disbursed", "oda", True, 2, 15.0, 9.0, 150.0),
        (None, "other", False, 1, None, 2.0, None),
    ]
    restated = totals.sort(key, nulls_last=True).select(
        "face_usd_at_dac", "grant_equiv_usd_at_dac", "dac_rate"
    )
    assert restated.rows() == [
        pytest.approx((5.6, 2.4, 12.5)),
        pytest.approx((0.8, 0.8, 12.5)),
        pytest.approx((12.0, 7.2, 12.5)),
        (None, pytest.approx(1.6), 12.5),
    ]


def test_btr_crs_unlinked_sums_the_climate_records_no_btr_row_reaches() -> None:
    # Of Norway's bilateral 2021 records, d21 is linked, u21 and c21 are not, m21
    # carries no Rio marker and the core contribution is not bilateral. Norway's
    # BTR also covers 2022, whose one record is linked, and 2023, past the CRS
    # years, and a party without a DAC code has no CRS side.
    crs = pl.concat(
        [
            CRS,
            crs_records(
                {"record_id": "u21", "year": 2021, "disbursement_usd": 20.0},
                {
                    "record_id": "c21",
                    "year": 2021,
                    "commitment_usd": 30.0,
                    "rio_mitigation": 1,
                },
                {
                    "record_id": "m21",
                    "year": 2021,
                    "disbursement_usd": 5.0,
                    "rio_mitigation": 0,
                },
                {"record_id": "d22", "year": 2022, "disbursement_usd": 10.0},
            ),
        ]
    )
    links = pl.DataFrame(
        {
            "task": ["btr_crs", "btr_crs", "trace"],
            "left_id": ["BTR-NOR-2021-00001", "BTR-NOR-2022-00001", "c21"],
            "right_id": ["d21", "d22", "u21"],
        }
    )
    btr = pl.DataFrame(
        {"provider_code": [NOR, NOR, NOR, None], "year": [2021, 2022, 2023, 2021]},
        schema={"provider_code": pl.Int32, "year": pl.Int16},
    )

    built = release.btr_crs_unlinked(
        crs.lazy(), btr=btr.lazy(), links=links.lazy()
    ).collect()

    schema.validate(schema.BTR_CRS_UNLINKED, built)
    assert built.sort("year", "flow").rows() == [
        (NOR, 2021, "commitments", 30.0),
        (NOR, 2021, "disbursements", 20.0),
        (NOR, 2022, "commitments", 0.0),
        (NOR, 2022, "disbursements", 0.0),
        (NOR, 2023, "commitments", None),
        (NOR, 2023, "disbursements", None),
    ]


@pytest.mark.parametrize(
    ("funding_source", "funding"),
    [
        ("ODA", "oda"),
        ("ODA (ETS)", "oda"),
        ("OOF", "oof"),
        ("OOF (ICF) OOF (ICF*)", "oof"),
        ("ODA/ OOF", "other"),
        ("OOF (ICF) OOF (ICF*) ODA", "other"),
        ("Other (OSEC)", "other"),
        ("Minagri/UGRA - INIA", "other"),
        (None, "other"),
    ],
)
def test_a_funding_source_classes_as_oda_oof_or_other(
    funding_source: str | None, funding: str
) -> None:
    frame = pl.DataFrame({"source": [funding_source]}, schema={"source": pl.String})

    assert frame.select(release.funding_class(pl.col("source"))).item() == funding


def test_provider_coefficients_come_from_the_coefficients_file() -> None:
    entries = {
        "NOR": [{"significant": 0.4, "principal": 1.0, "source": "S", "locator": "p"}],
        "EUU": [{"case_by_case": True, "source": "S", "locator": "p"}],
    }

    coefficients = release.provider_coefficients(entries)

    schema.validate(schema.PROVIDER_COEFFICIENTS, coefficients)
    # The EU's code comes from the party map; left-out columns are null or false.
    assert coefficients.select(
        "provider_code", "significant", "agency_code", "case_by_case"
    ).rows() == [(NOR, 0.4, None, False), (918, None, None, True)]


@pytest.mark.parametrize(
    ("entry", "message"),
    [
        ({"significant": 0.4, "source": "S", "locator": "p", "basis": "x"}, "basis"),
        ({"significant": 0.4, "principal": 1.0, "source": "S"}, "locator"),
    ],
)
def test_a_coefficients_entry_with_a_stray_or_missing_column_is_refused(
    entry: dict[str, object], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        release.provider_coefficients({"NOR": [entry]})


def test_the_coefficients_file_gives_a_valid_table() -> None:
    entries = yaml.safe_load(
        release.PROVIDER_COEFFICIENTS_FILE.read_text(encoding="utf-8")
    )

    coefficients = release.provider_coefficients(entries)

    schema.validate(schema.PROVIDER_COEFFICIENTS, coefficients)
    assert schema._coefficient_problems(coefficients) == []


@pytest.mark.parametrize(
    ("spans", "overlap"),
    [
        ([(None, None, 2020), (None, 2021, None)], None),
        ([(None, None, 2021), (None, 2021, None)], "2021: covers 2021 to 2021"),
        ([(None, None, None), (None, 2015, 2018)], "2015: covers 2015 to 2018"),
        ([(None, None, None), (1, None, None)], None),
    ],
)
def test_coefficient_rows_of_one_agency_cover_each_year_once(
    spans: list[tuple[int | None, int | None, int | None]], overlap: str | None
) -> None:
    coefficients = pl.DataFrame(
        [(NOR, *span) for span in spans],
        schema={
            "provider_code": pl.Int32,
            "agency_code": pl.Int32,
            "first_year": pl.Int16,
            "last_year": pl.Int16,
        },
        orient="row",
    )

    assert schema.coefficient_overlaps(coefficients) == (
        []
        if overlap is None
        else [
            f"provider_coefficients {NOR} None {overlap}, as does the row from "
            "no first year"
        ]
    )


def test_profile_pools_traceable_value_by_years_since_commitment(
    release_dir: Path,
) -> None:
    profile = _read(release_dir, "disbursement_profile").filter(group="all")
    share = dict(profile.select("years_since", "cumulative_share").rows())

    # Only R1 is traceable: 60 of 100 a year after commitment, all of it after two.
    assert (share[0], share[1], share[2]) == (0.0, 0.6, 1.0)
    assert set(profile["vintages"]) == {1}


def _overstate_r1(status: pl.DataFrame) -> pl.DataFrame:
    return status.with_columns(disbursed_usd=pl.col("disbursed_usd") + 1.0)


def _drop_r1(status: pl.DataFrame) -> pl.DataFrame:
    # R1 joins no status, so its status and disbursed amounts are null.
    return status.filter(pl.col("row_id") != "R1")


@pytest.mark.parametrize(
    ("edit", "error"),
    [(_overstate_r1, ReleaseInvariantError), (_drop_r1, SchemaError)],
)
@pytest.mark.parametrize("rebuild", [True, False])
def test_a_failing_assemble_leaves_the_last_good_release(
    release_dir: Path,
    config: BuildConfig,
    edit: Callable[[pl.DataFrame], pl.DataFrame],
    error: type[Exception],
    rebuild: bool,
) -> None:
    assemble = release_dir / "assemble"
    last_good = {path.name: path.read_bytes() for path in assemble.iterdir()}
    if not rebuild:
        shutil.rmtree(assemble)
    status = pl.read_parquet(table_path(release_dir, "review", "commitments"))
    _write(release_dir, "review", {"commitments": edit(status)})

    with pytest.raises(error):
        run_stages(
            REGISTRY, release="r", release_dir=release_dir, config=config,
            stages=["assemble"],
        )  # fmt: skip

    if rebuild:
        assert {path.name: path.read_bytes() for path in assemble.iterdir()} == (
            last_good
        )
    else:
        assert not assemble.exists()
    assert not [path for path in release_dir.iterdir() if path.name.startswith(".")]


def _refetch(release_dir: Path, config: BuildConfig) -> BuildConfig:
    write_recorded(
        release_dir, "fetch", {"raw_manifest": _raw_manifest("v2")}, config=config
    )
    return config


def _move_last_complete_year(release_dir: Path, config: BuildConfig) -> BuildConfig:
    return config.model_copy(
        update={"years": config.years.model_copy(update={"crs_last_complete": 2023})}
    )


def _drop_the_crs_file(release_dir: Path, config: BuildConfig) -> BuildConfig:
    return config.model_copy(
        update={"sources": config.sources.model_copy(update={"crs_file": None})}
    )


def _decide_a_pair(release_dir: Path, config: BuildConfig) -> BuildConfig:
    decision = {
        "task": "trace",
        "left_id": "d20",
        "right_id": "c19",
        "decision": "accept",
        "label_source": "human",
        "reviewer": "reviewer",
        "decided_at": datetime(2026, 11, 2, tzinfo=UTC),
    }
    pl.DataFrame([decision], schema=schema.REVIEW_DECISIONS.dtypes).write_parquet(
        config.sources.review_decisions
    )
    return config


def _retire_a_setting(release_dir: Path, config: BuildConfig) -> BuildConfig:
    path = record_path(release_dir, "fetch")
    record = StageRecord.model_validate_json(path.read_text())
    settings = {**record.settings, "sources.retired": 1}
    path.write_text(record.model_copy(update={"settings": settings}).model_dump_json())
    return config


@pytest.mark.parametrize(
    ("change", "stale", "reason"),
    [
        # Normalise read the fetch output that a new fetch replaced.
        (_refetch, "normalise", "another 'fetch' output"),
        # Allocate, which fill read, read the last complete CRS year.
        (_move_last_complete_year, "allocate", "years.crs_last_complete = 2024"),
        # A setting that becomes None changes like any other.
        (_drop_the_crs_file, "fetch", "and the build config sets None"),
        # Link read the decisions file, whose contents changed.
        (_decide_a_pair, "link", "sources.review_decisions = "),
        (_retire_a_setting, "fetch", "sources.retired = 1, which the build config no"),
    ],
)
def test_assemble_refuses_a_stage_built_from_other_inputs(
    release_dir: Path,
    config: BuildConfig,
    change: Callable[[Path, BuildConfig], BuildConfig],
    stale: str,
    reason: str,
) -> None:
    sealed = (release_dir / "assemble" / "release.json").read_bytes()
    changed = change(release_dir, config)

    with pytest.raises(StaleStageError, match=re.escape(reason)) as error:
        run_stages(
            REGISTRY, release="r", release_dir=release_dir, config=changed,
            stages=["assemble"],
        )  # fmt: skip

    assert error.value.stage == stale
    assert (release_dir / "assemble" / "release.json").read_bytes() == sealed


@pytest.mark.parametrize(
    ("module", "name", "value"),
    [
        (cf, "__version__", "9.9.9"),
        (provenance, "source_sha256", lambda: "0" * 64),
    ],
)
def test_assemble_refuses_stages_another_package_built(
    release_dir: Path,
    config: BuildConfig,
    monkeypatch: pytest.MonkeyPatch,
    module: object,
    name: str,
    value: object,
) -> None:
    monkeypatch.setattr(module, name, value)

    with pytest.raises(StaleStageError, match="package files other than") as error:
        run_stages(
            REGISTRY, release="r", release_dir=release_dir, config=config,
            stages=["assemble"],
        )  # fmt: skip

    assert error.value.stage == "fetch"


def test_a_stage_reruns_after_its_inputs_change(
    release_dir: Path, config: BuildConfig
) -> None:
    changed = _move_last_complete_year(release_dir, config)
    # Measure reads the last complete year too, and its tables are hand-written.
    write_recorded(release_dir, "measure", _measure(), config=changed)

    run_stages(
        REGISTRY, release="r", release_dir=release_dir, config=changed,
        stages=LATER_STAGES,
    )  # fmt: skip

    assert cf.load(path=release_dir / "assemble").manifest.release == "r"


def test_the_same_decisions_at_another_path_leave_every_stage_current(
    release_dir: Path, config: BuildConfig
) -> None:
    moved = release_dir / "moved" / "decisions.parquet"
    moved.parent.mkdir()
    config.sources.review_decisions.rename(moved)
    sources = config.sources.model_copy(update={"review_decisions": moved})

    run_stages(
        REGISTRY, release="r", release_dir=release_dir,
        config=config.model_copy(update={"sources": sources}), stages=["assemble"],
    )  # fmt: skip

    assert cf.load(path=release_dir / "assemble").manifest.release == "r"


def test_years_after_the_last_complete_year_leave_the_release(
    release_dir: Path, config: BuildConfig
) -> None:
    # With 2020 the last complete year, d21's 70 fills nothing and R1 holds d20's 60.
    changed = config.model_copy(
        update={"years": config.years.model_copy(update={"crs_last_complete": 2020})}
    )
    write_recorded(release_dir, "measure", _measure(), config=changed)

    run_stages(
        REGISTRY, release="r", release_dir=release_dir, config=changed,
        stages=LATER_STAGES,
    )  # fmt: skip

    for table in ("disbursements", "total_flows", "unallocated"):
        assert _read(release_dir, table)["year"].max() == 2020
    r1 = _read(release_dir, "commitments").row(
        by_predicate=pl.col("row_id") == "R1", named=True
    )
    assert (r1["status"], r1["disbursed_usd"]) == ("partly_disbursed", 60.0)


@pytest.mark.parametrize(
    ("stage", "switch", "carried", "refused"),
    [
        # The gap fill and the review stage's ledger check need ledger sources,
        # which the config leaves out.
        (
            Stage.FILL,
            "gap_fill",
            {"allocations": "allocate", "commitments": "allocate"},
            ConfigError,
        ),
        (Stage.REVIEW, "review", {"links": "link", "commitments": "fill"}, ConfigError),
    ],
)
def test_fill_and_review_pass_through_with_their_switch_off(
    release_dir: Path,
    config: BuildConfig,
    stage: Stage,
    switch: str,
    carried: dict[str, str],
    refused: type[Exception],
) -> None:
    run = registered(stage).run

    def context(*, on: bool) -> StageContext:
        return stage_context(release_dir, config=config, **{switch: on})

    output = run(context(on=False))

    assert set(output) == set(carried)
    for table, source in carried.items():
        written = pl.read_parquet(table_path(release_dir, source, table))
        assert output[table].lazy().collect().equals(written)
    with pytest.raises(refused):
        run(context(on=True))


# Enough rows per group that polars' threaded group sum, left in place, changes the
# last bit of a float total on most runs.
RERUN_ROWS = 10_000
RERUNS = 10


def _amounts(seed: int) -> np.ndarray:
    return np.random.default_rng(seed).lognormal(15, 3, RERUN_ROWS)


def _rerun_total_flows() -> pl.LazyFrame:
    crs = pl.LazyFrame(
        {
            "donor_code": np.arange(RERUN_ROWS) % 2,
            "agency_code": 1,
            "recipient_code": 248,
            "year": 2021,
            "bi_multi": 1,
            "commitment_usd": _amounts(1),
            "disbursement_usd": _amounts(2),
            "grant_equiv_usd": _amounts(3),
            "rio_adaptation": np.arange(RERUN_ROWS) % 3,
            "rio_mitigation": 0,
        },
        schema_overrides={"donor_code": pl.Int32, "agency_code": pl.Int32,
                          "recipient_code": pl.Int32, "year": pl.Int16,
                          "bi_multi": pl.Int8, "rio_adaptation": pl.Int8,
                          "rio_mitigation": pl.Int8},
    )  # fmt: skip
    return release.total_flows(crs)


def _rerun_commitments() -> pl.LazyFrame:
    row = np.arange(RERUN_ROWS)
    return pl.LazyFrame(
        {
            "row_id": [f"R{i}" for i in row],
            "provider_code": row % 2,
            "year": 2019,
            "climate_usd": _amounts(4),
            "face_usd": _amounts(5),
            "repeat_of": [None if i % 5 else "R0" for i in row],
        },
        schema_overrides={
            "provider_code": pl.Int32, "year": pl.Int16, "repeat_of": pl.String
        },
    )  # fmt: skip


def _rerun_quality() -> pl.LazyFrame:
    commitments = _rerun_commitments()
    return release.quality(
        commitments,
        disbursements=commitments.select(
            "row_id",
            record_id="row_id",
            allocated_usd="climate_usd",
            record_grant_equiv_usd="climate_usd",
            allocated_low_usd="face_usd",
            allocated_high_usd="climate_usd",
        ),
        objectives=commitments.select(
            record_id="row_id", label_source=pl.lit("jev")
        ).head(RERUN_ROWS // 2),
        links=commitments.select(
            task=pl.lit("crdf_crs"),
            left_id="row_id",
            probability=pl.lit(0.9),
            link_method=pl.lit("within_year"),
        ),
    )


@pytest.mark.parametrize("table", [_rerun_total_flows, _rerun_quality])
def test_release_tables_give_the_same_bits_on_every_run(
    table: Callable[[], pl.LazyFrame],
) -> None:
    first = table().collect()

    for _ in range(RERUNS):
        assert table().collect().sort(first.columns).equals(first.sort(first.columns))

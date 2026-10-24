"""The assemble stage on hand-built normalised tables, run through the stage runner
from link to assemble, and the fill and review pass-through."""

import json
import shutil
import tarfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import polars as pl
import pyarrow as pa
import pytest
from normalised import crdf_rows, crs_records

import climate_finance as cf
from climate_finance import schema
from climate_finance.build import (
    BuildConfig,
    load_config,
    release,
    run_stages,
    write_table,
)
from climate_finance.build.errors import ConfigError, ReleaseInvariantError
from climate_finance.build.stages import REGISTRY, Stage, StageContext, table_path
from climate_finance.errors import SchemaError
from climate_finance.schema import STAGE_TABLES

BUILD_TOML = Path(__file__).parents[1] / "build.toml"
NOR, IDA_CHANNEL = 8, 44002
# Norway commits 100 in 2019 under crs id X1 and disburses 60 in 2020 and 70 in 2021,
# so the commitment fills and 30 of 2021 is excess.
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
    schema={name: c.dtype for name, c in schema.CORE_CONTRIBUTIONS.columns.items()},
)
LATER_STAGES = ("link", "allocate", "fill", "review", "label", "assemble")
_DISBURSED = ("disbursed_usd", "disbursed_low_usd", "disbursed_high_usd")
SOURCES = release.SOURCES_FILE


def _empty(table: schema.TableSchema) -> pl.DataFrame:
    return pl.DataFrame(schema={name: c.dtype for name, c in table.columns.items()})


def _write(release_dir: Path, stage: str, frames: dict[str, pl.DataFrame]) -> None:
    for name, frame in frames.items():
        table = STAGE_TABLES[stage][name]
        write_table(frame, table, table_path(release_dir, stage, name))


@pytest.fixture
def config() -> BuildConfig:
    # The hand-written tables hold no truth group to set Jev's scale.
    base = load_config(BUILD_TOML)
    return base.model_copy(
        update={
            "key_repair": False,
            "recoveries": False,
            "jev_links": False,
            "sources": base.sources.model_copy(update={"btr_archive": None}),
        }
    )


@pytest.fixture
def release_dir(tmp_path: Path, config: BuildConfig) -> Path:
    """A release built from hand-written fetch, normalise and measure tables."""
    _write(
        tmp_path,
        "fetch",
        {
            "raw_manifest": pl.DataFrame(
                {
                    "source": ["crs"],
                    "url": [None],
                    "path": ["oecd/CRS.parquet"],
                    "sha256": ["0" * 64],
                    "etag": [None],
                    "version": ["v20261006"],
                    "fetched_at": [datetime(2026, 10, 21, tzinfo=UTC)],
                    "method": ["manual"],
                },
                schema={n: c.dtype for n, c in schema.RAW_MANIFEST.columns.items()},
            )
        },
    )
    _write(
        tmp_path,
        "normalise",
        {
            "crdf": CRDF,
            "crs": CRS,
            "core_contributions": CORE,
            "imputed_shares": _empty(schema.IMPUTED_SHARES),
            "external": _empty(schema.EXTERNAL),
        },
    )
    _write(
        tmp_path,
        "measure",
        {
            "agency_shares": _empty(schema.AGENCY_SHARES),
            "modelled": _empty(schema.MODELLED),
        },
    )
    run_stages(
        REGISTRY, release="r", release_dir=tmp_path, config=config, stages=LATER_STAGES
    )
    return tmp_path


def _read(release_dir: Path, table: str) -> pl.DataFrame:
    return pl.read_parquet(table_path(release_dir, "assemble", table))


def test_the_release_opens_and_matches_release_json(release_dir: Path) -> None:
    snapshot = cf.load(path=release_dir / "assemble")

    assert snapshot.manifest.source_versions == {"crs": "v20261006"}
    assert set(snapshot.manifest.tables) == set(schema.RELEASE_TABLES)


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


def test_an_allocation_moved_from_a_repeat_keeps_the_repeats_link() -> None:
    # R2 repeats R1 and sits in the crdf_crs group that traced record d20; allocate
    # moved d20's allocation from R2 to R1, which has no group of its own.
    links = pl.LazyFrame(
        {
            "task": ["crdf_crs", "trace"],
            "left_id": ["R2", "u-d20"],
            "right_id": ["u-c19", "u-c19"],
            "group_id": ["g", None],
            "probability": [1.0, 0.9],
            "link_method": ["crs_id_purpose", "within_year"],
            "ledger_check": [None, None],
        }
    )
    units = pl.LazyFrame({"unit_id": ["u-d20"], "record_ids": [["d20"]]})
    allocated = pl.LazyFrame({"row_id": ["R1"], "record_id": ["d20"]})

    carried = release.carrying_links(
        allocated, crdf=CRDF.lazy(), units=units, links=links
    ).collect()

    assert carried.select("row_id", "record_id", "link_method").rows() == [
        ("R1", "d20", "within_year")
    ]


def test_a_pair_under_two_methods_is_carried_by_the_one_allocation_runs_first() -> None:
    # Key repair runs before the crosswalk, so it carries the pair even where the
    # crosswalk's link is the more probable.
    links = pl.LazyFrame(
        {
            "task": ["crdf_crs", "trace", "trace"],
            "left_id": ["R1", "u-d20", "u-d20"],
            "right_id": ["u-c19", "u-c19", "u-c19"],
            "group_id": ["g", "crosswalk|1|1", "repair|1"],
            "probability": [1.0, 1.0, 0.9],
            "link_method": ["within_year", "crosswalk", "repair_title_recipient"],
            "ledger_check": [None, None, None],
        }
    )
    units = pl.LazyFrame({"unit_id": ["u-d20"], "record_ids": [["d20"]]})
    allocated = pl.LazyFrame({"row_id": ["R1"], "record_id": ["d20"]})

    carried = release.carrying_links(
        allocated, crdf=CRDF.lazy(), units=units, links=links
    ).collect()

    assert carried["link_method"].to_list() == ["repair_title_recipient"]


def test_quality_counts_repeats_on_their_own_line(release_dir: Path) -> None:
    quality = _read(release_dir, "quality").row(0, named=True)

    # R1, C and N count; R2 repeats R1.
    assert quality["commitments"] == 3
    assert quality["climate_usd"] == 40.0 + 25.0 - 5.0
    assert quality["linked_usd"] == 40.0
    assert quality["repeat_climate_usd"] == 40.0
    assert quality["grant_equiv_coverage"] == 0.5
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
        },
        schema_overrides={"year": pl.Int16},
    )
    allocations = pl.read_parquet(table_path(release_dir, "fill", "allocations"))
    _write(release_dir, "fill", {"allocations": pl.concat([allocations, fills])})
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
    assert notes == {
        "crs": None,
        "gcf_projects": release.WITHHELD_NOTE,
        "iati_gcf": None,
    }


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


def test_total_flows_sum_every_crs_record_by_its_higher_marker(
    release_dir: Path,
) -> None:
    totals = _read(release_dir, "total_flows")

    columns = ("year", "bi_multi", "flow", "rio_marker", "amount_usd")
    assert sorted(totals.select(columns).rows()) == [
        (2019, 1, "commitments", 2, 100.0),
        (2020, 1, "disbursements", 2, 60.0),
        (2021, 1, "disbursements", 2, 70.0),
        (2021, 1, "grant_equivalent", 2, 35.0),
        (2021, 2, "disbursements", 0, 9.0),
    ]


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
    assert built.select("row_id", "record_id", "year", "rio_marker").rows() == [
        ("BTR-NOR-2021-00001", "d21", 2021, 2)
    ]
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
        },
        schema_overrides={"year": pl.Int16, "provider_code": pl.Int32},
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


def test_btr_crs_unlinked_sums_the_climate_records_no_btr_row_reaches() -> None:
    # Of Norway's bilateral 2021 records, d21 is linked, u21 and c21 are not, m21
    # carries no Rio marker and the core contribution is not bilateral. Norway's
    # BTR also covers 2022, and a party without a DAC code has no CRS side.
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
            ),
        ]
    )
    links = pl.DataFrame(
        {
            "task": ["btr_crs", "trace"],
            "left_id": ["BTR-NOR-2021-00001", "c21"],
            "right_id": ["d21", "u21"],
        }
    )
    btr = pl.DataFrame(
        {"provider_code": [NOR, NOR, None], "year": [2021, 2022, 2021]},
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


def test_provider_coefficients_come_from_the_party_weights() -> None:
    weights = {
        "NOR": {"significant": 0.4, "confirmed": True},
        "EUU": {"significant": 0.4, "confirmed": False},
        "AUS": {"significant": None, "confirmed": False},
    }

    coefficients = release.provider_coefficients(weights)

    schema.validate(schema.PROVIDER_COEFFICIENTS, coefficients)
    # Australia has no estimate, and the EU's code comes from the party map.
    assert coefficients.select("provider_code", "confirmed").rows() == [
        (NOR, True),
        (918, False),
    ]


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
    run = next(entry.run for entry in REGISTRY if entry.name == stage)

    def context(*, on: bool) -> StageContext:
        switched = config.model_copy(update={switch: on})
        return StageContext(
            release="r", release_dir=release_dir, config=switched, offline=True
        )

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
            "recipient_code": 248,
            "year": 2021,
            "bi_multi": 1,
            "commitment_usd": _amounts(1),
            "disbursement_usd": _amounts(2),
            "grant_equiv_usd": _amounts(3),
            "rio_adaptation": np.arange(RERUN_ROWS) % 3,
            "rio_mitigation": 0,
        },
        schema_overrides={"donor_code": pl.Int32, "recipient_code": pl.Int32,
                          "year": pl.Int16, "bi_multi": pl.Int8,
                          "rio_adaptation": pl.Int8, "rio_mitigation": pl.Int8},
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
            record_grant_equiv_usd="climate_usd",
            allocated_low_usd="face_usd",
            allocated_high_usd="climate_usd",
        ),
        objectives=commitments.select(
            record_id="row_id", label_source=pl.lit("jev")
        ).head(RERUN_ROWS // 2),
        links=commitments.select(
            task=pl.lit("crdf_crs"), left_id="row_id", probability=pl.lit(0.9)
        ),
    )


@pytest.mark.parametrize("table", [_rerun_total_flows, _rerun_quality])
def test_release_tables_give_the_same_bits_on_every_run(
    table: Callable[[], pl.LazyFrame],
) -> None:
    first = table().collect()

    for _ in range(RERUNS):
        assert table().collect().sort(first.columns).equals(first.sort(first.columns))

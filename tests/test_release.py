"""The assemble stage on hand-built normalised tables, run through the stage runner
from link to assemble, and the fill and review pass-through."""

import shutil
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import polars as pl
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


def _empty(table: schema.TableSchema) -> pl.DataFrame:
    return pl.DataFrame(schema={name: c.dtype for name, c in table.columns.items()})


def _write(release_dir: Path, stage: str, frames: dict[str, pl.DataFrame]) -> None:
    for name, frame in frames.items():
        table = STAGE_TABLES[stage][name]
        write_table(frame, table, table_path(release_dir, stage, name))


@pytest.fixture
def config() -> BuildConfig:
    base = load_config(BUILD_TOML)
    return base.model_copy(
        update={
            "key_repair": False,
            "recoveries": False,
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

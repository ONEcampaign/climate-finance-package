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
from climate_finance.build.errors import ReleaseInvariantError
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
    {"record_id": "core", "year": 2021, "bi_multi": 2, "disbursement_usd": 9.0},
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
    return load_config(BUILD_TOML).model_copy(
        update={"key_repair": False, "recoveries": False}
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


def test_quality_counts_repeats_on_their_own_line(release_dir: Path) -> None:
    quality = _read(release_dir, "quality").row(0, named=True)

    # R1, C and N count; R2 repeats R1.
    assert quality["commitments"] == 3
    assert quality["climate_usd"] == 40.0 + 25.0 - 5.0
    assert quality["linked_usd"] == 40.0
    assert quality["repeat_climate_usd"] == 40.0
    assert quality["grant_equiv_coverage"] == 0.5


def test_core_contributions_count_disbursements_as_grant_equivalent(
    release_dir: Path,
) -> None:
    core = _read(release_dir, "core_contributions")

    assert dict(core.select("flow", "contribution_usd").rows()) == {
        "commitments": 12.0,
        "disbursements": 10.0,
        "grant_equivalent": 10.0,
    }


def test_total_flows_sum_every_crs_record(release_dir: Path) -> None:
    totals = _read(release_dir, "total_flows")

    assert sorted(totals.select("year", "bi_multi", "flow", "amount_usd").rows()) == [
        (2019, 1, "commitments", 100.0),
        (2020, 1, "disbursements", 60.0),
        (2021, 1, "disbursements", 70.0),
        (2021, 1, "grant_equivalent", 35.0),
        (2021, 2, "disbursements", 9.0),
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
    ("stage", "switch", "carried"),
    [
        (
            Stage.FILL,
            "gap_fill",
            {"allocations": "allocate", "commitments": "allocate"},
        ),
        (Stage.REVIEW, "review", {"links": "link", "commitments": "fill"}),
    ],
)
def test_fill_and_review_pass_through_with_their_switch_off(
    release_dir: Path,
    config: BuildConfig,
    stage: Stage,
    switch: str,
    carried: dict[str, str],
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
    with pytest.raises(NotImplementedError):
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
        },
        schema_overrides={"donor_code": pl.Int32, "recipient_code": pl.Int32,
                          "year": pl.Int16, "bi_multi": pl.Int8},
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
            "row_id", record_id="row_id", record_grant_equiv_usd="climate_usd"
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

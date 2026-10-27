"""The assemble stage on hand-built normalised tables, run through the stage runner
from link to assemble, and the fill and review pass-through."""

import re
import shutil
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import polars as pl
import pytest
from normalised import crdf_rows, crs_records
from stage_runs import BUILD_TOML, registered, stage_context, year_rows

import climate_finance as cf
from climate_finance import schema
from climate_finance.build import (
    BuildConfig,
    load_config,
    provenance,
    release,
    run_stages,
    write_stage,
    write_table,
)
from climate_finance.build.errors import ReleaseInvariantError, StaleStageError
from climate_finance.build.fetch import YEAR_SOURCES
from climate_finance.build.provenance import Provenance, StageRecord, record_path
from climate_finance.build.stages import REGISTRY, Stage, StageContext, table_path
from climate_finance.errors import SchemaError
from climate_finance.schema import STAGE_TABLES

NOR, IDA_CHANNEL = 8, 44002
# Norway commits 100 in 2019 under crs id X1 and disburses 60 in 2020 and 70 in 2021,
# so the commitment fills and 30 of 2021 is excess. Y1 commits 50 with no CRDF row
# and disburses 20 to it; Z9 disburses 15 with no commitment, and Z8 disburses 5
# with no Rio marker.
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
    {"record_id": "y19", "year": 2019, "crs_id": "Y1", "commitment_usd": 50.0},
    {"record_id": "y20", "year": 2020, "crs_id": "Y1", "disbursement_usd": 20.0},
    {"record_id": "z20", "year": 2020, "crs_id": "Z9", "disbursement_usd": 15.0},
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


def _empty(table: schema.TableSchema) -> pl.DataFrame:
    return pl.DataFrame(schema=table.dtypes)


def _write(release_dir: Path, stage: str, frames: dict[str, pl.DataFrame]) -> None:
    for name, frame in frames.items():
        table = STAGE_TABLES[stage][name]
        write_table(frame, table, table_path(release_dir, stage, name))


def _write_stage(
    release_dir: Path,
    stage: str,
    frames: dict[str, pl.DataFrame],
    *,
    config: BuildConfig,
    reads: tuple[str, ...] = (),
) -> None:
    """Write `frames` as `stage`'s output, recording that it read `reads`."""
    entry = registered(stage)
    provenance = Provenance(
        stage=stage, release_dir=release_dir, config=config, settings=entry.settings
    )
    for read in reads:
        provenance.read_stage(read)
    write_stage(entry, frames, release_dir, provenance=provenance)


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
def config() -> BuildConfig:
    return load_config(BUILD_TOML).model_copy(
        update={"key_repair": False, "recoveries": False}
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
                    "face_share": 0.5,
                    "adaptation_measured": True,
                },
            ],
            schema=schema.AGENCY_SPENDING.dtypes,
        ),
        "modelled": _empty(schema.MODELLED),
    }


@pytest.fixture
def release_dir(tmp_path: Path, config: BuildConfig) -> Path:
    """A release built from hand-written fetch, normalise and measure tables."""
    _write_stage(
        tmp_path,
        "fetch",
        {"raw_manifest": _raw_manifest("v20261006")},
        config=config,
    )
    _write_stage(
        tmp_path,
        "normalise",
        {
            "crdf": CRDF,
            "crs": CRS,
            "core_contributions": CORE,
            "imputed_shares": _empty(schema.IMPUTED_SHARES),
            "external": _empty(schema.EXTERNAL),
        },
        config=config,
        reads=("fetch",),
    )
    _write_stage(tmp_path, "measure", _measure(), config=config)
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


def test_quality_counts_certain_routes_only_where_every_link_takes_one() -> None:
    commitments = pl.LazyFrame(
        {
            "row_id": ["A", "B", "C"],
            "provider_code": [NOR] * 3,
            "year": [2019] * 3,
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


def test_unallocated_and_release_rows_sum_to_the_marked_disbursement(
    release_dir: Path,
) -> None:
    unallocated = _read(release_dir, "unallocated").sort("year")
    released = (
        _read(release_dir, "disbursements")
        .group_by("year")
        .agg(pl.col("allocated_usd").sum())
    )
    marked = {2020: 60.0 + 20.0 + 15.0, 2021: 70.0}

    assert unallocated.drop("provider_code").rows() == [
        (2020, 0.0, 20.0, 15.0),
        (2021, 30.0, 0.0, 0.0),
    ]
    totals = unallocated.join(released, on="year").select(
        "year",
        pl.sum_horizontal(
            "allocated_usd", "excess_usd", "outside_release_usd", "unlinked_marked_usd"
        ),
    )
    assert dict(totals.rows()) == marked


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
        (2019, 1, "commitments", 150.0),
        (2020, 1, "disbursements", 100.0),
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


def _refetch(release_dir: Path, config: BuildConfig) -> BuildConfig:
    _write_stage(
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
    _write_stage(release_dir, "measure", _measure(), config=changed)

    run_stages(
        REGISTRY, release="r", release_dir=release_dir, config=changed,
        stages=LATER_STAGES,
    )  # fmt: skip

    assert cf.load(path=release_dir / "assemble").manifest.release == "r"


def test_years_after_the_last_complete_year_leave_the_release(
    release_dir: Path, config: BuildConfig
) -> None:
    # With 2020 the last complete year, d21's 70 fills nothing and R1 holds d20's 60.
    changed = config.model_copy(
        update={"years": config.years.model_copy(update={"crs_last_complete": 2020})}
    )
    _write_stage(release_dir, "measure", _measure(), config=changed)

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
    run = registered(stage).run

    def context(*, on: bool) -> StageContext:
        return stage_context(release_dir, config=config, **{switch: on})

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
            "row_id",
            record_id="row_id",
            allocated_usd="climate_usd",
            record_grant_equiv_usd="climate_usd",
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

"""Release gates on a small build run through the stage runner, each failing on a
release doctored to break it."""

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest
from normalised import crdf_rows, crs_records
from releases import copy_release
from typer.testing import CliRunner

import climate_finance as cf
from climate_finance import schema
from climate_finance.build import load_config, run_stages, write_table
from climate_finance.build.cli import app
from climate_finance.build.errors import GateFailedError
from climate_finance.build.evaluate import design_path, draw_blind_sample
from climate_finance.build.gate import (
    GATES,
    Drift,
    DriftCause,
    gate_results_json,
    load_gates,
    run_gates,
)
from climate_finance.build.link.review import apply_decisions
from climate_finance.build.stages import REGISTRY, table_path
from climate_finance.schema import STAGE_TABLES, ReleaseManifest

BUILD_TOML = Path(__file__).parents[1] / "build.toml"
NOR = 8
# Norway commits 100 in 2019 under crs id X1 and disburses 60 in 2020 and 45 in
# 2021, so the commitment fills and 5 of 2021 is excess. CRDF row R1 publishes the
# commitment with a climate value of 40.
CRS = crs_records(
    {"record_id": "c19", "year": 2019, "crs_id": "X1", "commitment_usd": 100.0},
    {"record_id": "d20", "year": 2020, "crs_id": "X1", "disbursement_usd": 60.0},
    {"record_id": "d21", "year": 2021, "crs_id": "X1", "disbursement_usd": 45.0},
)
CRDF = crdf_rows(
    {"row_id": "R1", "year": 2019, "crs_id": "X1", "climate_usd": 40.0, "face_usd": 1e2}
)
CORE = pl.DataFrame(
    {
        "provider_code": [NOR],
        "channel_code": [44002],
        "year": [2021],
        "flow": ["disbursements"],
        "contribution_usd": [10.0],
    },
    schema={name: c.dtype for name, c in schema.CORE_CONTRIBUTIONS.columns.items()},
)
LATER_STAGES = ("link", "allocate", "fill", "review", "label", "assemble")
GATES_CONFIG = load_gates()


def _empty(table: schema.TableSchema) -> pl.DataFrame:
    return pl.DataFrame(schema={name: c.dtype for name, c in table.columns.items()})


def _write(release_dir: Path, stage: str, frames: dict[str, pl.DataFrame]) -> None:
    for name, frame in frames.items():
        write_table(
            frame, STAGE_TABLES[stage][name], table_path(release_dir, stage, name)
        )


def _edit(path: Path, edit: Callable[[pl.DataFrame], pl.DataFrame]) -> None:
    edit(pl.read_parquet(path)).write_parquet(path)


@pytest.fixture
def release_dir(tmp_path: Path) -> Path:
    """Release `r` built from hand-written fetch, normalise and measure tables."""
    release_dir = tmp_path / "r"
    manifest = pl.DataFrame(
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
    _write(release_dir, "fetch", {"raw_manifest": manifest})
    _write(
        release_dir,
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
        release_dir,
        "measure",
        {
            "agency_shares": _empty(schema.AGENCY_SHARES),
            "modelled": _empty(schema.MODELLED),
        },
    )
    config = load_config(BUILD_TOML).model_copy(
        update={"key_repair": False, "recoveries": False}
    )
    run_stages(
        REGISTRY,
        release="r",
        release_dir=release_dir,
        config=config,
        stages=LATER_STAGES,
    )
    return release_dir


def _report(release_dir: Path, **options: object) -> pl.DataFrame:
    defaults: dict[str, object] = {
        "decisions": release_dir.parent / "decisions.parquet",
        "first_release": True,
        "report_only": True,
    }
    return run_gates(release_dir, **(defaults | options))  # ty: ignore[invalid-argument-type]


def _passed(results: pl.DataFrame) -> dict[tuple[str, str | None], bool]:
    return {
        (gate, subject): passed
        for gate, subject, passed in results.select("gate", "subject", "passed").rows()
    }


def test_a_sound_release_passes_every_gate_but_precision_without_a_sample(
    release_dir: Path,
) -> None:
    results = _report(release_dir)

    assert results["gate"].unique(maintain_order=True).to_list() == list(GATES)
    assert _passed(results) == {
        ("precision", str(NOR)): False,
        ("conservation", None): True,
        ("over_allocation", "above_face"): True,
        ("over_allocation", "raw_links"): True,
        ("oecd_reconciliation", None): True,
        ("drift", None): True,
        ("gcf_rows", None): True,
    }
    assert results.filter(gate="precision")["detail"].to_list() == ["no blind sample"]


def test_a_failed_gate_raises_naming_the_provider_and_the_missing_previous_release(
    release_dir: Path,
) -> None:
    with pytest.raises(GateFailedError) as raised:
        run_gates(release_dir, decisions=release_dir.parent / "decisions.parquet")

    assert [failure.split(":")[0] for failure in raised.value.failures] == [
        f"precision {NOR}",
        "drift",
    ]
    assert "first_release" in raised.value.failures[1]


def _label(release_dir: Path, *, rejected: set[str]) -> Path:
    """Draw the blind sample, decide it, rejecting pairs whose disbursement unit
    holds a record in `rejected`, and record the decisions; return the design."""
    csv = release_dir.parent / "sample.csv"
    decisions = release_dir.parent / "decisions.parquet"
    draw_blind_sample(
        release_dir=release_dir,
        out_path=csv,
        decisions=decisions,
        rule=GATES_CONFIG.precision,
    )
    units = pl.read_parquet(table_path(release_dir, "link", "units")).explode(
        "record_ids"
    )
    rejected_units = units.filter(pl.col("record_ids").is_in(rejected))["unit_id"]
    sample = pl.read_csv(csv, infer_schema=False)
    sample.with_columns(
        decision=pl.when(pl.col("left_id").is_in(rejected_units))
        .then(pl.lit("reject"))
        .otherwise(pl.lit("accept")),
        reviewer=pl.lit("analyst"),
    ).write_csv(csv)
    apply_decisions(csv_path=csv, decisions_path=decisions)
    return design_path(csv)


def test_the_blind_sample_hides_the_score_and_lists_each_pair_once(
    release_dir: Path,
) -> None:
    csv = release_dir.parent / "sample.csv"
    draw_blind_sample(
        release_dir=release_dir,
        out_path=csv,
        decisions=release_dir.parent / "decisions.parquet",
        rule=GATES_CONFIG.precision,
    )
    sample = pl.read_csv(csv)

    # Two pairs, under the census below 60 pairs.
    assert sample.height == 2
    assert sample["sample_id"].n_unique() == 2
    assert not {"probability", "link_method", "stratum", "weight_usd"} & set(
        sample.columns
    )
    assert {"left_title", "right_usd", "decision", "batch"} <= set(sample.columns)


@pytest.mark.parametrize(
    ("rejected", "estimate", "passed"),
    [(set(), 1.0, True), ({"d20"}, 0.4, False)],
)
def test_precision_reads_the_blind_sample_by_allocated_value(
    release_dir: Path, rejected: set[str], estimate: float, passed: bool
) -> None:
    design = _label(release_dir, rejected=rejected)

    row = (
        _report(release_dir, sample=design).filter(gate="precision").row(0, named=True)
    )

    # d20 carried 60 of the 100 allocated; a complete census is exact.
    assert row["value"] == pytest.approx(estimate)
    assert row["lower"] == row["upper"] == row["value"]
    assert row["passed"] is passed


def test_a_waiver_lets_a_failed_precision_gate_through(release_dir: Path) -> None:
    design = _label(release_dir, rejected={"d20"})

    results = run_gates(
        release_dir,
        decisions=release_dir.parent / "decisions.parquet",
        sample=design,
        first_release=True,
        waive_precision=True,
    )

    assert results.filter(gate="precision").select("passed", "waived").row(0) == (
        False,
        True,
    )


def _halve_d20(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.with_columns(
        allocated_usd=pl.when(pl.col("record_id") == "d20")
        .then(pl.col("allocated_usd") / 2)
        .otherwise("allocated_usd")
    )


def _overstate_d21(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.with_columns(
        disbursement_usd=pl.when(pl.col("record_ids").list.contains("d21"))
        .then(500.0)
        .otherwise("disbursement_usd")
    )


def _overstate_r1_climate(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.with_columns(climate_usd=pl.col("climate_usd") * 1.1)


def _disburse_r1_above_face(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.with_columns(disbursed_usd=pl.col("face_usd") + 1.0)


def _add_gcf_fill(frame: pl.DataFrame) -> pl.DataFrame:
    fill = frame.head(1).with_columns(
        record_id=pl.lit("ledger:gcf:FP001:2022"),
        disbursement_source=pl.lit("bank_ledger"),
    )
    return pl.concat([frame, fill])


@pytest.mark.parametrize(
    ("stage", "table", "edit", "failing", "named"),
    [
        ("fill", "allocations", _halve_d20, ("conservation", None), "d20"),
        ("link", "units", _overstate_d21, ("over_allocation", "raw_links"), "1 of 1"),
        (
            "assemble",
            "commitments",
            _disburse_r1_above_face,
            ("over_allocation", "above_face"),
            "R1",
        ),
        (
            "normalise",
            "crdf",
            _overstate_r1_climate,
            ("oecd_reconciliation", None),
            f"{NOR}/2019",
        ),
        ("assemble", "disbursements", _add_gcf_fill, ("gcf_rows", None), "FP001"),
    ],
)
def test_each_gate_fails_on_a_release_doctored_to_break_it(
    release_dir: Path,
    stage: str,
    table: str,
    edit: Callable[[pl.DataFrame], pl.DataFrame],
    failing: tuple[str, str | None],
    named: str,
) -> None:
    _edit(table_path(release_dir, stage, table), edit)

    results = _report(release_dir)

    failed = {key for key, passed in _passed(results).items() if not passed}
    assert failed == {("precision", str(NOR)), failing}
    gate, subject = failing
    row = results.filter(
        pl.col("gate") == gate, pl.col("subject").eq_missing(subject)
    ).row(0, named=True)
    assert named in row["detail"]


def _previous(release_dir: Path, *, factor: float) -> Path:
    """The release at `release_dir` with every provider-year climate total scaled."""
    return copy_release(
        release_dir.parent / "previous",
        source=release_dir / "assemble",
        edits={
            "quality": lambda frame: frame.with_columns(
                climate_usd=pl.col("climate_usd") * factor
            )
        },
    )


@pytest.mark.parametrize(
    ("factor", "causes", "passed"),
    [
        (1.04, (), True),
        (2.0, (), False),
        (
            2.0,
            (DriftCause(release="r", provider_code=NOR, year=2019, cause="x"),),
            True,
        ),
        (
            2.0,
            (DriftCause(release="earlier", provider_code=NOR, year=2019, cause="x"),),
            False,
        ),
    ],
)
def test_drift_needs_a_recorded_cause_for_a_move_over_five_percent(
    release_dir: Path, factor: float, causes: tuple[DriftCause, ...], passed: bool
) -> None:
    gates = GATES_CONFIG.model_copy(
        update={"drift": Drift(max_move=0.05, causes=causes)}
    )

    row = (
        _report(
            release_dir,
            previous=_previous(release_dir, factor=factor),
            first_release=False,
            gates=gates,
        )
        .filter(gate="drift")
        .row(0, named=True)
    )

    assert row["passed"] is passed
    assert (f"{NOR}/2019" in row["detail"]) is not passed


def test_quality_shows_each_providers_precision_interval(
    release_dir: Path, tmp_path: Path
) -> None:
    design = _label(release_dir, rejected={"d20"})
    results = _report(release_dir, sample=design)
    release = copy_release(tmp_path / "gated", source=release_dir / "assemble")
    manifest = ReleaseManifest.model_validate_json(
        (release / "release.json").read_text()
    )
    gated = manifest.model_copy(update={"gates": gate_results_json(results)})
    (release / "release.json").write_text(gated.model_dump_json())

    precision = cf.load(path=release).quality().precision.to_polars()

    assert precision.select("provider_code", "estimate", "lower", "passed").rows() == [
        (NOR, pytest.approx(0.4), pytest.approx(0.4), False)
    ]


def test_the_sample_and_gate_commands_pass_their_options_through(
    release_dir: Path,
) -> None:
    runner = CliRunner()
    decisions = str(release_dir.parent / "decisions.parquet")
    csv = release_dir.parent / "sample.csv"

    drawn = runner.invoke(
        app, ["sample", str(release_dir), "--out", str(csv), "--decisions", decisions]
    )
    gate = [
        "gate",
        str(release_dir),
        "--decisions",
        decisions,
        "--sample",
        str(design_path(csv)),
    ]
    reported = runner.invoke(app, [*gate, "--first-release", "--report"])
    refused = runner.invoke(app, gate)

    assert drawn.exit_code == 0, drawn.output
    assert pl.read_csv(csv).height == 2
    assert reported.exit_code == 0, reported.output
    assert f"precision {NOR}: FAIL; continue after 0 labels (census)" in (
        reported.output
    )
    assert isinstance(refused.exception, GateFailedError)
    assert "first_release" in str(refused.exception)

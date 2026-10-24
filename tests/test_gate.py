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
from climate_finance.build.evaluate import (
    batch_packet_path,
    design_path,
    draw_blind_sample,
)
from climate_finance.build.gate import (
    GATES,
    Drift,
    DriftCause,
    gate_results_json,
    load_gates,
    run_gates,
)
from climate_finance.build.link.decisions import import_labels
from climate_finance.build.link.packet import MATCH_DEFINITION, packet_pairs_path
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
    config = load_config(BUILD_TOML)
    # The hand-written normalise tables hold no BTR rows and no truth group to set
    # Jev's scale.
    config = config.model_copy(
        update={
            "key_repair": False,
            "recoveries": False,
            "jev_links": False,
            "sources": config.sources.model_copy(update={"btr_archive": None}),
        }
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


def test_a_sound_release_passes_every_gate_but_the_samples_without_one(
    release_dir: Path,
) -> None:
    results = _report(release_dir)

    assert results["gate"].unique(maintain_order=True).to_list() == list(GATES)
    assert _passed(results) == {
        ("precision", str(NOR)): False,
        ("human_audit", None): False,
        ("conservation", None): True,
        ("over_allocation", "above_face"): True,
        ("over_allocation", "raw_links"): True,
        ("oecd_reconciliation", None): True,
        ("drift", None): True,
        ("withheld_rows", None): True,
    }
    assert results.filter(gate="precision")["detail"].to_list() == ["no blind sample"]


def test_a_failed_gate_raises_naming_the_provider_and_the_missing_previous_release(
    release_dir: Path,
) -> None:
    with pytest.raises(GateFailedError) as raised:
        run_gates(release_dir, decisions=release_dir.parent / "decisions.parquet")

    assert [failure.split(":")[0] for failure in raised.value.failures] == [
        f"precision {NOR}",
        "human_audit",
        "drift",
    ]
    assert "first_release" in raised.value.failures[2]


def _label(
    release_dir: Path, *, rejected: set[str], label_source: str = "human"
) -> Path:
    """Draw the blind sample, decide its packets as `label_source`, rejecting pairs
    whose disbursement unit holds a record in `rejected`, and record the decisions;
    return the design."""
    out = release_dir.parent / "sample"
    decisions = release_dir.parent / "decisions.parquet"
    draw_blind_sample(
        release_dir=release_dir,
        out_dir=out,
        decisions=decisions,
        rule=GATES_CONFIG.precision,
    )
    units = pl.read_parquet(table_path(release_dir, "link", "units")).explode(
        "record_ids"
    )
    rejected_units = units.filter(pl.col("record_ids").is_in(rejected))["unit_id"]
    agent = label_source == "agent"
    for packet in sorted(out.glob("*/batch-*.md")):
        labels = packet.with_suffix(".labels.csv")
        right = ~pl.col("left_id").is_in(rejected_units.implode())
        pl.read_parquet(packet_pairs_path(packet)).select(
            "pair",
            label=pl.when(right).then(pl.lit("match")).otherwise(pl.lit("no_match")),
            confidence=pl.when(right).then(0.9).otherwise(0.1),
            reason=pl.lit("read both units"),
        ).write_csv(labels)
        import_labels(
            packet=packet,
            labels_csv=labels,
            decisions_path=decisions,
            label_source=label_source,
            reviewer="agent-run" if agent else "analyst",
            model="claude-opus-5-5" if agent else None,
        )
    return design_path(out)


def test_the_blind_sample_hides_the_score_and_lists_each_pair_once(
    release_dir: Path,
) -> None:
    out = release_dir.parent / "sample"
    draw_blind_sample(
        release_dir=release_dir,
        out_dir=out,
        decisions=release_dir.parent / "decisions.parquet",
        rule=GATES_CONFIG.precision,
    )
    packet = batch_packet_path(out, provider=NOR, batch=1)
    sample = pl.read_parquet(packet_pairs_path(packet))
    text = packet.read_text()

    # Two pairs, under the census below 60 pairs, in one packet.
    assert sorted(out.glob("*/*.md")) == [packet]
    assert sample.height == 2
    assert sample["sample_id"].n_unique() == 2
    assert not {"probability", "link_method", "stratum", "weight_usd"} & set(
        sample.columns
    )
    assert "probability" not in text.lower()
    # Ids show, since reviewers see every field.
    assert MATCH_DEFINITION in text
    assert "X1" in text


@pytest.mark.parametrize(
    ("rejected", "label_source", "estimate", "passed"),
    [
        (set(), "human", 1.0, True),
        ({"d20"}, "human", 0.4, False),
        # An agent's blind decisions count as a person's, and the share shows.
        ({"d20"}, "agent", 0.4, False),
    ],
)
def test_precision_reads_the_blind_sample_by_allocated_value(
    release_dir: Path,
    rejected: set[str],
    label_source: str,
    estimate: float,
    passed: bool,
) -> None:
    design = _label(release_dir, rejected=rejected, label_source=label_source)

    row = (
        _report(release_dir, sample=design).filter(gate="precision").row(0, named=True)
    )

    # d20 carried 60 of the 100 allocated; a complete census is exact.
    assert row["value"] == pytest.approx(estimate)
    assert row["lower"] == row["upper"] == row["value"]
    assert row["passed"] is passed
    assert ("100% decided by agents" in row["detail"]) is (label_source == "agent")


def _units_of(release_dir: Path, record: str) -> pl.Series:
    units = pl.read_parquet(table_path(release_dir, "link", "units"))
    return units.filter(pl.col("record_ids").list.contains(record))["unit_id"]


@pytest.mark.parametrize("change", ["drop_pair", "change_method"])
def test_precision_refuses_a_sample_drawn_from_another_build(
    release_dir: Path, change: str
) -> None:
    design = _label(release_dir, rejected=set())
    d21 = _units_of(release_dir, "d21")
    trace = (pl.col("task") == "trace") & pl.col("left_id").is_in(d21.implode())
    _edit(
        table_path(release_dir, "review", "links"),
        lambda frame: (
            frame.filter(~trace)
            if change == "drop_pair"
            else frame.with_columns(
                link_method=pl.when(trace)
                .then(pl.lit("classifier"))
                .otherwise("link_method")
            )
        ),
    )

    row = (
        _report(release_dir, sample=design).filter(gate="precision").row(0, named=True)
    )

    assert row["passed"] is False
    assert "1 of 2 sampled pairs" in row["detail"]


@pytest.mark.parametrize(
    ("label_source", "share", "passed"), [("human", 1.0, True), ("agent", 0.0, False)]
)
def test_the_human_audit_reads_who_decided_the_draws(
    release_dir: Path, label_source: str, share: float, passed: bool
) -> None:
    design = _label(release_dir, rejected=set(), label_source=label_source)

    row = (
        _report(release_dir, sample=design)
        .filter(gate="human_audit")
        .row(0, named=True)
    )

    assert (row["value"], row["threshold"], row["passed"]) == (share, 0.1, passed)


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


def _add_withheld_fill(frame: pl.DataFrame) -> pl.DataFrame:
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
        (
            "assemble",
            "disbursements",
            _add_withheld_fill,
            ("withheld_rows", None),
            "FP001",
        ),
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
    assert failed == {("precision", str(NOR)), ("human_audit", None), failing}
    gate, subject = failing
    row = results.filter(
        pl.col("gate") == gate, pl.col("subject").eq_missing(subject)
    ).row(0, named=True)
    assert named in row["detail"]


def _drop_trace(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.filter(pl.col("task") != "trace")


@pytest.mark.parametrize(
    ("stage", "table", "edit", "failing"),
    [
        # Nothing linked: no pair to certify, no record to conserve, no unit to
        # measure.
        (
            "review",
            "links",
            _drop_trace,
            {
                ("precision", None),
                ("conservation", None),
                ("over_allocation", "raw_links"),
            },
        ),
        ("assemble", "disbursements", pl.DataFrame.clear, {("withheld_rows", None)}),
    ],
)
def test_a_gate_with_nothing_to_measure_fails(
    release_dir: Path,
    stage: str,
    table: str,
    edit: Callable[[pl.DataFrame], pl.DataFrame],
    failing: set[tuple[str, str | None]],
) -> None:
    _edit(table_path(release_dir, stage, table), edit)

    results = _report(release_dir)

    rows = {
        (gate, subject): (passed, value)
        for gate, subject, passed, value in results.select(
            "gate", "subject", "passed", "value"
        ).rows()
    }
    assert {key: rows[key] for key in failing} == dict.fromkeys(failing, (False, None))


def test_a_waiver_lets_an_empty_precision_gate_through(release_dir: Path) -> None:
    _edit(table_path(release_dir, "review", "links"), _drop_trace)

    results = _report(release_dir, waive_precision=True)

    assert results.filter(gate="precision").select("subject", "waived").rows() == [
        (None, True)
    ]


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


def _with_row(*, provider: int, year: int, scale: float) -> Callable:
    """An edit adding a copy of the first row as `provider` and `year`, with its
    amounts scaled by `scale`."""

    def edit(frame: pl.DataFrame) -> pl.DataFrame:
        amounts = [name for name, dtype in frame.schema.items() if dtype == pl.Float64]
        extra = frame.head(1).with_columns(
            pl.lit(provider, pl.Int32).alias("provider_code"),
            pl.lit(year, pl.Int16).alias("year"),
            *(pl.col(name) * scale for name in amounts),
        )
        return pl.concat([frame, extra])

    return edit


@pytest.mark.parametrize(
    ("current", "previous", "passed", "named"),
    [
        # This release adds a year the previous one lacked: named, not gated.
        (
            _with_row(provider=NOR, year=2020, scale=1.0),
            None,
            True,
            f"provider-years after 2019, not compared: {NOR}/2020",
        ),
        # A provider-year inside the previous release's years that it lacked.
        (
            _with_row(provider=NOR, year=2018, scale=1.0),
            None,
            False,
            f"{NOR}/2018 (new)",
        ),
        # An empty previous `quality` sets no baseline.
        (None, pl.DataFrame.clear, False, "quality table is empty"),
        # A year the previous release held vanishes.
        (
            None,
            _with_row(provider=NOR, year=2018, scale=1.0),
            False,
            f"{NOR}/2018 (gone)",
        ),
        # So does a whole provider.
        (None, _with_row(provider=9, year=2019, scale=1.0), False, "providers gone: 9"),
        # A vanished year that held nothing moves nothing.
        (
            None,
            _with_row(provider=NOR, year=2018, scale=0.0),
            True,
            "no provider-year",
        ),
    ],
)
def test_drift_compares_shared_years_and_gates_vanished_ones(
    release_dir: Path,
    current: Callable | None,
    previous: Callable | None,
    passed: bool,
    named: str,
) -> None:
    before = copy_release(
        release_dir.parent / "previous",
        source=release_dir / "assemble",
        edits={"quality": previous} if previous else None,
    )
    if current:
        _edit(table_path(release_dir, "assemble", "quality"), current)

    row = (
        _report(release_dir, previous=before, first_release=False)
        .filter(gate="drift")
        .row(0, named=True)
    )

    assert row["passed"] is passed
    assert named in row["detail"]


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


def test_the_sample_and_gate_commands_read_decisions_from_the_config(
    release_dir: Path,
) -> None:
    runner = CliRunner()
    decisions = release_dir.parent / "labels" / "decisions.parquet"
    config = release_dir.parent / "build.toml"
    config.write_text(
        BUILD_TOML.read_text().replace(
            'review_decisions = "data/review_decisions.parquet"',
            f'review_decisions = "{decisions}"',
        )
    )
    out = release_dir.parent / "sample"

    drawn = runner.invoke(
        app, ["sample", str(release_dir), "--out", str(out), "--config", str(config)]
    )
    gate = [
        "gate",
        str(release_dir),
        "--config",
        str(config),
        "--sample",
        str(design_path(out)),
    ]
    missing = runner.invoke(app, [*gate, "--first-release", "--report"])
    decisions.parent.mkdir()
    _empty(schema.REVIEW_DECISIONS).write_parquet(decisions)
    reported = runner.invoke(app, [*gate, "--first-release", "--report"])
    refused = runner.invoke(app, gate)

    assert drawn.exit_code == 0, drawn.output
    assert pl.read_parquet(design_path(out)).height == 2
    assert isinstance(missing.exception, FileNotFoundError)
    assert reported.exit_code == 0, reported.output
    assert f"precision {NOR}: FAIL; continue after 0 labels (census)" in (
        reported.output
    )
    assert isinstance(refused.exception, GateFailedError)
    assert "first_release" in str(refused.exception)

"""Release gates on a small build run through the stage runner, each failing on a
release doctored to break it."""

import json
from collections.abc import Callable
from pathlib import Path

import polars as pl
import pytest
from normalised import crdf_rows, crs_records
from releases import copy_release
from stage_runs import write_fetch, write_recorded, year_rows
from typer.testing import CliRunner

import climate_finance as cf
from climate_finance import schema
from climate_finance.build import evaluate, load_config, run_stages, write_table
from climate_finance.build.cli import app
from climate_finance.build.config import DriftCause
from climate_finance.build.errors import (
    GateFailedError,
    SampleDesignError,
)
from climate_finance.build.evaluate import (
    allocated_pairs,
    batch_packet_path,
    design_path,
    draw_blind_sample,
    read_sample_labels,
)
from climate_finance.build.fetch import CRS_FILE
from climate_finance.build.gate import (
    GATES,
    Drift,
    gate_results_json,
    listing_gate,
    load_gates,
    precision_gate,
    run_gates,
)
from climate_finance.build.link.decisions import import_labels
from climate_finance.build.link.packet import (
    MATCH_DEFINITION,
    NAMED_CODES,
    packet_pairs_path,
)
from climate_finance.build.stages import REGISTRY, raw_dir, table_path
from climate_finance.schema import STAGE_TABLES, ReleaseManifest

BUILD_TOML = Path(__file__).parent / "fixtures" / "build.toml"
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
# Norway repays 24 of principal and 3 of interest on the loan in 2020, and 6 and 1
# in 2021.
REPAYMENTS = [(2020, 24.0, 3.0), (2021, 6.0, 1.0)]
REFLOWS = pl.DataFrame(
    [
        {
            "record_id": f"p{year % 100}",
            "year": year,
            "donor_code": NOR,
            "agency_code": 1,
            "crs_id": "X1",
            "project_number": None,
            "title": None,
            "recipient_code": 248,
            "purpose_code": 23210,
            "channel_code": None,
            "bi_multi": 1,
            "flow_code": 13,
            "finance_t": 421,
            "received_usd": received,
            "interest_usd": interest,
            "commitment_date": None,
            "rio_adaptation": 0,
            "rio_mitigation": 2,
        }
        for year, received, interest in REPAYMENTS
    ],
    schema=schema.CRS_REFLOWS.dtypes,
)
# The bulk file's rows for them, in USD million, with the code columns
# `draw_blind_sample` reads names from.
RAW_CRS = pl.DataFrame(
    {
        "year": [year for year, *_ in REPAYMENTS],
        "donor_code": [NOR] * len(REPAYMENTS),
        "usd_received": [received / 1e6 for _, received, _ in REPAYMENTS],
        "usd_interest": [interest / 1e6 for _, _, interest in REPAYMENTS],
    },
    schema={
        "year": pl.Int16,
        "donor_code": pl.Int32,
        "usd_received": pl.Float64,
        "usd_interest": pl.Float64,
    },
).with_columns(
    *(pl.lit(None, pl.Int32).alias(f"{kind}_code") for kind in NAMED_CODES),
    *(pl.lit(None, pl.String).alias(f"{kind}_name") for kind in NAMED_CODES),
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
    manifest = year_rows().with_columns(
        version=pl.when(pl.col("source") == "crs").then(pl.lit("v20261006"))
    )
    write_fetch(release_dir, manifest, config=config)
    write_recorded(
        release_dir,
        "normalise",
        {
            "crdf": CRDF,
            "crdf_span": _empty(schema.CRDF_SPAN),
            "crs": CRS,
            "reflows": REFLOWS,
            "core_contributions": CORE,
            "imputed_shares": _empty(schema.IMPUTED_SHARES),
            "external": _empty(schema.EXTERNAL),
        },
        config=config,
        reads=("fetch",),
    )
    write_recorded(
        release_dir,
        "measure",
        {
            "agency_shares": _empty(schema.AGENCY_SHARES),
            "agency_spending": _empty(schema.AGENCY_SPENDING),
            "modelled": _empty(schema.MODELLED),
        },
        config=config,
    )
    run_stages(
        REGISTRY,
        release="r",
        release_dir=release_dir,
        config=config,
        stages=LATER_STAGES,
    )
    (raw_dir(release_dir) / CRS_FILE).parent.mkdir(parents=True)
    RAW_CRS.write_parquet(raw_dir(release_dir) / CRS_FILE)
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
        ("reflow_conservation", "received"): True,
        ("reflow_conservation", "interest"): True,
        ("over_allocation", "above_face"): True,
        ("over_allocation", "raw_links"): True,
        ("oecd_reconciliation", None): True,
        ("drift", None): True,
        ("withheld_rows", None): True,
        ("listed_codes", None): True,
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
    text = packet.read_text(encoding="utf-8")

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
        _report(release_dir, sample=design.parent)
        .filter(gate="precision")
        .row(0, named=True)
    )

    # d20 carried 60 of the 100 allocated; a complete census is exact.
    assert row["value"] == pytest.approx(estimate)
    assert row["lower"] == row["upper"] == row["value"]
    assert row["strata"] == [
        {
            "stratum": "key",
            "allocated_usd": pytest.approx(100.0),
            "labels": 2,
            "estimate": pytest.approx(estimate),
            "lower": pytest.approx(estimate),
            "upper": pytest.approx(estimate),
        }
    ]
    assert row["passed"] is passed
    assert ("100% decided by agents" in row["detail"]) is (label_source == "agent")


def _units_of(release_dir: Path, record: str) -> pl.Series:
    units = pl.read_parquet(table_path(release_dir, "link", "units"))
    return units.filter(pl.col("record_ids").list.contains(record))["unit_id"]


@pytest.mark.parametrize(
    "change",
    ["drop_pair", "change_method", "change_value", "drop_population", "old_frame"],
)
def test_precision_refuses_a_sample_drawn_from_another_build(
    release_dir: Path, change: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    if change == "old_frame":
        # A design drawn when the frame held self-linked pairs: its fingerprint
        # started from no frame tag.
        monkeypatch.setattr(evaluate, "_FRAME", b"")
    design = _label(release_dir, rejected=set())
    monkeypatch.undo()
    d21 = _units_of(release_dir, "d21")
    trace = (pl.col("task") == "trace") & pl.col("left_id").is_in(d21.implode())
    if change == "change_value":
        _edit(table_path(release_dir, "fill", "allocations"), _halve_d20)
    elif change == "drop_population":
        # A design drawn before designs recorded their population.
        _edit(design, lambda frame: frame.drop("population"))
    else:
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
        _report(release_dir, sample=design.parent)
        .filter(gate="precision")
        .row(0, named=True)
    )

    assert row["passed"] is False
    assert (row["value"], row["lower"], row["upper"]) == (None, None, None)
    assert row["code"] == "stale_sample"
    assert row["detail"].startswith("stale sample")


def test_precision_refuses_a_sample_after_the_population_grows(
    release_dir: Path,
) -> None:
    design = _label(release_dir, rejected=set())
    decisions = release_dir.parent / "decisions.parquet"
    pairs = allocated_pairs(release_dir, decisions=decisions)
    labels = read_sample_labels(design, decisions=decisions)
    # A hundred more pairs, each carrying as much as the two the census labelled.
    grown = pl.concat(
        [
            pairs,
            pl.DataFrame(
                {
                    "provider_code": NOR,
                    "task": "trace",
                    "left_id": [f"new-d{i}" for i in range(100)],
                    "right_id": [f"new-c{i}" for i in range(100)],
                    "link_method": pairs["link_method"][0],
                    "allocated_usd": pairs["allocated_usd"].max(),
                    "stratum": pairs["stratum"][0],
                },
                schema=pairs.schema,
            ),
        ]
    )

    current = precision_gate(pairs, labels=labels, rule=GATES_CONFIG.precision)
    stale = precision_gate(grown, labels=labels, rule=GATES_CONFIG.precision)

    assert current["passed"].to_list() == [True]
    assert stale["passed"].to_list() == [False]
    assert stale["value"].to_list() == [None]
    assert stale["detail"].str.starts_with("stale sample").to_list() == [True]


def _self_linked(provider: int, usd: float, *, like: pl.DataFrame) -> pl.DataFrame:
    """An allocated pair of `provider` linking a unit to itself, carrying `usd`."""
    return pl.DataFrame(
        {
            "provider_code": provider,
            "task": "trace",
            "left_id": f"own-{provider}",
            "right_id": f"own-{provider}",
            "link_method": "crs_id_purpose",
            "allocated_usd": usd,
            "stratum": "key",
        },
        schema=like.schema,
    )


def test_precision_counts_self_linked_value_right(release_dir: Path) -> None:
    design = _label(release_dir, rejected={"d20"})
    decisions = release_dir.parent / "decisions.parquet"
    pairs = allocated_pairs(release_dir, decisions=decisions)
    labels = read_sample_labels(design, decisions=decisions)
    # Norway's sampled 100 is 0.4 right; a unit linked to itself adds 900, and
    # provider 9 allocates only through one.
    owned = pl.concat(
        [
            pairs,
            _self_linked(NOR, 900.0, like=pairs),
            _self_linked(9, 50.0, like=pairs),
        ]
    )

    sampled = precision_gate(pairs, labels=labels, rule=GATES_CONFIG.precision)
    rows = precision_gate(owned, labels=labels, rule=GATES_CONFIG.precision)

    assert sampled["value"].to_list() == [pytest.approx(0.4)]
    assert sampled["passed"].to_list() == [False]
    nor, only_own = rows.rows(named=True)
    assert (nor["subject"], only_own["subject"]) == (str(NOR), "9")
    assert nor["value"] == nor["lower"] == pytest.approx(0.9 + 0.1 * 0.4)
    assert (nor["passed"], nor["code"]) == (True, None)
    assert "90.0% of value self-linked" in nor["detail"]
    assert nor["strata"] == sampled["strata"].to_list()[0]
    assert (only_own["value"], only_own["passed"], only_own["code"]) == (
        1.0,
        True,
        None,
    )


def test_a_non_finite_estimate_fails(release_dir: Path) -> None:
    design = _label(release_dir, rejected=set())
    decisions = release_dir.parent / "decisions.parquet"
    pairs = allocated_pairs(release_dir, decisions=decisions)
    labels = read_sample_labels(design, decisions=decisions).with_columns(
        weight_usd=float("nan")
    )

    row = precision_gate(pairs, labels=labels, rule=GATES_CONFIG.precision).row(
        0, named=True
    )

    assert (row["passed"], row["code"]) == (False, "non_finite_estimate")


@pytest.mark.parametrize("weight", [float("nan"), float("inf"), 0.0, -1.0, None])
def test_a_design_without_finite_positive_weights_is_refused(
    release_dir: Path, weight: float | None
) -> None:
    design = _label(release_dir, rejected=set())
    pl.read_parquet(design).with_columns(
        stratum_usd=pl.lit(weight, pl.Float64)
    ).write_parquet(design)

    with pytest.raises(SampleDesignError, match="stratum_usd"):
        read_sample_labels(design, decisions=release_dir.parent / "decisions.parquet")


def test_a_decision_counts_only_for_the_pair_it_decided(release_dir: Path) -> None:
    design = _label(release_dir, rejected=set())
    decisions = release_dir.parent / "decisions.parquet"
    # A redraw that reuses both sample ids for the other pair each.
    redrawn = release_dir.parent / "redrawn.parquet"
    pl.read_parquet(design).with_columns(
        pl.col("left_id", "right_id").reverse()
    ).write_parquet(redrawn)

    drawn = read_sample_labels(design, decisions=decisions)
    swapped = read_sample_labels(redrawn, decisions=decisions)

    assert drawn["correct"].to_list() == [True, True]
    assert swapped["correct"].to_list() == [None, None]
    assert swapped["label_source"].to_list() == [None, None]


@pytest.mark.parametrize(
    ("label_source", "share", "passed"), [("human", 1.0, True), ("agent", 0.0, False)]
)
def test_the_human_audit_reads_who_decided_the_draws(
    release_dir: Path, label_source: str, share: float, passed: bool
) -> None:
    design = _label(release_dir, rejected=set(), label_source=label_source)

    row = (
        _report(release_dir, sample=design.parent)
        .filter(gate="human_audit")
        .row(0, named=True)
    )

    assert (row["value"], row["threshold"], row["passed"]) == (share, 0.1, passed)


def test_a_persons_blind_decision_stands_over_an_agents_imported_later(
    release_dir: Path,
) -> None:
    design = _label(release_dir, rejected=set())
    _edit(
        release_dir.parent / "decisions.parquet",
        lambda frame: pl.concat(
            [
                frame,
                frame.with_columns(
                    decision=pl.lit("reject"),
                    label_source=pl.lit("agent"),
                    model=pl.lit("claude-opus-5-5"),
                    round=pl.lit("label"),
                    decided_at=pl.col("decided_at") + pl.duration(days=1),
                ),
            ]
        ),
    )

    results = _report(release_dir, sample=design.parent)

    assert results.filter(gate="precision")["value"].to_list() == [1.0]
    assert results.filter(gate="human_audit")["value"].to_list() == [1.0]


def test_a_waiver_lets_a_failed_precision_gate_through(release_dir: Path) -> None:
    design = _label(release_dir, rejected={"d20"})

    results = run_gates(
        release_dir,
        decisions=release_dir.parent / "decisions.parquet",
        sample=design.parent,
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


def _add_withheld_untraced(frame: pl.DataFrame) -> pl.DataFrame:
    return pl.concat(
        [frame, frame.head(1).with_columns(record_id=pl.lit("ledger:gcf:FP002:2021"))]
    )


def _send_to_unlisted_recipient(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.with_columns(recipient_code=pl.lit(99999, pl.Int32))


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
        (
            "assemble",
            "untraced",
            _add_withheld_untraced,
            ("withheld_rows", None),
            "untraced ledger:gcf:FP002",
        ),
        (
            "assemble",
            "disbursements",
            _send_to_unlisted_recipient,
            ("listed_codes", None),
            "recipient 99999",
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


def _drop_d21(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.filter(pl.col("record_id") != "d21")


@pytest.mark.parametrize(
    ("edit", "passed"),
    [
        # d21 lies after the last complete year, so allocation never reads it.
        (_drop_d21, True),
        # d20 lies inside it and loses half its allocation.
        (_halve_d20, False),
    ],
)
def test_conservation_covers_the_years_allocation_reads(
    release_dir: Path, edit: Callable[[pl.DataFrame], pl.DataFrame], passed: bool
) -> None:
    manifest_path = release_dir / "assemble" / "release.json"
    manifest = ReleaseManifest.model_validate_json(
        manifest_path.read_text(encoding="utf-8")
    )
    manifest.settings["years.crs_last_complete"] = 2020
    manifest_path.write_text(manifest.model_dump_json(), encoding="utf-8")
    _edit(table_path(release_dir, "fill", "allocations"), edit)

    row = _report(release_dir).filter(gate="conservation").row(0, named=True)

    assert row["passed"] is passed


def _drop_p20(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.filter(pl.col("record_id") != "p20")


def _overstate_interest(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.with_columns(interest_usd=pl.col("interest_usd") + 1.0)


def _understate_raw_received(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.with_columns(usd_received=pl.col("usd_received") / 2)


def _release_reflows(release_dir: Path) -> Path:
    return table_path(release_dir, "assemble", "reflows")


@pytest.mark.parametrize(
    ("path", "edit", "failing", "named"),
    [
        # A repayment the normalise table drops, or the release table loses, leaves
        # the raw CRS's total unmet for both amounts.
        (
            lambda release_dir: table_path(release_dir, "normalise", "reflows"),
            _drop_p20,
            {"received", "interest"},
            f"{NOR}/2020",
        ),
        (_release_reflows, _drop_p20, {"received", "interest"}, f"{NOR}/2020"),
        # Interest alone disagrees, or the raw file holds more principal.
        (_release_reflows, _overstate_interest, {"interest"}, f"{NOR}/2020"),
        (
            lambda release_dir: raw_dir(release_dir) / CRS_FILE,
            _understate_raw_received,
            {"received"},
            f"{NOR}/2021",
        ),
    ],
)
def test_reflow_conservation_fails_where_a_table_disagrees_with_the_others(
    release_dir: Path,
    path: Callable[[Path], Path],
    edit: Callable[[pl.DataFrame], pl.DataFrame],
    failing: set[str],
    named: str,
) -> None:
    _edit(path(release_dir), edit)

    rows = _report(release_dir).filter(gate="reflow_conservation")

    assert {r["subject"] for r in rows.iter_rows(named=True) if not r["passed"]} == (
        failing
    )
    for row in rows.filter(pl.col("subject").is_in(failing)).iter_rows(named=True):
        assert named in row["detail"]


@pytest.mark.parametrize(
    "path",
    [
        lambda release_dir: raw_dir(release_dir) / CRS_FILE,
        lambda release_dir: table_path(release_dir, "normalise", "reflows"),
    ],
    ids=["raw CRS", "normalise reflows"],
)
def test_reflow_conservation_refuses_when_an_input_is_missing(
    release_dir: Path, path: Callable[[Path], Path]
) -> None:
    path(release_dir).unlink()

    rows = _report(release_dir).filter(gate="reflow_conservation")

    assert rows.select("subject", "passed", "code").rows() == [
        ("received", False, "missing_input"),
        ("interest", False, "missing_input"),
    ]


def test_raw_links_leave_out_commitments_the_release_does_not_carry(
    release_dir: Path,
) -> None:
    # A CRS commitment of 10 with no CRDF row disburses 500 in its own record.
    _edit(
        table_path(release_dir, "link", "units"),
        lambda frame: pl.concat(
            [
                frame,
                frame.head(1).with_columns(
                    unit_id=pl.lit("u_other"),
                    crs_id=pl.lit("X2"),
                    commitment_usd=pl.lit(10.0),
                    disbursement_usd=pl.lit(500.0),
                    record_ids=pl.lit(["other"]),
                ),
            ]
        ),
    )
    _edit(
        table_path(release_dir, "review", "links"),
        lambda frame: pl.concat(
            [
                frame,
                frame.filter(task="trace")
                .head(1)
                .with_columns(left_id=pl.lit("u_other"), right_id=pl.lit("u_other")),
            ]
        ),
    )

    row = _report(release_dir).filter(subject="raw_links").row(0, named=True)

    assert (row["passed"], row["value"]) == (True, 0.0)


def _drop_trace(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.filter(pl.col("task") != "trace")


@pytest.mark.parametrize(
    ("tables", "edit", "failing"),
    [
        # Nothing linked: no pair to certify, no record to conserve, no unit to
        # measure.
        (
            [("review", "links")],
            _drop_trace,
            {
                ("precision", None),
                ("conservation", None),
                ("over_allocation", "raw_links"),
            },
        ),
        (
            [("assemble", "disbursements")],
            pl.DataFrame.clear,
            {("withheld_rows", None)},
        ),
        # No commitment on either side: nothing to reconcile.
        (
            [("assemble", "commitments"), ("normalise", "crdf")],
            pl.DataFrame.clear,
            {("oecd_reconciliation", None)},
        ),
    ],
)
def test_a_gate_with_nothing_to_measure_fails(
    release_dir: Path,
    tables: list[tuple[str, str]],
    edit: Callable[[pl.DataFrame], pl.DataFrame],
    failing: set[tuple[str, str | None]],
) -> None:
    for stage, table in tables:
        _edit(table_path(release_dir, stage, table), edit)

    results = _report(release_dir)

    rows = {
        (gate, subject): (passed, value)
        for gate, subject, passed, value in results.select(
            "gate", "subject", "passed", "value"
        ).rows()
    }
    assert {key: rows[key] for key in failing} == dict.fromkeys(failing, (False, None))


@pytest.mark.parametrize(
    ("pairs", "outcome"),
    [
        # Agency 0, 1 and null stand for the provider itself; 5/2 is KfW.
        ([(NOR, 0), (NOR, 1), (NOR, None), (5, 2)], (True, 0.0)),
        ([(NOR, 98)], (False, 1.0)),
        ([(99999, None)], (False, 1.0)),
        ([], (False, None)),
    ],
)
def test_listed_codes_need_a_listing_row_for_each_agency_beyond_the_provider(
    pairs: list[tuple[int, int | None]], outcome: tuple[bool, float | None]
) -> None:
    table = pl.LazyFrame(
        pairs,
        schema={"provider_code": pl.Int32, "agency_code": pl.Int32},
        orient="row",
    )

    row = listing_gate({"disbursements": table}).row(0, named=True)

    assert (row["passed"], row["value"]) == outcome


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
    gates = GATES_CONFIG.model_copy(update={"drift": Drift(max_move=0.05)})

    row = (
        _report(
            release_dir,
            previous=_previous(release_dir, factor=factor),
            first_release=False,
            drift_causes=causes,
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
    results = _report(release_dir, sample=design.parent)
    release = copy_release(tmp_path / "gated", source=release_dir / "assemble")
    manifest = ReleaseManifest.model_validate_json(
        (release / "release.json").read_text(encoding="utf-8")
    )
    gated = manifest.model_copy(update={"gates": gate_results_json(results)})
    (release / "release.json").write_text(gated.model_dump_json(), encoding="utf-8")

    precision = cf.load(path=release).quality().precision
    recorded = json.loads((release / "release.json").read_text(encoding="utf-8"))[
        "gates"
    ]

    assert recorded["precision"][0]["strata"][0]["estimate"] == pytest.approx(0.4)
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
        BUILD_TOML.read_text(encoding="utf-8").replace(
            'review_decisions = "data/review_decisions.parquet"',
            f"review_decisions = {json.dumps(str(decisions))}",
        ),
        encoding="utf-8",
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
        str(out),
    ]
    missing = runner.invoke(app, [*gate, "--first-release", "--report"])
    decisions.parent.mkdir()
    _empty(schema.REVIEW_DECISIONS).write_parquet(decisions)
    reported = runner.invoke(app, [*gate, "--first-release", "--report"])
    refused = runner.invoke(app, gate)

    assert drawn.exit_code == 0, drawn.output
    assert pl.read_parquet(design_path(out)).height == 2
    # A refusal prints as one line naming its class, without a traceback.
    assert missing.exit_code == 1
    assert missing.output.startswith("ReviewDecisionsError: ")
    assert str(decisions) in missing.output
    assert "cf-build import-labels" in missing.output
    assert reported.exit_code == 0, reported.output
    assert f"precision {NOR}: FAIL; continue after 0 labels (census)" in (
        reported.output
    )
    # Without --report every row still prints before the command fails.
    assert refused.exit_code == 1
    assert refused.output.startswith(reported.output.split("\n")[0])
    assert "drift: FAIL" in refused.output
    assert "first_release" in refused.output

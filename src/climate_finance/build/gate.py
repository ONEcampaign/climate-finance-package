"""Release gates, with thresholds from `gates.yaml`.

`run_gates` reads a build's release directory, `<out_dir>/<release>`: the release
tables in `assemble/` and the stage tables behind them. It returns one row per gate
in `GATES` order, with a row per provider for precision and one per check for
over-allocation:

- `precision`: each provider's blind-sample estimate passes the sequential rule
  (`evaluate.estimate_precision`). Without a blind sample every provider fails.
- `conservation`: each linked CRS record's allocations, the unallocated excess
  included, sum to its disbursement.
- `over_allocation`: no commitment row is disbursed above its face value, and few
  linked commitment units are linked to more than `raw_factor` times their face.
- `oecd_reconciliation`: release climate commitments by provider and year, rows
  that repeat an earlier vintage included, match the published CRDF.
- `drift`: provider-year climate commitments and disbursements move by at most
  `max_move` from the previous release, unless a recorded cause covers the move.
- `gcf_rows`: no release row comes from Green Climate Fund data, whose terms bar
  redistribution.

Every value is a largest gap or a count, and a row passes at or below its threshold,
except precision, whose lower bound passes at or above it.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import yaml
from pydantic import BaseModel, ConfigDict

import climate_finance as cf
from climate_finance.build import evaluate
from climate_finance.build.errors import GateFailedError
from climate_finance.build.normalise.crs import trace_population
from climate_finance.build.sums import ordered_sum
from climate_finance.schema import ReleaseManifest

GATES_YAML = Path(__file__).with_name("gates.yaml")
GATES = (
    "precision",
    "conservation",
    "over_allocation",
    "oecd_reconciliation",
    "drift",
    "gcf_rows",
)
RESULT_SCHEMA = pl.Schema(
    {
        "gate": pl.String(),
        "subject": pl.String(),
        "value": pl.Float64(),
        "lower": pl.Float64(),
        "upper": pl.Float64(),
        "threshold": pl.Float64(),
        "passed": pl.Boolean(),
        "waived": pl.Boolean(),
        "detail": pl.String(),
    }
)
# Where a disbursement row's record id shows Green Climate Fund data: the GCF's
# ledger and readiness list, and its IATI file. The GCF project list
# (`gcf_projects`) only links CRDF rows to CRS records and adds no row.
GCF_RECORD_PREFIXES = ("ledger:gcf:", "iati:gcf:")
# The provider-year totals the drift gate compares, from the `quality` table.
DRIFT_MEASURES = ("climate_usd", "disbursed_climate_usd")
# How many failing items a row's detail names.
_SHOWN = 5


class _Section(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class Conservation(_Section):
    tolerance_usd: float


class OverAllocation(_Section):
    tolerance_usd: float
    raw_factor: float
    max_share: float


class Reconciliation(_Section):
    tolerance: float


class DriftCause(_Section):
    """Why a provider-year total moved in `release`."""

    release: str
    provider_code: int
    year: int
    cause: str


class Drift(_Section):
    max_move: float
    causes: tuple[DriftCause, ...] = ()


class GateConfig(_Section):
    """The contents of `gates.yaml`."""

    precision: evaluate.PrecisionRule
    conservation: Conservation
    over_allocation: OverAllocation
    oecd_reconciliation: Reconciliation
    drift: Drift


def load_gates(path: Path = GATES_YAML) -> GateConfig:
    """Read the gate thresholds at `path`."""
    return GateConfig.model_validate(yaml.safe_load(path.read_text()))


def run_gates(
    release_dir: Path,
    *,
    decisions: Path,
    sample: Path | None = None,
    previous: Path | None = None,
    first_release: bool = False,
    report_only: bool = False,
    waive_precision: bool = False,
    gates: GateConfig | None = None,
) -> pl.DataFrame:
    """Evaluate every gate on the build at `release_dir`.

    Args:
        release_dir: a build's directory, `<out_dir>/<release>`.
        decisions: the review-decisions parquet holding the blind-sample labels.
        sample: the blind sample's design file (`evaluate.design_path`); without
            one, every provider fails precision.
        previous: the previous release, a directory `cf.load(path=...)` opens.
        first_release: pass the drift gate when there is no previous release.
        report_only: return the results without raising on a failure.
        waive_precision: let failed precision rows pass, marked `waived`.
        gates: thresholds; `gates.yaml` when not given.

    Returns:
        Rows in `RESULT_SCHEMA`, in `GATES` order.

    Raises:
        GateFailedError: when a row fails and is not waived, unless `report_only`.
    """
    from climate_finance.build.stages import table_path

    gates = gates or load_gates()
    assembled = release_dir / "assemble"

    def scan(stage: str, table: str) -> pl.LazyFrame:
        return pl.scan_parquet(table_path(release_dir, stage, table))

    def released(table: str) -> pl.LazyFrame:
        return pl.scan_parquet(assembled / f"{table}.parquet")

    manifest = ReleaseManifest.model_validate_json(
        (assembled / "release.json").read_text()
    )
    links, units = scan("review", "links"), scan("link", "units")
    results = pl.concat(
        [
            precision_gate(
                evaluate.allocated_pairs(release_dir, decisions=decisions),
                sample=sample,
                decisions=decisions,
                rule=gates.precision,
            ),
            conservation_gate(
                allocations=scan("fill", "allocations"),
                links=links,
                units=units,
                crs=trace_population(scan("normalise", "crs")),
                config=gates.conservation,
            ),
            over_allocation_gate(
                commitments=released("commitments"),
                links=links,
                units=units,
                config=gates.over_allocation,
            ),
            reconciliation_gate(
                commitments=released("commitments"),
                crdf=scan("normalise", "crdf"),
                config=gates.oecd_reconciliation,
            ),
            drift_gate(
                quality=released("quality"),
                release=manifest.release,
                previous=previous,
                first_release=first_release,
                config=gates.drift,
            ),
            gcf_gate(released("disbursements")),
        ]
    ).with_columns(
        waived=(pl.col("gate") == "precision") & ~pl.col("passed") & waive_precision
    )
    failed = results.filter(~pl.col("passed") & ~pl.col("waived"))
    if failed.height and not report_only:
        raise GateFailedError(
            [
                " ".join(filter(None, (gate, subject))) + f": {detail}"
                for gate, subject, detail in failed.select(
                    "gate", "subject", "detail"
                ).iter_rows()
            ]
        )
    return results


def gate_results_json(results: pl.DataFrame) -> dict[str, list[dict[str, object]]]:
    """`run_gates` rows for `release.json`, keyed by gate name."""
    return {
        gate: results.filter(gate=gate).drop("gate").to_dicts()
        for gate in GATES
        if results.filter(gate=gate).height
    }


def _row(
    gate: str,
    *,
    value: float | None,
    threshold: float,
    passed: bool,
    detail: str,
    subject: str | None = None,
) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "gate": gate,
                "subject": subject,
                "value": value,
                "lower": None,
                "upper": None,
                "threshold": threshold,
                "passed": passed,
                "waived": False,
                "detail": detail,
            }
        ],
        schema=RESULT_SCHEMA,
    )


def _largest(values: pl.Series) -> float:
    return max(values.to_list(), default=0.0)


def _named(items: list[str]) -> str:
    shown = ", ".join(items[:_SHOWN])
    return f"{len(items)}, such as {shown}" if len(items) > _SHOWN else shown


def precision_gate(
    pairs: pl.DataFrame,
    *,
    sample: Path | None,
    decisions: Path,
    rule: evaluate.PrecisionRule,
) -> pl.DataFrame:
    """One row per provider with allocated pairs (`evaluate.allocated_pairs`): its
    estimate and bounds, passing when the sequential rule decides `pass`."""
    estimates = (
        evaluate.estimate_precision(
            evaluate.read_sample_labels(sample, decisions=decisions), rule=rule
        )
        if sample is not None
        else pl.DataFrame(schema=evaluate.ESTIMATE_SCHEMA)
    )
    providers = pairs.select("provider_code").unique()
    rows = providers.join(estimates, on="provider_code", how="left").sort(
        "provider_code"
    )
    detail = (
        pl.when(pl.col("design").is_null())
        .then(pl.lit("no blind sample"))
        .otherwise(
            pl.format(
                "{} after {} labels ({})",
                pl.col("decision"),
                pl.col("labels"),
                pl.col("design"),
            )
        )
    )
    return rows.select(
        gate=pl.lit("precision"),
        subject=pl.col("provider_code").cast(pl.String),
        value="estimate",
        lower="lower",
        upper="upper",
        threshold=pl.lit(rule.threshold),
        passed=(pl.col("decision") == "pass").fill_null(value=False),
        waived=pl.lit(value=False),
        detail=detail,
    ).cast(RESULT_SCHEMA)


def conservation_gate(
    *,
    allocations: pl.LazyFrame,
    links: pl.LazyFrame,
    units: pl.LazyFrame,
    crs: pl.LazyFrame,
    config: Conservation,
) -> pl.DataFrame:
    """The largest gap between a linked CRS record's disbursement and its
    allocations, the null-row and excess rows included."""
    linked = (
        links.filter(task="trace")
        .select(unit_id="left_id")
        .unique()
        .join(units.select("unit_id", record_id="record_ids"), on="unit_id")
        .explode("record_id")
        .select("record_id")
        .unique()
    )
    given = (
        allocations.filter(disbursement_source="crs")
        .group_by("record_id")
        .agg(given_usd=ordered_sum(pl.col("allocated_usd")))
    )
    gaps = (
        crs.join(linked, on="record_id")
        .filter(pl.col("disbursement_usd") > 0)
        .join(given, on="record_id", how="left")
        .select(
            "record_id",
            gap=(pl.col("disbursement_usd") - pl.col("given_usd").fill_null(0.0)).abs(),
        )
        .filter(pl.col("gap") > config.tolerance_usd)
        .sort("gap", "record_id", descending=[True, False])
        .collect()
    )
    return _row(
        "conservation",
        value=_largest(gaps["gap"]),
        threshold=config.tolerance_usd,
        passed=gaps.is_empty(),
        detail=(
            f"records whose allocations miss their disbursement: "
            f"{_named(gaps['record_id'].to_list())}"
            if gaps.height
            else "every linked record's disbursement is allocated or excess"
        ),
    )


def over_allocation_gate(
    *,
    commitments: pl.LazyFrame,
    links: pl.LazyFrame,
    units: pl.LazyFrame,
    config: OverAllocation,
) -> pl.DataFrame:
    """Two rows: the largest amount by which a commitment row is disbursed above
    its face, and the share of linked commitment units whose links bring more than
    `raw_factor` times their face.

    A commitment unit's raw linked disbursement spreads each disbursement unit
    linked to it over every commitment unit that unit links to, by face, before
    allocation's year order and face cap.
    """
    above = (
        commitments.filter(pl.col("face_usd") > 0)
        .select("row_id", excess=pl.col("disbursed_usd") - pl.col("face_usd"))
        .filter(pl.col("excess") > config.tolerance_usd)
        .sort("excess", "row_id", descending=[True, False])
        .collect()
    )
    trace = (
        links.filter(task="trace")
        .select(d_unit="left_id", c_unit="right_id")
        .unique()
        .join(units.select(d_unit="unit_id", disbursed="disbursement_usd"), on="d_unit")
        .join(units.select(c_unit="unit_id", face="commitment_usd"), on="c_unit")
    )
    raw = (
        trace.with_columns(reach=pl.col("face").sum().over("d_unit"))
        .with_columns(spread=pl.col("disbursed") * pl.col("face") / pl.col("reach"))
        .group_by("c_unit")
        .agg(pl.col("face").first(), raw_usd=ordered_sum(pl.col("spread")))
        .collect()
    )
    over = raw.filter(pl.col("raw_usd") > config.raw_factor * pl.col("face"))
    share = over.height / raw.height if raw.height else 0.0
    return pl.concat(
        [
            _row(
                "over_allocation",
                subject="above_face",
                value=_largest(above["excess"]),
                threshold=config.tolerance_usd,
                passed=above.is_empty(),
                detail=(
                    f"rows disbursed above face: {_named(above['row_id'].to_list())}"
                    if above.height
                    else "no row is disbursed above its face"
                ),
            ),
            _row(
                "over_allocation",
                subject="raw_links",
                value=share,
                threshold=config.max_share,
                passed=share < config.max_share,
                detail=(
                    f"{over.height} of {raw.height} linked commitment units are "
                    f"linked to more than {config.raw_factor} times their face"
                ),
            ),
        ]
    )


def reconciliation_gate(
    *, commitments: pl.LazyFrame, crdf: pl.LazyFrame, config: Reconciliation
) -> pl.DataFrame:
    """The largest relative gap, by provider and year, between the release's climate
    commitments and the published CRDF's.

    The published total holds the recipient-perspective rows as the OECD publishes
    them, aggregate parents in place of their rebuilt children. The release total
    holds every `commitments` row, repeats of an earlier vintage included.
    """
    key = ["provider_code", "year"]
    published = (
        crdf.filter(
            pl.col("perspective") == "recipient", pl.col("parent_row_id").is_null()
        )
        .group_by(key)
        .agg(published_usd=ordered_sum(pl.col("climate_usd")))
    )
    built = commitments.group_by(key).agg(
        released_usd=ordered_sum(pl.col("climate_usd"))
    )
    released, published_usd = pl.col("released_usd"), pl.col("published_usd")
    difference = (released.fill_null(0.0) - published_usd.fill_null(0.0)).abs()
    gaps = (
        built.join(published, on=key, how="full", coalesce=True)
        .select(
            *key,
            gap=pl.when(difference == 0)
            .then(0.0)
            .otherwise(difference / published_usd.fill_null(0.0).abs()),
        )
        .filter(pl.col("gap") > config.tolerance)
        .sort("gap", *key, descending=[True, False, False])
        .collect()
    )
    named = [f"{provider}/{year}" for provider, year in gaps.select(key).iter_rows()]
    return _row(
        "oecd_reconciliation",
        value=_largest(gaps["gap"]),
        threshold=config.tolerance,
        passed=gaps.is_empty(),
        detail=(
            f"provider-years off the published CRDF: {_named(named)}"
            if gaps.height
            else "every provider-year matches the published CRDF"
        ),
    )


def drift_gate(
    *,
    quality: pl.LazyFrame,
    release: str,
    previous: Path | None,
    first_release: bool,
    config: Drift,
) -> pl.DataFrame:
    """The largest relative move of a provider-year total (`DRIFT_MEASURES`) from
    the previous release that no cause in `config` explains for `release`.

    Without a previous release the gate fails, naming the `first_release` override.
    """
    if previous is None:
        return _row(
            "drift",
            value=None,
            threshold=config.max_move,
            passed=first_release,
            detail=(
                "first release: no previous release to compare"
                if first_release
                else "no previous release to compare; give the previous release, or "
                "pass first_release (cf-build gate --first-release) for the first"
            ),
        )
    key = ["provider_code", "year"]
    before = cf.load(path=previous).table("quality")
    totals = [
        table.group_by(key).agg(
            *(ordered_sum(pl.col(m)).alias(f"{m}_{side}") for m in DRIFT_MEASURES)
        )
        for table, side in ((quality, "now"), (before, "before"))
    ]
    explained = pl.LazyFrame(
        [(c.provider_code, c.year) for c in config.causes if c.release == release],
        schema={"provider_code": pl.Int32(), "year": pl.Int16()},
        orient="row",
    ).with_columns(explained=pl.lit(value=True))
    moves = []
    for measure in DRIFT_MEASURES:
        now = pl.col(f"{measure}_now").fill_null(0.0)
        then = pl.col(f"{measure}_before").fill_null(0.0)
        moves.append(
            pl.when(now == then)
            .then(0.0)
            .otherwise((now - then).abs() / then.abs())
            .alias(measure)
        )
    unexplained = (
        totals[0]
        .join(totals[1], on=key, how="full", coalesce=True)
        .select(*key, move=pl.max_horizontal(moves))
        .join(explained, on=key, how="left")
        .filter(pl.col("move") > config.max_move, pl.col("explained").is_null())
        .sort("move", *key, descending=[True, False, False])
        .collect()
    )
    named = [
        f"{provider}/{year}" for provider, year in unexplained.select(key).iter_rows()
    ]
    return _row(
        "drift",
        value=_largest(unexplained["move"]),
        threshold=config.max_move,
        passed=unexplained.is_empty(),
        detail=(
            f"provider-years that moved without a recorded cause: {_named(named)}"
            if unexplained.height
            else "no provider-year moved without a recorded cause"
        ),
    )


def gcf_gate(disbursements: pl.LazyFrame) -> pl.DataFrame:
    """The number of `disbursements` rows from Green Climate Fund data
    (`GCF_RECORD_PREFIXES`)."""
    rows = (
        disbursements.filter(
            pl.any_horizontal(
                pl.col("record_id").str.starts_with(prefix)
                for prefix in GCF_RECORD_PREFIXES
            )
        )
        .select("record_id")
        .collect()
    )
    records = rows["record_id"].unique().sort().to_list()
    return _row(
        "gcf_rows",
        value=float(rows.height),
        threshold=0.0,
        passed=rows.is_empty(),
        detail=(
            f"disbursements rows from GCF records: {_named(records)}"
            if rows.height
            else "no release row comes from GCF data"
        ),
    )

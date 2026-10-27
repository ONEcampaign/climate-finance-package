"""Release gates, with thresholds from `gates.yaml`.

`run_gates` reads a build's release directory, `<out_dir>/<release>`: the release
tables in `assemble/` and the stage tables behind them. It returns one row per gate
in `GATES` order, with a row per provider for precision and one per check for
over-allocation:

- `precision`: each provider's blind-sample estimate passes the sequential rule
  (`evaluate.estimate_precision`), with the value of pairs that link a unit to
  itself (`evaluate.self_linked`) counted right, and its row lists each sampled
  stratum's estimate and interval in `strata`. A provider whose every pair links a
  unit to itself passes at 1 without a sample. Without a blind sample every other
  provider fails, and so does a provider whose sample was drawn from another
  population of allocated pairs (`evaluate.population_fingerprints`) or whose
  design records no population.
- `human_audit`: at least `min_share` of the blind sample's draws carry a person's
  decision.
- `conservation`: each linked CRS record up to `years.crs_last_complete`, the
  records allocation reads, has allocations, the unallocated excess included, that
  sum to its disbursement.
- `reflow_conservation`: by provider and year, the bulk CRS's principal repaid
  (`usd_received`) and interest paid (`usd_interest`), the normalise `reflows`
  table's and the release `reflows` table's, summed over every basis, agree from the
  first CRS year to `years.crs_last_complete`. Both rows fail when the build lacks
  the raw CRS, one of the tables or the years.
- `over_allocation`: no commitment row is disbursed above its face value, and few
  of the commitment units behind the release's commitment rows are linked to more
  than `raw_factor` times their face.
- `oecd_reconciliation`: release climate commitments by provider and year, rows
  that repeat an earlier vintage included, match the published CRDF.
- `drift`: provider-year climate commitments and disbursements move by at most
  `max_move` from the previous release, unless a recorded cause covers the move.
- `withheld_rows`: no release row comes from a source the release withholds, such
  as the Green Climate Fund's ledger, whose terms allow only private study, research
  and teaching (`schema.withheld_record`).
- `listed_codes`: every provider, agency and recipient code in the release tables has
  a row in the package's listings (`listings.unlisted_codes`), so a query can refuse
  a code the listings lack.

Every value is a largest gap, a count or a share. A row passes at or below its
threshold, except that the raw-links share passes below it, and precision's lower
bound and the human-audit share pass at or above it. A gate with nothing to
measure fails: no allocated pairs, no linked record with a disbursement, no
repayment in the raw CRS, no linked commitment unit, no provider-year in the release
or the published CRDF, no disbursement row, no provider code, or an empty sample. A
precision or human_audit row that could not estimate says why in `code`:
`no_allocated_pairs`, `no_sample`, `empty_sample` or `stale_sample`, and a precision
row whose estimate or bound is NaN or infinite fails as `non_finite_estimate`. A
reflow_conservation row that could not read an input fails as `missing_input`. The
other gates' rows carry no `code`.
`waive_precision` lets precision and human_audit rows fail.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import polars as pl
import yaml
from pydantic import BaseModel, ConfigDict

import climate_finance as cf
from climate_finance.build import evaluate, listings
from climate_finance.build.errors import GateFailedError
from climate_finance.build.normalise.crs import allocated_population
from climate_finance.build.sums import ordered_sum
from climate_finance.schema import (
    RELEASE_TABLES,
    ReleaseManifest,
    scan_table,
    withheld_record,
)

GATES_YAML = Path(__file__).with_name("gates.yaml")
GATES = (
    "precision",
    "human_audit",
    "conservation",
    "reflow_conservation",
    "over_allocation",
    "oecd_reconciliation",
    "drift",
    "withheld_rows",
    "listed_codes",
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
        "code": pl.String(),
        "strata": pl.List(evaluate.STRATUM_ESTIMATE),
    }
)
# The provider-year totals the drift gate compares, from the `quality` table.
DRIFT_MEASURES = ("climate_usd", "disbursed_climate_usd")
# The reflow_conservation rows: each repayment's column in the raw CRS (USD million)
# and in the `reflows` tables (USD).
REFLOW_MEASURES = {
    "received": ("usd_received", "received_usd"),
    "interest": ("usd_interest", "interest_usd"),
}
_USD_PER_MILLION = 1e6
# How many failing items a row's detail names.
_SHOWN = 5
# The gates `waive_precision` lets fail: both certify the blind sample.
WAIVABLE = ("precision", "human_audit")
# The details of a `stale_sample` precision row.
_STALE = (
    "stale sample: drawn from another population of allocated pairs, as pairs, link "
    "methods, strata or allocated values changed; draw a new sample"
)
_UNFINGERPRINTED = (
    "stale sample: the design records no population fingerprint, so nothing ties it "
    "to this build's allocated pairs; draw a new sample"
)


class _Section(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class HumanAudit(_Section):
    min_share: float


class Conservation(_Section):
    tolerance_usd: float


class ReflowConservation(_Section):
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
    human_audit: HumanAudit
    conservation: Conservation
    reflow_conservation: ReflowConservation
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
            one, every provider fails precision and the human audit fails.
        previous: the previous release, a directory `cf.load(path=...)` opens.
        first_release: pass the drift gate when there is no previous release.
        report_only: return the results without raising on a failure.
        waive_precision: let failed precision and human_audit rows pass, marked
            `waived`.
        gates: thresholds, read from `gates.yaml` when not given.

    Returns:
        Rows in `RESULT_SCHEMA`, in `GATES` order.

    Raises:
        GateFailedError: when a row fails and is not waived, unless `report_only`.
        ReviewDecisionsError: when `sample` is given and `decisions` does not
            exist.
    """
    from climate_finance.build.fetch import CRS_FILE
    from climate_finance.build.stages import raw_dir, table_path

    gates = gates or load_gates()
    assembled = release_dir / "assemble"

    def scan(stage: str, table: str) -> pl.LazyFrame:
        return scan_table(table_path(release_dir, stage, table))

    def released(table: str) -> pl.LazyFrame:
        return scan_table(assembled / f"{table}.parquet")

    manifest = ReleaseManifest.model_validate_json(
        (assembled / "release.json").read_text()
    )
    raw_crs = raw_dir(release_dir) / CRS_FILE
    first_year = manifest.years.get("crs", (None,))[0]
    last_year = manifest.settings.get("years.crs_last_complete")
    links, units = scan("review", "links"), scan("link", "units")
    labels = (
        None
        if sample is None
        else evaluate.read_sample_labels(sample, decisions=decisions)
    )
    results = pl.concat(
        [
            precision_gate(
                evaluate.allocated_pairs(release_dir, decisions=decisions),
                labels=labels,
                rule=gates.precision,
            ),
            human_audit_gate(labels, config=gates.human_audit),
            conservation_gate(
                allocations=scan("fill", "allocations"),
                links=links,
                units=units,
                crs=scan("normalise", "crs"),
                last_year=last_year if isinstance(last_year, int) else None,
                config=gates.conservation,
            ),
            reflow_conservation_gate(
                raw=pl.scan_parquet(raw_crs) if raw_crs.exists() else None,
                staged=_scan_if_present(
                    table_path(release_dir, "normalise", "reflows")
                ),
                released=_scan_if_present(assembled / "reflows.parquet"),
                years=(
                    (first_year, last_year)
                    if isinstance(first_year, int) and isinstance(last_year, int)
                    else None
                ),
                config=gates.reflow_conservation,
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
            withheld_gate({name: released(name) for name in RELEASE_TABLES}),
            listing_gate({name: released(name) for name in RELEASE_TABLES}),
        ]
    ).with_columns(
        waived=pl.col("gate").is_in(WAIVABLE) & ~pl.col("passed") & waive_precision
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
    code: str | None = None,
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
                "code": code,
                "strata": None,
            }
        ],
        schema=RESULT_SCHEMA,
    )


def _scan_if_present(path: Path) -> pl.LazyFrame | None:
    return scan_table(path) if path.exists() else None


def _largest(values: pl.Series) -> float:
    return max(values.to_list(), default=0.0)


def _named(items: list[str]) -> str:
    shown = ", ".join(items[:_SHOWN])
    return f"{len(items)}, such as {shown}" if len(items) > _SHOWN else shown


def precision_gate(
    pairs: pl.DataFrame,
    *,
    labels: pl.DataFrame | None,
    rule: evaluate.PrecisionRule,
) -> pl.DataFrame:
    """One row per provider with allocated pairs (`evaluate.allocated_pairs`) or
    sampled draws (`labels`, from `evaluate.read_sample_labels`): its estimate and
    bounds, with its self-linked value counted right
    (`evaluate.self_linked_shares`), passing when the sequential rule decides
    `pass`. The detail gives the self-linked share of the provider's value. A
    provider whose every pair is self-linked has nothing to sample and passes at 1.

    A provider fails unestimated, coded `stale_sample`, when its sample's population
    fingerprint is not that of its allocated pairs in this build
    (`evaluate.population_fingerprints`): pairs were added or dropped, or changed
    method, stratum or value, so the sample and its weights describe another
    population. So does every provider of a design that records no population, as
    nothing ties it to this build. With no allocated pairs one row fails, as nothing
    is certified.
    """
    if pairs.is_empty():
        return _row(
            "precision",
            value=None,
            threshold=rule.threshold,
            passed=False,
            detail="no allocated pairs: nothing to certify",
            code="no_allocated_pairs",
        )
    if labels is None:
        labels = pl.DataFrame(schema=evaluate.DESIGN_SCHEMA)
    if "population" in labels.columns:
        stale = (
            labels.select("provider_code", "population")
            .unique()
            .join(
                evaluate.population_fingerprints(pairs),
                on=["provider_code", "population"],
                how="anti",
            )
            .select("provider_code", stale=pl.lit(_STALE))
        )
    else:
        stale = (
            labels.select("provider_code")
            .unique()
            .with_columns(stale=pl.lit(_UNFINGERPRINTED))
        )
    fresh = labels.join(stale, on="provider_code", how="anti")
    certain = evaluate.self_linked_shares(pairs)
    estimates = (
        evaluate.estimate_precision(fresh, rule=rule, certain=certain)
        if fresh.height
        else pl.DataFrame(schema=evaluate.ESTIMATE_SCHEMA)
    ).drop("self_linked_share")
    providers = pl.concat(
        [pairs.select("provider_code"), labels.select("provider_code")]
    ).unique()
    rows = (
        providers.join(estimates, on="provider_code", how="left")
        .join(stale, on="provider_code", how="left")
        .join(certain, on="provider_code", how="left")
        .with_columns(pl.col("self_linked_share").fill_null(0.0))
        .sort("provider_code")
    )
    share = pl.col("self_linked_share")
    # Every pair links a unit to itself, so there is nothing to sample.
    whole = (share == 1) & pl.col("stale").is_null()
    certain_part = (
        pl.when(share > 0)
        .then(pl.format(", {}% of value self-linked", (share * 100).round(1)))
        .otherwise(pl.lit(""))
    )
    detail = (
        pl.when(pl.col("stale").is_not_null())
        .then("stale")
        .when(whole)
        .then(pl.lit("every allocated pair links a unit to itself"))
        .when(pl.col("design").is_null())
        .then(pl.lit("no blind sample"))
        .otherwise(
            pl.format(
                "{} after {} labels ({})",
                pl.col("decision"),
                pl.col("labels"),
                pl.col("design"),
            )
            + pl.when(pl.col("agent_share") > 0)
            .then(
                pl.format(
                    ", {}% decided by agents",
                    (pl.col("agent_share") * 100).round(0).cast(pl.Int32),
                )
            )
            .otherwise(pl.lit(""))
            + certain_part
        )
    )
    exact = pl.when(whole).then(1.0)
    return rows.select(
        gate=pl.lit("precision"),
        subject=pl.col("provider_code").cast(pl.String),
        value=exact.otherwise("estimate"),
        lower=exact.otherwise("lower"),
        upper=exact.otherwise("upper"),
        threshold=pl.lit(rule.threshold),
        passed=(
            whole | ((pl.col("decision") == "pass") & pl.col("stale").is_null())
        ).fill_null(value=False),
        waived=pl.lit(value=False),
        detail=detail,
        code=pl.when(pl.col("stale").is_not_null())
        .then(pl.lit("stale_sample"))
        .when(whole)
        .then(None)
        .when(pl.col("design").is_null())
        .then(pl.lit("no_sample"))
        .when(evaluate.non_finite())
        .then(pl.lit("non_finite_estimate")),
        strata="strata",
    ).cast(RESULT_SCHEMA)


def human_audit_gate(
    labels: pl.DataFrame | None, *, config: HumanAudit
) -> pl.DataFrame:
    """The share of the blind sample's draws (`labels`, from
    `evaluate.read_sample_labels`) whose standing decision is a person's, over every
    provider. It fails without a sample or with an empty one."""
    if labels is None or labels.is_empty():
        return _row(
            "human_audit",
            value=None,
            threshold=config.min_share,
            passed=False,
            detail="no blind sample" if labels is None else "the blind sample is empty",
            code="no_sample" if labels is None else "empty_sample",
        )
    people = int((labels["label_source"] == "human").sum())
    share = people / labels.height
    return _row(
        "human_audit",
        value=share,
        threshold=config.min_share,
        passed=share >= config.min_share,
        detail=f"{people} of {labels.height} draws decided by a person",
    )


def conservation_gate(
    *,
    allocations: pl.LazyFrame,
    links: pl.LazyFrame,
    units: pl.LazyFrame,
    crs: pl.LazyFrame,
    last_year: int | None,
    config: Conservation,
) -> pl.DataFrame:
    """The largest gap between a linked CRS record's disbursement and its
    allocations, the null-row and excess rows included, over the records allocation
    reads (`allocated_population`) up to `last_year`, `years.crs_last_complete`. It
    fails when `last_year` is None or no linked record has a disbursement."""
    if last_year is None:
        return _row(
            "conservation",
            value=None,
            threshold=config.tolerance_usd,
            passed=False,
            detail="release.json records no years.crs_last_complete",
        )
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
    disbursed = (
        allocated_population(crs, last_year=last_year)
        .join(linked, on="record_id")
        .filter(pl.col("disbursement_usd") > 0)
        .join(given, on="record_id", how="left")
        .select(
            "record_id",
            gap=(pl.col("disbursement_usd") - pl.col("given_usd").fill_null(0.0)).abs(),
        )
        .collect()
    )
    if disbursed.is_empty():
        return _row(
            "conservation",
            value=None,
            threshold=config.tolerance_usd,
            passed=False,
            detail="no linked record with a disbursement: nothing to conserve",
        )
    gaps = disbursed.filter(pl.col("gap") > config.tolerance_usd).sort(
        "gap", "record_id", descending=[True, False]
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


def reflow_conservation_gate(
    *,
    raw: pl.LazyFrame | None,
    staged: pl.LazyFrame | None,
    released: pl.LazyFrame | None,
    years: tuple[int, int] | None,
    config: ReflowConservation,
) -> pl.DataFrame:
    """Two rows, `received` and `interest`: the largest gap in USD, over provider
    and year, between the bulk CRS's repayments (`raw`, in USD million), the
    normalise `reflows` table's (`staged`) and the release `reflows` table's
    (`released`), summed over every basis, for `years`, the first and last year
    inclusive.

    Both rows fail, coded `missing_input` and naming each input that is `None`, when
    one cannot be read. A row also fails when no repayment of its kind is in any of
    the three.
    """
    if raw is None or staged is None or released is None or years is None:
        absent = [
            name
            for name, held in (
                ("raw CRS", raw),
                ("normalise reflows", staged),
                ("release reflows", released),
                ("CRS years", years),
            )
            if held is None
        ]
        return pl.concat(
            _row(
                "reflow_conservation",
                subject=subject,
                value=None,
                threshold=config.tolerance_usd,
                passed=False,
                detail=f"cannot read {', '.join(absent)}: nothing to conserve",
                code="missing_input",
            )
            for subject in REFLOW_MEASURES
        )
    key = ["provider_code", "year"]

    def totals(
        frame: pl.LazyFrame, *, provider: str, column: str, scale: float, name: str
    ) -> pl.LazyFrame:
        return (
            frame.filter(pl.col("year").is_between(*years))
            .select(
                pl.col(provider).alias("provider_code"),
                "year",
                (pl.col(column).fill_null(0.0) * scale).alias("usd"),
            )
            .filter(pl.col("usd") != 0)
            .group_by(key)
            .agg(ordered_sum(pl.col("usd")).alias(name))
        )

    rows = []
    for subject, (raw_column, column) in REFLOW_MEASURES.items():
        sums = ["raw_usd", "staged_usd", "released_usd"]
        compared = (
            totals(
                raw,
                provider="donor_code",
                column=raw_column,
                scale=_USD_PER_MILLION,
                name=sums[0],
            )
            .join(
                totals(
                    staged,
                    provider="donor_code",
                    column=column,
                    scale=1.0,
                    name=sums[1],
                ),
                on=key,
                how="full",
                coalesce=True,
            )
            .join(
                totals(
                    released,
                    provider="provider_code",
                    column=column,
                    scale=1.0,
                    name=sums[2],
                ),
                on=key,
                how="full",
                coalesce=True,
            )
            .select(
                *key,
                gap=pl.max_horizontal(pl.col(sums).fill_null(0.0))
                - pl.min_horizontal(pl.col(sums).fill_null(0.0)),
            )
            .collect()
        )
        gaps = compared.filter(pl.col("gap") > config.tolerance_usd).sort(
            "gap", *key, descending=[True, False, False]
        )
        named = [
            f"{provider}/{year}" for provider, year in gaps.select(key).iter_rows()
        ]
        rows.append(
            _row(
                "reflow_conservation",
                subject=subject,
                value=None if compared.is_empty() else _largest(gaps["gap"]),
                threshold=config.tolerance_usd,
                passed=gaps.is_empty() and not compared.is_empty(),
                detail=(
                    f"no {subject} in the raw CRS, the normalise table or the "
                    "release table: nothing to conserve"
                    if compared.is_empty()
                    else f"provider-years whose {subject} differs across the raw "
                    f"CRS, the normalise table and the release table: {_named(named)}"
                    if gaps.height
                    else f"every provider-year's {subject} agrees across the raw "
                    "CRS, the normalise table and the release table"
                ),
            )
        )
    return pl.concat(rows)


def over_allocation_gate(
    *,
    commitments: pl.LazyFrame,
    links: pl.LazyFrame,
    units: pl.LazyFrame,
    config: OverAllocation,
) -> pl.DataFrame:
    """Two rows: the largest amount by which a commitment row is disbursed above
    its face, and the share of linked commitment units whose links bring more than
    `raw_factor` times their face. The share passes below `max_share`, and fails
    when no commitment unit is linked.

    The share counts the CRS commitment units that a `commitments` row reaches
    through a `crdf_crs` link, the climate commitments the release carries. CRS
    commitments the release does not publish stay out of it. A commitment unit's
    raw linked disbursement spreads each disbursement unit linked to it over every
    commitment unit that unit links to, released or not, by face, before
    allocation's year order and face cap.
    """
    # No commitment row means no row above its face, so an empty `above` passes.
    # Conservation fails a build with nothing linked.
    above = (
        commitments.filter(pl.col("face_usd") > 0)
        .select("row_id", excess=pl.col("disbursed_usd") - pl.col("face_usd"))
        .filter(pl.col("excess") > config.tolerance_usd)
        .sort("excess", "row_id", descending=[True, False])
        .collect()
    )
    released = (
        links.filter(task="crdf_crs")
        .join(commitments.select(left_id="row_id"), on="left_id", how="semi")
        .select(c_unit="right_id")
        .unique()
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
        .join(released, on="c_unit", how="semi")
        .collect()
    )
    over = raw.filter(pl.col("raw_usd") > config.raw_factor * pl.col("face"))
    share = over.height / raw.height if raw.height else None
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
                passed=share is not None and share < config.max_share,
                detail=(
                    f"{over.height} of {raw.height} linked commitment units the "
                    f"release carries are linked to more than {config.raw_factor} "
                    "times their face"
                    if raw.height
                    else "no linked commitment unit the release carries: nothing to "
                    "measure"
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
    holds every `commitments` row, repeats of an earlier vintage included. It fails
    when neither holds a provider-year.
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
    compared = (
        built.join(published, on=key, how="full", coalesce=True)
        .select(
            *key,
            gap=pl.when(difference == 0)
            .then(0.0)
            .otherwise(difference / published_usd.fill_null(0.0).abs()),
        )
        .collect()
    )
    if compared.is_empty():
        return _row(
            "oecd_reconciliation",
            value=None,
            threshold=config.tolerance,
            passed=False,
            detail="no climate commitment in the release or the published CRDF: "
            "nothing to reconcile",
        )
    gaps = compared.filter(pl.col("gap") > config.tolerance).sort(
        "gap", *key, descending=[True, False, False]
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

    A provider-year both releases hold moves by its change over its previous
    total, or by 1 when that total was 0. One the previous release held and this
    one lacks moves by 1 when its previous total was above 0, so a year or a whole
    provider that vanishes fails unless a cause explains it, and so does one only
    this release holds in a year the previous release covered. A provider-year in a
    year after the previous release's last, a new CRS year, is named in the detail
    and not gated. Without a previous release, or with one whose `quality` table is
    empty, the gate fails, naming the `first_release` override.
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
    last_year = before.select(pl.col("year").max()).collect().item()
    if last_year is None:
        return _row(
            "drift",
            value=None,
            threshold=config.max_move,
            passed=first_release,
            detail="the previous release's quality table is empty: no baseline to "
            "compare; give another previous release, or pass first_release "
            "(cf-build gate --first-release)",
        )
    totals = [
        table.group_by(key).agg(
            *(ordered_sum(pl.col(m)).alias(f"{m}_{side}") for m in DRIFT_MEASURES),
            pl.lit(value=True).alias(f"in_{side}"),
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
            .when(then == 0)
            .then(1.0)
            .otherwise((now - then).abs() / then.abs())
        )
    compared = (
        totals[0]
        .join(totals[1], on=key, how="full", coalesce=True)
        .with_columns(pl.col("in_now", "in_before").fill_null(value=False))
        .collect()
    )
    later = pl.col("year") > last_year
    new = compared.filter(later).sort(key)
    unexplained = (
        compared.filter(~later)
        .select(*key, "in_now", "in_before", move=pl.max_horizontal(moves))
        .join(explained.collect(), on=key, how="left")
        .filter(pl.col("move") > config.max_move, pl.col("explained").is_null())
        .sort("move", *key, descending=[True, False, False])
    )
    gone = set(compared.filter("in_before")["provider_code"].unique()) - set(
        compared.filter("in_now")["provider_code"].unique()
    )
    named = [
        f"{provider}/{year}" + ("" if now and then else " (new)" if now else " (gone)")
        for provider, year, now, then in unexplained.select(
            *key, "in_now", "in_before"
        ).iter_rows()
    ]
    parts = [
        f"provider-years that moved without a recorded cause: {_named(named)}"
        if unexplained.height
        else "no provider-year moved without a recorded cause"
    ]
    if gone:
        parts.append(f"providers gone: {_named([str(code) for code in sorted(gone)])}")
    if new.height:
        added = [f"{provider}/{year}" for provider, year in new.select(key).iter_rows()]
        parts.append(f"provider-years after {last_year}, not compared: {_named(added)}")
    return _row(
        "drift",
        value=_largest(unexplained["move"]),
        threshold=config.max_move,
        passed=unexplained.is_empty(),
        detail="; ".join(parts),
    )


def withheld_gate(tables: Mapping[str, pl.LazyFrame]) -> pl.DataFrame:
    """The number of rows of the release tables in `tables` that carry a
    `record_id` and whose record comes from a source the release withholds
    (`schema.withheld_record`). It fails on an empty `disbursements`."""
    if tables["disbursements"].select(pl.len()).collect().item() == 0:
        return _row(
            "withheld_rows",
            value=None,
            threshold=0.0,
            passed=False,
            detail="the release holds no disbursements: nothing to check",
        )
    rows = pl.concat(
        frame.filter(withheld_record(pl.col("record_id"))).select(
            "record_id", table=pl.lit(name)
        )
        for name, frame in tables.items()
        if "record_id" in frame.collect_schema()
    ).collect()
    records = [
        f"{table} {record}"
        for table, record in rows.select("table", "record_id")
        .unique()
        .sort(pl.all())
        .iter_rows()
    ]
    return _row(
        "withheld_rows",
        value=float(rows.height),
        threshold=0.0,
        passed=rows.is_empty(),
        detail=(
            f"rows from withheld records: {_named(records)}"
            if rows.height
            else "no release row comes from a withheld source"
        ),
    )


def listing_gate(tables: Mapping[str, pl.LazyFrame]) -> pl.DataFrame:
    """The number of provider, agency and recipient codes in the release tables in
    `tables` that the listings lack (`listings.unlisted_codes`). It fails when the
    tables hold no provider code."""
    if not listings.held_providers(tables):
        return _row(
            "listed_codes",
            value=None,
            threshold=0.0,
            passed=False,
            detail="the release holds no provider code: nothing to check",
        )
    unlisted = listings.unlisted_codes(tables)
    return _row(
        "listed_codes",
        value=float(len(unlisted)),
        threshold=0.0,
        passed=not unlisted,
        detail=(
            f"codes the listings lack: {_named(unlisted)}; run `cf-build listings`"
            if unlisted
            else "every provider, agency and recipient code is listed"
        ),
    )

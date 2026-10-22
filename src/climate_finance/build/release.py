"""The assemble stage and release packing.

Assemble projects the stage tables into the release tables the analysis layer reads.
The stage writer validates and writes them, `seal_release` checks
`schema.release_invariants` and adds `release.json`, and only then does the release
replace `<release_dir>/assemble/`, the directory `cf.load(path=...)` opens.

`commitments` holds the CRDF rows the trace follows: recipient-perspective rows,
with an aggregate parent left out where its rebuilt children stand in for it. Rows
that repeat an earlier vintage (`repeat_of`) stay in `commitments` with their
amounts; `quality` and `disbursement_profile` leave them out of their totals, and
`quality.repeat_climate_usd` reports them as their own line.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from climate_finance import __version__
from climate_finance.build.allocate import counted_rows, years_since_commitment
from climate_finance.build.errors import ReleaseInvariantError, StageInputMissingError
from climate_finance.build.label import inherit_markers
from climate_finance.build.sums import ordered_sum
from climate_finance.query import BILATERAL, MULTILATERAL_OUTFLOW
from climate_finance.schema import (
    BTR,
    COMMITMENTS,
    CORE_CONTRIBUTIONS,
    DISBURSEMENT_PROFILE,
    DISBURSEMENTS,
    PROVIDER_COEFFICIENTS,
    QUALITY,
    RELEASE_TABLES,
    TOTAL_FLOWS,
    ReleaseManifest,
    TableEntry,
    TableSchema,
    release_invariants,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from climate_finance.build.stages import StageContext

# A commitment's link counts as high probability from this value.
HIGH_PROBABILITY = 0.8
# `disbursement_profile` groups and the Bi_Multi codes each pools. `all` holds the
# codes the recipient view counts.
PROFILE_GROUPS = {
    "all": (*BILATERAL, MULTILATERAL_OUTFLOW),
    "bilateral": BILATERAL,
    "multilateral": (MULTILATERAL_OUTFLOW,),
}
# The CRS amount column behind each flow of `total_flows`.
_FLOW_AMOUNTS = {
    "commitments": "commitment_usd",
    "disbursements": "disbursement_usd",
    "grant_equivalent": "grant_equiv_usd",
}


def seal_release(
    context: StageContext,
    tables: Mapping[str, pl.DataFrame],
    sha256: Mapping[str, str],
    out_dir: Path,
) -> None:
    """Check the written release tables and write `release.json` beside them.

    Raises:
        ReleaseInvariantError: when the tables break `schema.release_invariants`.
    """
    if problems := release_invariants(tables):
        raise ReleaseInvariantError(problems)
    manifest = ReleaseManifest(
        release=context.release,
        created_at=datetime.now(UTC),
        package_version=__version__,
        tables={
            name: TableEntry(sha256=sha256[name], rows=tables[name].height)
            for name in RELEASE_TABLES
        },
        source_versions=source_versions(
            context.read("fetch", "raw_manifest").collect()
        ),
        model_versions={
            "jev": context.config.jev.model,
            "embedding": (
                f"{context.config.embedding.model}@{context.config.embedding.revision}"
            ),
        },
        gates={},
    )
    (out_dir / "release.json").write_text(manifest.model_dump_json(indent=2) + "\n")


def assemble(context: StageContext) -> dict[str, pl.DataFrame]:
    """Every release table, each input read from the last stage that writes it."""
    crdf = context.read("normalise", "crdf")
    crs = context.read("normalise", "crs")
    links = context.read("review", "links")
    allocations = context.read("fill", "allocations")
    status = context.read("review", "commitments")
    objectives = context.read("label", "objectives")

    commitments = release_commitments(crdf, status=status).collect()
    disbursements = release_disbursements(
        allocations,
        crdf=crdf,
        crs=crs,
        units=context.read("link", "units"),
        links=links,
    ).collect()
    return {
        "commitments": commitments,
        "disbursements": disbursements,
        "objectives": objectives.collect(),
        "agency_shares": context.read("measure", "agency_shares").collect(),
        "modelled": context.read("measure", "modelled").collect(),
        "core_contributions": core_contributions(
            context.read("normalise", "core_contributions")
        ).collect(),
        "btr": _optional(context, "normalise", BTR),
        "disbursement_profile": disbursement_profile(
            commitments.lazy(),
            status=status,
            allocations=allocations,
            last_year=context.config.years.crs_last_complete,
        ).collect(),
        "quality": quality(
            commitments.lazy(),
            disbursements=disbursements.lazy(),
            objectives=objectives,
            links=links,
        ).collect(),
        "total_flows": total_flows(crs).collect(),
        # Provider coefficients come from the BTR link, which is not implemented.
        "provider_coefficients": _empty(PROVIDER_COEFFICIENTS),
        "jev_level_map": context.read("label", "jev_level_map").collect(),
    }


def release_commitments(crdf: pl.LazyFrame, *, status: pl.LazyFrame) -> pl.LazyFrame:
    """The CRDF rows allocation follows, with their status and disbursed amounts."""
    return (
        counted_rows(crdf)
        .join(
            status.select(
                "row_id",
                "status",
                "disbursed_usd",
                "disbursed_low_usd",
                "disbursed_high_usd",
                "project_disbursing",
            ),
            on="row_id",
            how="left",
            validate="1:1",
        )
        .select(list(COMMITMENTS.columns))
    )


def release_disbursements(
    allocations: pl.LazyFrame,
    *,
    crdf: pl.LazyFrame,
    crs: pl.LazyFrame,
    units: pl.LazyFrame,
    links: pl.LazyFrame,
) -> pl.LazyFrame:
    """Allocations to CRDF commitments, with their CRS record and link.

    Each row carries the record's own facts, the markers it inherits from the
    commitment (`label.inherit_markers`), and the link that carried it (see
    `carrying_links`). A row no trace link carried, such as a bank-ledger fill, has
    a null method and probability.
    """
    allocated = allocations.filter(
        pl.col("row_id").is_not_null(), pl.col("rule") != "excess"
    )
    record = crs.select(
        "record_id",
        "agency_code",
        "recipient_code",
        "channel_code",
        "bi_multi",
        "purpose_code",
        "flow_code",
        "project_number",
        "title",
        provider_code="donor_code",
        record_disbursement_usd="disbursement_usd",
        record_grant_equiv_usd="grant_equiv_usd",
    )
    return (
        inherit_markers(allocated, commitments=crdf, crs=crs)
        .join(record, on="record_id", how="left", validate="m:1")
        .join(
            carrying_links(allocated, crdf=crdf, units=units, links=links),
            on=["row_id", "record_id"],
            how="left",
            validate="m:1",
        )
        .select(list(DISBURSEMENTS.columns))
    )


def carrying_links(
    allocated: pl.LazyFrame,
    *,
    crdf: pl.LazyFrame,
    units: pl.LazyFrame,
    links: pl.LazyFrame,
) -> pl.LazyFrame:
    """Per allocation's (row_id, record_id), the link that carried it.

    That is the most probable `trace` link from the record's unit to a commitment
    unit of a `crdf_crs` group of the row or of a row that repeats it, the lowest
    unit id among equals. Allocate moves a repeat's allocations to its first row
    (`repeat_of`), so the link can sit in the repeat's group.
    """
    record_units = units.select(d_unit="unit_id", record_id="record_ids").explode(
        "record_id"
    )
    groups = links.filter(pl.col("task") == "crdf_crs")
    first_rows = crdf.select("row_id", first_row=pl.coalesce("repeat_of", "row_id"))
    row_units = (
        groups.select(row_id="left_id", group_id="group_id")
        .unique()
        .join(first_rows, on="row_id", how="left")
        .select(row_id=pl.coalesce("first_row", "row_id"), group_id="group_id")
        .unique()
        .join(
            groups.select(c_unit="right_id", group_id="group_id").unique(),
            on="group_id",
        )
        .drop("group_id")
    )
    trace = links.filter(pl.col("task") == "trace").select(
        "link_method",
        "ledger_check",
        d_unit="left_id",
        c_unit="right_id",
        link_probability="probability",
    )
    return (
        allocated.select("row_id", "record_id")
        .unique()
        .join(record_units, on="record_id")
        .join(row_units, on="row_id")
        .join(trace, on=["d_unit", "c_unit"])
        .sort(
            "row_id",
            "record_id",
            "link_probability",
            "c_unit",
            descending=[False, False, True, False],
        )
        .group_by("row_id", "record_id", maintain_order=True)
        .agg(pl.col("link_method", "link_probability", "ledger_check").first())
    )


def core_contributions(stage: pl.LazyFrame) -> pl.LazyFrame:
    """Core contributions for every flow.

    MULTI publishes commitments and disbursements. The grant-equivalent rows equal
    the disbursement rows, as the DAC's grant-equivalent measure counts core
    contributions to multilateral agencies at face value.
    """
    grant_equivalent = stage.filter(pl.col("flow") == "disbursements").with_columns(
        flow=pl.lit("grant_equivalent")
    )
    return pl.concat([stage, grant_equivalent]).select(list(CORE_CONTRIBUTIONS.columns))


def total_flows(crs: pl.LazyFrame) -> pl.LazyFrame:
    """Every CRS record's amounts by provider, recipient, year, Bi_Multi and flow.

    A key and flow with no reported amount has no row.
    """
    keys = ["provider_code", "recipient_code", "year", "bi_multi"]
    return (
        crs.select(
            "recipient_code",
            "year",
            "bi_multi",
            provider_code="donor_code",
            **_FLOW_AMOUNTS,
        )
        .unpivot(
            index=keys, on=list(_FLOW_AMOUNTS), variable_name="flow", value_name="usd"
        )
        .drop_nulls("usd")
        .group_by(*keys, "flow")
        .agg(amount_usd=ordered_sum(pl.col("usd")))
        .select(list(TOTAL_FLOWS.columns))
    )


def quality(
    commitments: pl.LazyFrame,
    *,
    disbursements: pl.LazyFrame,
    objectives: pl.LazyFrame,
    links: pl.LazyFrame,
) -> pl.LazyFrame:
    """Coverage of the counted commitments' climate value, by provider and year.

    A commitment is linked when a `crdf_crs` link reaches a CRS commitment, and high
    probability when its most probable such link reaches `HIGH_PROBABILITY`. It is
    model-labelled when one of its allocated records carries a Jev objective label,
    and report-labelled otherwise. `grant_equiv_coverage` is the share of the
    linked commitments' allocated records with a grant equivalent. Repeats count
    only in `repeat_climate_usd`.
    """
    best = (
        links.filter(pl.col("task") == "crdf_crs")
        .group_by(row_id="left_id")
        .agg(best_probability=pl.col("probability").max())
    )
    jev = (
        objectives.filter(pl.col("label_source") == "jev")
        .select("record_id", jev=pl.lit(True))
        .unique()
    )
    by_row = (
        disbursements.select(
            "row_id",
            "record_id",
            has_grant_equiv=pl.col("record_grant_equiv_usd").is_not_null(),
        )
        .unique()
        .join(jev, on="record_id", how="left")
        .group_by("row_id")
        .agg(
            records=pl.len(),
            grant_equiv_records=pl.col("has_grant_equiv").sum(),
            model_labelled=pl.col("jev").any(),
        )
    )
    climate = pl.col("climate_usd")
    counted = pl.col("repeat_of").is_null()
    linked = counted & pl.col("best_probability").is_not_null()
    model = counted & pl.col("model_labelled").fill_null(False)
    return (
        commitments.join(best, on="row_id", how="left")
        .join(by_row, on="row_id", how="left")
        .group_by("provider_code", "year")
        .agg(
            commitments=counted.sum().cast(pl.Int32),
            climate_usd=ordered_sum(climate.filter(counted)),
            linked_usd=ordered_sum(climate.filter(linked)),
            linked_high_probability_usd=ordered_sum(
                climate.filter(linked, pl.col("best_probability") >= HIGH_PROBABILITY)
            ),
            model_labelled_usd=ordered_sum(climate.filter(model)),
            report_labelled_usd=ordered_sum(climate.filter(counted & ~model)),
            repeat_climate_usd=ordered_sum(climate.filter(~counted)),
            linked_records=pl.col("records").filter(linked).sum(),
            with_grant_equiv=pl.col("grant_equiv_records").filter(linked).sum(),
        )
        .with_columns(
            grant_equiv_coverage=pl.when(pl.col("linked_records") > 0).then(
                pl.col("with_grant_equiv") / pl.col("linked_records")
            )
        )
        .select(list(QUALITY.columns))
    )


def disbursement_profile(
    commitments: pl.LazyFrame,
    *,
    status: pl.LazyFrame,
    allocations: pl.LazyFrame,
    last_year: int,
) -> pl.LazyFrame:
    """Cumulative disbursed share of traceable climate value by years since
    commitment, per group in `PROFILE_GROUPS`.

    Traceable commitments with a positive face value enter, repeats left out. Each
    one's disbursed share of face weighs by its climate value. A commitment counts
    at `years_since` only when its year plus `years_since` is at most `last_year`,
    so each row pools the vintages observed that long, and `vintages` counts them.
    """
    traceable = (
        commitments.filter(pl.col("repeat_of").is_null(), pl.col("face_usd") > 0)
        .join(status.filter(pl.col("traceable")).select("row_id"), on="row_id")
        .select("row_id", "year", "bi_multi", "face_usd", "climate_usd")
        .collect()
    )
    observed = (
        years_since_commitment(
            allocations.collect(), crdf=traceable.lazy(), last_year=last_year
        )
        .join(traceable.lazy(), on="row_id")
        .with_columns(
            disbursed_climate=pl.col("climate_usd")
            * (pl.col("cumulative_disbursed_usd") / pl.col("face_usd")).clip(
                upper_bound=1.0
            )
        )
    )
    return pl.concat(
        observed.filter(pl.col("bi_multi").is_in(codes))
        .group_by("years_since")
        .agg(
            cumulative_share=ordered_sum(pl.col("disbursed_climate"))
            / ordered_sum(pl.col("climate_usd")),
            vintages=pl.col("year").n_unique().cast(pl.Int16),
        )
        .with_columns(group=pl.lit(group))
        for group, codes in PROFILE_GROUPS.items()
    ).select(list(DISBURSEMENT_PROFILE.columns))


def source_versions(raw_manifest: pl.DataFrame) -> dict[str, str]:
    """Each source's version from `raw_manifest`.

    A one-file source gives its recorded version, else its sha256. A many-file
    source gives the sha256 of its files' sha256 in path order.
    """
    versions = {}
    for (source,), files in raw_manifest.sort("path").group_by(
        "source", maintain_order=True
    ):
        if files.height == 1:
            versions[str(source)] = files["version"][0] or files["sha256"][0]
        else:
            joined = "\n".join(files["sha256"]).encode()
            versions[str(source)] = hashlib.sha256(joined).hexdigest()
    return dict(sorted(versions.items()))


def _optional(context: StageContext, stage: str, table: TableSchema) -> pl.DataFrame:
    """An optional stage table, or an empty one where the stage left it out."""
    try:
        return context.read(stage, table.name).collect()
    except StageInputMissingError:
        return _empty(table)


def _empty(table: TableSchema) -> pl.DataFrame:
    return pl.DataFrame(
        schema={name: column.dtype for name, column in table.columns.items()}
    )


def pack_release(release_dir: Path, *, out_path: Path) -> Path:
    """Pack a release as `.tar.zst` with its sources and licences file."""
    raise NotImplementedError("slice S10b")


def cut_release(
    release: str, *, config: Path, out_dir: Path, waive_precision: bool = False
) -> Path:
    """Build, gate and pack `release`; record a precision waiver in release.json."""
    raise NotImplementedError("slice S10b")


def upload_release(pack: Path, *, target: str) -> str:
    """Upload a packed release to `target`; return its URL."""
    raise NotImplementedError("slice S10b")

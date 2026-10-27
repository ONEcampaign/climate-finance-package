"""The assemble stage and release packing.

Assemble projects the stage tables into the release tables the analysis layer reads.
The stage writer validates and writes them, `seal_release` checks
`schema.release_invariants` and adds `release.json`, and only then does the release
replace `<release_dir>/assemble/`, the directory `cf.load(path=...)` opens.

`commitments` holds the CRDF rows the trace follows: recipient-perspective rows,
with an aggregate parent left out where its rebuilt children stand in for it. Rows
that repeat an earlier vintage (`repeat_of`) stay in `commitments` with their
amounts. `quality` and `disbursement_profile` leave them out of their totals, and
`quality.repeat_climate_usd` reports them as their own line.

`untraced` holds, per CRS record, the Rio-marked disbursement that reaches no
commitment: excess beyond a full commitment, and records linked to nothing
(`allocate.untraced_records`). The query counts it at the record's own markers.
`unallocated` reports, by provider and year, every Rio-marked CRS disbursement
(`marked_usd`) and the parts of it outside `disbursements`: the two parts
`untraced` holds, allocations to commitments with no CRDF row, and the unlinked
fund records whose money the funds' IATI route counts. `schema.release_invariants`
checks that these parts and the Rio-marked records' `disbursements` rows sum to
`marked_usd`.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from climate_finance import __version__
from climate_finance.build.allocate import (
    counted_rows,
    marked_records,
    untraced_records,
    years_since_commitment,
)
from climate_finance.build.config import RELEASE_SETTINGS
from climate_finance.build.errors import ReleaseInvariantError, StageInputMissingError
from climate_finance.build.fetch import fetched_years, last_complete_year
from climate_finance.build.label import inherit_markers
from climate_finance.build.link.gap_fill import routed_records
from climate_finance.build.provenance import setting_value
from climate_finance.build.sums import ordered_sum
from climate_finance.schema import (
    BILATERAL,
    BTR,
    CERTAIN_ROUTES,
    COMMITMENTS,
    CONTRIBUTION_KEY,
    CORE_CONTRIBUTIONS,
    DISBURSEMENT_PROFILE,
    DISBURSEMENTS,
    INVARIANT_TOLERANCE_USD,
    MULTILATERAL_OUTFLOW,
    PROVIDER_COEFFICIENTS,
    QUALITY,
    RECIPIENT_VIEW,
    RELEASE_TABLES,
    TOP_CHANNEL,
    TOTAL_FLOWS,
    UNALLOCATED,
    UNTRACED,
    ReleaseManifest,
    TableEntry,
    TableSchema,
    aggregate_channel,
    measured_adaptation,
    release_invariants,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from climate_finance.build.stages import StageContext

# `disbursement_profile` groups and the Bi_Multi codes each pools. `all` holds the
# codes the recipient view counts.
PROFILE_GROUPS = {
    "all": RECIPIENT_VIEW,
    "bilateral": BILATERAL,
    "multilateral": (MULTILATERAL_OUTFLOW,),
}
# The CRS amount column behind each flow of `total_flows`.
_FLOW_AMOUNTS = {
    "commitments": "commitment_usd",
    "disbursements": "disbursement_usd",
    "grant_equivalent": "grant_equiv_usd",
}


# The release tables whose years bound the provider view's attributed finance:
# core contributions, and the agency shares each method reads.
COVERAGE_TABLES = ("core_contributions", "agency_shares", "agency_spending")


def _table_years(
    tables: Mapping[str, pl.DataFrame], names: Sequence[str]
) -> dict[str, tuple[int, int]]:
    """The first and last year of each of `names` in `tables` that holds a row."""
    return {
        name: tables[name].select(pl.min("year"), pl.max("year").alias("last")).row(0)
        for name in names
        if not tables[name].is_empty()
    }


def seal_release(
    context: StageContext,
    tables: Mapping[str, pl.DataFrame],
    sha256: Mapping[str, str],
    out_dir: Path,
) -> None:
    """Check the written release tables and write `release.json` beside them,
    with the `RELEASE_SETTINGS` of the build config.

    Its `years` holds the years the build read from each source, and the years
    `core_contributions`, `agency_shares` and `agency_spending` hold, which bound
    attributed finance.

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
        years=fetched_years(context) | _table_years(tables, COVERAGE_TABLES),
        model_versions={
            "jev": context.config.jev.model,
            "embedding": (
                f"{context.config.embedding.model}@{context.config.embedding.revision}"
            ),
        },
        gates={},
        settings={
            setting: setting_value(context.config, setting)
            for setting in RELEASE_SETTINGS
        },
    )
    (out_dir / "release.json").write_text(manifest.model_dump_json(indent=2) + "\n")


def assemble(context: StageContext) -> dict[str, pl.DataFrame]:
    """Every release table, each input read from the last stage that writes it."""
    crdf = context.read("normalise", "crdf")
    last_year = last_complete_year(context)
    # Later CRS years hold early reports, which allocate leaves out too.
    crs = context.read("normalise", "crs").filter(pl.col("year") <= last_year)
    links = context.read("review", "links")
    allocations = context.read("fill", "allocations")
    status = context.read("review", "commitments")
    objectives = context.read("label", "objectives").collect()
    marked = marked_records(
        allocations,
        crs=crs,
        last_year=last_year,
        routed=routed_records(crs, allocations=allocations, crdf=crdf),
    )

    commitments = release_commitments(crdf, status=status, links=links).collect()
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
        "objectives": objectives,
        "agency_shares": context.read("measure", "agency_shares").collect(),
        "agency_spending": context.read("measure", "agency_spending").collect(),
        "modelled": context.read("measure", "modelled").collect(),
        "core_contributions": core_contributions(
            context.read("normalise", "core_contributions")
        ).collect(),
        "btr": _optional(context, "normalise", BTR),
        "disbursement_profile": disbursement_profile(
            commitments.lazy(),
            status=status,
            allocations=allocations,
            last_year=last_year,
        ).collect(),
        "quality": quality(
            commitments.lazy(),
            disbursements=disbursements.lazy(),
            objectives=objectives.lazy(),
            links=links,
        ).collect(),
        "untraced": release_untraced(untraced_records(marked), crs=crs).collect(),
        "unallocated": unallocated(marked).collect(),
        "total_flows": total_flows(crs).collect(),
        # Provider coefficients come from the BTR link, which is not implemented.
        "provider_coefficients": pl.DataFrame(schema=PROVIDER_COEFFICIENTS.dtypes),
    }


def release_commitments(
    crdf: pl.LazyFrame, *, status: pl.LazyFrame, links: pl.LazyFrame
) -> pl.LazyFrame:
    """The CRDF rows allocation follows, with their status and disbursed amounts.

    `ledger_undisbursed` is true where the bank's ledger confirms no disbursement,
    as `confirmed_undisbursed` on every `crdf_crs` link of the row, and null where
    no ledger covers the row or it shows money moving. It takes no other value.
    """
    undisbursed = (
        links.filter(pl.col("task") == "crdf_crs")
        .group_by(row_id="left_id")
        .agg(
            ledger_undisbursed=pl.when(
                (pl.col("ledger_check") == "confirmed_undisbursed")
                .fill_null(value=False)
                .all()
            ).then(pl.lit(value=True))
        )
    )
    return (
        counted_rows(crdf)
        .join(undisbursed, on="row_id", how="left", validate="1:1")
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

    A CRS allocation carries the record's own facts, the markers it inherits from
    the commitment (`label.inherit_markers`), and the link that carried it (see
    `with_carrying_links`). A fill (`fill_disbursements`) has no CRS record or link.
    """
    allocated = allocations.filter(
        pl.col("row_id").is_not_null(), pl.col("rule") != "excess"
    )
    from_crs = allocated.filter(pl.col("disbursement_source") == "crs")
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
    traced = (
        inherit_markers(
            with_carrying_links(from_crs, units=units, links=links),
            commitments=crdf,
            crs=crs,
        )
        .join(record, on="record_id", how="left", validate="m:1")
        .select(list(DISBURSEMENTS.columns))
    )
    filled = fill_disbursements(
        allocated.filter(pl.col("disbursement_source") != "crs"), crdf=crdf
    )
    return pl.concat([traced, filled])


def fill_disbursements(fills: pl.LazyFrame, *, crdf: pl.LazyFrame) -> pl.LazyFrame:
    """Bank-ledger and IATI fills as disbursements rows.

    A fill has no CRS record, so it takes provider, agency, recipient, channel,
    Bi_Multi, purpose, project number, title, markers and climate share from the
    commitment it fills, and its flow code is null. Its record is the fill's key and
    year (`ledger:<publisher>:<key>:<year>` or `iati:...`), whose disbursement is
    what the fill gives every commitment on it. There is no grant equivalent. Markers
    come from the commitment (`components` where it has a climate share), and the
    method, probability and ledger check are null.
    """
    commitment = crdf.select(
        "row_id",
        "agency_code",
        "recipient_code",
        "channel_code",
        "bi_multi",
        "purpose_code",
        "project_number",
        "title",
        "climate_share",
        provider_code="provider_code",
        inherited_rio_adaptation="rio_adaptation",
        inherited_rio_mitigation="rio_mitigation",
    )
    records = fills.group_by("record_id").agg(
        record_disbursement_usd=ordered_sum(pl.col("allocated_usd"))
    )
    return (
        fills.join(commitment, on="row_id", how="left", validate="m:1")
        .join(records, on="record_id", how="left", validate="m:1")
        .with_columns(
            flow_code=pl.lit(None, pl.Int32),
            record_grant_equiv_usd=pl.lit(None, pl.Float64),
            record_rio_adaptation=pl.lit(None, pl.Int8),
            record_rio_mitigation=pl.lit(None, pl.Int8),
            marker_source=pl.when(pl.col("climate_share").is_not_null())
            .then(pl.lit("components"))
            .otherwise(pl.lit("commitment")),
            link_method=pl.lit(None, pl.String),
            link_probability=pl.lit(None, pl.Float64),
            ledger_check=pl.lit(None, pl.String),
        )
        .select(list(DISBURSEMENTS.columns))
    )


def with_carrying_links(
    allocated: pl.LazyFrame, *, units: pl.LazyFrame, links: pl.LazyFrame
) -> pl.LazyFrame:
    """CRS allocations with the `link_probability` and `ledger_check` of the
    `trace` link that allocation names as their carrier, from the record's unit to
    `commitment_unit_id` under `link_method`."""
    record_units = units.select(left_id="unit_id", record_id="record_ids").explode(
        "record_id"
    )
    trace = links.filter(pl.col("task") == "trace").select(
        "left_id",
        "link_method",
        "ledger_check",
        commitment_unit_id="right_id",
        link_probability="probability",
    )
    return (
        allocated.join(record_units, on="record_id", how="left", validate="m:1")
        .join(
            trace,
            on=["left_id", "commitment_unit_id", "link_method"],
            how="left",
            validate="m:1",
        )
        .drop("left_id")
    )


def core_contributions(stage: pl.LazyFrame) -> pl.LazyFrame:
    """Core contributions to leaf channels for every flow, with each provider's
    unitemised remainder on `TOP_CHANNEL`.

    MULTI publishes aggregate channels beside their leaves (`aggregate_channel`), so
    the aggregates leave and each dollar counts once. Where a provider's
    `TOP_CHANNEL` total for a year and flow exceeds its leaves by more than
    `INVARIANT_TOLERANCE_USD`, the difference stays as a `TOP_CHANNEL` row, so the
    provider's total is conserved. That row carries no agency share, so it adds
    nothing to attributed climate finance. MULTI publishes commitments and
    disbursements. The grant-equivalent rows equal the disbursement rows, as the
    DAC's grant-equivalent measure counts core contributions to multilateral
    agencies at face value.

    Raises:
        ReleaseInvariantError: when a provider's leaves for a year and flow exceed
            its `TOP_CHANNEL` total, or it reports aggregates without that total.
    """
    key = list(CONTRIBUTION_KEY)
    channel, amount = pl.col("channel_code"), pl.col("contribution_usd")
    aggregate = aggregate_channel(channel)
    totals = (
        stage.group_by(key)
        .agg(
            top=amount.filter(channel == TOP_CHANNEL).first(),
            leaves=ordered_sum(amount.filter(~aggregate)),
            aggregated=aggregate.any(),
        )
        .sort(key)
        .collect()
    )
    over = totals.filter(pl.col("leaves") - pl.col("top") > INVARIANT_TOLERANCE_USD)
    untopped = totals.filter(pl.col("top").is_null(), "aggregated")
    problems = [
        f"core_contributions {provider} {year} {flow}: leaves sum to {leaves}, above "
        f"the {TOP_CHANNEL} total of {top}"
        for provider, year, flow, top, leaves, _ in over.iter_rows()
    ] + [
        f"core_contributions {provider} {year} {flow}: aggregate channels without "
        f"a {TOP_CHANNEL} total"
        for provider, year, flow, *_ in untopped.iter_rows()
    ]
    if problems:
        raise ReleaseInvariantError(problems)
    remainder = totals.filter(
        pl.col("top") - pl.col("leaves") > INVARIANT_TOLERANCE_USD
    ).select(
        *key,
        channel_code=pl.lit(TOP_CHANNEL, pl.Int32),
        contribution_usd=pl.col("top") - pl.col("leaves"),
    )
    kept = pl.concat(
        [stage.filter(~aggregate), remainder.lazy()], how="diagonal_relaxed"
    )
    grant_equivalent = kept.filter(pl.col("flow") == "disbursements").with_columns(
        flow=pl.lit("grant_equivalent")
    )
    return pl.concat([kept, grant_equivalent]).select(list(CORE_CONTRIBUTIONS.columns))


def total_flows(crs: pl.LazyFrame) -> pl.LazyFrame:
    """Every CRS record's amounts by provider, agency, recipient, year, Bi_Multi and
    flow.

    `assemble` passes the records up to `years.crs_last_complete`, as allocation
    reads them, so the denominator covers the years `disbursements` does.

    A key and flow with no reported amount has no row.
    """
    keys = ["provider_code", "agency_code", "recipient_code", "year", "bi_multi"]
    return (
        crs.select(
            "agency_code",
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

    A commitment is linked when a `crdf_crs` link reaches a CRS commitment, and
    reaches it through a certain route when every such link's method is in
    `CERTAIN_ROUTES`. It is model-labelled when one of its allocated records carries
    a Jev objective label, and report-labelled otherwise. `disbursed_climate_usd` is
    the climate value of its disbursements' `allocated_high_usd`, and
    `certain_disbursed_climate_usd` of their `allocated_low_usd`, each at the
    commitment's climate share of face. `grant_equiv_coverage` is the share of the
    linked commitments' allocated value whose record has a grant equivalent, null
    where nothing is allocated to them. Repeats count only in `repeat_climate_usd`.
    """
    routes = (
        links.filter(pl.col("task") == "crdf_crs")
        .group_by(row_id="left_id")
        .agg(certain=pl.col("link_method").is_in(CERTAIN_ROUTES).all())
    )
    jev = (
        objectives.filter(pl.col("label_source") == "jev")
        .select("record_id", jev=pl.lit(True))
        .unique()
    )
    allocated = pl.col("allocated_usd")
    by_row = (
        disbursements.join(jev, on="record_id", how="left")
        .group_by("row_id")
        .agg(
            allocated_usd=ordered_sum(allocated),
            valued_usd=ordered_sum(
                allocated.filter(pl.col("record_grant_equiv_usd").is_not_null())
            ),
            model_labelled=pl.col("jev").any(),
            high=ordered_sum(pl.col("allocated_high_usd")),
            low=ordered_sum(pl.col("allocated_low_usd")),
        )
    )
    climate = pl.col("climate_usd")
    face = pl.col("face_usd")
    counted = pl.col("repeat_of").is_null()
    linked = counted & pl.col("certain").is_not_null()
    model = counted & pl.col("model_labelled").fill_null(False)
    return (
        commitments.join(routes, on="row_id", how="left")
        .join(by_row, on="row_id", how="left")
        .group_by("provider_code", "year")
        .agg(
            commitments=counted.sum().cast(pl.Int32),
            climate_usd=ordered_sum(climate.filter(counted)),
            linked_usd=ordered_sum(climate.filter(linked)),
            certain_route_usd=ordered_sum(climate.filter(linked, pl.col("certain"))),
            disbursed_climate_usd=ordered_sum(
                (climate * pl.col("high") / face).filter(
                    counted, face > 0, pl.col("high").is_not_null()
                )
            ),
            certain_disbursed_climate_usd=ordered_sum(
                (climate * pl.col("low") / face).filter(
                    counted, face > 0, pl.col("low").is_not_null()
                )
            ),
            model_labelled_usd=ordered_sum(climate.filter(model)),
            report_labelled_usd=ordered_sum(climate.filter(counted & ~model)),
            repeat_climate_usd=ordered_sum(climate.filter(~counted)),
            linked_allocated=ordered_sum(allocated.filter(linked).drop_nulls()),
            linked_valued=ordered_sum(pl.col("valued_usd").filter(linked).drop_nulls()),
        )
        .with_columns(
            grant_equiv_coverage=pl.when(pl.col("linked_allocated") > 0).then(
                pl.col("linked_valued") / pl.col("linked_allocated")
            )
        )
        .select(list(QUALITY.columns))
    )


def release_untraced(untraced: pl.LazyFrame, *, crs: pl.LazyFrame) -> pl.LazyFrame:
    """`untraced_records` rows with their CRS record's own facts and markers, the
    adaptation marker null before `ADAPTATION_FIRST_YEAR`, and its disbursement and
    grant equivalent as `disbursements` carries them."""
    record = crs.select(
        "record_id",
        "year",
        "agency_code",
        "recipient_code",
        "channel_code",
        "bi_multi",
        "purpose_code",
        "flow_code",
        "project_number",
        "title",
        "rio_adaptation",
        "rio_mitigation",
        provider_code="donor_code",
        record_disbursement_usd="disbursement_usd",
        record_grant_equiv_usd="grant_equiv_usd",
    )
    return (
        untraced.join(record, on="record_id", how="left", validate="1:1")
        .with_columns(rio_adaptation=measured_adaptation(pl.col("rio_adaptation")))
        .select(list(UNTRACED.columns))
    )


def unallocated(marked: pl.LazyFrame) -> pl.LazyFrame:
    """Rio-marked CRS disbursement (`marked_usd`) and its parts outside
    `disbursements`, by provider and year, from `marked_records`.

    A linked record's allocations split its disbursement into the `disbursements`
    rows, `excess_usd`, and `outside_release_usd` for commitments with no CRDF row.
    An unlinked record counts its whole disbursement in `unlinked_marked_usd`, or in
    `fund_unlinked_usd` where a fund's IATI route counts its money (`route_fund`),
    as `untraced_records` leaves it out. Years are disbursement years.
    """
    unlinked = ~pl.col("linked")
    disbursement = pl.col("disbursement_usd")
    return (
        marked.group_by("provider_code", "year")
        .agg(
            ordered_sum(pl.col("excess_usd", "outside_release_usd")),
            marked_usd=ordered_sum(disbursement),
            unlinked_marked_usd=ordered_sum(
                disbursement.filter(unlinked, ~pl.col("route_fund"))
            ),
            fund_unlinked_usd=ordered_sum(
                disbursement.filter(unlinked, pl.col("route_fund"))
            ),
        )
        .select(list(UNALLOCATED.columns))
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
        years_since_commitment(allocations, crdf=traceable.lazy(), last_year=last_year)
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
        return pl.DataFrame(schema=table.dtypes)


def pack_release(release_dir: Path, *, out_path: Path) -> Path:
    """Pack a release as `.tar.zst` with its sources and licences file."""
    raise NotImplementedError("pack_release is not implemented")


def cut_release(
    release: str, *, config: Path, out_dir: Path, waive_precision: bool = False
) -> Path:
    """Build, gate and pack `release`; record a precision waiver in release.json."""
    raise NotImplementedError("cut_release is not implemented")


def upload_release(pack: Path, *, target: str) -> str:
    """Upload a packed release to `target`; return its URL."""
    raise NotImplementedError("upload_release is not implemented")

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

The release leaves out the Green Climate Fund's own records, whose terms allow only
private study, research and teaching: every release table with a `record_id` drops
their rows (`schema.withheld_record`), as `disbursements` drops their fills and
`objectives` their labels, and each commitment they reached states their amount as
`withheld_usd`, so its disbursed amounts and `quality` keep them.
`schema.release_invariants` and the `withheld_rows` gate refuse any release table
with a `record_id` that holds one.
`sources.json` notes the sources whose rows stay out (`WITHHELD_SOURCES`).
The packed release, the one published, also leaves out the row-level BTR tables
`btr` and `btr_links` (`WITHHELD_TABLES`) until the UNFCCC confirms its terms cover
them. Its `btr_totals` holds what the reconciliation reads. A build keeps both
tables in `<release_dir>/assemble/`, so a build from BTR workbooks a person
downloads from the UNFCCC gives the row-level reconciliation.

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
import io
import json
import shutil
import tarfile
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import quote

import httpx
import polars as pl
import pyarrow as pa
import yaml

from climate_finance import __version__
from climate_finance import build as build_package
from climate_finance.build import gate
from climate_finance.build.allocate import (
    counted_rows,
    marked_records,
    untraced_records,
    years_since_commitment,
)
from climate_finance.build.config import RELEASE_SETTINGS, load_config
from climate_finance.build.errors import (
    BuildError,
    GateFailedError,
    ReleaseInvariantError,
    StageInputMissingError,
)
from climate_finance.build.fetch import BTR_SOURCE, fetched_years, last_complete_year
from climate_finance.build.label import inherit_markers
from climate_finance.build.link.gap_fill import routed_records
from climate_finance.build.measure import load_provider_facts
from climate_finance.build.provenance import setting_value
from climate_finance.build.readers.btr import party_provider
from climate_finance.build.sums import ordered_sum
from climate_finance.schema import (
    BILATERAL,
    BTR,
    BTR_CRS_UNLINKED,
    BTR_LINKS,
    BTR_TOTALS,
    BTR_WORKBOOKS,
    CERTAIN_ROUTES,
    COMMITMENTS,
    CONTRIBUTION_KEY,
    CORE_CONTRIBUTIONS,
    DISBURSEMENT_PROFILE,
    DISBURSEMENTS,
    INVARIANT_TOLERANCE_USD,
    MODELLED,
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
    sha256_file,
    withheld_record,
)
from climate_finance.snapshot import IndexEntry, ReleaseIndex, load, release_dirname

if TYPE_CHECKING:
    from collections.abc import Collection, Mapping, Sequence

    from climate_finance.build.stages import StageContext

# httpx bounds each socket operation, not the whole upload, so a 150 to 400 MB pack
# passes at any steady rate. Read waits for the server to store the body and answer.
UPLOAD_TIMEOUT = httpx.Timeout(60.0, read=300.0)
# `disbursement_profile` groups and the Bi_Multi codes each pools. `all` holds the
# codes the recipient view counts.
PROFILE_GROUPS = {
    "all": RECIPIENT_VIEW,
    "bilateral": BILATERAL,
    "multilateral": (MULTILATERAL_OUTFLOW,),
}
# The terms each source's data comes under, by `raw_manifest` source name or the
# start of one. The longest matching start wins.
_OECD = (
    "OECD Terms and Conditions, https://www.oecd.org/en/about/terms-conditions.html, "
    "and the dataset terms in each dataset's metadata. This is an adaptation of an "
    "original work by the OECD. The opinions expressed and arguments employed in "
    "the adaptation should not be reported as representing the official views of "
    "the OECD or of its member countries."
)
_WORLD_BANK = (
    "World Bank Data Catalog licence (CC BY 4.0 by default; per dataset), "
    "https://datacatalog.worldbank.org/public-licenses"
)
_IATI = "the publisher's licence in the IATI Registry, https://iatiregistry.org"
_GCF = (
    "Green Climate Fund website terms: private study, research and teaching only, "
    "and no commercial derivative works without written consent; the projects API "
    "states no terms of its own, https://www.greenclimate.fund/terms-and-conditions"
)
_UNFCCC = (
    "UNFCCC terms of use: public domain if no change to the content is introduced "
    "and the source is acknowledged, https://unfccc.int/this-site/terms-of-use"
)
# The `sources.json` entry citing the annual reports behind `providers.toml`'s
# `report_ratios`.
REPORTS_SOURCE = "provider_reports"
SOURCE_LICENCES = {
    "crs": _OECD,
    "crdf": _OECD,
    "multisystem": _OECD,
    "imputed_shares": _OECD,
    "dac_exchange": _OECD,
    "gcf_projects": _GCF,
    "ledger_gcf": _GCF,
    "iati": _IATI,
    "ledger_iati": _IATI,
    "ledger_finances_one": _WORLD_BANK,
    "ledger_imf": "IMF copyright and terms: derivative works with attribution; "
    "commercial use and automated bulk download need permission, "
    "https://www.imf.org/en/About/copyright-and-terms",
    "adaptation_labels": "Weikmans et al.; fits the adaptation cut, no rows ship",
    "unfccc": _UNFCCC,
    REPORTS_SOURCE: "single figures quoted from each provider's annual report, "
    "cited by title, year and page in `citations`; the publisher's terms apply",
}
# Sources whose rows the release leaves out (`schema.WITHHELD_RECORD_PREFIXES`), by
# `raw_manifest` source name or the start of one, and the note `sources.json` gives
# them.
WITHHELD_SOURCES = ("gcf_projects", "ledger_gcf")
WITHHELD_NOTE = (
    "Green Climate Fund rows are left out of the release, as its website terms "
    "allow only private study, research and teaching, and no commercial derivative "
    "works without written consent; `cf-build` rebuilds them from the GCF's API."
)
# Release tables the pack leaves out, and the note `release.json` and `sources.json`
# give them.
BTR_WITHHELD_NOTE = (
    "The row-level BTR tables btr and btr_links are left out of the published "
    "release until the UNFCCC confirms its terms cover them; btr_totals holds the "
    "reconciliation totals. To build them, download the BTR workbooks from the "
    "UNFCCC, point sources.btr_archive in build.toml at them, run "
    "`cf-build build RELEASE --config build.toml` and open the build with "
    '`cf.load(path="releases/RELEASE/assemble")`.'
)
WITHHELD_TABLES = {BTR.name: BTR_WITHHELD_NOTE, BTR_LINKS.name: BTR_WITHHELD_NOTE}
# The coefficients each provider applies when it reports to the UNFCCC, with their
# sources, keyed by ISO 3 code.
PROVIDER_COEFFICIENTS_FILE = Path(__file__).with_name("provider_coefficients.yaml")
# The file in a pack that lists each source with its version, URLs and licence.
SOURCES_FILE = "sources.json"
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
    """Every release table, each input read from the last stage that writes it, with
    no row whose `record_id` the release withholds (`schema.withheld_record`)."""
    crdf = context.read("normalise", "crdf")
    last_year = last_complete_year(context)
    # Later CRS years hold early reports, which allocate leaves out too.
    crs = context.read("normalise", "crs").filter(pl.col("year") <= last_year)
    links = context.read("review", "links")
    btr = _optional(context, "normalise", BTR)
    allocations = context.read("fill", "allocations")
    status = context.read("review", "commitments")
    objectives = context.read("label", "objectives").collect()
    marked = marked_records(
        allocations,
        crs=crs,
        last_year=last_year,
        routed=routed_records(crs, allocations=allocations, crdf=crdf),
    )

    disbursements = release_disbursements(
        allocations,
        crdf=crdf,
        crs=crs,
        units=context.read("link", "units"),
        links=links,
    ).collect()
    withheld = withheld_record(pl.col("record_id"))
    commitments = release_commitments(
        crdf,
        status=status,
        links=links,
        withheld=disbursements.lazy().filter(withheld),
    ).collect()
    linked_btr = btr_links(links, crs=crs).collect()
    tables = {
        "commitments": commitments,
        "disbursements": disbursements,
        "objectives": objectives,
        "agency_shares": context.read("measure", "agency_shares").collect(),
        "agency_spending": context.read("measure", "agency_spending").collect(),
        "modelled": context.read("measure", "modelled").collect(),
        "core_contributions": core_contributions(
            context.read("normalise", "core_contributions")
        ).collect(),
        "btr": btr,
        "btr_links": linked_btr,
        "btr_totals": btr_totals(btr.lazy(), links=linked_btr.lazy()).collect(),
        "btr_crs_unlinked": btr_crs_unlinked(
            crs, btr=btr.lazy(), links=links
        ).collect(),
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
        "provider_coefficients": provider_coefficients(
            yaml.safe_load(PROVIDER_COEFFICIENTS_FILE.read_text(encoding="utf-8"))
        ),
    }
    # The tables above read every record, so quality and `withheld_usd` count the
    # withheld ones; the release leaves out their rows.
    return {
        name: frame.filter(~withheld) if "record_id" in frame.columns else frame
        for name, frame in tables.items()
    }


def release_commitments(
    crdf: pl.LazyFrame,
    *,
    status: pl.LazyFrame,
    links: pl.LazyFrame,
    withheld: pl.LazyFrame,
) -> pl.LazyFrame:
    """The CRDF rows allocation follows, with their status and disbursed amounts, and
    as `withheld_usd` the sum of their `withheld` disbursements rows.

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
    withheld_usd = withheld.group_by("row_id").agg(
        withheld_usd=ordered_sum(pl.col("allocated_usd"))
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
        .join(withheld_usd, on="row_id", how="left", validate="1:1")
        .with_columns(pl.col("withheld_usd").fill_null(0.0))
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
    """CRS allocations with the record's unit (`record_unit_id`) and the
    `link_probability` and `ledger_check` of the `trace` link that allocation names
    as their carrier, from that unit to `commitment_unit_id` under `link_method`."""
    record_units = units.select(
        record_unit_id="unit_id", record_id="record_ids"
    ).explode("record_id")
    trace = links.filter(pl.col("task") == "trace").select(
        "link_method",
        "ledger_check",
        record_unit_id="left_id",
        commitment_unit_id="right_id",
        link_probability="probability",
    )
    return allocated.join(
        record_units, on="record_id", how="left", validate="m:1"
    ).join(
        trace,
        on=["record_unit_id", "commitment_unit_id", "link_method"],
        how="left",
        validate="m:1",
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
    """Every CRS record's amounts by provider, agency, recipient, year, Bi_Multi,
    flow and the record's two Rio markers (0 for none).

    `assemble` passes the records up to `years.crs_last_complete`, as allocation
    reads them, so the denominator covers the years `disbursements` does.

    A key and flow with no reported amount has no row.
    """
    keys = [
        "provider_code",
        "agency_code",
        "recipient_code",
        "year",
        "bi_multi",
        "rio_adaptation",
        "rio_mitigation",
    ]
    return (
        crs.select(
            "agency_code",
            "recipient_code",
            "year",
            "bi_multi",
            pl.col("rio_adaptation", "rio_mitigation").fill_null(0).cast(pl.Int8),
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


def btr_links(links: pl.LazyFrame, *, crs: pl.LazyFrame) -> pl.LazyFrame:
    """The `btr_crs` links with their CRS record's facts."""
    return (
        links.filter(pl.col("task") == "btr_crs")
        .select("link_method", "probability", row_id="left_id", record_id="right_id")
        .join(
            crs.select(
                "record_id",
                "year",
                "recipient_code",
                "project_number",
                "title",
                "commitment_usd",
                "disbursement_usd",
                pl.col("rio_adaptation", "rio_mitigation").fill_null(0).cast(pl.Int8),
                provider_code="donor_code",
            ),
            on="record_id",
            how="inner",
            validate="m:1",
        )
        .select(list(BTR_LINKS.columns))
    )


def btr_totals(btr: pl.LazyFrame, *, links: pl.LazyFrame) -> pl.LazyFrame:
    """The BTR rows summed by `BTR_TOTALS.sort_key`. A row is linked when `links`
    (`btr_links`) holds a link from it."""
    linked = links.select("row_id").unique().with_columns(linked=pl.lit(value=True))
    return (
        btr.join(linked, on="row_id", how="left", validate="1:1")
        .with_columns(
            pl.col("linked").fill_null(value=False),
            funding=funding_class(pl.col("funding_source")),
        )
        .group_by(*BTR_TOTALS.sort_key, "provider_code")
        .agg(
            rows=pl.len().cast(pl.Int32),
            **{
                name: pl.when(pl.col(name).is_not_null().any()).then(
                    ordered_sum(pl.col(name).drop_nulls())
                )
                for name in (
                    "face_usd",
                    "grant_equiv_usd",
                    "amount_domestic",
                    "face_usd_at_dac",
                    "grant_equiv_usd_at_dac",
                )
            },
            dac_rate=pl.col("dac_rate").max(),
        )
        .select(list(BTR_TOTALS.columns))
    )


def btr_crs_unlinked(
    crs: pl.LazyFrame, *, btr: pl.LazyFrame, links: pl.LazyFrame
) -> pl.LazyFrame:
    """Each provider-year of `btr` and flow of `BTR_CRS_UNLINKED`, with the sum of
    that flow over the provider's bilateral climate-marked CRS records of the year
    that no `btr_crs` link in `links` reaches, null for a provider-year without a
    record in `crs`."""
    flows = {"commitments": "commitment_usd", "disbursements": "disbursement_usd"}
    reached = (
        links.filter(pl.col("task") == "btr_crs").select(record_id="right_id").unique()
    )
    unlinked = (
        crs.filter(
            pl.col("bi_multi").is_in(BILATERAL),
            pl.max_horizontal("rio_adaptation", "rio_mitigation") >= 1,
        )
        .join(reached, on="record_id", how="anti")
        .select("year", provider_code="donor_code", **flows)
        .unpivot(
            index=["provider_code", "year"],
            on=list(flows),
            variable_name="flow",
            value_name="usd",
        )
        .group_by("provider_code", "year", "flow")
        .agg(amount_usd=ordered_sum(pl.col("usd").drop_nulls()))
    )
    covered = (
        crs.select("year", provider_code="donor_code")
        .unique()
        .with_columns(covered=pl.lit(True))
    )
    return (
        btr.select("provider_code", "year")
        .drop_nulls()
        .unique()
        .join(pl.LazyFrame({"flow": list(flows)}), how="cross")
        .join(unlinked, on=["provider_code", "year", "flow"], how="left")
        .join(covered, on=["provider_code", "year"], how="left")
        .with_columns(
            amount_usd=pl.when(pl.col("covered")).then(
                pl.col("amount_usd").fill_null(0.0)
            )
        )
        .select(list(BTR_CRS_UNLINKED.columns))
    )


def funding_class(funding_source: pl.Expr) -> pl.Expr:
    """`oda` or `oof` where a BTR funding source names that flow and not the other,
    as "ODA (ETS)" or "OOF (ICF) OOF (ICF*)" do, and `other` otherwise, as for
    "ODA/OOF", "Other (OSEC)" or none."""
    named = funding_source.str.to_uppercase().str.extract_all(r"\b(?:ODA|OOF)\b")
    flows = named.list.unique()
    return (
        pl.when(flows.list.len() == 1)
        .then(flows.list.first().str.to_lowercase())
        .otherwise(pl.lit("other"))
    )


def provider_coefficients(
    entries: Mapping[str, Sequence[Mapping[str, object]]],
) -> pl.DataFrame:
    """The `provider_coefficients` table from `entries`, the coefficients file's rows
    by ISO 3 code.

    An entry leaves out the columns that are null, and `case_by_case` where it is
    false.

    Raises:
        ValueError: when a party has no DAC provider code, or an entry names a column
            the table does not have or lacks its source.
    """
    columns = set(PROVIDER_COEFFICIENTS.columns) - {"provider_code"}
    rows = []
    for party, rows_of_party in entries.items():
        code = party_provider(party)
        if code is None:
            raise ValueError(f"{party}: no DAC provider code")
        for entry in rows_of_party:
            if unknown := sorted(set(entry) - columns):
                raise ValueError(f"{party}: unknown columns {unknown}")
            if missing := sorted({"source", "locator"} - set(entry)):
                raise ValueError(f"{party}: no {' or '.join(missing)}")
            rows.append(
                dict.fromkeys(columns)
                | {"case_by_case": False}
                | dict(entry)
                | {"provider_code": code}
            )
    return pl.DataFrame(
        rows,
        schema={
            name: column.dtype for name, column in PROVIDER_COEFFICIENTS.columns.items()
        },
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
    """Pack the release built in `release_dir` as `out_path`, a `.tar.zst`.

    The pack holds one directory, `climate-finance-<release>/`, with the release
    tables but those in `WITHHELD_TABLES`, `release.json` and `sources.json`, and
    nothing else the assemble stage writes, such as its `stage.json`. Its
    `release.json` is the build's with the withheld tables moved from `tables` to
    `withheld_tables`. `sources.json` lists every source the fetch stage recorded,
    the BTR archive under `fetch.BTR_SOURCE` among them, and `REPORTS_SOURCE` when
    its `modelled` rows use a provider's report ratios, each with its version, URLs
    and licence, and a note on each source whose rows the release leaves out.
    Members go in name order with fixed owners and the release's creation time,
    so one release packs to the same bytes.

    Raises:
        BuildError: when a source has no entry in `SOURCE_LICENCES`, or when the
            release holds BTR rows and the raw manifest records no BTR archive.
    """
    assembled = release_dir / "assemble"
    manifest = ReleaseManifest.model_validate_json(
        (assembled / "release.json").read_text()
    )
    workbooks = release_dir / "normalise" / f"{BTR_WORKBOOKS.name}.parquet"
    sources = release_sources(
        pl.read_parquet(release_dir / "fetch" / "raw_manifest.parquet"),
        versions=manifest.source_versions,
        btr_rows=manifest.tables[BTR.name].rows,
        btr_workbooks=pl.read_parquet(workbooks) if workbooks.exists() else None,
        citations=report_citations(
            set(
                pl.read_parquet(assembled / f"{MODELLED.name}.parquet")["provider_code"]
            )
        ),
    )
    published = manifest.model_copy(
        update={
            "tables": {
                name: entry
                for name, entry in manifest.tables.items()
                if name not in WITHHELD_TABLES
            },
            "withheld_tables": WITHHELD_TABLES,
        }
    )
    # Tables stream from disk; the two JSON files are written from memory.
    files: dict[str, Path | bytes] = {
        f"{name}.parquet": assembled / f"{name}.parquet" for name in published.tables
    }
    files["release.json"] = (published.model_dump_json(indent=2) + "\n").encode()
    files[SOURCES_FILE] = (json.dumps(sources, indent=2) + "\n").encode()
    root = release_dirname(manifest.release)
    mtime = int(manifest.created_at.timestamp())
    partial = out_path.with_name(f"{out_path.name}.part")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with (
        pa.output_stream(str(partial), compression="zstd") as stream,
        tarfile.open(fileobj=stream, mode="w|", format=tarfile.PAX_FORMAT) as tar,
    ):
        for name in sorted(files):
            member = files[name]
            if isinstance(member, bytes):
                tar.addfile(
                    _member(f"{root}/{name}", len(member), mtime), io.BytesIO(member)
                )
                continue
            with member.open("rb") as handle:
                tar.addfile(
                    _member(f"{root}/{name}", member.stat().st_size, mtime), handle
                )
    partial.replace(out_path)
    return out_path


def release_sources(
    raw_manifest: pl.DataFrame,
    *,
    versions: Mapping[str, str],
    btr_rows: int,
    btr_workbooks: pl.DataFrame | None,
    citations: Sequence[str],
) -> list[dict[str, object]]:
    """Each source in `raw_manifest`, and `REPORTS_SOURCE` with its `citations` when
    there are any, with its version from `versions`, its URLs and its licence, and
    as `note` `WITHHELD_NOTE` for a source in `WITHHELD_SOURCES` and
    `BTR_WITHHELD_NOTE` for the BTR archive, `fetch.BTR_SOURCE`.

    The BTR entry lists as `workbooks` each row of `btr_workbooks` (the normalise
    stage's `btr_workbooks`): the workbook's file name, its party, the Table III.1
    sheets read, the rows kept, the rows dropped for each reason, and the reporting
    tool's draft state in the file name. A workbook giving no rows shows there, so a
    party that reports nothing reads apart from one that was not loaded.

    Raises:
        BuildError: when a source has no entry in `SOURCE_LICENCES`, or when the
            release holds BTR rows (`btr_rows`) and `raw_manifest` no BTR archive.
    """
    urls = {
        str(source): sorted(set(frame["url"].drop_nulls()))
        for (source,), frame in raw_manifest.group_by("source")
    }
    if btr_rows and BTR_SOURCE not in urls:
        raise BuildError(
            f"The release holds {btr_rows} BTR rows but the raw manifest records "
            f"no {BTR_SOURCE} source to credit them to"
        )
    if citations:
        urls[REPORTS_SOURCE] = []
    if unlicensed := sorted(source for source in urls if _licence(source) is None):
        raise BuildError(f"Sources without a licence in SOURCE_LICENCES: {unlicensed}")
    return [
        {
            "source": source,
            "version": versions.get(source),
            "urls": urls[source],
            "licence": _licence(source),
            **({"note": note} if (note := _source_note(source)) else {}),
            **(
                {"workbooks": btr_workbooks.to_dicts()}
                if source == BTR_SOURCE and btr_workbooks is not None
                else {}
            ),
            **({"citations": list(citations)} if source == REPORTS_SOURCE else {}),
        }
        for source in sorted(urls)
    ]


def _source_note(source: str) -> str | None:
    if source.startswith(WITHHELD_SOURCES):
        return WITHHELD_NOTE
    return BTR_WITHHELD_NOTE if source == BTR_SOURCE else None


def report_citations(providers: Collection[int]) -> list[str]:
    """The sources `providers.toml` cites for the report ratios of `providers`'s
    modelled ranges, each once, in order."""
    return sorted(
        {
            ratio.source
            for entry in load_provider_facts().modelled
            if entry.provider_code in providers
            for ratio in entry.report_ratios
        }
    )


def _licence(source: str) -> str | None:
    starts = [start for start in SOURCE_LICENCES if source.startswith(start)]
    return SOURCE_LICENCES[max(starts, key=len)] if starts else None


def _member(name: str, size: int, mtime: int) -> tarfile.TarInfo:
    member = tarfile.TarInfo(name)
    member.size = size
    member.mtime = mtime
    member.mode = 0o644
    return member


def cut_release(
    release: str,
    *,
    config: Path,
    out_dir: Path,
    sample: Path | None = None,
    previous: Path | None = None,
    first_release: bool = False,
    waive_precision: str | None = None,
) -> Path:
    """Build `release`, run the gates, record their results and pack it.

    The gates read the blind-sample labels from the config's review decisions, the
    sample's design from `sample` and the previous release from `previous` (see
    `gate.run_gates`). Their rows, keyed by gate name, the previous release's name
    and any waiver go into `release.json`. The pack is
    `<out_dir>/climate-finance-<release>.tar.zst`.

    Args:
        waive_precision: the reason to let the precision and human-audit gates
            fail, recorded as `precision_waiver` with `precision_gate_waived` true.

    Raises:
        GateFailedError: when a gate fails and is not waived.
        BuildError: when `waive_precision` gives a blank reason.
    """
    if waive_precision is not None and not waive_precision.strip():
        raise BuildError("--waive-precision needs a reason, recorded in release.json")
    build_package.build(release, config=config, out_dir=out_dir)
    release_dir = out_dir / release
    results = gate.run_gates(
        release_dir,
        decisions=load_config(config).sources.review_decisions,
        sample=sample,
        previous=previous,
        first_release=first_release,
        waive_precision=waive_precision is not None,
    )
    manifest_path = release_dir / "assemble" / "release.json"
    manifest = ReleaseManifest.model_validate_json(manifest_path.read_text())
    gated = manifest.model_copy(
        update={
            "gates": gate.gate_results_json(results),
            "precision_gate_waived": waive_precision is not None,
            "precision_waiver": waive_precision,
            "previous_release": None
            if previous is None
            else load(path=previous).manifest.release,
        }
    )
    manifest_path.write_text(gated.model_dump_json(indent=2) + "\n")
    return pack_release(
        release_dir, out_path=out_dir / f"{release_dirname(release)}.tar.zst"
    )


def packed_manifest(pack: Path) -> ReleaseManifest:
    """The `release.json` inside the packed release at `pack`.

    Raises:
        BuildError: when the pack holds no `release.json`.
    """
    with (
        pa.input_stream(str(pack), compression="zstd") as stream,
        tarfile.open(fileobj=stream, mode="r|") as tar,
    ):
        for member in tar:
            if Path(member.name).name == "release.json" and (
                file := tar.extractfile(member)
            ):
                return ReleaseManifest.model_validate_json(file.read())
    raise BuildError(f"{pack} holds no release.json")


def unpublishable(manifest: ReleaseManifest) -> list[str]:
    """Why `manifest`'s release may not be published: a gate of `gate.WAIVABLE`
    with no recorded row, and any recorded gate row neither passed nor waived. A
    row counts as waived only on a gate of `gate.WAIVABLE`, with the waiver's
    reason recorded as `precision_waiver`."""
    problems = [
        f"{name}: release.json records no result"
        for name in gate.WAIVABLE
        if not manifest.gates.get(name)
    ]
    for name, rows in manifest.gates.items():
        if not isinstance(rows, list):
            problems.append(f"{name}: release.json records a malformed result")
            continue
        for row in rows:
            if not isinstance(row, dict):
                problems.append(f"{name}: release.json records a malformed row")
                continue
            label = f"{name} {row['subject']}" if row.get("subject") else name
            if row.get("passed") is True:
                continue
            if row.get("waived") is not True:
                problems.append(f"{label}: failed")
            elif name not in gate.WAIVABLE:
                problems.append(f"{label}: waived, but only {gate.WAIVABLE} can be")
            elif not manifest.precision_waiver:
                problems.append(f"{label}: waived with no recorded reason")
    return problems


def publishable(pack: Path) -> ReleaseManifest:
    """The `release.json` inside the packed release at `pack`, once its gates
    allow publishing it.

    Raises:
        GateFailedError: when the pack's `release.json` records a gate row
            neither passed nor waived, a waiver without its reason, or no result
            for the precision and human-audit gates (`unpublishable`).
    """
    manifest = packed_manifest(pack)
    if problems := unpublishable(manifest):
        raise GateFailedError(problems)
    return manifest


def upload_release(pack: Path, *, target: str) -> str:
    """Upload a packed release to `target`; return its URL.

    `target` is a directory, or an http(s) URL that takes the pack by PUT at
    `<target>/<pack name>`. A pack its gates keep from publishing is never
    uploaded.

    Raises:
        GateFailedError: when the pack's gates keep it from publishing
            (`publishable`).
        httpx.HTTPStatusError: when the server refuses the upload.
    """
    publishable(pack)
    if not target.startswith(("http://", "https://")):
        directory = Path(target)
        directory.mkdir(parents=True, exist_ok=True)
        copy = directory / pack.name
        shutil.copy2(pack, copy)
        return copy.resolve().as_uri()
    url = f"{target.rstrip('/')}/{quote(pack.name)}"
    with pack.open("rb") as body:
        httpx.put(url, content=body, timeout=UPLOAD_TIMEOUT).raise_for_status()
    return url


def add_to_index(
    index: ReleaseIndex, *, release: str, pack: Path, url: str
) -> ReleaseIndex:
    """`index` with `release` published at `url` as its newest release.

    Raises:
        GateFailedError: when the pack's gates keep it from publishing
            (`publishable`).
        BuildError: when `release` is not the release the pack holds.
    """
    if (packed := publishable(pack).release) != release:
        raise BuildError(f"{pack} holds release {packed!r}, not {release!r}")
    return index.model_copy(
        update={
            "latest": release,
            "releases": {
                **index.releases,
                release: IndexEntry(url=url, sha256=sha256_file(pack)),
            },
        }
    )

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

The release leaves out the Green Climate Fund's own records, whose terms bar
redistribution: `disbursements` drops its fill rows (`schema.withheld_record`), and
each commitment they reached states their amount as `withheld_usd`, so its
disbursed amounts and `quality` keep them. `sources.json` notes the sources whose
rows stay out (`WITHHELD_SOURCES`).
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
from climate_finance.build.allocate import counted_rows, years_since_commitment
from climate_finance.build.config import load_config
from climate_finance.build.errors import (
    BuildError,
    ReleaseInvariantError,
    StageInputMissingError,
)
from climate_finance.build.label import inherit_markers
from climate_finance.build.readers.btr import party_provider
from climate_finance.build.sums import ordered_sum
from climate_finance.query import BILATERAL, MULTILATERAL_OUTFLOW
from climate_finance.schema import (
    BTR,
    BTR_LINKS,
    COMMITMENTS,
    CORE_CONTRIBUTIONS,
    DISBURSEMENT_PROFILE,
    DISBURSEMENTS,
    LINK_METHODS,
    PROVIDER_COEFFICIENTS,
    QUALITY,
    RELEASE_TABLES,
    TOTAL_FLOWS,
    ReleaseManifest,
    TableEntry,
    TableSchema,
    aggregate_channels,
    release_invariants,
    sha256_file,
    withheld_record,
)
from climate_finance.snapshot import IndexEntry, ReleaseIndex, load, release_dirname

if TYPE_CHECKING:
    from collections.abc import Mapping

    from climate_finance.build.stages import StageContext

# A commitment's link counts as high probability from this value.
HIGH_PROBABILITY = 0.8
# httpx bounds each socket operation, not the whole upload, so a 150 to 400 MB pack
# passes at any steady rate. Read waits for the server to store the body and answer.
UPLOAD_TIMEOUT = httpx.Timeout(60.0, read=300.0)
# `disbursement_profile` groups and the Bi_Multi codes each pools. `all` holds the
# codes the recipient view counts.
PROFILE_GROUPS = {
    "all": (*BILATERAL, MULTILATERAL_OUTFLOW),
    "bilateral": BILATERAL,
    "multilateral": (MULTILATERAL_OUTFLOW,),
}
# The terms each source's data comes under, by `raw_manifest` source name or the
# start of one. The longest matching start wins.
_OECD = (
    "OECD Terms and Conditions; dataset terms in each dataset's metadata, "
    "https://www.oecd.org/en/about/terms-conditions.html"
)
_WORLD_BANK = (
    "World Bank Data Catalog licence (CC BY 4.0 by default; per dataset), "
    "https://datacatalog.worldbank.org/public-licenses"
)
_IATI = "the publisher's licence in the IATI Registry, https://iatiregistry.org"
_GCF = (
    "Green Climate Fund terms: no commercial redistribution or derivative works "
    "without written consent, https://www.greenclimate.fund/terms-and-conditions"
)
_UNFCCC = (
    "UNFCCC terms of use: public domain with source acknowledged, "
    "https://unfccc.int/this-site/terms-of-use"
)
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
    "foreignassistance": "US Government work, free of copyright, "
    "https://foreignassistance.gov/about",
    "adaptation_labels": "Weikmans et al.; fits the adaptation cut, no rows ship",
    "btr": _UNFCCC,
    "unfccc": _UNFCCC,
}
# Sources whose rows the release leaves out (`schema.WITHHELD_RECORD_PREFIXES`), by
# `raw_manifest` source name or the start of one, and the note `sources.json` gives
# them.
WITHHELD_SOURCES = ("gcf_projects", "ledger_gcf")
WITHHELD_NOTE = (
    "Green Climate Fund rows are left out of the release, as its terms bar "
    "redistribution; `cf-build` rebuilds them from the GCF's API."
)
# Each BTR party's weight on Significant activities, estimated from its linked pairs
# (`readers.btr.write_party_weights`) and confirmed by a reviewer.
PARTY_WEIGHTS = Path(__file__).with_name("unfccc_party_weights.yaml")
PARTY_WEIGHT_BASIS = "btr_crs_ratio"
# The file in a pack that lists each source with its version, URLs and licence.
SOURCES_FILE = "sources.json"
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
    btr = _optional(context, "normalise", BTR)
    allocations = context.read("fill", "allocations")
    status = context.read("review", "commitments")
    objectives = context.read("label", "objectives")

    disbursements = release_disbursements(
        allocations,
        crdf=crdf,
        crs=crs,
        units=context.read("link", "units"),
        links=links,
    ).collect()
    withheld = withheld_record(pl.col("record_id"))
    commitments = release_commitments(
        crdf, status=status, withheld=disbursements.lazy().filter(withheld)
    ).collect()
    return {
        "commitments": commitments,
        "disbursements": disbursements.filter(~withheld),
        "objectives": objectives.collect(),
        "agency_shares": context.read("measure", "agency_shares").collect(),
        "modelled": context.read("measure", "modelled").collect(),
        "core_contributions": core_contributions(
            context.read("normalise", "core_contributions")
        ).collect(),
        "btr": btr,
        "btr_links": btr_links(links, crs=crs).collect(),
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
        "provider_coefficients": provider_coefficients(
            yaml.safe_load(PARTY_WEIGHTS.read_text(encoding="utf-8"))
        )
        if not btr.is_empty()
        else _empty(PROVIDER_COEFFICIENTS),
    }


def release_commitments(
    crdf: pl.LazyFrame, *, status: pl.LazyFrame, withheld: pl.LazyFrame
) -> pl.LazyFrame:
    """The CRDF rows allocation follows, with their status and disbursed amounts, and
    as `withheld_usd` the sum of their `withheld` disbursements rows."""
    withheld_usd = withheld.group_by("row_id").agg(
        withheld_usd=ordered_sum(pl.col("allocated_usd"))
    )
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
    `carrying_links`). A fill (`fill_disbursements`) has no CRS record or link.
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
        inherit_markers(from_crs, commitments=crdf, crs=crs)
        .join(record, on="record_id", how="left", validate="m:1")
        .join(
            carrying_links(from_crs, crdf=crdf, units=units, links=links),
            on=["row_id", "record_id"],
            how="left",
            validate="m:1",
        )
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
    what the fill gives every commitment on it; there is no grant equivalent. Markers
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


def carrying_links(
    allocated: pl.LazyFrame,
    *,
    crdf: pl.LazyFrame,
    units: pl.LazyFrame,
    links: pl.LazyFrame,
) -> pl.LazyFrame:
    """Per allocation's (row_id, record_id), the link that carried it: its method,
    probability and ledger check, and its disbursement (`d_unit`) and commitment
    (`c_unit`) units.

    That is the `trace` link from the record's unit to a commitment unit of a
    `crdf_crs` group of the row or of a row that repeats it whose method comes
    first in `LINK_METHODS`, since allocation runs that method's pass first; then
    the most probable, then the lowest unit id. Allocate moves a repeat's
    allocations to its first row (`repeat_of`), so the link can sit in the
    repeat's group.
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
        method_rank=pl.col("link_method").replace_strict(
            {method: rank for rank, method in enumerate(LINK_METHODS)},
            return_dtype=pl.Int16,
        ),
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
            "method_rank",
            "link_probability",
            "c_unit",
            descending=[False, False, False, True, False],
        )
        .group_by("row_id", "record_id", maintain_order=True)
        .agg(
            pl.col(
                "link_method", "link_probability", "ledger_check", "d_unit", "c_unit"
            ).first()
        )
    )


def core_contributions(stage: pl.LazyFrame) -> pl.LazyFrame:
    """Core contributions to leaf channels for every flow.

    MULTI publishes aggregate channels beside their members, and each provider's
    members sum to its aggregate, so the aggregates leave (`aggregate_channels`).
    MULTI publishes commitments and disbursements. The grant-equivalent rows equal
    the disbursement rows, as the DAC's grant-equivalent measure counts core
    contributions to multilateral agencies at face value.
    """
    stage = stage.join(aggregate_channels(stage), on="channel_code", how="anti")
    grant_equivalent = stage.filter(pl.col("flow") == "disbursements").with_columns(
        flow=pl.lit("grant_equivalent")
    )
    return pl.concat([stage, grant_equivalent]).select(list(CORE_CONTRIBUTIONS.columns))


def total_flows(crs: pl.LazyFrame) -> pl.LazyFrame:
    """Every CRS record's amounts by provider, recipient, year, Bi_Multi, flow and
    the record's higher Rio marker (0 for none).

    A key and flow with no reported amount has no row.
    """
    keys = ["provider_code", "recipient_code", "year", "bi_multi", "rio_marker"]
    return (
        crs.select(
            "recipient_code",
            "year",
            "bi_multi",
            provider_code="donor_code",
            rio_marker=_rio_marker(),
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
                provider_code="donor_code",
                rio_marker=_rio_marker(),
            ),
            on="record_id",
            how="inner",
            validate="m:1",
        )
        .select(list(BTR_LINKS.columns))
    )


def provider_coefficients(weights: Mapping[str, Mapping[str, object]]) -> pl.DataFrame:
    """One row per party in `weights` (the party-weights file) with an estimated
    weight and a DAC provider code, confirmed as the file says."""
    rows = [
        {
            "provider_code": code,
            "significant": entry["significant"],
            "basis": PARTY_WEIGHT_BASIS,
            "confirmed": entry["confirmed"],
        }
        for party, entry in weights.items()
        if entry["significant"] is not None
        and (code := party_provider(party)) is not None
    ]
    return pl.DataFrame(
        rows,
        schema={
            name: column.dtype for name, column in PROVIDER_COEFFICIENTS.columns.items()
        },
    )


def _rio_marker() -> pl.Expr:
    """A CRS record's higher Rio marker, 0 for none."""
    return (
        pl.max_horizontal("rio_adaptation", "rio_mitigation").fill_null(0).cast(pl.Int8)
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
    and report-labelled otherwise. `disbursed_climate_usd` is the climate value of
    its disbursements' `allocated_high_usd`, and `certain_disbursed_climate_usd` of
    their `allocated_low_usd`, each at the commitment's climate share of face.
    `grant_equiv_coverage` is the share of the linked commitments' allocated records
    with a grant equivalent. Repeats count only in `repeat_climate_usd`.
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
    disbursed = disbursements.group_by("row_id").agg(
        high=ordered_sum(pl.col("allocated_high_usd")),
        low=ordered_sum(pl.col("allocated_low_usd")),
    )
    climate = pl.col("climate_usd")
    face = pl.col("face_usd")
    counted = pl.col("repeat_of").is_null()
    linked = counted & pl.col("best_probability").is_not_null()
    model = counted & pl.col("model_labelled").fill_null(False)
    return (
        commitments.join(best, on="row_id", how="left")
        .join(by_row, on="row_id", how="left")
        .join(disbursed, on="row_id", how="left")
        .group_by("provider_code", "year")
        .agg(
            commitments=counted.sum().cast(pl.Int32),
            climate_usd=ordered_sum(climate.filter(counted)),
            linked_usd=ordered_sum(climate.filter(linked)),
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
    """Pack the release built in `release_dir` as `out_path`, a `.tar.zst`.

    The pack holds one directory, `climate-finance-<release>/`, with the release
    tables, `release.json` and `sources.json`. `sources.json` lists every source the
    fetch stage recorded, and `btr` when the release holds BTR rows, each with its
    version, URLs and licence, and a note on each source whose rows the release
    leaves out. Members go in name order with fixed owners and the
    release's creation time, so one release packs to the same bytes.

    Raises:
        BuildError: when a source has no entry in `SOURCE_LICENCES`.
    """
    assembled = release_dir / "assemble"
    manifest = ReleaseManifest.model_validate_json(
        (assembled / "release.json").read_text()
    )
    sources = release_sources(
        pl.read_parquet(release_dir / "fetch" / "raw_manifest.parquet"),
        versions=manifest.source_versions,
        btr_rows=manifest.tables[BTR.name].rows,
    )
    root = release_dirname(manifest.release)
    mtime = int(manifest.created_at.timestamp())
    partial = out_path.with_name(f"{out_path.name}.part")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with (
        pa.output_stream(str(partial), compression="zstd") as stream,
        tarfile.open(fileobj=stream, mode="w|", format=tarfile.PAX_FORMAT) as tar,
    ):
        for path in sorted(assembled.iterdir()):
            with path.open("rb") as handle:
                tar.addfile(
                    _member(f"{root}/{path.name}", path.stat().st_size, mtime), handle
                )
        body = (json.dumps(sources, indent=2) + "\n").encode()
        tar.addfile(
            _member(f"{root}/{SOURCES_FILE}", len(body), mtime), io.BytesIO(body)
        )
    partial.replace(out_path)
    return out_path


def release_sources(
    raw_manifest: pl.DataFrame, *, versions: Mapping[str, str], btr_rows: int
) -> list[dict[str, object]]:
    """Each source in `raw_manifest`, and `btr` when `btr_rows` is positive, with its
    version from `versions`, its URLs and its licence, and `WITHHELD_NOTE` as `note`
    for a source in `WITHHELD_SOURCES`.

    Raises:
        BuildError: when a source has no entry in `SOURCE_LICENCES`.
    """
    urls = {
        str(source): sorted(set(frame["url"].drop_nulls()))
        for (source,), frame in raw_manifest.group_by("source")
    }
    if btr_rows:
        urls["btr"] = []
    if unlicensed := sorted(source for source in urls if _licence(source) is None):
        raise BuildError(f"Sources without a licence in SOURCE_LICENCES: {unlicensed}")
    return [
        {
            "source": source,
            "version": versions.get(source),
            "urls": urls[source],
            "licence": _licence(source),
            **({"note": WITHHELD_NOTE} if source.startswith(WITHHELD_SOURCES) else {}),
        }
        for source in sorted(urls)
    ]


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
    waive_precision: bool = False,
) -> Path:
    """Build `release`, run the gates, record their results and pack it.

    The gates read the blind-sample labels from the config's review decisions, the
    sample's design from `sample` and the previous release from `previous` (see
    `gate.run_gates`). Their rows, keyed by gate name, the previous release's name
    and `waive_precision` go into `release.json`; the pack is
    `<out_dir>/climate-finance-<release>.tar.zst`.

    Raises:
        GateFailedError: when a gate fails; `waive_precision` lets the precision
            and human-audit gates fail.
    """
    build_package.build(release, config=config, out_dir=out_dir)
    release_dir = out_dir / release
    results = gate.run_gates(
        release_dir,
        decisions=load_config(config).sources.review_decisions,
        sample=sample,
        previous=previous,
        first_release=first_release,
        waive_precision=waive_precision,
    )
    manifest_path = release_dir / "assemble" / "release.json"
    manifest = ReleaseManifest.model_validate_json(manifest_path.read_text())
    gated = manifest.model_copy(
        update={
            "gates": gate.gate_results_json(results),
            "precision_gate_waived": waive_precision,
            "previous_release": None
            if previous is None
            else load(path=previous).manifest.release,
        }
    )
    manifest_path.write_text(gated.model_dump_json(indent=2) + "\n")
    return pack_release(
        release_dir, out_path=out_dir / f"{release_dirname(release)}.tar.zst"
    )


def upload_release(pack: Path, *, target: str) -> str:
    """Upload a packed release to `target`; return its URL.

    `target` is a directory, or an http(s) URL that takes the pack by PUT at
    `<target>/<pack name>`.

    Raises:
        httpx.HTTPStatusError: when the server refuses the upload.
    """
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
    """`index` with `release` published at `url` as its newest release."""
    return index.model_copy(
        update={
            "latest": release,
            "releases": {
                **index.releases,
                release: IndexEntry(url=url, sha256=sha256_file(pack)),
            },
        }
    )

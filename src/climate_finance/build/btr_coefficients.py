"""The climate coefficients each BTR party's own reporting implies (`btr_coefficients`).

A BTR Table 1 row states the climate-specific amount of an activity, and `btr_links`
ties it to the CRS records of the same activity and year. Rows and records that share
links form a connected component, so a row linked to several records, or a record
linked from several rows, counts its money once. A component's share is its BTR amount
over the CRS amount of its records. A share up to `MAX_SHARE` counts at most 1, as
BTR and CRS amounts differ by rounding; a component above it leaves the evidence, and
the table reports it.

A party's status label does not say which CRS flow its amounts follow: the UK labels
its spending "committed", and its principal-marked components sit at a share of 1 on
disbursements. Each party and status takes the flow on which the median share of its
principal-marked components lies nearer 1. Principal-marked activities count in full
for every party that states a coefficient, so a party and status whose principal
median on that flow lies outside `ALIGNED`, or that has fewer than
`MIN_PRINCIPAL_COMPONENTS` such components, reports amounts that do not line up with
its CRS records, and gives no coefficient.

A scope of `provider_coefficients` (a provider, or one of its agencies with a row of
its own) takes, per Rio marker, the CRS-weighted share of its aligned components whose
records all belong to the scope and carry that highest marker. It needs
`MIN_COMPONENTS` such components and `MIN_LINKED_USD` of CRS amount, the least
evidence at which no coefficient of the first BTRs rests on a few activities.
"""

import numpy as np
import polars as pl
from scipy.sparse import coo_array
from scipy.sparse.csgraph import connected_components

from climate_finance.build.sums import ordered_sum
from climate_finance.schema import (
    BTR_COEFFICIENTS,
    COEFFICIENT_MARKERS,
    stated_markers,
)

# The least evidence behind a coefficient: components, and their CRS amount in USD.
# On the first BTRs (2021 and 2022) a component bootstrap gives every coefficient at
# these thresholds a standard deviation of 0.12 or less.
MIN_COMPONENTS = 20
MIN_LINKED_USD = 20e6
# The principal-marked components a party and status needs, and the band their median
# share must fall in, for its amounts to count as lining up with its CRS records.
MIN_PRINCIPAL_COMPONENTS = 5
ALIGNED = (0.9, 1.1)
# The highest share a component can have and stay in the evidence. A share above it is
# a link to the wrong, smaller record or a claim larger than the money the CRS
# records: on the first BTRs the UK's components above it are BII's capital increase
# (USD 728m of BTR on 265m of CRS, a share of 2.74) and links to records under a
# tenth of the BTR amount. Capped at 1 they lifted the UK's significant coefficient
# from 0.40 to 0.59.
MAX_SHARE = 1.1
# Each BTR status and the CRS amounts it may follow.
_FLOWS = {"committed": "commitment_usd", "disbursed": "disbursement_usd"}
_LEVELS = dict(zip((1, 2), COEFFICIENT_MARKERS, strict=True))
_LEVELS_BY_NAME = {name: level for level, name in _LEVELS.items()}
_SCOPE = ["provider_code", "agency_code"]


def btr_coefficients(
    btr: pl.DataFrame, *, links: pl.DataFrame, coefficients: pl.DataFrame
) -> pl.DataFrame:
    """The `btr_coefficients` table: per scope of `coefficients` whose provider is a
    BTR party, and per Rio marker, the share its BTR implies and the coefficient
    `PROVIDER` takes.

    Args:
        btr: The `btr` table. Rows without a status or a face value in USD at the DAC
            rate, and aggregate rows, are left out.
        links: The `btr_links` table with each record's `agency_code`.
        coefficients: The `provider_coefficients` table.

    Returns:
        One row per scope and marker, as `schema.BTR_COEFFICIENTS` describes them.
    """
    components, records = btr_components(btr, links=links, coefficients=coefficients)
    kept = pl.col("btr_usd") <= MAX_SHARE * pl.col("gross")
    evidence = (
        components.filter(pl.col("aligned"), pl.col("single"), pl.col("gross") > 0)
        .group_by("provider_code", "scope", "marker")
        .agg(
            components=kept.sum().cast(pl.Int32),
            linked_usd=ordered_sum(pl.col("gross").filter(kept)),
            climate_usd=ordered_sum(pl.min_horizontal("btr_usd", "gross").filter(kept)),
            dropped=(~kept).sum().cast(pl.Int32),
            dropped_usd=ordered_sum(pl.col("gross").filter(~kept)),
            dropped_btr_usd=ordered_sum(pl.col("btr_usd").filter(~kept)),
            evidence_first_year=pl.col("first_year").filter(kept).min(),
            evidence_last_year=pl.col("last_year").filter(kept).max(),
        )
    )
    # A scope is reached where any of its records is linked, whatever component
    # the record sits in.
    reached = records.select(
        "provider_code", "scope", "marker", reached=pl.lit(value=True)
    ).unique()
    aligned = (
        components.select("provider_code", "status", "aligned")
        .unique()
        .group_by("provider_code")
        .agg(aligned=pl.col("aligned").any())
    )
    enough = (pl.col("components") >= MIN_COMPONENTS) & (
        pl.col("linked_usd") >= MIN_LINKED_USD
    )
    coefficient = pl.col("climate_usd") / pl.col("linked_usd")
    scope_markers = stated_markers(coefficients).join(
        btr.select("provider_code").drop_nulls().unique(),
        on="provider_code",
        how="semi",
    )
    keys = ["provider_code", "scope", "marker"]
    return (
        scope_markers.with_columns(
            scope=pl.col("agency_code"),
            marker=pl.col("marker").replace_strict(
                _LEVELS_BY_NAME, return_dtype=pl.Int8
            ),
        )
        .join(evidence, on=keys, how="left", nulls_equal=True)
        .join(reached, on=keys, how="left", nulls_equal=True)
        .join(aligned, on="provider_code", how="left")
        .with_columns(
            pl.col("components", "dropped").fill_null(0),
            pl.col("linked_usd", "dropped_usd", "dropped_btr_usd").fill_null(0.0),
            coefficient=pl.when(enough.fill_null(value=False)).then(coefficient),
            reason=pl.when(enough.fill_null(value=False))
            .then(pl.lit(None, pl.String))
            .when(pl.col("reached").is_null())
            .then(pl.lit("no_links"))
            .when(~pl.col("aligned").fill_null(value=False))
            .then(pl.lit("unaligned"))
            .otherwise(pl.lit("thin")),
        )
        .with_columns(
            marker=pl.col("marker").replace_strict(_LEVELS, return_dtype=pl.String),
            source=pl.when(pl.col("states"))
            .then(pl.lit("stated"))
            .when(pl.col("coefficient").is_not_null())
            .then(pl.lit("btr"))
            .otherwise(pl.lit("fallback")),
        )
        .select(*(pl.col(name).cast(column.dtype) for name, column in _COLUMNS.items()))
        .sort(*BTR_COEFFICIENTS.sort_key, nulls_last=False)
    )


def btr_components(
    btr: pl.DataFrame, *, links: pl.DataFrame, coefficients: pl.DataFrame
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """The components behind `btr_coefficients`, and the linked records with their
    scope and highest marker.

    Each component has its provider, status and scope, its highest `marker`,
    `single` where its records share one provider, scope and marker, its BTR amount
    (`btr_usd`) and BTR years, `gross`, the CRS amount on the flow its party and
    status follow, and `aligned`, whether that party and status pass the principal
    check. A component's provider and scope are the least of its records', so a
    mixed component reads the same on every run.
    """
    rows = btr.filter(
        pl.col("status").is_in(list(_FLOWS)),
        ~pl.col("is_aggregate"),
        pl.col("face_usd_at_dac").is_not_null(),
    ).select("row_id", "year", "status", btr_usd="face_usd_at_dac")
    edges = links.select("row_id", "record_id").join(
        rows.select("row_id", "status"), on="row_id"
    )
    edges = edges.with_columns(component=_components(edges))
    marker = pl.max_horizontal("rio_adaptation", "rio_mitigation").fill_null(0)
    records = _scoped(
        edges.select("component", "status", "record_id")
        .unique()
        .join(
            links.unique("record_id").with_columns(marker=marker.cast(pl.Int8)),
            on="record_id",
            validate="m:1",
        ),
        coefficients=coefficients,
    )
    btr_amounts = (
        edges.select("component", "row_id")
        .unique()
        .join(rows, on="row_id")
        .group_by("component")
        .agg(
            btr_usd=ordered_sum(pl.col("btr_usd")),
            first_year=pl.col("year").min(),
            last_year=pl.col("year").max(),
        )
    )
    components = (
        records.group_by("component")
        .agg(
            pl.col("provider_code", "status").min(),
            scope=pl.col("scope").min(),
            single=(pl.col("provider_code").n_unique() == 1)
            & (pl.col("scope").n_unique() == 1)
            & (pl.col("marker").n_unique() == 1),
            marker=pl.col("marker").max(),
            **{
                amount: ordered_sum(pl.col(amount).fill_null(0.0))
                for amount in _AMOUNTS
            },
        )
        .join(btr_amounts, on="component", validate="1:1")
        .with_columns(
            **{
                f"share_{status}": pl.when(pl.col(amount) > 0).then(
                    pl.col("btr_usd") / pl.col(amount)
                )
                for status, amount in _FLOWS.items()
            }
        )
    )
    flows = _flows(components)
    return (
        components.join(flows.drop("provider_code", "status"), on="component"),
        records.select("component", "record_id", "provider_code", "scope", "marker"),
    )


_AMOUNTS = tuple(_FLOWS.values())
_COLUMNS = BTR_COEFFICIENTS.columns


def _components(edges: pl.DataFrame) -> pl.Series:
    """The connected component of each of `edges` (`row_id`, `status`, `record_id`)
    in the graph of BTR rows and CRS records. A record linked from a committed and a
    disbursed row is a node of each status, as each follows its own CRS flow."""
    ends = edges.select(
        row=pl.concat_str(pl.lit("row:"), "row_id"),
        record=pl.concat_str("status", pl.lit(":"), "record_id"),
    )
    nodes = pl.concat([ends["row"], ends["record"]]).unique().sort()
    index = {node: position for position, node in enumerate(nodes)}
    a = np.fromiter((index[node] for node in ends["row"]), dtype=np.int64)
    b = np.fromiter((index[node] for node in ends["record"]), dtype=np.int64)
    graph = coo_array(
        (np.ones(len(a), dtype=np.int8), (a, b)), shape=(len(nodes), len(nodes))
    )
    _, labels = connected_components(graph, directed=False)
    return pl.Series("component", labels[a], dtype=pl.Int64)


def _scoped(records: pl.DataFrame, *, coefficients: pl.DataFrame) -> pl.DataFrame:
    """`records` with `scope`: their agency where it has a `provider_coefficients`
    row of its own, else null for the provider's row."""
    own = (
        coefficients.filter(pl.col("agency_code").is_not_null())
        .select(*_SCOPE)
        .unique()
        .with_columns(scope=pl.col("agency_code"))
    )
    return records.join(own, on=_SCOPE, how="left")


def _flows(components: pl.DataFrame) -> pl.DataFrame:
    """Per provider and status, the CRS amount (`gross`) its components follow and
    whether their principal-marked shares bear it out (`aligned`)."""
    principal = components.filter(pl.col("single"), pl.col("marker") == 2)
    off = {
        status: (pl.col(f"share_{status}").median() - 1).abs().fill_null(np.inf)
        for status in _FLOWS
    }
    medians = principal.group_by("provider_code", "status").agg(
        principal=pl.len(),
        follows=pl.when(off["committed"] <= off["disbursed"])
        .then(pl.lit("committed"))
        .otherwise(pl.lit("disbursed")),
        **{f"median_{status}": pl.col(f"share_{status}").median() for status in _FLOWS},
    )
    median = (
        pl.when(pl.col("follows") == "committed")
        .then(pl.col("median_committed"))
        .otherwise(pl.col("median_disbursed"))
    )
    statuses = components.select("provider_code", "status").unique()
    return (
        statuses.join(medians, on=["provider_code", "status"], how="left")
        .with_columns(
            aligned=(pl.col("principal") >= MIN_PRINCIPAL_COMPONENTS).fill_null(
                value=False
            )
            & median.is_between(*ALIGNED).fill_null(value=False),
            follows=pl.coalesce("follows", "status"),
        )
        .join(
            components.select("component", "provider_code", "status", *_AMOUNTS),
            on=["provider_code", "status"],
        )
        .with_columns(
            gross=pl.when(pl.col("follows") == "committed")
            .then(pl.col(_FLOWS["committed"]))
            .otherwise(pl.col(_FLOWS["disbursed"]))
        )
        .select("component", "provider_code", "status", "aligned", "gross")
    )

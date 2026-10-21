"""Id-less bilateral aggregate rows rebuilt from bulk records.

Some bilateral providers report their development finance institutions' climate
finance to the CRDF as aggregate rows: no usable crs id, no agency, project or
title, only provider, year, recipient (often a region), flow category and Rio
markers. The bulk CRS lists the records they add up, which no CRDF row carries.

The pool is the provider's non-core records of the row's year whose (provider, crs
id, year) no CRDF row of the perspective carries, summed by agency, crs id, project
number, recipient, purpose, flow category and markers, with a positive commitment.
Three routes run in order:

- partition: within a cell (provider, year, flow category, mitigation and
  adaptation markers), a Rio-marked record goes to the row of its own recipient
  when the cell has one, else to the row of its continent, else to the 998 row. A
  row whose records sum to its climate value within 0.1% matches. Where two rows
  share a recipient, the records sent there directly and those rolled up from a
  country are the two candidate parts, paired with the rows the closer way round.
- cell: rows a cell leaves unmatched take every record of the cell not yet used, pro
  rata, when the cell's records and rows agree in total within 1%.
- single record: rows still left pair one to one, closest first, with a pool record
  of the same provider, year and recipient within 1% of the row, markers aside.

A matched row keeps its amounts and gains a child row per record, with the record's
agency, crs id, project number, title, recipient and purpose, the parent's other
fields, its amounts scaled by the record's share of the row's records, and
`parent_row_id` naming the row.
"""

import polars as pl

from climate_finance import schema
from climate_finance.build.normalise.crs import trace_population
from climate_finance.build.normalise.ids import content_id
from climate_finance.build.normalise.vintages import crs_ids, pair_one_to_one

_BILATERAL = 1
# A row matches its records when they sum to it within these shares of the row.
_PART_TOLERANCE = 0.001
_CELL_TOLERANCE = 0.01
_SINGLE_TOLERANCE = 0.01
# The CRDF prints the bulk CRS `category` as its flow code: ODA 10, OOF 21, private
# flows 30 and private development finance 60. The bulk flow codes map onto them.
FLOW_CATEGORY = {11: 10, 13: 10, 19: 10, 14: 21, 30: 30, 60: 60}
# DAC regional recipient codes for each continent, and the recipient code ranges of
# its countries and sub-regions, as the bulk CRS `region_name` groups them.
_CONTINENTS = (
    (89, ((1, 99),)),
    (298, ((100, 299), (1027, 1030))),
    (498, ((300, 499), (1031, 1032))),
    (798, ((500, 799),)),
    (889, ((800, 899), (1033, 1035))),
)
_CELL = ["provider_code", "year", "flow_code", "m", "a"]
_AMOUNTS = (
    "face_usd",
    "climate_usd",
    "adaptation_usd",
    "mitigation_usd",
    "overlap_usd",
)
# A child takes these from its record, and the rest from its parent.
_RECORD_FIELDS = ("agency_code", "crs_id", "project_number", "title", "purpose_code")
# The fields a pool record is summed by.
_RECORD_KEY = (
    "agency_code",
    "crs_id",
    "project_number",
    "recipient_code",
    "purpose_code",
    "flow_code",
    "m",
    "a",
)


def rebuild_aggregates(crdf: pl.LazyFrame, *, crs: pl.LazyFrame) -> pl.LazyFrame:
    """Add child rows with `parent_row_id` for aggregates the bulk records rebuild.

    Args:
        crdf: Normalised CRDF rows, both perspectives.
        crs: Normalised CRS records.
    """
    rows = crdf.collect()
    children = [
        _rebuild(rows.filter(perspective=perspective), crs=crs)
        for perspective in rows["perspective"].unique().sort()
    ]
    return pl.concat([rows, *children]).lazy()


def continent(recipient_code: pl.Expr) -> pl.Expr:
    """The DAC regional code of a recipient's continent; 998 outside them all."""
    regions = (
        pl.when(pl.any_horizontal(recipient_code.is_between(*r) for r in ranges)).then(
            pl.lit(code, pl.Int32)
        )
        for code, ranges in _CONTINENTS
    )
    return pl.coalesce(*regions, pl.lit(schema.UNSPECIFIED_RECIPIENT, pl.Int32))


def aggregate_rows(rows: pl.DataFrame, *, crs: pl.LazyFrame) -> pl.DataFrame:
    """Bilateral rows with a climate value and no crs id the CRS knows.

    EIB rows (the only rows with an `id_scheme`) are left out: the EIB is a
    development bank inside a bilateral provider, EU Institutions, and its rows
    carry ids of their own scheme that the EIB recovery route matches.
    """
    donors = crs.select(provider_code="donor_code").unique().collect()
    known = crs_ids(crs).with_columns(known=pl.lit(True)).collect()
    return (
        rows.filter(
            pl.col("bi_multi") == _BILATERAL,
            pl.col("id_scheme").is_null(),
            pl.col("climate_usd") > 0,
            pl.col("parent_row_id").is_null(),
        )
        .join(donors, on="provider_code", how="semi")
        .join(known, on=["provider_code", "crs_id"], how="left")
        .filter(pl.col("known").is_null())
        .drop("known")
    )


def _rebuild(rows: pl.DataFrame, *, crs: pl.LazyFrame) -> pl.DataFrame:
    """Child rows for one perspective's aggregate rows."""
    parents = aggregate_rows(rows, crs=crs).with_columns(
        target=pl.col("recipient_code"),
        m=pl.col("rio_mitigation").fill_null(0),
        a=pl.col("rio_adaptation").fill_null(0),
    )
    records = _pool(parents, rows=rows, crs=crs)
    links = _match(parents, records)
    return _children(parents, records, links)


def _pool(
    parents: pl.DataFrame, *, rows: pl.DataFrame, crs: pl.LazyFrame
) -> pl.DataFrame:
    """Bulk records no CRDF row carries, summed, numbered by `record`."""
    carried = rows.select(
        donor_code="provider_code", crs_id="crs_id", year="year"
    ).drop_nulls()
    scope = parents.select(donor_code="provider_code", year="year").unique()
    keys = ["donor_code", "year", *_RECORD_KEY]
    return (
        trace_population(crs)
        .join(scope.lazy(), on=["donor_code", "year"], how="semi")
        .join(carried.lazy(), on=["donor_code", "crs_id", "year"], how="anti")
        .with_columns(
            flow_code=pl.col("flow_code").replace_strict(
                FLOW_CATEGORY, return_dtype=pl.Int32
            ),
            m=pl.col("rio_mitigation").fill_null(0),
            a=pl.col("rio_adaptation").fill_null(0),
        )
        .group_by(keys)
        .agg(
            pl.col("commitment_usd").sum().alias("unit_commitment"),
            pl.col("title").min(),
        )
        .filter(pl.col("unit_commitment") > 0)
        .sort(keys, nulls_last=True)
        .collect()
        .with_row_index("record")
        .rename({"donor_code": "provider_code", "unit_commitment": "commitment_usd"})
        .with_columns(continent=continent(pl.col("recipient_code")))
    )


def _match(parents: pl.DataFrame, records: pl.DataFrame) -> pl.DataFrame:
    """The records each matched row rebuilds from, as `row_id` and `record`."""
    marked = records.filter((pl.col("m") > 0) | (pl.col("a") > 0))
    links: list[pl.DataFrame] = []
    used: set[int] = set()
    cells = marked.partition_by(_CELL, as_dict=True)
    for key, rows in parents.sort("row_id").partition_by(_CELL, as_dict=True).items():
        if key not in cells:
            continue
        cell = _targets(cells[key], codes=set(rows["target"]))
        matched = _partition(rows, cell)
        links.extend(matched)
        used.update(r for link in matched for r in link["record"])
        left = rows.filter(~pl.col("row_id").is_in(_row_ids(matched)))
        total = float(rows["climate_usd"].sum())
        gap = abs(float(cell["commitment_usd"].sum()) - total)
        cell_agrees = gap <= _CELL_TOLERANCE * total
        if left.height and cell_agrees:
            rest = cell.filter(~pl.col("record").is_in(used))["record"]
            links.extend(
                pl.DataFrame({"record": rest}).with_columns(row_id=pl.lit(row_id))
                for row_id in left["row_id"]
            )
            used.update(rest)
    links.append(
        _single_records(
            parents.filter(~pl.col("row_id").is_in(_row_ids(links))),
            records.filter(~pl.col("record").is_in(used)),
        )
    )
    return pl.concat(
        [link.select("row_id", pl.col("record").cast(pl.UInt32)) for link in links]
    )


def _targets(cell: pl.DataFrame, *, codes: set[int]) -> pl.DataFrame:
    """Each record's target row code: its recipient, its continent, or 998."""
    recipient, region = pl.col("recipient_code"), pl.col("continent")
    target = (
        pl.when(recipient.is_in(codes))
        .then(recipient)
        .when(region.is_in(codes))
        .then(region)
        .when(pl.lit(schema.UNSPECIFIED_RECIPIENT in codes))
        .then(pl.lit(schema.UNSPECIFIED_RECIPIENT, pl.Int32))
    )
    return cell.with_columns(target=target, direct=recipient == target)


def _partition(rows: pl.DataFrame, cell: pl.DataFrame) -> list[pl.DataFrame]:
    """Links for the rows of one cell whose parts sum to them within 0.1%."""
    links: list[pl.DataFrame] = []
    for (code,), group in rows.partition_by("target", as_dict=True).items():
        records = cell.filter(pl.col("target") == code)
        parts = [records.filter(direct=d) for d in (False, True)]
        parts = [part for part in parts if part.height]
        amounts = group["climate_usd"].to_list()
        ids = group["row_id"].to_list()
        if len(ids) == 1 and parts:
            assigned = [(ids[0], records)]
        elif len(ids) == len(parts) == 2:
            sums = [part["commitment_usd"].sum() for part in parts]
            order = min(
                ((0, 1), (1, 0)),
                key=lambda p: sum(abs(sums[p[i]] - amounts[i]) for i in range(2)),
            )
            assigned = [(ids[i], parts[order[i]]) for i in range(2)]
        else:
            continue
        for row_id, part in assigned:
            amount = amounts[ids.index(row_id)]
            if abs(part["commitment_usd"].sum() - amount) <= _PART_TOLERANCE * amount:
                links.append(part.select("record").with_columns(row_id=pl.lit(row_id)))
    return links


def _single_records(rows: pl.DataFrame, records: pl.DataFrame) -> pl.DataFrame:
    """Rows paired one to one with the closest record within 1%."""
    gap = (pl.col("commitment_usd") - pl.col("climate_usd")).abs() / pl.col(
        "climate_usd"
    )
    candidates = (
        rows.select("row_id", "provider_code", "year", "target", "climate_usd")
        .join(
            records.select(
                "record",
                "provider_code",
                "year",
                "commitment_usd",
                target="recipient_code",
            ),
            on=["provider_code", "year", "target"],
        )
        .with_columns(gap=gap)
        .filter(pl.col("gap") <= _SINGLE_TOLERANCE)
        .sort("gap", "row_id", "record")
    )
    return pair_one_to_one(candidates, left=["row_id"], right=["record"]).select(
        "row_id", "record"
    )


def _row_ids(links: list[pl.DataFrame]) -> list[str]:
    return [row_id for link in links for row_id in link["row_id"].unique()]


def _children(
    parents: pl.DataFrame, records: pl.DataFrame, links: pl.DataFrame
) -> pl.DataFrame:
    """One child row per link, in the `crdf` schema."""
    renamed = {name: f"record_{name}" for name in records.columns if name != "record"}
    commitment = pl.col("record_commitment_usd")
    share = commitment / commitment.sum().over("row_id")
    record_key = [f"record_{name}" for name in _RECORD_KEY]
    return (
        links.join(records.rename(renamed), on="record")
        .join(parents, on="row_id")
        .with_columns(
            pl.col(_AMOUNTS) * share,
            *(pl.col(f"record_{name}").alias(name) for name in _RECORD_FIELDS),
            recipient_code=pl.col("record_recipient_code"),
            id_scheme=pl.lit(None, pl.String),
            parent_row_id=pl.col("row_id"),
        )
        .with_columns(
            row_id=content_id(["parent_row_id", *record_key], order_by=["record"])
        )
        .select(list(schema.CRDF.columns))
    )

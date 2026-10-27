"""Rewrite the unit ids in review decisions to the units of the current build.

Usage: uv run python scripts/remap_decision_ids.py --decisions DECISIONS
    --old-units UNITS --crs CRS [--dry-run] [--keep-unknown]

DECISIONS is the review-decisions table, UNITS the link stage's `units` of the build
the decisions were made on, and CRS the normalise stage's `crs` table. The script
rebuilds, with the current `build_units`, the units of every line a decision names,
and maps each old unit id to the new unit that holds its CRS records. A unit of one
purpose keeps its id; a line filed under several purposes, once one unit per
purpose, now has one id. Before writing it copies DECISIONS to `<DECISIONS>.bak`.

It writes nothing, and exits 1, when an old unit's records fall in more than one new
unit or in none, when a decision names a unit id that UNITS lacks (unless
`--keep-unknown`, which leaves such ids as they are), when two decisions map to one
sort key, or when the backup already exists.
"""

import argparse
import shutil
import sys
from pathlib import Path

import polars as pl

from climate_finance import schema
from climate_finance.build.link.keys import build_units
from climate_finance.build.normalise.crs import trace_population

# The fields every record of a line shares, which select the CRS records to rebuild.
LINE = ["donor_code", "crs_id", "recipient_code", "year"]
SIDES = ("left_id", "right_id")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--decisions", type=Path, required=True)
    parser.add_argument("--old-units", type=Path, required=True)
    parser.add_argument("--crs", type=Path, required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--keep-unknown", action="store_true")
    args = parser.parse_args()

    decisions = pl.read_parquet(args.decisions)
    schema.validate(schema.REVIEW_DECISIONS, decisions)
    named = pl.concat([decisions[side] for side in SIDES]).drop_nulls().unique()
    old = (
        pl.scan_parquet(args.old_units)
        .filter(pl.col("unit_id").is_in(named.implode()))
        .select("unit_id", "record_ids", *LINE)
        .collect()
    )
    print(f"{decisions.height} decisions name {named.len()} ids, {old.height} units")

    # Every trace record of the lines the decisions name, so the new ids number
    # their identity groups as a full build does.
    lines = old.select(LINE).unique().lazy()
    records = trace_population(pl.scan_parquet(args.crs)).join(
        lines, on=LINE, how="semi", nulls_equal=True
    )
    new = build_units(records).collect()
    holders = new.select(new_id="unit_id", record_id="record_ids").explode("record_id")
    mapping = (
        old.select(old_id="unit_id", record_id="record_ids")
        .explode("record_id")
        .join(holders, on="record_id", how="left")
        .group_by("old_id")
        .agg(pl.col("new_id").drop_nulls().unique())
    )
    if (bad := mapping.filter(pl.col("new_id").list.len() != 1)).height:
        print(f"refused: {bad.height} old units map to no new unit or to several:")
        print(bad)
        return 1
    ids = dict(mapping.with_columns(pl.col("new_id").list.first()).iter_rows())

    # A crdf_crs decision's left id is a CRDF row, which keeps its id.
    unit_sides = {
        "left_id": pl.col("task") == "trace",
        "right_id": pl.col("task").is_in(["trace", "crdf_crs"]),
    }
    unknown = {
        side: decisions.filter(
            is_unit, pl.col(side).is_not_null(), ~pl.col(side).is_in(list(ids))
        )[side].to_list()
        for side, is_unit in unit_sides.items()
    }
    if any(unknown.values()) and not args.keep_unknown:
        print(f"refused: decisions name unit ids the old units lack: {unknown}")
        return 1
    mapped = decisions.with_columns(
        pl.when(is_unit).then(pl.col(side).replace(ids)).otherwise(pl.col(side))
        for side, is_unit in unit_sides.items()
    )
    changed = mapped.with_columns(
        old_left=decisions["left_id"], old_right=decisions["right_id"]
    ).filter(
        (pl.col("left_id") != pl.col("old_left"))
        | pl.col("right_id").ne_missing(pl.col("old_right"))
    )
    print(f"{changed.height} decisions change:")
    with pl.Config(tbl_rows=-1, tbl_cols=-1):
        print(changed.select("task", "decision", *SIDES, "old_left", "old_right"))

    key = list(schema.REVIEW_DECISIONS.sort_key)
    if (repeated := mapped.filter(pl.len().over(key) > 1)).height:
        print(f"refused: {repeated.height} decisions share a sort key once mapped:")
        print(repeated)
        return 1
    schema.validate(schema.REVIEW_DECISIONS, mapped)
    if args.dry_run:
        print("dry run: nothing written")
        return 0
    backup = args.decisions.with_name(args.decisions.name + ".bak")
    if backup.exists():
        print(f"refused: {backup} exists")
        return 1
    shutil.copy2(args.decisions, backup)
    mapped.write_parquet(args.decisions)
    print(f"wrote {args.decisions}; the original is at {backup}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

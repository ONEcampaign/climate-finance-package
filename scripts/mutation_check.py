"""Remove each allocation, status and key-repair rule in turn and name the tests
that fail.

Usage: uv run python scripts/mutation_check.py

Each mutation rewrites one rule in a copy of the package's `build/allocate.py` or
`build/link/key_repair.py`, runs that module's tests against the copy, and lists the
failing tests. Exits 1 when the unmutated copy fails, when the copy is not the
module the tests import, or when removing a rule fails no test.
"""

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
# Rule name -> (text in build/allocate.py, text that removes the rule).
ALLOCATE = {
    "fifo_oldest_first": ("cyear.cast(pl.Int32)", "pl.lit(0, dtype=pl.Int32)"),
    "pro_rata_to_headroom": (
        "* available / block",
        "/ pl.len().over(group_order).cast(pl.Float64)",
    ),
    "face_cap": (".clip(0.0, block)", ".clip(0.0)"),
    "excess": ("allocated_usd=pl.Series(left),", "allocated_usd=pl.lit(0.0),"),
    "excess_cascade": (
        '.join(records.select("record_id", "r_idx", "year"), on="record_id")',
        '.join(records.select("record_id", "r_idx", "year"), on="record_id")'
        '.filter(pl.col("pass") == pl.col("pass").min().over("record_id"))',
    ),
    "later_passes_take_the_remainder": (
        'left[index] = np.maximum(left[index] - taken["taken"].to_numpy(), 0.0)',
        "left[index] = left[index]",
    ),
    "pooling_refused": ("    _check_pooling(unit_pairs)\n", ""),
    "one_group_per_method": (
        'if (bad := split.filter(pl.col("group_id") > 1)).height:',
        "if (bad := split.clear()).height:",
    ),
    "group_of_one_method": (
        'agg(pl.col("method").n_unique())',
        'agg(pl.col("method").n_unique().clip(upper_bound=1))',
    ),
    "record_years_in_order": (
        '.sort("year", "rank", "class_year", nulls_last=True)',
        '.sort(-pl.col("year"), "rank", "class_year", nulls_last=True)',
    ),
    "methods_in_order": (
        "enumerate(LINK_METHODS)",
        "enumerate(reversed(LINK_METHODS))",
    ),
    "floors_first": ("nulls_last=True)", "nulls_last=False)"),
    "parent_exempt_from_floors": (
        'floored = ~pl.col("method").is_in(NO_FLOOR_METHODS)',
        "floored = pl.lit(True)",
    ),
    "dated_own_year_first": (
        "+ pl.when(same_year & (cyear != year)).then(_LATE).otherwise(0)",
        "+ 0",
    ),
    "dated_donors_only": ('pl.col("donor_code").is_in(list(dated))', "pl.lit(True)"),
    "dated_donor_test": (
        'early = (pl.col("commitment_year") < pl.col("year")).fill_null(False)',
        "early = pl.lit(True)",
    ),
    "window_first": ("pl.when(window & ~active)", "pl.when(pl.lit(False))"),
    "window_donors_only": (
        'pl.col("donor_code").is_in(list(windowed))',
        "pl.lit(True)",
    ),
    "window_donor_test": (
        'in_window = (year >= pl.coalesce("start_year", "cyear")) & (\n'
        '        year <= pl.col("end_year") + WINDOW_GRACE\n'
        "    )",
        "in_window = pl.lit(True)",
    ),
    "rows_within_both_faces": (
        'pl.max_horizontal("crs_face", "crdf_face")',
        'pl.col("crs_face")',
    ),
    "aggregate_parents_left_out": (
        'return recipient.join(parents, on="row_id", how="anti")',
        "return recipient",
    ),
    "repeats_to_first_rows": ("    to_rows = _to_first_rows(to_rows, crdf=crdf)\n", ""),
    "first_row_face_cap": (
        ".then(pl.min_horizontal(amount, headroom.fill_null(0.0)))",
        ".then(amount)",
    ),
    "status_over_crs_face": (
        'face = pl.col("crs_face_usd")',
        'face = pl.col("face_usd")',
    ),
    "disbursed_at_ninety_percent": ("ratio >= DISBURSED_RATIO", "ratio >= 1.0"),
    "not_traceable": ('.when(~pl.col("traceable"))', ".when(pl.lit(False))"),
    "neither_class": (' & pl.col("neither").is_null()', ""),
    "pending_review": ('pl.when(pl.col("pending"))', "pl.when(pl.lit(False))"),
    "project_disbursing": ('& pl.col("id_disbursing"),', "& pl.lit(False),"),
    "earlier_tranches_only": (
        'pl.col("first_disbursing") < pl.col("placed_year")',
        'pl.col("first_disbursing") <= pl.col("placed_year")',
    ),
    "cancellations_reported": (
        'crs.filter(pl.col("commitment_usd") < 0)',
        'crs.filter(pl.col("commitment_usd") != 0)',
    ),
    "cumulative_by_years_since": (
        '.fill_null(0.0)\n            .cum_sum()\n            .over("row_id")',
        ".fill_null(0.0)",
    ),
}

# Rule name -> (text in build/link/key_repair.py, text that removes the rule).
KEY_REPAIR = {
    "rule_new_crs_id": (
        '"repair_new_crs_id": lambda d, c: _candidates(d, c, on=_PN, key=_PN),',
        '"repair_new_crs_id": lambda d, c: _candidates(d, c, on=_PN, key=_PN).clear(),',
    ),
    "rule_pn_variant": (
        "_candidates(d, c, on=_CORE, key=_CORE),",
        "_candidates(d, c, on=_CORE, key=_CORE).clear(),",
    ),
    "rule_wb_parent": (
        "_candidates(d, c, on=_LOAN, key=_LOAN),",
        "_candidates(d, c, on=_LOAN, key=_LOAN).clear(),",
    ),
    "rule_afdb_scientific": (
        '"repair_afdb_scientific": _rounded_numbers,',
        '"repair_afdb_scientific": lambda d, c: _rounded_numbers(d, c).clear(),',
    ),
    "rule_title_recipient": (
        "d, c.join(unique_titles, on=_TITLE_KEY)\n        ),",
        "d, c.join(unique_titles, on=_TITLE_KEY)\n        ).clear(),",
    ),
    "first_year_floor": (
        'pl.col("floor_year").is_null() | (pl.col("floor_year") >= first_year),',
        "True,",
    ),
    "reached_commitments_left": (
        'links.select(unit_id="right_id"), on="unit_id", how="anti"',
        'links.select(unit_id="right_id").clear(), on="unit_id", how="anti"',
    ),
    "rules_take_commitments": (
        'pairs.select(unit_id="right_id"), on="unit_id", how="anti"',
        'pairs.select(unit_id="right_id").clear(), on="unit_id", how="anti"',
    ),
    "own_crs_id_left": (
        'return _whole(pairs, pl.col("crs_id").ne_missing(pl.col("crs_id_c")))',
        "return pairs",
    ),
    "title_min_chars": (
        ".str.len_chars() >= TITLE_MIN_CHARS)",
        ".str.len_chars() >= 0)",
    ),
    "unique_title_commitments": ('.filter(pl.col("ids") == 1)', ""),
    "record_year": ('            pl.col("placed_year_c") <= pl.col("year"),\n', ""),
    "floor": (
        '            pl.col("floor_year").is_null()\n'
        '            | (pl.col("placed_year_c") >= pl.col("floor_year")),\n',
        "",
    ),
    "rounded_length": ('        full.str.len_chars() == pl.col("digits"),\n', ""),
    "rounded_title_choice": (
        'pl.col("title_ratio") >= TITLE_AGREES\n    ).filter',
        "pl.lit(True)\n    ).filter",
    ),
    "rounded_group_by_full_number": (
        'key=(*_PLACE, "project_number_c"),',
        'key=(*_PLACE, "prefix", "digits"),',
    ),
    "agreement": (
        'agrees = (pl.col("title_ratio") >= TITLE_AGREES) | (',
        "agrees = pl.lit(True) | (",
    ),
    "whole_group": (
        'return pairs.filter(condition.all().over("left_id", "key"))',
        "return pairs.filter(condition)",
    ),
    "one_group": ('            pl.col("key").n_unique().over("left_id") == 1,\n', ""),
    "max_commitments": (
        'pl.col("right_id").n_unique().over("left_id") <= MAX_COMMITMENTS,',
        "pl.lit(True),",
    ),
}

# Module under src -> (its tests, its mutations).
TARGETS = {
    Path("climate_finance/build/allocate.py"): ("tests/test_allocate.py", ALLOCATE),
    Path("climate_finance/build/link/key_repair.py"): (
        "tests/test_key_repair.py",
        KEY_REPAIR,
    ),
}


def _pytest(src: Path, tests: str) -> tuple[int, list[str]]:
    """Run `tests` against the package under `src`."""
    env = {**os.environ, "PYTHONPATH": str(src)}
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", tests],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    failed = [
        line.split("::", 1)[1].split(" ", 1)[0]
        for line in result.stdout.splitlines()
        if line.startswith("FAILED ")
    ]
    return result.returncode, failed


def _imported_from(src: Path, module: Path) -> Path:
    env = {**os.environ, "PYTHONPATH": str(src)}
    name = ".".join(module.with_suffix("").parts)
    result = subprocess.run(
        [sys.executable, "-c", f"import {name} as m; print(m.__file__)"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    return Path(result.stdout.strip()).resolve()


def main() -> int:
    survivors = []
    total = 0
    with tempfile.TemporaryDirectory() as scratch:
        src = Path(scratch).resolve()
        shutil.copytree(
            ROOT / "src" / "climate_finance",
            src / "climate_finance",
            ignore=shutil.ignore_patterns("__pycache__"),
        )
        for module, (tests, mutations) in TARGETS.items():
            target = src / module
            if _imported_from(src, module) != target:
                print(f"The tests do not import {target}; nothing would be mutated.")
                return 1
            code, failed = _pytest(src, tests)
            if code != 0:
                print(f"The unmutated {module} fails: {failed}")
                return 1
            original = target.read_text()
            for name, (text, replacement) in mutations.items():
                if (count := original.count(text)) != 1:
                    print(
                        f"{name}: expected one match of the rule's text, found {count}"
                    )
                    return 1
                target.write_text(original.replace(text, replacement))
                _, failed = _pytest(src, tests)
                print(f"{name}: {len(failed)} failed: {failed}")
                if not failed:
                    survivors.append(name)
            target.write_text(original)
            total += len(mutations)

    if survivors:
        print(f"No test fails without: {survivors}")
        return 1
    print(f"Every one of {total} rules has a failing test.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

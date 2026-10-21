"""Remove each allocation and status rule in turn and name the tests that fail.

Usage: uv run python scripts/mutation_check.py

Each mutation rewrites one rule in a copy of the package's `build/allocate.py`, runs
`tests/test_allocate.py` against the copy, and lists the failing tests. Exits 1 when
the unmutated copy fails, when the copy is not the module the tests import, or when
removing a rule fails no test.
"""

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODULE = Path("climate_finance/build/allocate.py")
TESTS = "tests/test_allocate.py"

# Rule name -> (text in allocate.py, text that removes the rule).
MUTATIONS = {
    "fifo_oldest_first": ("cyear.cast(pl.Int32)", "pl.lit(0, dtype=pl.Int32)"),
    "pro_rata_to_headroom": (
        "* available / block",
        "/ pl.len().over(group_order).cast(pl.Float64)",
    ),
    "face_cap": (".clip(0.0, block)", ".clip(0.0)"),
    "excess": (
        'allocated_usd=(pl.col("total") - pl.col("capacity")).clip(lower_bound=0.0)',
        "allocated_usd=pl.lit(0.0)",
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


def _pytest(src: Path) -> tuple[int, list[str]]:
    """Run the allocation tests against the package under `src`."""
    env = {**os.environ, "PYTHONPATH": str(src)}
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", TESTS],
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


def _imported_from(src: Path) -> Path:
    env = {**os.environ, "PYTHONPATH": str(src)}
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import climate_finance.build.allocate as m; print(m.__file__)",
        ],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    return Path(result.stdout.strip()).resolve()


def main() -> int:
    original = (ROOT / "src" / MODULE).read_text()
    with tempfile.TemporaryDirectory() as scratch:
        src = Path(scratch).resolve()
        shutil.copytree(
            ROOT / "src" / "climate_finance",
            src / "climate_finance",
            ignore=shutil.ignore_patterns("__pycache__"),
        )
        target = src / MODULE
        if _imported_from(src) != target:
            print(f"The tests do not import {target}; nothing would be mutated.")
            return 1
        code, failed = _pytest(src)
        if code != 0:
            print(f"The unmutated module fails: {failed}")
            return 1

        survivors = []
        for name, (text, replacement) in MUTATIONS.items():
            if (count := original.count(text)) != 1:
                print(f"{name}: expected one match of the rule's text, found {count}")
                return 1
            target.write_text(original.replace(text, replacement))
            _, failed = _pytest(src)
            print(f"{name}: {len(failed)} failed: {failed}")
            if not failed:
                survivors.append(name)
        target.write_text(original)

    if survivors:
        print(f"No test fails without: {survivors}")
        return 1
    print(f"Every one of {len(MUTATIONS)} rules has a failing test.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

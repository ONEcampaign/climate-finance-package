"""Remove each allocation, status, key-tier and key-repair rule in turn and name the
tests that fail.

Usage: uv run python scripts/mutation_check.py

Each mutation rewrites one rule in a copy of the package's `build/allocate.py`,
`build/link/keys.py` or `build/link/key_repair.py`, runs the tests that exercise that
module against the copy, and lists the failing tests. Exits 1 when `stale_targets`
(which `tests/test_mutation_targets.py` runs in the test suite) finds a rule whose
text does not occur exactly once, a removal that does not parse, a module with no
mutations or a missing test file. It also exits 1 when the unmutated copy fails, when
the copy is not the module the tests import, or when removing a rule fails no test.
"""

import argparse
import ast
import os
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Mapping
from functools import cache
from pathlib import Path
from typing import NamedTuple

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
    "pooling_refused": ("    _check_pooling(\n", "    (\n"),
    "own_record_certain": (
        'certain=pl.col("certain") | (pl.col("record_id") == pl.col("c_record"))',
        'certain=pl.col("certain")',
    ),
    "own_commitments_first": (
        "own = _own_commitments(pairs, commitments=commitments, crs=crs)",
        'own = pairs.clear().select("group_id", "record_id", "c_idx")',
    ),
    "own_by_date_and_title": (
        "matched = _same_date_and_title(rest, facts=facts, crs=crs).select(columns)",
        "matched = rest.clear().select(columns)",
    ),
    "own_world_bank_line": (
        "lines = _own_lines(unmatched, facts=facts).select(columns)",
        "lines = unmatched.clear().select(columns)",
    ),
    "one_group_per_method": (
        'if (bad := split.filter(pl.col("group_id") > 1)).height:',
        "if (bad := split.clear()).height:",
    ),
    "group_of_one_method": (
        'agg(pl.col("method").n_unique())',
        'agg(pl.col("method").n_unique().clip(upper_bound=1))',
    ),
    "record_years_in_order": (
        ".sort(key, nulls_last=True)",
        '.sort(key, descending=[name == "year" for name in key], nulls_last=True)',
    ),
    "split_parts_in_order": (
        'key = ["split", "estimated", *by]',
        'key = ["estimated", *by]',
    ),
    "certain_groups_first": (
        'key = ["split", "estimated", *by]',
        'key = ["split", *by]',
    ),
    "methods_in_order": (
        "enumerate(LINK_METHODS)",
        "enumerate(reversed(LINK_METHODS))",
    ),
    "floors_first": ("nulls_last=True)", "nulls_last=False)"),
    "parent_exempt_from_floors": (
        "    floored = ~(\n"
        '        pl.col("method").is_in(NO_FLOOR_METHODS)\n'
        '        & (pl.col("donor_code") == EU_INSTITUTIONS)\n'
        "    )",
        "    floored = pl.lit(True)",
    ),
    "parent_exempt_for_the_eu_only": (
        '        & (pl.col("donor_code") == EU_INSTITUTIONS)\n',
        "",
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
    "repeat_in_its_first_rows_group_takes_no_weight": (
        'pl.when(pl.col("first_in_group"))',
        "pl.when(pl.lit(False))",
    ),
    "first_row_face_cap": (
        ".then(pl.min_horizontal(amount, headroom.fill_null(0.0)))",
        ".then(amount)",
    ),
    "status_over_crs_face": (
        'face = pl.col("crs_face_usd")',
        'face = pl.col("face_usd")',
    ),
    "status_keeps_crs_face": ('crs_face_usd=pl.col("crs_face_usd"),', ""),
    "status_sums_ordered": (
        'disbursed_usd=ordered_sum(pl.col("allocated_usd")),\n'
        '            disbursed_low_usd=ordered_sum(pl.col("allocated_low_usd")),\n'
        '            disbursed_high_usd=ordered_sum(pl.col("allocated_high_usd")),',
        'disbursed_usd=pl.col("allocated_usd").sum(),\n'
        '            disbursed_low_usd=pl.col("allocated_low_usd").sum(),\n'
        '            disbursed_high_usd=pl.col("allocated_high_usd").sum(),',
    ),
    "disbursed_at_ninety_percent": ("ratio >= DISBURSED_RATIO", "ratio >= 1.0"),
    "not_traceable": ('.when(~pl.col("traceable"))', ".when(pl.lit(False))"),
    "untraceable_agency": ('& pl.col("untraced").is_null()', ""),
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

# Rule name -> (text in build/link/keys.py, text that removes the rule).
KEYS = {
    "first_tier_wins": (
        "        remaining = remaining.join(\n"
        '            tier_links.select(unit_id="left_id"), on="unit_id", how="anti"\n'
        "        )\n",
        "",
    ),
    "record_year": ('(placed_year <= pl.col("year")) & ', ""),
    "floor": (" & (floor.is_null() | (placed_year >= floor))", ""),
    "tier_floor_seen": (
        'floor_seen(pl.col("placed_year"), first_year=first_year, over=["unit_id"])',
        "pl.lit(True)",
    ),
    "own_crs_id_passes_unheld_floor": (
        'floor = pl.when(pl.col("floor_held")).then(floor)',
        "floor = floor",
    ),
    "eu_parent_floor_exempt": (
        'pl.when(pl.col("donor_code") != schema.EU_INSTITUTIONS)',
        "pl.when(pl.lit(True))",
    ),
    "crs_id_purpose_tier": (
        '("donor_code", "crs_id", "purpose_code", "recipient_code"),',
        '("donor_code", "crs_key"),',
    ),
    "crs_key_recipient": (
        '.then(pl.concat_str("crs_id", "recipient_code", separator="|"))',
        '.then(pl.col("crs_id"))',
    ),
    "parent_same_place": (".then(lettered)", ".then(crs_id)"),
    "pn_date_tier_one_unit": (
        'Tier("pn_recipient_date", (*AWARD_KEY, "line_date"), unique_by="unit_id"),',
        'Tier("pn_recipient_date", (*AWARD_KEY, "line_date"), unique_by="crs_id"),',
    ),
    "line_date": (
        "return pl.when(~placeholder).then(commitment_date)",
        "return pl.when(pl.lit(False)).then(commitment_date)",
    ),
    "pn_purpose_tier": (
        '(*AWARD_KEY, "purpose_code"), unique_by="crs_id"',
        'AWARD_KEY, unique_by="crs_id"',
    ),
    "pn_unique_crs_id": ("pl.col(by).n_unique().over(key) == 1", "pl.lit(True)"),
    "pn_key_keeps_merged_separators": (
        '            .then(pl.col("project_number"))\n'
        '            .otherwise(pl.col("pn_key"))',
        '            .then(pl.col("pn_key"))\n            .otherwise(pl.col("pn_key"))',
    ),
    "component_awards": (
        '.then(pl.concat_str(number, "purpose_code", separator="|"))',
        ".then(pl.col(number))",
    ),
    "components_disburse_uncommitted": (
        '.join(uncommitted, on=award, how="semi")',
        "",
    ),
    "split_line_unreached_no_pick": (
        'pick=pl.when("reached").then("unit_id"),',
        'pick="unit_id",',
    ),
    "split_line_own_purpose": ('.then("own_pick")', '.then("default_pick")'),
    "split_line_purpose_category": (
        'pl.coalesce("category_pick", "default_pick")',
        'pl.col("default_pick")',
    ),
    "wb_loan_tier": (
        'project_number.str.split(".").list.get(1, null_on_oob=True)',
        "pl.lit(None, dtype=pl.String)",
    ),
    "wb_loan_world_bank_only": (
        "donor_code.is_in(WORLD_BANK_DONORS)",
        "pl.lit(True)",
    ),
}

# Rule name -> (text in build/link/key_repair.py, text that removes the rule).
KEY_REPAIR = {
    "rule_new_crs_id": (
        "            d, c, key=_PN, programmes=programmes[_PN]\n        ),",
        "            d, c, key=_PN, programmes=programmes[_PN]\n        ).clear(),",
    ),
    "rule_pn_variant": (
        "            d, c, key=_CORE, programmes=programmes[_CORE]\n        ),",
        "            d, c, key=_CORE, programmes=programmes[_CORE]\n        ).clear(),",
    ),
    "rule_afdb_scientific": (
        '"repair_afdb_scientific": _rounded_numbers,',
        '"repair_afdb_scientific": lambda d, c: _rounded_numbers(d, c).clear(),',
    ),
    "rule_title_recipient": (
        "d, c.join(unique_titles, on=_TITLE_KEY)\n        ),",
        "d, c.join(unique_titles, on=_TITLE_KEY)\n        ).clear(),",
    ),
    "rule_stray_country": (
        'agreements = {"repair_stray_country": _AGREES | pl.col("records_agree")}',
        'agreements = {"repair_stray_country": pl.lit(False)}',
    ),
    "stray_records_agree": ('_AGREES | pl.col("records_agree")', "_AGREES"),
    "stray_country_unreached_only": (
        '~pl.col("unit_id").is_in(reached.implode())',
        "pl.lit(True)",
    ),
    "first_year_floor": (
        "rule(disbursements, commitments).filter(seen),",
        "rule(disbursements, commitments),",
    ),
    "reached_commitments_left": (
        'commitments = committed.join(closed, on="unit_id", how="anti")',
        "commitments = committed",
    ),
    "spare_lines_stay_open": (
        '    closed = links.select(unit_id="right_id").join(\n'
        '        _spare_lines(facts, links=links), on="unit_id", how="anti"\n'
        "    )",
        '    closed = links.select(unit_id="right_id")',
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
        '_AGREES = (pl.col("title_ratio") >= TITLE_AGREES) | (',
        "_AGREES = pl.lit(True) | (",
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

type Mutations = Mapping[str, tuple[str, str]]
type Targets = Mapping[Path, tuple[tuple[str, ...], Mutations]]

# Module under src -> (the tests that exercise it, its mutations). The review tests
# hold the only split groups, so they test the split-part order of allocation.
TARGETS: Targets = {
    Path("climate_finance/build/allocate.py"): (
        ("tests/test_allocate.py", "tests/test_review.py"),
        ALLOCATE,
    ),
    Path("climate_finance/build/link/keys.py"): (
        ("tests/test_link.py", "tests/test_key_repair.py"),
        KEYS,
    ),
    Path("climate_finance/build/link/key_repair.py"): (
        ("tests/test_key_repair.py",),
        KEY_REPAIR,
    ),
}


class Problem(NamedTuple):
    """A target the mutation run cannot use. `subject` is the mutation, module or
    test file, and `code` one of `not_one_match`, `unparsable`, `unchanged`,
    `no_mutations` and `missing_tests`."""

    subject: str
    code: str
    detail: str = ""


def stale_targets(targets: Targets = TARGETS, *, root: Path = ROOT) -> list[Problem]:
    """Each mutation whose text does not occur exactly once in its module, or whose
    replacement leaves code that does not parse or parses as the original, each
    module with no mutations, and each listed test file missing under `root`."""
    problems = []
    for module, (tests, mutations) in targets.items():
        problems.extend(
            Problem(test, "missing_tests", str(module))
            for test in tests
            if not (root / test).is_file()
        )
        if not mutations:
            problems.append(Problem(module.as_posix(), "no_mutations"))
        path = root / "src" / module
        for name, (text, replacement) in mutations.items():
            if found := _mutation_problem(path, text, replacement):
                problems.append(Problem(name, *found))
    return problems


@cache
def _parsed(path: Path) -> tuple[str, str]:
    """The source of `path` and the dump of its AST, read once per process."""
    source = path.read_text(encoding="utf-8")
    return source, ast.dump(ast.parse(source))


@cache
def _mutation_problem(
    path: Path, text: str, replacement: str
) -> tuple[str, str] | None:
    """The `Problem` code and detail of one mutation of `path`, or None."""
    original, tree = _parsed(path)
    if (count := original.count(text)) != 1:
        return "not_one_match", f"{count} matches in {path.name}"
    try:
        mutated = ast.dump(ast.parse(original.replace(text, replacement)))
    except SyntaxError as error:
        return "unparsable", str(error)
    if mutated == tree:
        return "unchanged", path.name
    return None


def _pytest(src: Path, tests: tuple[str, ...]) -> tuple[int, list[str]]:
    """Run `tests` against the package under `src`.

    Polars runs on one thread. The tests hold small frames, so a full thread pool
    makes a run several times slower on a busy machine, and one thread runs each
    test the same way on every run."""
    env = {**os.environ, "PYTHONPATH": str(src), "POLARS_MAX_THREADS": "1"}
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", *tests],
        cwd=ROOT,
        env=env,
        capture_output=True,
        encoding="utf-8",
        check=False,
    )
    failed = [
        line.split(" ", 2)[1]
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
        encoding="utf-8",
        check=True,
    )
    return Path(result.stdout.strip()).resolve()


def main() -> int:
    argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0]).parse_args()
    if problems := stale_targets():
        for problem in problems:
            print(f"{problem.subject}: {problem.code} {problem.detail}")
        return 1
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
            original = target.read_text(encoding="utf-8")
            for name, (text, replacement) in mutations.items():
                target.write_text(original.replace(text, replacement), encoding="utf-8")
                _, failed = _pytest(src, tests)
                print(f"{name}: {len(failed)} failed: {failed}", flush=True)
                if not failed:
                    survivors.append(name)
            target.write_text(original, encoding="utf-8")
            total += len(mutations)

    if survivors:
        print(f"No test fails without: {survivors}")
        return 1
    print(f"Every one of {total} rules has a failing test.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

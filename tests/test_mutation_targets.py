"""The rules `scripts/mutation_check.py` removes still occur in their modules, and
the tests it runs on them exist."""

import importlib.util
from collections.abc import Callable, Mapping
from pathlib import Path
from types import ModuleType

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "mutation_check.py"
ALLOCATE = Path("climate_finance/build/allocate.py")
KEYS = Path("climate_finance/build/link/keys.py")
KEY_REPAIR = Path("climate_finance/build/link/key_repair.py")


def _script() -> ModuleType:
    spec = importlib.util.spec_from_file_location("mutation_check", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    script = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(script)
    return script


script = _script()


def test_targets_hold_the_mutated_modules() -> None:
    assert set(script.TARGETS) == {ALLOCATE, KEYS, KEY_REPAIR}


type Targets = dict[Path, tuple[tuple[str, ...], Mapping[str, tuple[str, str]]]]


def _replace(
    module: Path,
    *,
    tests: tuple[str, ...] | None = None,
    mutations: Mapping[str, tuple[str, str]] | None = None,
) -> Callable[[Targets], Targets]:
    """A change to `TARGETS` that swaps `module`'s tests or mutations."""

    def change(targets: Targets) -> Targets:
        old_tests, old_mutations = targets[module]
        return {
            **targets,
            module: (
                old_tests if tests is None else tests,
                old_mutations if mutations is None else mutations,
            ),
        }

    return change


@pytest.mark.parametrize(
    ("change", "problems"),
    [
        (lambda targets: targets, []),
        (
            _replace(ALLOCATE, tests=("tests/test_allocate.py", "tests/renamed.py")),
            [("tests/renamed.py", "missing_tests")],
        ),
        (_replace(KEYS, mutations={}), [(KEYS.as_posix(), "no_mutations")]),
        (
            _replace(KEY_REPAIR, mutations={"gone": ("no such rule", "")}),
            [("gone", "not_one_match")],
        ),
        (
            _replace(
                KEY_REPAIR,
                mutations={"bad": ("MAX_COMMITMENTS = 5", "MAX_COMMITMENTS =")},
            ),
            [("bad", "unparsable")],
        ),
        (
            _replace(
                KEY_REPAIR,
                mutations={"same": ("MAX_COMMITMENTS = 5", "MAX_COMMITMENTS  =  5")},
            ),
            [("same", "unchanged")],
        ),
    ],
)
def test_every_mutation_target_is_usable(
    change: Callable[[Targets], Targets], problems: list[tuple[str, str]]
) -> None:
    found = script.stale_targets(change(dict(script.TARGETS)))
    assert [(problem.subject, problem.code) for problem in found] == problems

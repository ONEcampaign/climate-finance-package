"""The README and `docs/` pages describe the code as it is.

Each ```python block runs against the fixture release, in order within its page and in
one namespace, and an ```output block right after it holds the exact stdout. A block
fenced ```python skip is shown but not run. Every `cf-build` command in a ```bash
block names a real command and real options, the release-table reference names every
column, and the Python reference names every `Spec` and `Method` field.
"""

import contextlib
import io
import re
import shlex
import shutil
import textwrap
from collections.abc import Iterator
from pathlib import Path
from typing import NamedTuple

import pytest
from make_fixture import FIXTURE_DIR

from climate_finance import snapshot
from climate_finance.methods import Method
from climate_finance.schema import RELEASE_TABLES
from climate_finance.snapshot import IndexEntry, ReleaseIndex, release_dirname
from climate_finance.spec import Spec

ROOT = Path(__file__).parent.parent
PAGES = sorted([ROOT / "README.md", *(ROOT / "docs").rglob("*.md")])
FENCE = re.compile(
    r"^(?P<indent> *)```(?P<info>[^\n]*)\n(?P<code>.*?)^(?P=indent)```$",
    re.MULTILINE | re.DOTALL,
)


class Block(NamedTuple):
    info: str
    code: str
    line: int


def blocks(page: Path) -> list[Block]:
    text = page.read_text()
    return [
        Block(
            match["info"].strip(),
            textwrap.dedent(match["code"]),
            text[: match.start()].count("\n") + 2,
        )
        for match in FENCE.finditer(text)
    ]


@pytest.fixture
def fixture_as_latest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """`cf.load()` with no arguments opens the fixture release from a cache directory,
    through the loader's own cache path."""
    cache = tmp_path / "cache"
    shutil.copytree(FIXTURE_DIR, cache / release_dirname("fixture"))
    index = ReleaseIndex(
        latest="fixture",
        releases={"fixture": IndexEntry(url="https://example.org/pack", sha256="0")},
    )
    monkeypatch.setattr(snapshot, "_shipped_index", lambda: index)
    monkeypatch.setenv("CLIMATE_FINANCE_CACHE", str(cache))
    work = tmp_path / "work"
    work.mkdir()
    monkeypatch.chdir(work)
    return work


@pytest.mark.parametrize("page", PAGES, ids=lambda page: str(page.relative_to(ROOT)))
@pytest.mark.usefixtures("fixture_as_latest")
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_python_blocks_run_and_print_what_the_page_shows(page: Path) -> None:
    found = blocks(page)
    namespace: dict[str, object] = {"__name__": "__docs__"}
    for index, block in enumerate(found):
        if block.info != "python":
            continue
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            exec(compile(block.code, f"{page.name}:{block.line}", "exec"), namespace)
        following = found[index + 1] if index + 1 < len(found) else None
        if following and following.info == "output":
            assert out.getvalue().strip() == following.code.strip(), (
                f"{page.name}:{block.line}"
            )


def commands(page: Path) -> Iterator[tuple[int, list[str]]]:
    """Each `cf-build` invocation in the page's bash blocks, continuations joined."""
    for block in blocks(page):
        if block.info != "bash":
            continue
        for line in block.code.replace("\\\n", " ").splitlines():
            words = shlex.split(line, comments=True)
            if words[:1] == ["cf-build"]:
                yield block.line, words[1:]


COMMAND_PAGES = [page for page in PAGES if list(commands(page))]


@pytest.mark.parametrize(
    "page", COMMAND_PAGES, ids=lambda page: str(page.relative_to(ROOT))
)
def test_cf_build_examples_use_real_commands_and_options(page: Path) -> None:
    import typer.main

    from climate_finance.build.cli import app

    group = typer.main.get_command(app)
    for line, words in commands(page):
        name = words[0]
        if name.startswith("-"):
            continue
        assert name in group.commands, f"{page.name}:{line} runs `cf-build {name}`"
        options = {
            option
            for param in group.commands[name].params
            for option in (*param.opts, *param.secondary_opts)
        }
        used = {
            word.split("=")[0]
            for word in (word.strip("[]") for word in words[1:])
            if word.startswith("--")
        }
        assert used <= options, f"{page.name}:{line} uses {sorted(used - options)}"


def test_release_table_reference_names_every_table_and_column() -> None:
    text = (ROOT / "docs" / "reference" / "release-tables.md").read_text()
    missing = [
        f"{name}.{column}" if column else name
        for name, table in RELEASE_TABLES.items()
        for column in (None, *table.columns)
        if f"`{column or name}`" not in text
    ]
    assert not missing


def test_python_reference_names_every_spec_and_method_field() -> None:
    text = (ROOT / "docs" / "reference" / "python-api.md").read_text()
    missing = [
        field
        for field in (*Spec.model_fields, *Method.model_fields)
        if f"`{field}`" not in text
    ]
    assert not missing

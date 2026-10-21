"""The base install imports no build dependency and no pandas.

CI runs this file in a venv holding only the base package, with CF_BASE_INSTALL=1,
which makes the base-only checks mandatory there instead of skipped.
"""

import os
import subprocess
import sys
from importlib.util import find_spec
from pathlib import Path

import pytest

BUILD_ONLY = (
    "climate_finance.build",
    "typer",
    "oda_reader",
    "splink",
    "duckdb",
    "sklearn",
    "openpyxl",
    "resolvekit",
)
COMPAT_ONLY = ("pandas",)
# pydeflate imports pandas, so only a currency or price conversion may load it.
CONVERSION_ONLY = ("pydeflate",)
FIXTURE = Path(__file__).parent / "fixtures" / "snapshot"
BASE_INSTALL = os.environ.get("CF_BASE_INSTALL") == "1"


def _run(code: str) -> subprocess.CompletedProcess[str]:
    """Run `code` in a fresh interpreter so this process's imports cannot leak in."""
    return subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=False
    )


def test_importing_the_package_loads_no_build_or_compat_module() -> None:
    result = _run(
        "import sys, climate_finance\n"
        f"print(sorted(m for m in {BUILD_ONLY + COMPAT_ONLY!r} if m in sys.modules))"
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "[]"


def test_a_usd_query_loads_no_build_compat_or_conversion_module() -> None:
    result = _run(
        "import sys\n"
        "from pathlib import Path\n"
        "import climate_finance as cf\n"
        f"result = cf.load(path=Path({str(FIXTURE)!r})).spending()\n"
        "assert result.table.to_polars().height\n"
        "assert result.rows().height\n"
        f"loaded = {BUILD_ONLY + COMPAT_ONLY + CONVERSION_ONLY!r}\n"
        "print(sorted(m for m in loaded if m in sys.modules))"
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "[]"


def test_the_build_extra_is_absent_in_a_base_install() -> None:
    if not BASE_INSTALL:
        pytest.skip("checked only in the base-install job (CF_BASE_INSTALL=1)")

    assert find_spec("typer") is None


def test_importing_the_build_without_its_extra_names_the_extra() -> None:
    if not BASE_INSTALL:
        pytest.skip("checked only in the base-install job (CF_BASE_INSTALL=1)")

    result = _run("import climate_finance.build")

    assert result.returncode != 0
    assert "ImportError" in result.stderr
    assert "[build]" in result.stderr

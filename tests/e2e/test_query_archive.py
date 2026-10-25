"""The archive release, queried in a venv holding only the base package, with
sockets blocked: Germany under ONE, OECD and RANGE, its 2022 rows, a comparison,
quality and the disbursement profile.
"""

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.slow

REPO = Path(__file__).parents[2]
QUERIES = Path(__file__).parent / "base_only_queries.py"
# Cumulative share of traceable climate value disbursed, by years since commitment.
# Year 8 pools the 2000 to 2016 vintages; the 2015 and 2016 vintages alone give 0.70.
PROFILE_TARGET = {0: 0.23, 3: 0.46, 8: 0.66}
PROFILE_TOLERANCE = 0.02


@pytest.fixture(scope="module")
def base_python(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A venv with the package installed without extras, from uv's cache."""
    uv = shutil.which("uv")
    assert uv, "the base-only check needs uv on PATH"
    venv = tmp_path_factory.mktemp("base") / "venv"
    python = venv / "bin" / "python"
    subprocess.run([uv, "venv", "--python", sys.executable, venv], check=True)
    # Editable, so the venv runs this checkout's source and no cached build of it.
    subprocess.run(
        [uv, "pip", "install", "--offline", "--python", python, "-e", REPO],
        check=True,
    )
    return python


@pytest.fixture(scope="module")
def answers(base_python: Path, release: Path) -> dict:
    run = subprocess.run(
        [base_python, QUERIES, release], capture_output=True, text=True, check=False
    )
    assert run.returncode == 0, run.stderr
    return json.loads(run.stdout)


def test_the_venv_holds_no_build_dependency(answers: dict) -> None:
    assert answers["build_modules"] == []


def test_germany_spans_one_to_oecd_under_range(answers: dict) -> None:
    totals = answers["totals"]
    for name, methods in totals.items():
        for method, amounts in methods.items():
            print(
                f"{name} {method}: "
                + ", ".join(
                    f"{key} {amount / 1e9:.2f}" for key, amount in amounts.items()
                )
            )
    one = totals["ONE"]["ONE"]["value"]
    oecd = totals["OECD"]["OECD"]["value"]

    assert 0 < one < oecd
    assert totals["RANGE"]["ONE"]["value"] == pytest.approx(one)
    # A method's own low counts only what certain links carried, so the range's low
    # is ONE's low, below ONE's value.
    assert totals["RANGE"]["OECD"]["low"] == pytest.approx(totals["ONE"]["ONE"]["low"])
    assert totals["RANGE"]["OECD"]["low"] < one
    assert totals["RANGE"]["OECD"]["high"] == pytest.approx(oecd)
    assert answers["compare"] == pytest.approx({"ONE": one, "OECD": oecd})


def test_the_2022_rows_leave_out_repeats(answers: dict) -> None:
    rows = answers["rows_2022"]

    assert rows["rows"] > 0
    assert rows["years"] == [2022]
    assert rows["repeats"] == 0


def test_quality_reports_germany(answers: dict) -> None:
    (germany,) = answers["quality"]

    assert germany["provider_code"] == 5
    assert germany["provider"] == "Germany"
    assert 0 < germany["link_rate"] <= 1


def test_the_disbursement_profile_meets_its_targets(answers: dict) -> None:
    profile = {int(year): share for year, share in answers["profile"].items()}
    print(profile)

    for year, target in PROFILE_TARGET.items():
        assert profile[year] == pytest.approx(target, abs=PROFILE_TOLERANCE)

"""A type checker accepts every documented call of `spending()`, `Spec()` and `run()`,
and flags a value outside a field's choices.

The script runs through `ty`, the type checker the project uses, against this
environment's installed package.
"""

import re
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = """\
import climate_finance as cf

release = cf.load()
release.spending(providers="Japan", years=2022)
release.spending(
    providers=["Germany", "EIB", 701],
    recipients="Kenya",
    years=range(2015, 2023),
    method="OECD",
    flow="commitments",
    view="provider",
    currency="EUR",
    prices=2022,
    oda_only=True,
    measure="share_of_total",
    untraced=False,
)
release.spending(providers=cf.Agency(provider=918, agency=3), recipients=[248, "KEN"])
release.spending(method=cf.methods.RANGE)
release.spending(method=["ONE", cf.methods.OECD])
spec = cf.Spec(providers="Japan", years=[2021, 2022], method="oecd")
release.run(spec)
release.run(spec.replace(years=2023))
release.run(cf.Spec.model_validate_json(spec.model_dump_json()))
years: tuple[int, ...] | None = spec.years
release.spending(flow="commitment")  # flagged
cf.Spec(flow="commitment")  # flagged
release.spending(years="2022")  # flagged
release.spending(spec)  # flagged
"""


def test_documented_calls_type_check_and_bad_values_are_flagged(
    tmp_path: Path,
) -> None:
    pytest.importorskip("ty")
    script = tmp_path / "user.py"
    script.write_text(SCRIPT)

    command = [sys.executable, "-m", "ty", "check", "--python", sys.prefix]
    checked = subprocess.run(
        [*command, "--output-format", "concise", str(script)],
        capture_output=True,
        text=True,
        check=False,
    )

    errors = re.findall(r"user\.py:(\d+):\d+: error", checked.stdout)
    flagged = {int(line) for line in errors}
    expected = {
        number
        for number, line in enumerate(SCRIPT.splitlines(), start=1)
        if line.endswith("# flagged")
    }
    assert flagged == expected, checked.stdout

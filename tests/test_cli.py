from pathlib import Path

import pytest
import typer
from typer.core import TyperGroup
from typer.testing import CliRunner

from climate_finance.build.cli import app


def test_cf_build_declares_every_command() -> None:
    group = typer.main.get_command(app)

    assert isinstance(group, TyperGroup)
    assert CliRunner().invoke(app, ["--help"]).exit_code == 0
    assert set(group.commands) == {
        "build",
        "sample",
        "gate",
        "queue",
        "import-labels",
        "label",
        "ingest-unfccc",
        "release",
        "publish",
        "names",
        "listings",
    }


@pytest.mark.parametrize("sources", [[], ["sample", "--packet", "queue.md"]])
def test_label_takes_a_sample_or_a_packet(tmp_path: Path, sources: list[str]) -> None:
    result = CliRunner().invoke(
        app, ["label", str(tmp_path), *sources, "--reviewer", "analyst"]
    )

    assert result.exit_code == 2

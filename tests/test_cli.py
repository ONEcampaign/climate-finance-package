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
        "ingest-unfccc",
        "release",
        "names",
        "listings",
    }

"""`cf-build`: the build's command line. Each command delegates to its module."""

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from climate_finance.build import build as run_build
from climate_finance.build import evaluate, gate, release
from climate_finance.build.link import decisions as decisions_module
from climate_finance.build.link import review
from climate_finance.build.readers import btr
from climate_finance.build.stages import Stage

app = typer.Typer(no_args_is_help=True, help="Build and release climate-finance data.")
console = Console()

ConfigOption = Annotated[Path, typer.Option(help="Path to build.toml.")]
OutDirOption = Annotated[Path, typer.Option(help="Directory releases are built in.")]


@app.command("build")
def build_command(
    release_name: Annotated[str, typer.Argument(metavar="RELEASE")],
    config: ConfigOption = Path("build.toml"),
    out_dir: OutDirOption = Path("releases"),
    stage: Annotated[
        list[Stage] | None, typer.Option(help="Run only these stages.")
    ] = None,
    offline: Annotated[bool, typer.Option(help="Refuse network access.")] = False,
) -> None:
    """Run the build stages and write their tables."""
    manifest = run_build(
        release_name, config=config, out_dir=out_dir, stages=stage, offline=offline
    )
    for stage_name, tables in manifest.tables.items():
        console.print(f"{stage_name}: {len(tables)} tables")


@app.command()
def sample(
    release_dir: Path,
    out: Annotated[Path, typer.Option(help="Where to write the sample CSV.")],
) -> None:
    """Draw the stratified blind sample for labelling."""
    console.print(evaluate.draw_blind_sample(release_dir=release_dir, out_path=out))


@app.command("gate")
def gate_command(
    release_dir: Path,
    report: Annotated[bool, typer.Option(help="Report failures without failing.")] = (
        False
    ),
) -> None:
    """Evaluate the release gates."""
    console.print(gate.run_gates(release_dir, report_only=report))


@app.command()
def queue(
    release_dir: Path,
    out: Annotated[Path, typer.Option(help="Where to write the queue packet.")],
    ids: Annotated[
        bool, typer.Option(help="Show crs ids and project numbers in the packet.")
    ] = True,
) -> None:
    """Write the review queue as a labelling packet, its pairs file beside it."""
    console.print(
        review.write_queue_packet(release_dir=release_dir, out=out, show_ids=ids)
    )


@app.command("import-labels")
def import_labels(
    packet: Path,
    labels: Path,
    source: Annotated[str, typer.Option(help="Who decided: human or agent.")],
    reviewer: Annotated[str, typer.Option(help="The person or agent run.")],
    model: Annotated[str | None, typer.Option(help="The agent's model.")] = None,
    label_round: Annotated[
        str | None,
        typer.Option("--round", help="The agent's round: label, second, adjudicate."),
    ] = None,
    decisions: Annotated[Path, typer.Option(help="Review-decisions parquet.")] = Path(
        "data/review_decisions.parquet"
    ),
) -> None:
    """Record a labels CSV (pair,label,confidence,reason) answering a packet."""
    console.print(
        decisions_module.import_labels(
            packet=packet,
            labels_csv=labels,
            decisions_path=decisions,
            label_source=source,
            reviewer=reviewer,
            model=model,
            label_round=label_round,
        )
    )


@app.command("ingest-unfccc")
def ingest_unfccc(
    archive_dir: Path,
    out: Annotated[Path, typer.Option(help="Where to write the parsed rows.")],
) -> None:
    """Parse an archive of BTR workbooks listed in its manifest."""
    console.print(btr.ingest_unfccc(archive_dir, out_path=out))


@app.command("release")
def release_command(
    release_name: Annotated[str, typer.Argument(metavar="RELEASE")],
    config: ConfigOption = Path("build.toml"),
    out_dir: OutDirOption = Path("releases"),
    waive_precision: Annotated[
        bool, typer.Option(help="Pack despite a failed precision gate; recorded.")
    ] = False,
) -> None:
    """Build, gate and pack a release."""
    console.print(
        release.cut_release(
            release_name,
            config=config,
            out_dir=out_dir,
            waive_precision=waive_precision,
        )
    )

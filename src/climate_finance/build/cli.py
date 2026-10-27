"""`cf-build`: the build's command line. Each command delegates to its module."""

import webbrowser
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from climate_finance.build import build as run_build
from climate_finance.build import (
    evaluate,
    gate,
    listings,
    load_config,
    names,
    release,
)
from climate_finance.build.link import decisions as decisions_module
from climate_finance.build.link import labelling, review
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
    out: Annotated[
        Path, typer.Option(help="Directory for the sample's packets and design.")
    ],
    config: ConfigOption = Path("build.toml"),
) -> None:
    """Draw the stratified blind sample: a labelling packet per provider and batch,
    and the design beside them. The review decisions come from the config."""
    console.print(
        evaluate.draw_blind_sample(
            release_dir=release_dir,
            out_dir=out,
            decisions=load_config(config).sources.review_decisions,
            rule=gate.load_gates().precision,
        )
    )


SampleOption = Annotated[
    Path | None,
    typer.Option(help="The blind sample's design.parquet, written by `sample`."),
]
PreviousOption = Annotated[
    Path | None, typer.Option(help="The previous release's directory, for drift.")
]
FirstReleaseOption = Annotated[
    bool, typer.Option(help="Pass the drift gate with no previous release.")
]


@app.command("gate")
def gate_command(
    release_dir: Path,
    config: ConfigOption = Path("build.toml"),
    sample: SampleOption = None,
    previous: PreviousOption = None,
    first_release: FirstReleaseOption = False,
    report: Annotated[bool, typer.Option(help="Report failures without failing.")] = (
        False
    ),
) -> None:
    """Evaluate the release gates, one line per gate row. The blind sample's labels
    come from the config's review decisions."""
    results = gate.run_gates(
        release_dir,
        decisions=load_config(config).sources.review_decisions,
        sample=sample,
        previous=previous,
        first_release=first_release,
        report_only=report,
    )
    for row in results.iter_rows(named=True):
        outcome = "pass" if row["passed"] else "waived" if row["waived"] else "FAIL"
        name = " ".join(filter(None, (row["gate"], row["subject"])))
        console.print(f"{name}: {outcome}; {row['detail']}", markup=False)


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
    partial: Annotated[
        bool, typer.Option(help="Accept a CSV that answers only some of the pairs.")
    ] = False,
) -> None:
    """Record a labels CSV (pair,label,confidence,reason) answering a packet. A
    person's answer replaces the same person's earlier answer on the pair."""
    console.print(
        decisions_module.import_labels(
            packet=packet,
            labels_csv=labels,
            decisions_path=decisions,
            label_source=source,
            reviewer=reviewer,
            model=model,
            label_round=label_round,
            partial=partial,
        )
    )


@app.command("label")
def label_command(
    release_dir: Path,
    sample_dir: Path,
    reviewer: Annotated[str, typer.Option(help="The person labelling.")],
    config: ConfigOption = Path("build.toml"),
    port: Annotated[
        int, typer.Option(help="Local port to serve on; 0 picks a free one.")
    ] = 0,
    browser: Annotated[bool, typer.Option(help="Open the page in a browser.")] = True,
) -> None:
    """Serve a page on 127.0.0.1 for labelling a person's share of the blind sample
    in SAMPLE_DIR, a tenth of each provider's draws. Each answer is saved at once to
    a labels CSV beside its packet and to the config's review decisions. Ctrl-C
    stops the server."""
    session = labelling.Session(
        release_dir=release_dir,
        sample_dir=sample_dir,
        decisions=load_config(config).sources.review_decisions,
        reviewer=reviewer,
        share=gate.load_gates().human_audit.min_share,
    )
    server = labelling.make_server(session, port=port)
    url = f"http://127.0.0.1:{server.server_port}/"
    console.print(f"Labelling {session.state()['total']} pairs at {url}; Ctrl-C stops.")
    if browser:
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        console.print("Stopped; answers are saved.")
    finally:
        server.server_close()


@app.command("ingest-unfccc")
def ingest_unfccc(
    archive_dir: Path,
    out: Annotated[Path, typer.Option(help="Where to write the parsed rows.")],
) -> None:
    """Parse an archive of BTR workbooks listed in its manifest."""
    console.print(btr.ingest_unfccc(archive_dir, out_path=out))


@app.command("listings")
def listings_command(
    raw: Annotated[
        Path, typer.Argument(help="A raw directory holding the OECD files in oecd/.")
    ],
) -> None:
    """Add every provider, agency and recipient code the OECD files carry to the
    listing that lacks it. Run `cf-build names` after."""
    for listing, added in listings.refresh_listings(raw).items():
        console.print(f"{listings.LISTING_FILES[listing]}: {added.height} added")
        for row in added.select(*listings.LISTING_KEYS[listing], "name").iter_rows():
            console.print("  " + ", ".join(map(str, row)), markup=False)


@app.command("names")
def names_command(
    check: Annotated[
        bool, typer.Option(help="Fail when the committed table is out of date.")
    ] = False,
) -> None:
    """Bake the alias table behind `providers=` and `recipients=` into names.csv."""
    if check:
        if not names.names_are_current():
            console.print(
                f"{names.NAMES_CSV.name} is out of date; run `cf-build names`."
            )
            raise typer.Exit(code=1)
        console.print(f"{names.NAMES_CSV.name} is up to date.")
        return
    table = names.write_names()
    counts = ", ".join(
        f"{kind} {count}"
        for kind, count in table.group_by("kind").len().sort("kind").iter_rows()
    )
    console.print(f"{names.NAMES_CSV.name}: {table.height} rows ({counts})")


@app.command("release")
def release_command(
    release_name: Annotated[str, typer.Argument(metavar="RELEASE")],
    config: ConfigOption = Path("build.toml"),
    out_dir: OutDirOption = Path("releases"),
    sample: SampleOption = None,
    previous: PreviousOption = None,
    first_release: FirstReleaseOption = False,
    waive_precision: Annotated[
        str | None,
        typer.Option(
            metavar="REASON",
            help="Pack despite failed precision and human-audit gates, recording "
            "REASON in release.json.",
        ),
    ] = None,
) -> None:
    """Build, gate and pack a release."""
    console.print(
        release.cut_release(
            release_name,
            config=config,
            out_dir=out_dir,
            sample=sample,
            previous=previous,
            first_release=first_release,
            waive_precision=waive_precision,
        )
    )

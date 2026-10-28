"""`cf-build`: the build's command line. Each command delegates to its module."""

import os
import webbrowser
from collections.abc import Sequence
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console
from typer.core import TyperGroup

from climate_finance.build import build as run_build
from climate_finance.build import (
    evaluate,
    gate,
    listings,
    load_config,
    names,
    release,
)
from climate_finance.build import publish as publish_module
from climate_finance.build.link import decisions as decisions_module
from climate_finance.build.link import labelling, review
from climate_finance.build.readers import btr
from climate_finance.build.stages import Stage
from climate_finance.errors import ClimateFinanceError

# Set to 1 for the full traceback of a refusal in place of its one-line message.
TRACEBACK_VAR = "CF_BUILD_TRACEBACK"


class _Commands(TyperGroup):
    """Prints a refusal the package raises as one line, with a line for each error
    that caused it, and exits with status 1. `TRACEBACK_VAR` set to 1 keeps the
    traceback."""

    # Typer's own click.Context is private (`typer._click`), and `typer.Context`
    # narrows it, so the override takes Any.
    def invoke(self, ctx: Any) -> Any:
        try:
            return super().invoke(ctx)
        except ClimateFinanceError as error:
            if os.environ.get(TRACEBACK_VAR) == "1":
                raise
            lines = [f"{type(error).__name__}: {error}"]
            cause = error.__cause__
            while cause is not None:
                lines.append(f"  caused by {type(cause).__name__}: {cause}")
                cause = cause.__cause__
            lines.append(f"Set {TRACEBACK_VAR}=1 for the full traceback.")
            Console(stderr=True, soft_wrap=True).print("\n".join(lines), markup=False)
            raise typer.Exit(code=1) from None


app = typer.Typer(
    cls=_Commands, no_args_is_help=True, help="Build and release climate-finance data."
)
console = Console()

ConfigOption = Annotated[Path, typer.Option(help="Path to build.toml.")]
OutDirOption = Annotated[Path, typer.Option(help="Directory releases are built in.")]
BuildDirArgument = Annotated[
    Path,
    typer.Argument(
        metavar="BUILD_DIR", help="A build's directory, <out-dir>/<RELEASE>."
    ),
]


@app.command("build")
def build_command(
    release_name: Annotated[str, typer.Argument(metavar="RELEASE")],
    config: ConfigOption = Path("build.toml"),
    out_dir: OutDirOption = Path("releases"),
    stage: Annotated[
        list[Stage] | None, typer.Option(help="Run only these stages.")
    ] = None,
    offline: Annotated[
        bool | None,
        typer.Option(
            help="Refuse network access, or fetch afresh. Unset, the config's "
            "`offline` applies."
        ),
    ] = None,
    reuse_sources: Annotated[
        bool,
        typer.Option(
            help="Reuse the sources an earlier build of RELEASE fetched, while Jev "
            "may still answer online unless the build is offline."
        ),
    ] = False,
) -> None:
    """Run the build stages and write their tables."""
    build_config = load_config(config)
    if offline is None:
        source = f"offline = {str(build_config.offline).lower()} in {config}"
    else:
        build_config = build_config.model_copy(update={"offline": offline})
        source = "--offline" if offline else "--no-offline"
    mode = "offline" if build_config.offline else "online"
    if reuse_sources and not build_config.offline:
        mode += ", reusing the fetched sources"
    console.print(f"Building {release_name} {mode} ({source}).", markup=False)
    manifest = run_build(
        release_name,
        config=build_config,
        out_dir=out_dir,
        stages=stage,
        reuse_sources=reuse_sources,
    )
    for stage_name, tables in manifest.tables.items():
        console.print(f"{stage_name}: {len(tables)} tables")


@app.command()
def sample(
    build_dir: BuildDirArgument,
    out: Annotated[
        Path, typer.Option(help="Directory for the sample's packets and design.")
    ],
    config: ConfigOption = Path("build.toml"),
) -> None:
    """Draw the stratified blind sample: a labelling packet per provider and batch,
    and the design beside them. The review decisions come from the config."""
    console.print(
        evaluate.draw_blind_sample(
            release_dir=build_dir,
            out_dir=out,
            decisions=load_config(config).sources.review_decisions,
            rule=gate.load_gates().precision,
        )
    )


SampleOption = Annotated[
    Path | None,
    typer.Option(help="The blind sample's directory, the --out of `sample`."),
]
PreviousOption = Annotated[
    Path | None,
    typer.Option(
        help="The previous published release, for drift: its pack, the copy "
        "cf.load() downloaded, or its build directory with its pack beside it."
    ),
]
FirstReleaseOption = Annotated[
    bool,
    typer.Option(
        help="Pass the drift gate with no previous release; `publish` refuses it "
        "once a release is published."
    ),
]


@app.command("gate")
def gate_command(
    build_dir: BuildDirArgument,
    config: ConfigOption = Path("build.toml"),
    sample: SampleOption = None,
    previous: PreviousOption = None,
    first_release: FirstReleaseOption = False,
    report: Annotated[
        bool, typer.Option(help="Exit with status 0 when a gate fails.")
    ] = False,
) -> None:
    """Evaluate the release gates, one line per gate row, and exit with status 1
    when a row fails. The blind sample's labels come from the config's review
    decisions, and the causes of drift from its `drift_causes`."""
    build_config = load_config(config)
    with release.previous_baseline(previous) as baseline:
        results = gate.run_gates(
            build_dir,
            decisions=build_config.sources.review_decisions,
            sample=sample,
            previous=None if baseline is None else baseline.release,
            first_release=first_release,
            drift_causes=build_config.drift_causes,
            report_only=True,
        )
    failed = 0
    for row in results.iter_rows(named=True):
        outcome = "pass" if row["passed"] else "waived" if row["waived"] else "FAIL"
        failed += outcome == "FAIL"
        name = " ".join(filter(None, (row["gate"], row["subject"])))
        console.print(f"{name}: {outcome}; {row['detail']}", markup=False)
    if failed and not report:
        console.print(f"{failed} gate rows failed.")
        raise typer.Exit(code=1)


@app.command()
def queue(
    build_dir: BuildDirArgument,
    out: Annotated[Path, typer.Option(help="Where to write the queue packet.")],
    ids: Annotated[
        bool, typer.Option(help="Show crs ids and project numbers in the packet.")
    ] = True,
) -> None:
    """Write the review queue as a labelling packet, its pairs file beside it."""
    console.print(
        review.write_queue_packet(release_dir=build_dir, out=out, show_ids=ids)
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
    config: ConfigOption = Path("build.toml"),
    partial: Annotated[
        bool, typer.Option(help="Accept a CSV that answers only some of the pairs.")
    ] = False,
) -> None:
    """Record a labels CSV (pair,label,confidence,reason) answering a packet in the
    config's review decisions. A person's answer replaces the same person's earlier
    answer on the pair."""
    console.print(
        decisions_module.import_labels(
            packet=packet,
            labels_csv=labels,
            decisions_path=load_config(config).sources.review_decisions,
            label_source=source,
            reviewer=reviewer,
            model=model,
            label_round=label_round,
            partial=partial,
        )
    )


@app.command("label")
def label_command(
    build_dir: BuildDirArgument,
    sample_dir: Annotated[
        Path,
        typer.Argument(
            metavar="SAMPLE_DIR", help="The sample directory, the --out of `sample`."
        ),
    ],
    reviewer: Annotated[str, typer.Option(help="The person labelling.")],
    config: ConfigOption = Path("build.toml"),
    port: Annotated[
        int, typer.Option(help="Local port to serve on; 0 picks a free one.")
    ] = 0,
    browser: Annotated[bool, typer.Option(help="Open the page in a browser.")] = True,
) -> None:
    """Serve a page on 127.0.0.1 for labelling a person's share of the blind sample
    in SAMPLE_DIR: the pairs no certain rule linked, to a tenth of all draws, and 50
    spot checks of the certain rules. Each answer is saved at once to a labels CSV
    beside its packet and to the config's review decisions. Ctrl-C stops the
    server."""
    session = labelling.Session(
        release_dir=build_dir,
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
    offline: Annotated[
        bool | None,
        typer.Option(
            help="Refuse network access, or fetch afresh. Unset, the config's "
            "`offline` applies, and the build reuses the sources an earlier build of "
            "RELEASE fetched."
        ),
    ] = None,
) -> None:
    """Build, gate and pack a release. The build reads the review decisions, the
    blind sample's labels among them, so it runs every stage."""
    console.print(
        release.cut_release(
            release_name,
            config=config,
            out_dir=out_dir,
            sample=sample,
            previous=previous,
            first_release=first_release,
            waive_precision=waive_precision,
            offline=offline,
        )
    )


@app.command()
def publish(
    pack: Annotated[
        Path,
        typer.Argument(
            metavar="PACK", exists=True, dir_okay=False, help="A pack `release` wrote."
        ),
    ],
    repo: Annotated[
        str, typer.Option(help="The GitHub repository whose `data` release holds it.")
    ] = publish_module.PUBLISH_REPO,
    dry_run: Annotated[
        bool,
        typer.Option(
            help="Check the pack against the published index and print the gh "
            "commands that would change GitHub; the new index goes to a temporary "
            "file."
        ),
    ] = False,
) -> None:
    """Publish a pack. Check its gates, settings and name against the published
    index, upload it with `gh` to the GitHub release tagged `data`, creating that
    release if needed, then replace releases.json there and read it back. The new
    index is written beside the pack once published. Each gh command prints as it
    runs. Run one publish at a time."""

    def gh(args: Sequence[str]) -> str:
        console.print(
            f"gh {publish_module.shell_join(args)}", markup=False, soft_wrap=True
        )
        return publish_module.run_gh(args)

    published = publish_module.publish_release(pack, repo=repo, gh=gh, dry_run=dry_run)
    if dry_run:
        for command in published.commands:
            console.print(
                f"would run: gh {publish_module.shell_join(command)}",
                markup=False,
                soft_wrap=True,
            )
    done = "Would publish" if dry_run else "Published"
    written = "new index previewed in" if dry_run else "index written to"
    console.print(
        f"{done} {published.release} at {published.url}\n"
        f"sha256 {published.sha256}\n"
        f"latest {published.latest}; {written} {published.index}",
        markup=False,
        soft_wrap=True,
    )

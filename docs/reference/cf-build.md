# cf-build

`cf-build` (introduced in 2.0) builds and releases climate-finance data. Install it with the `build` extra (`pip install "climate-finance[build]"`). Importing `climate_finance.build` without the extra raises an `ImportError` that names it.

```bash
cf-build --help
```

## Build layout

`cf-build build RELEASE` runs the stages in order and writes each stage's tables to `<out-dir>/<RELEASE>/<stage>/`, sorted by key. Two offline builds from one raw manifest give the same sha256 for every table. The stages are `fetch`, `normalise`, `link`, `allocate`, `fill`, `review`, `label`, `measure`, and `assemble`. `cf.load(path=...)` opens `<out-dir>/<RELEASE>/assemble/` as a release directory.

## `cf-build build`

```bash
cf-build build RELEASE [--config build.toml] [--out-dir releases] [--stage STAGE] [--offline]
```

Runs the build stages and writes their tables.

- `RELEASE` is the release name.
- `--config` (default `build.toml`) sets the build configuration. Relative paths in it resolve against the file.
- `--out-dir` (default `releases`) sets the directory where releases are built.
- `--stage` (repeatable) runs only the named stages. A stage that reads a table missing from earlier stages raises `StageInputMissingError`.
- `--offline` refuses network access.

The `build.toml` switches are `offline`, `key_repair` and `recoveries` (both on by default), `gap_fill`, `review`, `jev_labels`, `jev_links`, and `embed`. `skip_eib_routes` names EIB recovery routes to omit so a test can measure what a route adds. A release builds with an empty list. `[years] crdf` takes an inclusive pair of years or `"all"`, every year the recipient-perspective CRDF holds, and the build records the years it resolved in `release.json`. The sections `[years]`, `[sources]`, `[jev]`, and `[embedding]` set the years, the pinned source files and their sha256, the Jev model version, cost cap and band, and the embedding model.

## `cf-build sample`

```bash
cf-build sample RELEASE_DIR --out DIR [--config build.toml]
```

Draws the stratified blind sample of a build's allocated links. It writes a labelling packet per provider and batch to `DIR/<provider>/batch-<NN>.md`, and the design, one row per draw, to `DIR/design.parquet`. The packets show both records of a pair and omit the score, link method, and stratum. The command reads earlier decisions from `sources.review_decisions` in the configuration. A missing decisions file is treated as an empty set of decisions.

## `cf-build queue`

```bash
cf-build queue RELEASE_DIR --out FILE [--ids | --no-ids]
```

Writes the review queue as a labelling packet, with its pairs file beside it. `--no-ids` hides crs ids and project numbers and blanks them in the records' text.

## `cf-build import-labels`

```bash
cf-build import-labels PACKET LABELS --source human --reviewer NAME [--decisions data/review_decisions.parquet]
```

Records a labels CSV that answers a packet. The CSV has the columns `pair`, `label` (`match` or `no_match`), `confidence` (0 to 1, on the label's side of 0.5), and `reason`, and answers each pair of the packet once. A person's decision on a pair overrides any agent's, whatever the import order.

- `--source` records who decided (`human` or `agent`).
- `--reviewer` names the person or the agent run.
- `--model` sets the agent's model. It is required for an agent and refused for a person.
- `--round` sets an agent's round (`label`, `second`, or `adjudicate`), defaulting to `label` for an agent.
- `--decisions` (default `data/review_decisions.parquet`) sets the table the decisions append to.

The command raises `ReviewDecisionsError` when the packet differs from its pairs file, or when the CSV's pairs differ from the packet's.

## `cf-build gate`

```bash
cf-build gate RELEASE_DIR [--config build.toml] [--sample FILE] [--previous DIR] [--first-release] [--report]
```

Evaluates the [release gates](release-gates.md) and prints one line per gate row.

- `--sample` passes the `design.parquet` that `sample` wrote. The gate reads the sample's labels from `sources.review_decisions` in the configuration and raises `FileNotFoundError` when that file is missing. Without `--sample`, every provider fails precision and the human audit fails.
- `--previous` sets the previous release's directory for the drift gate.
- `--first-release` passes the drift gate when no previous release exists.
- `--report` prints failures and exits cleanly.

## `cf-build ingest-unfccc`

```bash
cf-build ingest-unfccc ARCHIVE_DIR --out FILE
```

Parses an archive of BTR workbooks listed in its manifest and writes the rows to `FILE` for inspection. The build reads the workbooks directly when `sources.btr_archive` in `build.toml` points to the archive directory.

## `cf-build release`

```bash
cf-build release RELEASE [--config build.toml] [--out-dir releases] [--sample FILE] [--previous DIR] [--first-release] [--waive-precision]
```

Builds, gates, and packs a release. It writes `<out-dir>/climate-finance-<RELEASE>.tar.zst`, which holds the release tables, `release.json` with the gate results, and `sources.json`. Members go in name order with fixed owners and the release's creation time, so one release packs to the same bytes.

- `--waive-precision` packs despite failed precision and human-audit gates, marks the failed rows `waived`, and records the waiver in `release.json`.

The command raises `GateFailedError` when a gate fails, and `BuildError` when a source lacks a licence entry.

## Next

- [How to cut a release](../how-to/cut-a-release.md) runs these commands in order.
- [How to review links](../how-to/review-links.md) covers `queue` and `import-labels`.

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

The `build.toml` switches are `offline`, `key_repair` and `recoveries` (both on by default), `gap_fill`, `review`, `jev_labels`, `jev_links`, `embed`, and `jev_flags`. `jev_flags` has the flag stage ask Jev four questions about each climate-marked CRS record from `jev_flags_first_year` (default 2015) to `years.crs_last_complete`, and fails with a `ConfigError` when the CRS years the build reads start after `jev_flags_first_year`. `skip_eib_routes` names EIB recovery routes to omit so a test can measure what a route adds. A release builds with an empty list. `[years] crdf` takes an inclusive pair of years or `"all"`, every year the recipient-perspective CRDF holds, and the build records the years it resolved in `release.json`. The sections `[years]`, `[sources]`, `[jev]`, and `[embedding]` set the years, the pinned source files and their sha256, the Jev model version, cost cap and band, and the embedding model.

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
cf-build import-labels PACKET LABELS --source human --reviewer NAME [--decisions data/review_decisions.parquet] [--partial]
```

Records a labels CSV that answers a packet. The CSV has the columns `pair`, `label` (`match` or `no_match`), `confidence` (0 to 1, on the label's side of 0.5), and `reason`, and answers each pair of the packet once. A person's decision on a pair overrides any agent's, whatever the import order. A person's import replaces that person's earlier decision on the same pair, and an agent's import appends.

- `--source` records who decided (`human` or `agent`).
- `--reviewer` names the person or the agent run.
- `--model` sets the agent's model. It is required for an agent and refused for a person.
- `--round` sets an agent's round (`label`, `second`, or `adjudicate`), defaulting to `label` for an agent.
- `--decisions` (default `data/review_decisions.parquet`) sets the table the decisions go to.
- `--partial` accepts a CSV that answers only some of the packet's pairs, each once.

The command raises `ReviewDecisionsError` when the packet differs from its pairs file, or when the CSV's pairs differ from the packet's.

## `cf-build label`

```bash
cf-build label RELEASE_DIR SAMPLE_DIR --reviewer NAME [--config build.toml] [--port N] [--browser | --no-browser]
```

Serves a page on `127.0.0.1` for labelling one person's share of the blind sample in `SAMPLE_DIR`, and opens it in a browser. The share is the judgement calls, the pairs whose link no certain rule made, in a stable hash order of their sample ids until their draws reach `human_audit.min_share` of all the sample's draws, with 50 spot checks of pairs a certain rule linked spread among them. A pair already decided by a person stays in the queue, counted as done and skipped. `RELEASE_DIR` supplies the records the page shows.

- `--reviewer` names the person in the decisions.
- `--config` (default `build.toml`) names the file whose `sources.review_decisions` receives the decisions.
- `--port` sets the port. The default of 0 picks a free one.
- `--no-browser` prints the address without opening it.

Each answer is written at once to `batch-<NN>.<reviewer>.labels.csv` beside its packet and recorded with `import-labels --source human`. [How to label the blind sample](../how-to/label-the-blind-sample.md) walks through the page.

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
cf-build release RELEASE [--config build.toml] [--out-dir releases] [--sample FILE] [--previous DIR] [--first-release] [--waive-precision REASON]
```

Builds, gates, and packs a release. It writes `<out-dir>/climate-finance-<RELEASE>.tar.zst`, which holds the release tables, `release.json` with the gate results, and `sources.json`. Members go in name order with fixed owners and the release's creation time, so one release packs to the same bytes.

- `--waive-precision REASON` packs despite failed precision and human-audit gates, marks the failed rows `waived`, and records the waiver and `REASON` in `release.json` as `precision_gate_waived` and `precision_waiver`.

The command raises `GateFailedError` when a gate fails, and `BuildError` when a source lacks a licence entry or `RELEASE` is `fixture`, the name reserved for the synthetic test release. `release.upload_release` and `release.add_to_index` refuse a pack of that name too.

## `cf-build listings`

```bash
cf-build listings RAW
```

Adds every provider, agency, and recipient code the OECD files in `RAW/oecd/` carry to the listing in `src/climate_finance/data` that lacks it: `oecd_classification.csv` for providers, `agencies.csv` for agencies, and `recipients.csv` for recipients. `RAW` is a build's raw directory, `releases/<RELEASE>/fetch/raw`, or an archive laid out the same way. The command reads the CRS (`CRS*.parquet`) for donors, agencies, and recipients, the recipient-perspective CRDF (`crdf_recipient*.parquet`) for all three, the provider-perspective CRDF (`crdf_provider*.parquet`) for providers and agencies, and MULTI (`multisystem*.parquet`) for donors below the donor groups from 20000. CRS and MULTI rows count from the first year the recipient-perspective CRDF holds. Agency 0 and a null agency stand for the provider and are left out.

Listed rows stay as they are. Each added code takes the name the files give it in the latest year they carry it, and an added provider has no French name or type. The command prints the rows it added and fails with `ListingsError` when a file kind is missing or no file names a code. The listings are committed and ship in the wheel, so the release gate `listed_codes` and the package read them without raw data. Run `cf-build names` after a refresh.

## `cf-build names`

```bash
cf-build names [--check]
```

Bakes the alias table behind `providers=` and `recipients=` into `src/climate_finance/data/names.csv`, which ships in the wheel, so the analyst package resolves names without resolvekit. Each row maps one folded name (case, accents and surrounding spaces dropped) to a DAC provider or recipient code, and to an agency code when the name is an agency's. The names come from the listings' English and French names, `build/aliases.toml`, the ISO codes and aliases of the resolvekit places and organisations that carry a listing code, and the names of the agencies `agencies.csv` lists, from that file and resolvekit's `org.governments` pack, best first. An agency `agencies.csv` lacks gets no rows, since no release table holds it. Each name keeps its rows from its best source only, so a name with several rows there is ambiguous.

The bake fails when a listing name does not resolve to its own code, when `aliases.toml` names a code or agency that the listings lack or a listing name hides, and when an ISO code or resolvekit alias of a provider is also the name of one of its agencies, which would widen the agency to the whole provider. Such a name is settled by a line in `aliases.toml`.

- `--check` rebakes without writing and exits with status 1 when `names.csv` differs. CI runs it in the locked lint job, so a resolvekit bump or an `aliases.toml` edit without a rebake fails there.

## Next

- [How to cut a release](../how-to/cut-a-release.md) runs these commands in order.
- [How to review links](../how-to/review-links.md) covers `queue` and `import-labels`.

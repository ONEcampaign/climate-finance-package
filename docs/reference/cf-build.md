# cf-build

`cf-build` (introduced in 2.0) builds and releases climate-finance data. Run it from a source checkout with the `build` extra (`uv sync --extra build`). `listings` and `names` rewrite files in `src/climate_finance/data`, and a change to any package file makes every built stage stale, so it runs from the checkout. Importing `climate_finance.build` without the extra raises an `ImportError` that names it.

```bash
cf-build --help
```

A command that refuses its input, such as a misspelt `build.toml` key or a failed gate, prints one line naming the error class, for example `ConfigError: ...`, and a `caused by` line for each error behind it, then exits with status 1. Set `CF_BUILD_TRACEBACK=1` to get the full traceback instead.

## Terms

- A build directory is `<out-dir>/<RELEASE>`, with a directory per stage. `BUILD_DIR` names one.
- A release directory is one `cf.load(path=...)` opens, holding `release.json`: a build's `assemble/` or an unpacked pack. `--previous` takes a pack, a release directory, or a build directory. Given a pack, or a build directory with its pack beside it, the drift gate reads a private copy of the pack, unpacked, so it compares with the bytes whose sha256 `release` records; a rebuilt directory is never read in its place. A copy `cf.load()` downloaded is read in place with the sha256 `cf.load()` stamped on it. Any other directory is read in place and records no pack.
- The sample directory is the `--out` of `sample`, holding the packets and `design.parquet`. `label`, `gate`, and `release` take the directory.
- A pack is the `climate-finance-<RELEASE>.tar.zst` that `release` writes and `publish` uploads.

## Build layout

`cf-build build RELEASE` runs the stages in order and writes each stage's tables to `<out-dir>/<RELEASE>/<stage>/`, sorted by key. Two offline builds from one raw manifest give the same sha256 for every table. The stages are `fetch`, `normalise`, `link`, `allocate`, `fill`, `review`, `label`, `flag`, `measure`, and `assemble`. `cf.load(path=...)` opens `<out-dir>/<RELEASE>/assemble/` as a release directory.

## `cf-build build`

```bash
cf-build build RELEASE [--config build.toml] [--out-dir releases] [--stage STAGE] [--offline | --no-offline] [--reuse-sources]
```

Runs the build stages and writes their tables.

- `RELEASE` is the release name.
- `--config` (default `build.toml`) sets the build configuration. Relative paths in it resolve against the file.
- `--out-dir` (default `releases`) sets the directory where releases are built.
- `--stage` (repeatable) runs only the named stages. A stage that reads a table missing from earlier stages raises `StageInputMissingError`.
- `--offline` refuses network access, and `--no-offline` fetches, whatever the config's `offline`. Without either, the config decides. The command prints which applied.
- `--reuse-sources` has fetch reuse the sources an earlier build of `RELEASE` fetched, checked against its raw manifest, and makes no download. Network access still follows `--offline` and the config, so Jev answers questions that new review decisions raise. An offline build always reuses the sources.

The `build.toml` switches are `offline`, `key_repair` and `recoveries` (both on by default), `gap_fill`, `review`, `jev_labels`, `jev_links`, `embed`, and `jev_flags`. `jev_flags` has the flag stage ask Jev four questions about each climate-marked CRS record from `jev_flags_first_year` (default 2015) to `years.crs_last_complete`, and fails with a `ConfigError` when the CRS years the build reads start after `jev_flags_first_year`. `skip_eib_routes` names EIB recovery routes to omit so a test can measure what a route adds. A release builds with an empty list. `[years] crdf` takes an inclusive pair of years or `"all"`, every year the recipient-perspective CRDF holds, and the build records the years it resolved in `release.json`. The sections `[years]`, `[sources]`, `[jev]`, and `[embedding]` set the years, the pinned source files and their sha256, the Jev model version, cost cap and band, and the embedding model. Each `[[drift_causes]]` entry names a `release`, `provider_code`, `year`, and `cause` that explain a move the drift gate would otherwise fail. They live in `build.toml` because an edit to a package file makes every built stage stale.

## `cf-build sample`

```bash
cf-build sample BUILD_DIR --out DIR [--config build.toml]
```

Draws the stratified blind sample of a build's allocated links. It writes a labelling packet per provider and batch to `DIR/<provider>/batch-<NN>.md`, and the design, one row per draw, to `DIR/design.parquet`. The packets show both records of a pair and omit the score, link method, and stratum. The command reads earlier decisions from `sources.review_decisions` in the configuration. A missing decisions file is treated as an empty set of decisions.

## `cf-build queue`

```bash
cf-build queue BUILD_DIR --out FILE [--ids | --no-ids] [--task trace | --task btr_crs]
```

Writes the review queue as a labelling packet, with its pairs file beside it. `--no-ids` hides crs ids and project numbers and blanks them in the records' text. `--task btr_crs` writes the BTR calibration draw in place of the trace pairs, each BTR row beside a CRS record, ids always shown.

## `cf-build import-labels`

```bash
cf-build import-labels PACKET LABELS --source human --reviewer NAME [--config build.toml] [--partial]
```

Records a labels CSV that answers a packet. The CSV has the columns `pair`, `label` (`match` or `no_match`), `confidence` (0 to 1, on the label's side of 0.5), and `reason`, and answers each pair of the packet once. A person's decision on a pair overrides any agent's, whatever the import order. A person's import replaces that person's earlier decision on the same pair, and an agent's import appends.

- `--source` records who decided (`human` or `agent`).
- `--reviewer` names the person or the agent run.
- `--model` sets the agent's model. It is required for an agent and refused for a person.
- `--round` sets an agent's round (`label`, `second`, or `adjudicate`), defaulting to `label` for an agent.
- `--config` (default `build.toml`) names the file whose `sources.review_decisions` receives the decisions, the table the build, `sample`, `label`, and `gate` read.
- `--partial` accepts a CSV that answers only some of the packet's pairs, each once.

The command raises `ReviewDecisionsError` when the packet differs from its pairs file, or when the CSV's pairs differ from the packet's.

## `cf-build label`

```bash
cf-build label BUILD_DIR SAMPLE_DIR --reviewer NAME [--config build.toml] [--port N] [--browser | --no-browser]
cf-build label BUILD_DIR --packet FILE --reviewer NAME [--config build.toml] [--port N] [--browser | --no-browser]
```

Serves a page on `127.0.0.1` for labelling one person's share of the blind sample in `SAMPLE_DIR`, and opens it in a browser. The share is the judgement calls, the pairs whose link no certain rule made, in a stable hash order of their sample ids until their draws reach `human_audit.min_share` of all the sample's draws, with 50 spot checks of pairs a certain rule linked spread among them. A pair already decided by a person stays in the queue, counted as done and skipped. `BUILD_DIR` supplies the records the page shows.

- `--reviewer` names the person in the decisions.
- `--packet` serves every pair of a review queue packet written by `queue`, in place of `SAMPLE_DIR`, in the packet's order. Its answers carry no sample id, so the build reads them as queue decisions.
- `--config` (default `build.toml`) names the file whose `sources.review_decisions` receives the decisions.
- `--port` sets the port. The default of 0 picks a free one.
- `--no-browser` prints the address without opening it.

Each answer is written at once to `batch-<NN>.<reviewer>.labels.csv` beside its packet and recorded with `import-labels --source human`. [How to label the blind sample](../how-to/label-the-blind-sample.md) walks through the page.

## `cf-build gate`

```bash
cf-build gate BUILD_DIR [--config build.toml] [--sample DIR] [--previous PATH] [--first-release] [--report]
```

Evaluates the [release gates](release-gates.md), prints one line per gate row, and exits with status 1 when a row fails.

- `--sample` passes the sample directory that `sample` wrote. The gate reads the sample's labels from `sources.review_decisions` in the configuration and raises `ReviewDecisionsError` when that file is missing. Without `--sample`, every provider fails precision and the human audit fails.
- `--previous` sets the previous release for the drift gate: a pack, a release directory, or a build directory, read as `release` reads it, so `gate` previews the comparison the cut makes. A path that does not exist raises `BuildError`, and a file that is no pack raises `ReleaseIntegrityError`. The drift gate reads its causes from `drift_causes` in the configuration.
- `--first-release` passes the drift gate when no previous release exists. `publish` refuses a release cut this way once the index lists a release.
- `--report` exits with status 0 when a row fails.

## `cf-build ingest-unfccc`

```bash
cf-build ingest-unfccc ARCHIVE_DIR --out FILE
```

Parses an archive of BTR workbooks listed in its manifest and writes the rows to `FILE` for inspection. The build reads the workbooks directly when `sources.btr_archive` in `build.toml` points to the archive directory.

## `cf-build release`

```bash
cf-build release RELEASE [--config build.toml] [--out-dir releases] [--sample DIR] [--previous PATH] [--first-release] [--waive-precision REASON] [--offline | --no-offline]
```

Builds, gates, and packs a release. The build runs every stage, since the review decisions it reads hold the blind sample's labels. It writes `<out-dir>/climate-finance-<RELEASE>.tar.zst`, which holds the release tables, `release.json` with the gate results, and `sources.json`. Members go in name order with fixed owners and the release's creation time, so one release packs to the same bytes.

- `--waive-precision REASON` packs despite failed precision and human-audit gates, marks the failed rows `waived`, and records the waiver and `REASON` in `release.json` as `precision_gate_waived` and `precision_waiver`.
- `--offline` refuses network access, so fetch reuses the sources and Jev answers from its cache alone. `--no-offline` fetches afresh and lets Jev answer. Without either, the configuration's `offline` sets network access, and an earlier build of `RELEASE` in `--out-dir` that fetched its sources has fetch reuse them, so the release comes from the sources the blind sample was drawn on while Jev answers the questions new review decisions raise.
- `--sample`, `--previous`, and `--first-release` act as in `gate`.

The command raises `GateFailedError` when a gate fails. Before it builds, it raises `UnpublishedSettingsError` when the configuration sets a switch a published release does not use, and `SyntheticReleaseError` when `RELEASE` is `synthetic-example`, the name reserved for the synthetic release `cf.load_example()` opens. `publish` refuses such packs with the same errors. It raises `BuildError` when a source lacks a licence entry.

## `cf-build publish`

```bash
cf-build publish PACK [--repo ONEcampaign/climate-finance-package] [--dry-run]
```

Publishes a pack where `cf.load()` finds it. Every pack and the release index, `releases.json`, are assets of the GitHub release tagged `data` on `--repo`, and `cf.load(refresh=True)` reads the index from there. The command runs the GitHub CLI, `gh`, which must be logged in with an account that can write releases to the repository, and prints each `gh` command as it runs.

The command refuses the pack, changing nothing on GitHub, in these cases:

- A gate has no result, a gate row neither passed nor was waived, or a waiver has no reason (`GateFailedError`).
- `release.json` records a setting other than a release's: `key_repair`, `recoveries`, `review`, `jev_links`, or `jev_flags` off, `gap_fill`, `jev_labels`, or `embed` on, CRDF years other than `"all"`, or an EIB route skipped (`UnpublishedSettingsError`).
- The release name is not a published name, a year and a lowercase letter with an optional revision such as `2026a` or `2026a.1`, or the pack's file is named for another release (`ReleaseNameError`).
- The index already lists the release (`AlreadyPublishedError`).
- The drift gate compared the release with a release other than the latest published one, with none once the index lists a release, or with a copy that did not come from the pack the index lists under that name (`DriftBaselineError`). A release that sorts before the latest needs a comparison with a release the index lists.
- The `data` release holds packs but no `releases.json`, as after a publish that stopped before its index was in place (`MissingIndexError`). The message names the recovery index that publish wrote, `publish-recovery/releases.json` beside the pack, and the `gh release upload` command that attaches it.
- The `data` release is a draft, whose assets only maintainers can download (`DraftReleaseError`).
- The published `releases.json` is not JSON or not an index the package reads, such as one whose `latest` is missing from its `releases`, or whose `latest` is not a published name (`PublishedIndexError`).
- `--repo` is not `owner/name` (`RepositoryNameError`).

Each class is a `BuildError` with its own `code`, such as `already_published` for `AlreadyPublishedError`, so a script tells the refusals apart by class or code.

Every check, the sha256, and the upload read one private copy of the pack, so a pack replaced at its path meanwhile is never published unchecked. Before anything on GitHub changes, the new index is written to `publish-recovery/releases.json` beside the pack. The command then uploads the pack to the `data` release, creating that release if it is missing, published and not marked latest. The new `releases.json` goes up as `releases.next.json` and replaces the old index by a delete and a rename, so the index never names a missing pack and is missing only between those two calls. The command raises `MissingAssetError` when `releases.next.json` is not on the release to rename. It then reads the index back and raises `IndexReplacedError` when it differs from the one written, which means another publish replaced it meanwhile. Publish again. Run one publish at a time. The new index adds the release's URL, sha256, and data format, keeps every entry and field of the old one, and moves `latest` only to a later release, ordered by year, letter, and revision. Once read back, it moves to `releases.json` beside the pack, to ship in the wheel's `src/climate_finance/data/releases.json` at the next package release. Each `gh` call takes an argument list with no shell and runs for at most an hour. A `gh` that is missing, fails, or runs longer raises `GhError`.

- `--repo` sets the repository whose `data` release holds the packs, as `owner/name`.
- `--dry-run` reads the published index and checks the pack against it, prints the `gh` commands that would change GitHub without running them, and writes the new index to a temporary file it names. Nothing beside the pack changes.

## `cf-build listings`

```bash
cf-build listings RAW
```

Adds every provider, agency, and recipient code the OECD files in `RAW/oecd/` carry to the listing in `src/climate_finance/data` that lacks it: `oecd_classification.csv` for providers, `agencies.csv` for agencies, and `recipients.csv` for recipients. `RAW` is a build's raw directory, `releases/<RELEASE>/fetch/raw`, or an archive laid out the same way. The command reads the CRS (`CRS*.parquet`) for donors, agencies, and recipients, the recipient-perspective CRDF (`crdf_recipient*.parquet`) for all three, the provider-perspective CRDF (`crdf_provider*.parquet`) for providers and agencies, and MULTI (`multisystem*.parquet`) for donors below the donor groups from 20000. CRS and MULTI rows count from the first year the recipient-perspective CRDF holds. Agency 0 and a null agency stand for the provider and are left out.

Listed rows keep their names. Each added code takes the name the files give it in the latest year they carry it, and an added provider has no French name or type. Two columns follow the files for every row. A provider's `mdb` is true when the CRDF's `provider_type` is "Multilateral development bank" on every row of the latest year that holds the provider. A recipient's `type` is `country`, `regional`, or `unspecified`. A code the CRDF or CRS puts in the income group "Part I unallocated by income" is not one country, and of those, a code in the region "Developing countries unspecified" (CRDF) or "Regional and Unspecified" (CRS) is `unspecified` and the rest `regional`. Each recipient takes the type of the latest year that gives it an income group, and one the files give no income group keeps its listed type. A provider type the CRDF leaves blank counts as no bank. The command prints the rows it added and fails with `ListingsError` when a file kind is missing or no file names a code. It also fails, before writing anything, when a recipient has no income group in the files and no type in `recipients.csv`. Add such a recipient to `recipients.csv` with its type, `country`, `regional`, or `unspecified`, and rerun. The listings are committed and ship in the wheel, so the release gate `listed_codes` and the package read them without raw data. Run `cf-build names` after a refresh.

## `cf-build names`

```bash
cf-build names [--check]
```

Bakes the alias table behind `providers=` and `recipients=` into `src/climate_finance/data/names.csv`, which ships in the wheel, so the analyst package resolves names without resolvekit. Each row maps one folded name (case, accents and surrounding spaces dropped) to a DAC provider or recipient code, and to an agency code when the name is an agency's. The names come from the listings' English and French names, the names and groups in `build/aliases.toml`, the ISO codes and aliases of the resolvekit places and organisations that carry a listing code, and the names of the agencies `agencies.csv` lists, from that file and resolvekit's `org.governments` pack, best first. An agency `agencies.csv` lacks gets no rows, since no release table holds it. Each name keeps its rows from its best source only, so a name with several rows there is ambiguous, unless it is a group. A group, such as `"World Bank" = [901, 905]` under `[groups]`, names two providers or more, and `providers=` takes them all.

The bake fails when a listing name does not resolve to its own code, when `aliases.toml` names a code or agency that the listings lack or a listing name hides, a group of fewer than two providers, or a name that is both a curated name and a group, and when an ISO code or resolvekit alias of a provider is also the name of one of its agencies, which would widen the agency to the whole provider. Such a name is settled by a line in `aliases.toml`.

- `--check` rebakes without writing and exits with status 1 when `names.csv` differs. CI runs it in the locked lint job, so a resolvekit bump or an `aliases.toml` edit without a rebake fails there.

## Next

- [How to cut a release](../how-to/cut-a-release.md) runs these commands in order.
- [How to review links](../how-to/review-links.md) covers `queue` and `import-labels`.

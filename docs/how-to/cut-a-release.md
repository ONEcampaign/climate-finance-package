# How to cut a release

Cutting a release requires a source checkout with the `build` extra, the repository's `build.toml`, and the OECD bulk CRS file. Publishing it requires the GitHub CLI, `gh`, logged in with an account that can write releases to the repository.

## Before you start

- Download the bulk CRS parquet through a browser and set `[sources].crs_file` to its path and sha256. The OECD's bot challenge blocks scripted downloads, and the fetch stage attempts a direct download when `crs_file` is absent.
- Check the switches in `build.toml`. The repository's file holds the settings of a published release: `key_repair`, `recoveries`, `review`, `jev_links`, and `jev_flags` on, the `[sources.ledgers]` section set for the ledger check, `gap_fill`, `jev_labels`, and `embed` off, and `crdf = "all"` under `[years]`. `release.json` records every switch under `settings`, and `cf-build publish` refuses a pack built with any of these settings changed, or with an EIB route skipped. [cf-build](../reference/cf-build.md) lists all switches.
- Keep the latest published release on hand for the drift gate: its pack, the copy `cf.load("2025a", refresh=True).path` downloads, or its build directory with its pack still beside it. Given a pack or a build directory beside one, the gate reads a private copy of the pack, never the build's own tables, which a rebuild may have changed. `release.json` records the sha256 of the pack the gate read, and `cf-build publish` refuses a pack whose drift gate compared it with any other release or another pack of that name, so `--first-release` serves only while no release is published.
- Name the release as published releases are named: a year and a lowercase letter, with a revision after a re-cut, such as `2026a` or `2026a.1`. `cf-build publish` refuses any other name, so a candidate such as `2026a-rc` can be cut and checked but cannot be published.

## Steps

1. Build the release. Stages write to `releases/2026a/<stage>/`, and the release tables go to `releases/2026a/assemble/`.

   ```bash
   cf-build build 2026a --config build.toml --out-dir releases
   ```

2. Clear the review queue. Follow [How to review links](review-links.md) to write the queue, import decisions from people and agents, and rebuild. Repeat until the queue holds only pairs you accept leaving open.

3. Draw the blind sample in batches per provider from the build's allocated links. A link that joins a unit to itself stays out of the sample, and the precision gate counts its value right.

   ```bash
   cf-build sample releases/2026a --out sample
   ```

   `sample` writes one packet per provider and batch to `sample/<provider>/batch-<NN>.md` and saves the design to `sample/design.parquet`. Packets omit the score, link method, and stratum. Agents label every draw, and a person decides at least one draw in ten, which the `human_audit` gate checks. [How to label the blind sample](label-the-blind-sample.md) shows a person how. Record each set of labels with the `import-labels` command covered in the review guide, into the decisions file that `sources.review_decisions` names.

4. Check the gates after each batch. `--report` prints failures without stopping.

   ```bash
   cf-build gate releases/2026a --sample sample --previous releases/2025a --report
   ```

   The command prints every row, and without `--report` exits with status 1 when a row fails. Each precision row shows the number of labels read and the sequential rule's verdict: `pass`, `fail`, or `continue`. Label the next batch for every provider marked `continue`, and for every provider whose `stratum_precision` row fails as `no_stratum_draws` or `too_few_draws`, even when its precision row passed. A stratum that takes the minimum of 50 draws has its draws spread over the whole sequence, so it reaches 50 only near the cap of 500. A `fail` means the sample puts that provider's precision below the threshold, which blocks the release unless waived with `--waive-precision`. A fail at any look stands, so stop labelling that provider: more labels cannot turn it into a pass, and its row carries the code `failed_at_earlier_look` once a later look is read.

5. Cut the release. `release` builds, runs the gates, and packs the archive, stopping with `GateFailedError` if any gate fails. The build reads the review decisions, which hold the sample's labels, so `release` runs every stage. When an earlier build of the release fetched its sources, the build reuses them, so the release comes from the sources the sample was drawn on, and Jev answers the questions the new labels raise. `--no-offline` fetches afresh, and a source that changed makes the sample stale. `--offline` refuses all network access, Jev included.

   ```bash
   cf-build release 2026a --config build.toml --out-dir releases --sample sample --previous releases/2025a
   ```

   The command writes `releases/climate-finance-2026a.tar.zst` and records the gate rows in `release.json`. To pack with failed precision, stratum-precision, or human-audit gates, pass `--waive-precision` with the reason. The flag marks the failed rows `waived`, sets `precision_gate_waived` in `release.json`, which surfaces in `release.quality().precision`, and records the reason as `precision_waiver`.

6. Publish the pack. Every pack and the release index, `releases.json`, are assets of one GitHub release tagged `data`, where `cf.load(refresh=True)` reads the index. The index maps each release name to its pack's URL, sha256, and data format, and names the latest release. Check the pack against the published index first, then publish it.

   ```bash
   cf-build publish releases/climate-finance-2026a.tar.zst --dry-run
   cf-build publish releases/climate-finance-2026a.tar.zst
   ```

   `publish` refuses a pack whose `release.json` records no result for a gate, a gate row that neither passed nor was waived, a waiver without its reason, or settings other than a release's. It also refuses a name that is not a published name or that the index already lists, and a pack whose drift gate compared it with anything but the latest published release's pack. Otherwise it uploads the pack to the `data` release, creating that release if it is missing, published and not marked latest. The new `releases.json` goes up as `releases.next.json`, replaces the old index by a delete and a rename, and is read back. The new index keeps every entry and field of the old one, and moves `latest` only to a later release. Each `gh` command prints as it runs, and `--dry-run` prints the commands that change GitHub without running them and writes the new index to a temporary file it names.

   Run one publish at a time. When the index read back differs from the one written, another publish replaced it meanwhile, and `publish` stops with `BuildError`. Publish again. Before it changes anything on GitHub, `publish` writes the new index to `releases/publish-recovery/releases.json`. When a publish stops before its index is in place, the `data` release can hold packs but no `releases.json`, and the next `publish` refuses with the `gh release upload` command that attaches that recovery index. Upload it, then publish again.

   Once the index reads back, `publish` moves it to `releases/releases.json`. Copy it to `src/climate_finance/data/releases.json` before the next package release, so `cf.load()` finds the release without `refresh=True`. The pack's `sources.json` records each source's licence and the rows excluded from the release.

## Verify it worked

Unpack and load the archive.

```bash
mkdir unpacked
tar --zstd -xf releases/climate-finance-2026a.tar.zst -C unpacked
python -c "import climate_finance as cf; print(list(cf.load(path='unpacked/climate-finance-2026a').manifest.gates))"
```

`load()` verifies every table's sha256 against `release.json`, and the command prints the nine gate names.

## Troubleshooting

- `GateFailedError` on `withheld_rows` means a row from a withheld source reached `disbursements`. [How linking works](../explanation/how-linking-works.md) lists the withheld sources.
- `BuildError: Sources without a licence` means a fetched source is missing from `SOURCE_LICENCES` in `climate_finance/build/release.py`. Add its licence terms before packing.
- When the drift gate fails on a provider that changed for a known reason, add a `[[drift_causes]]` entry to `build.toml` with the release, provider code, year, and cause.
- `BuildError: release '2026a' is already published` means the index lists a pack of that name, which users may have cached. Cut the release again under a new name, such as `2026a.1`.

## Next

- [Release gates](../reference/release-gates.md) lists each gate's checks and thresholds.
- [Release tables](../reference/release-tables.md) describes the files in the pack and each source's licence terms.

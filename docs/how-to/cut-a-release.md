# How to cut a release

Cutting a release requires the `build` extra, a `build.toml`, and the OECD bulk CRS file.

## Before you start

- Download the bulk CRS parquet through a browser and set `[sources].crs_file` to its path and sha256. The OECD's bot challenge blocks scripted downloads, and the fetch stage attempts a direct download when `crs_file` is absent.
- Check the switches in `build.toml`. The shipped file holds the settings of a published release: `key_repair`, `recoveries`, `review`, and `jev_links` on, the `[sources.ledgers]` section set for the ledger check, `gap_fill`, `jev_labels`, and `embed` off, and `crdf = "all"` under `[years]`. `release.json` records every switch under `settings`. [cf-build](../reference/cf-build.md) lists all switches.
- Keep a previous release's directory on hand for the drift gate, or pass `--first-release` on the initial release.

## Steps

1. Build the release. Stages write to `releases/2026a/<stage>/`, and the release tables go to `releases/2026a/assemble/`.

    ```bash
    cf-build build 2026a --config build.toml --out-dir releases
    ```

2. Clear the review queue. Follow [How to review links](review-links.md) to write the queue, import decisions from people and agents, and rebuild. Repeat until the queue holds only pairs you accept leaving open.

3. Draw the blind sample in batches per provider from the build's allocated links.

    ```bash
    cf-build sample releases/2026a --out sample
    ```

    `sample` writes one packet per provider and batch to `sample/<provider>/batch-<NN>.md` and saves the design to `sample/design.parquet`. Packets omit the score, link method, and stratum. Agents label every draw, and a person decides at least one draw in ten, which the `human_audit` gate checks. Record each set of labels with the `import-labels` command covered in the review guide, appending to the decisions file that `sources.review_decisions` names.

4. Check the gates after each batch. `--report` prints failures without stopping.

    ```bash
    cf-build gate releases/2026a --sample sample/design.parquet --previous releases/2025a/assemble --report
    ```

    Each precision row shows the number of labels read and the sequential rule's verdict: `pass`, `fail`, or `continue`. Label the next batch for every provider marked `continue`. A `fail` means the sample puts that provider's precision below the threshold, which blocks the release unless waived with `--waive-precision`.

5. Cut the release. `release` builds, runs the gates, and packs the archive, stopping with `GateFailedError` if any gate fails.

    ```bash
    cf-build release 2026a --config build.toml --out-dir releases --sample sample/design.parquet --previous releases/2025a/assemble
    ```

    The command writes `releases/climate-finance-2026a.tar.zst` and records the gate rows in `release.json`. To pack with failed precision or human-audit gates, pass `--waive-precision` with the reason. The flag marks the failed rows `waived`, sets `precision_gate_waived` in `release.json`, which surfaces in `release.quality().precision`, and records the reason as `precision_waiver`.

6. Publish the pack and register it in `releases.json`, which maps each release name to its URL and sha256 and marks the latest release. Call `release.upload_release` and `release.add_to_index` in `climate_finance.build`. Both raise `GateFailedError` for a pack with any recorded gate row that neither passed nor was waived, and for a waiver without its reason, so `upload_release` puts no such pack at its URL. `add_to_index` also raises `BuildError` when its `release` differs from the release the pack holds. The pack's `sources.json` records each source's licence and the rows excluded from the release.

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
- When the drift gate fails on a provider that changed for a known reason, add an entry to `drift.causes` in `gates.yaml` with the release, provider, and year.

## Next

- [Release gates](../reference/release-gates.md) lists each gate's checks and thresholds.
- [Release tables](../reference/release-tables.md) describes the files in the pack and each source's licence terms.

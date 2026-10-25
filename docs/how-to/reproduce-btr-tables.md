# How to reproduce the BTR tables

A published release ships the BTR reconciliation totals (`btr_totals` and `btr_crs_unlinked`) and leaves out the parsed rows of each party's Biennial Transparency Report (`btr`) and their links to CRS records (`btr_links`). The UNFCCC's terms make its documents public domain only where no change to the content is introduced, and parsed rows change it. Build the two tables locally from workbooks you download yourself, and `release.reconcile()` then works row by row on that build.

## Before you start

- Install the `build` extra with `pip install "climate-finance[build]"`.
- Have a `build.toml` and the bulk CRS file, set up as in [How to cut a release](cut-a-release.md).
- Download the workbooks in a browser from the UNFCCC's [first Biennial Transparency Reports](https://unfccc.int/first-biennial-transparency-reports) page. For each party, take the common tabular format tables on support, the workbook that holds Table III.1. Canada and the EU publish theirs as a zip of workbooks. Keep the zip whole, since the build reads it as it is.

## Steps

1. Put the workbooks in one directory, `data/raw/unfccc_btr1` in this example, and write `manifest.json` beside them. The manifest lists each file once, with the party's name as the UNFCCC writes it, the file name, its size in bytes, and whether the download succeeded.

   ```json
   [
     {
       "party": "Germany",
       "name": "Germany__DEU-CTF-FTC-2024.xlsx",
       "bytes": 489598,
       "ok": true
     },
     {
       "party": "Norway",
       "name": "Norway__NOR-CTF-FTC-2024.xlsx",
       "bytes": 245580,
       "ok": true
     }
   ]
   ```

   `wc -c <file>` gives the size. The build refuses a listed file that is missing, a file whose size differs from the manifest, an entry marked `"ok": false`, and a party name it has no ISO 3 code for.

2. Point the build at the directory. In `build.toml`, set `btr_archive` under `[sources]`.

   ```toml
   [sources]
   btr_archive = "data/raw/unfccc_btr1"
   ```

3. Check that every workbook parses before you run the whole build. `ingest-unfccc` reads each workbook the manifest lists and writes the rows to one parquet file.

   ```bash
   cf-build ingest-unfccc data/raw/unfccc_btr1 --out btr.parquet
   ```

   A workbook whose layout it cannot read, whose amounts contradict the party's unit, or whose status falls outside `committed` and `disbursed` stops the read with the file named. A draft and a final copy that give the same party's year also stop it, so keep one copy of each.

4. Build. Every build writes `btr`, `btr_links`, `btr_totals`, and `btr_crs_unlinked` into `releases/<RELEASE>/assemble/` beside the other tables. Only the packed release that `cf-build release` writes leaves out `btr` and `btr_links`, so open the assemble directory and not a pack.

   ```bash
   cf-build build 2026a-btr --config build.toml --out-dir releases
   ```

5. Open the build and reconcile row by row.

   ```python skip
   import climate_finance as cf

   release = cf.load(path="releases/2026a-btr/assemble")
   reconciliation = release.reconcile(parties=["DEU"], years=[2021, 2022])
   rows = reconciliation.rows("DEU", 2021)
   ```

## Verify it worked

`release.table("btr")` holds rows for each party you downloaded. The build's `normalise/btr_workbooks.parquet` lists each workbook with the rows kept and dropped by reason, and the `sources.json` of a pack that `cf-build release` writes carries the same list. A party whose workbook gives no rows has every row dropped there, most often because its amounts are blank or a notation key such as "NR".

## Next

- [How to reconcile a party's BTR report with OECD data](reconcile-with-btr.md) reads the comparison.
- [Release tables](../reference/release-tables.md) lists the columns of `btr` and `btr_links`.

# Trace Japan's climate finance to Bangladesh

It takes about ten minutes to query a figure, read its value and low bound, trace the CRS records behind it, and check how much of Japan's climate finance the package links.

The examples run on the small synthetic release from the package tests, so the amounts are invented. You need Python 3.12 or later and `pip install climate-finance`.

## Step 1. Load a release

A release is a folder of parquet tables. `load()` opens the latest release in the index shipped with the package, and `load(path=...)` opens a release directory on disk. On first run `load()` downloads the files to `~/.cache/climate-finance` and checks each table's sha256 against `release.json`.

```python
import climate_finance as cf

release = cf.load()
print(release)
```

```output
Release('fixture', synthetic figures): commitments 2021 to 2022, disbursements 2021 to 2024; 17 tables
```

The line names the release, marks its figures as synthetic, and gives the years each flow covers. Every result from `release` records the release name, so a figure always identifies its source data.

## Step 2. Ask for a figure

`spending()` takes providers and recipients by name or DAC code. It returns disbursements under ONE's method by default.

```python
result = release.spending(
    providers="Japan",
    recipients="Bangladesh",
    years=range(2021, 2024),
)
for row in result.table.iter_rows(named=True):
    print(row["year"], round(row["value"] / 1e6, 1), round(row["low"] / 1e6, 1))
```

```output
2021 13.3 0.0
2022 18.3 0.0
2023 48.3 12.0
```

The columns are year, climate `value` in USD million, and the `low` bound. In 2021 the value is 13.3 and the low bound is 0.0, because an estimated link connects the disbursement to its commitment. Only certain links, which rest on the donor's own ids, count in the low bound. The 2023 value includes 8.0 of untraced disbursement, the part of one CRS record's disbursement that no commitment could take, which the `untraced` column shows. The table also holds a `high` bound, explained in [Low and high bounds](explanation/low-and-high-bounds.md).

## Step 3. See the records behind the figure

`rows()` returns one row per CRS record allocated to a commitment. Filter it to disbursements and print the link method for each.

```python
for row in result.rows(origin="disbursement").iter_rows(named=True):
    print(row["year"], row["record_id"], row["link_method"], row["status"])
```

```output
2021 JPN-1-R0 weak_classifier partly_disbursed
2022 JPN-1-R1 splink_jev partly_disbursed
2022 JPN-3-R0 crosswalk partly_disbursed
2023 JPN-1-R2 title_recipient partly_disbursed
2023 JPN-SPLIT crs_id partly_disbursed
2023 JPN-3-R1 crosswalk_amount partly_disbursed
2023 JPN-SPLIT pn_recipient partly_disbursed
```

A link is certain when two conditions hold. First, it is a `crs_id`, `crs_id_purpose`, `crs_id_parent`, or `wb_loan` link whose records agree on recipient and project number, or a record's link to its own commitment. Second, the commitment's CRDF row reached its CRS commitment by a certain route. In this release only the 2023 `crs_id` link qualifies, which gives the 12.0 in the `low` column above. Every other method estimates the link from a project number, a title, a trained model, or a crosswalk, and counts in `value` alone. The synthetic release also carries `weak_classifier`, `crosswalk_amount`, and `title_recipient` as disbursement links, which the schema permits and no build writes. [How linking works](explanation/how-linking-works.md) lists the methods a build writes.

`status` belongs to the commitment. `partly_disbursed` means its linked records cover less than 90% of the matched CRS commitment's face value.

## Step 4. Read the method

`explain()` writes the method, view, flow, filters, and currency in plain text, so a table pasted into a report carries its own footnote.

```python
print(result.explain().splitlines()[-2:])
```

```output
['Filtered to providers Japan; recipients Bangladesh; years 2021 to 2023.', 'Amounts in USD at current prices.']
```

Print the full string to see how `value` is calculated. Significant Rio markers count at 40% of face value, principal markers count at 100%, and each activity counts once under its higher marker.

## Step 5. Change what is counted

`spending()` counts disbursements in the recipient view by default. `flow` switches to commitments or grant equivalents. `view="provider"` adds a provider's core contributions to multilateral agencies, multiplied by each agency's climate share.

```python
for flow in ("commitments", "disbursements"):
    flows = release.spending(providers="Japan", years=[2021, 2022], flow=flow)
    print(flow, round(flows.table["value"].sum() / 1e6, 1))
```

```output
commitments 130.0
disbursements 38.0
```

Japan's climate commitments for 2021 and 2022 total 130.0. The 38.0 is what Japan disbursed in those two years against climate commitments of any year the release holds, so the two figures measure different money. `oda_only=True`, `currency=`, and `prices=` filter or convert a query the same way. [Python API](reference/python-api.md) lists the parameters.

## Step 6. Ask for one objective

The table splits `value` into `adaptation`, `mitigation`, `cross_cutting`, and `unsplit` columns, so an adaptation question needs no filter. What the columns hold depends on the method's `highest_marker` rule. ONE and PROVIDER set it. Each activity counts once, under its higher Rio marker, so `adaptation` holds the activities whose adaptation marker is the higher, `cross_cutting` holds those with equal markers, and adaptation finance is the sum of the two. The recipe below applies to these two methods.

```python
import polars as pl

germany = release.spending(providers="Germany", years=range(2021, 2024))
adaptation = (
    germany.table.group_by("year")
    .agg(pl.sum("adaptation", "cross_cutting"))
    .sort("year")
)
for row in adaptation.iter_rows(named=True):
    print(
        row["year"],
        round(row["adaptation"] / 1e6, 1),
        round(row["cross_cutting"] / 1e6, 1),
    )
```

```output
2021 12.7 0.0
2022 15.2 1.7
2023 12.7 1.7
```

Under ONE, Germany's adaptation finance for 2022 is 15.2 million USD, or 16.8 with the cross-cutting activities. OECD (`method=cf.methods.OECD`) counts each marker on its own, so its `adaptation` holds every activity marked for adaptation and its `cross_cutting` is the overlap with mitigation. Under OECD, adaptation finance is the `adaptation` column alone, and adding `cross_cutting` counts the overlap twice. `method.highest_marker` says which rule a method follows. `low` and `high` bound only `value`, the total over all objectives.

## Step 7. Check how much traces

`quality()` reports how much of a provider's climate value matches a CRS commitment, and how its commitments split by status.

```python
summary = release.quality(providers="Japan").summary().row(0, named=True)
print(
    round(summary["link_rate"], 2),
    round(summary["disbursed_share"], 2),
    round(summary["partly_disbursed_share"], 2),
)
```

```output
1.0 0.31 0.69
```

Every Japanese commitment in this release matches a CRS commitment (`link_rate` 1.0), which says nothing yet about what disbursed. Commitments disbursed to 90% or more hold 31% of the climate value, and partly disbursed commitments hold 69%.

This synthetic release skips the blind-sample gate, so `release.quality().precision` is empty. In a published release, that table holds each provider's measured link precision and interval.

## Next

- [About the methods](explanation/methods.md) compares ONE, OECD, and PROVIDER weights for the same query.
- [Low and high bounds](explanation/low-and-high-bounds.md) explains which rows set each bound.

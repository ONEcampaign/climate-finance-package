# How to reconcile a party's BTR report with OECD data

Reconcile a party's Biennial Transparency Report (BTR) total against the OECD figures for the same party and year to find where the two disagree, or to check whether a figure from `spending()` is close to what the donor told the UNFCCC. The comparison covers the bilateral total of Table III.1. The workbooks' Tables III.2 and III.3 are unread.

A published release holds the reconciliation aggregates, so `snap.reconcile()` compares totals there. The BTR rows behind them, and `rows(party, year)`, need a local build from the workbooks, which [How to reproduce the BTR tables](reproduce-btr-tables.md) sets up.

## Before you start

Load a release. Parties are ISO 3 codes, such as `"DEU"`, `"NOR"`, and `"EUU"`. The examples run on the synthetic test release, so their numbers are invented.

```python
import climate_finance as cf

snap = cf.load()
```

## Steps

1. Reconcile the parties and years you need. `parties=None` compares every party in the release, and `years` defaults to 2015 to 2022.

   ```python
   reconciliation = snap.reconcile(parties=["DEU", "NOR"], years=[2021, 2022])
   table = reconciliation.table.to_polars()
   for row in table.iter_rows(named=True):
       ratio = row["btr_over_comparator"]
       print(
           row["party"],
           row["year"],
           row["status"],
           row["comparator"],
           ratio and round(ratio, 2),
       )
   ```

   ```output
   DEU 2021 committed crdf_at_party_weight 0.55
   DEU 2022 committed crdf_at_party_weight 0.88
   NOR 2021 disbursed crs_disbursed_at_party_weight 0.66
   NOR 2022 disbursed crs_disbursed_at_party_weight 0.66
   ```

   A party can report some activities as committed and others as disbursed in one year. The table holds one row per party, year, and status, and a row with no status sits on its own row and reads against all four comparators, with a null `comparator`. `btr_face` is the BTR total for that status.

2. Read each row against the comparator its `comparator` column names, with its ratio in `btr_over_comparator`. A committed row compares with the party's CRDF commitments at its own weight, and a disbursed row with its CRS disbursements on Rio-marked records at the same weight. The disbursement comparator reads each record's higher marker, so the both-significant coefficient does not apply to it. The other flow's ratios are null on the row. For a party that reports grant equivalents, `btr_grant_equivalent_over_crs_grant_equivalent` sets its grant-equivalent total against the CRS grant equivalent at the same weight. A party with no weight gets a null grant-equivalent comparator and ratio.

   `crdf_at_party_weight` weighs the party's CRDF commitments at its significant (`party_weight`) and principal (`party_principal`) coefficients from `provider_coefficients`, and at Germany's coefficient for activities significant on both objectives. `weight_source` names the document they come from. A party that assesses each activity case by case and states no default has null weighted comparators.

   The BTR side converts each amount from the party's own currency at the DAC exchange rate of the year, the rate the OECD side uses, and the row shows the rate the party's own USD figure implies. A row with `thin_comparator` true sets the BTR total against less than USD 10m, so its ratio says little.

3. Find where a gap comes from. `unfccc_only_usd` sums the BTR rows that no CRS record links to at a probability of 0.5 or more, split by the BTR funding source into ODA, other official flows, and the rest. `oecd_only_usd` sums the party-year's climate-marked CRS records of the row's flow that no BTR row links to. Read the two together: a large amount on both sides of one year usually means links the build missed, so an unlinked BTR row is not evidence that the OECD lacks the activity.

4. On a local build, list the rows behind a gap. `rows(party, year)` returns every BTR row of that party and year with each CRS record linked to it, and prefixes the CRS facts with `crs_`. `unfccc_only` is true for a BTR row that no CRS record links to.

   ```python
   linked = reconciliation.rows("DEU", 2021)
   print(linked.select("row_id", "record_id", "unfccc_only").rows())
   ```

   ```output
   [('BTR-DEU-2021', 'DEU-B-R1', False), ('BTR-DEU-2021', 'DEU-B-R2', False)]
   ```

## What the comparison measures

A BTR row links to a CRS record at a probability of 0.5 or more. The share of a party's reported dollars that links is a link rate, and it says nothing about whether the two amounts agree. In the build behind the release, 78.2% of the dollars Norway, Germany, and the UK report link, and 72% of all links come from the BTR Splink model.

Both sides read years as calendar years, as the workbooks and the CRS give them. Australia reports a July to June year, and Japan labels some titles by its fiscal year, so their years can shift by part of a year against the CRS.

`sources.json` lists each workbook the build read, with its party, the sheets read, and the rows kept and dropped by reason, such as blank amounts or a notation key like "NR". A party whose workbook gives no rows has none to compare.

## Verify it worked

Every party you asked for appears in `table["party"]`. A missing party has no BTR rows in the build, and a missing year has none for that year.

## Troubleshooting

- All comparators are null. The party reports no CRS data and has no DAC provider code.
- The ratio is far from 1. Check `thin_comparator`, then read `unfccc_only_usd` beside `oecd_only_usd`, and on a local build read `rows(party, year)`.

## Next

- [Python API](../reference/python-api.md) lists every column of the reconciliation table.
- [About the methods](../explanation/methods.md) explains provider coefficients.

# How to reconcile a party's BTR report with OECD data

Reconcile a party's Biennial Transparency Report (BTR) total against the OECD figures for the same party and year to find where the two disagree, or to check whether a figure from `spending()` is close to what the donor told the UNFCCC. The comparison covers the bilateral total of Table III.1. The workbooks' Tables III.2 and III.3 are unread.

A published release holds the reconciliation aggregates, so `release.reconcile()` compares totals there. The BTR rows behind them, and `rows(provider=..., year=...)`, need a local build from the workbooks, which [How to reproduce the BTR tables](reproduce-btr-tables.md) sets up.

## Before you start

Load a release. `providers=` names parties as `spending()` names providers, so `"Germany"`, `"DEU"`, and `5` name one party, and `"EU Institutions"` and `"EUU"` the EU. The examples run on the synthetic example release, so their numbers are invented. `cf.load()` opens the published release.

```python
import climate_finance as cf

release = cf.load_example()
```

## Steps

1. Reconcile the parties and years you need. `providers=None` compares every party in the release, and `years=None` every year the BTR tables hold.

   ```python
   reconciliation = release.reconcile(providers=["Germany", "Norway"], years=[2021, 2022])
   table = reconciliation.table
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
   NOR 2021 disbursed crs_disbursed_at_party_weight 0.73
   NOR 2022 disbursed crs_disbursed_at_party_weight 0.73
   ```

   A party can report some activities as committed and others as disbursed in one year. The table holds one row per party, year, and status, and a row with no status sits on its own row and reads against all four comparators, with a null `comparator`. `btr_face` is the BTR total for that status.

2. Read each row against the comparator its `comparator` column names, with its ratio in `btr_over_comparator`. A committed row compares with the party's CRDF commitments at its own weight, and a disbursed row with its CRS disbursements on Rio-marked records at the same weight. The other flow's ratios are null on the row. For a party that reports grant equivalents, `btr_grant_equivalent_over_crs_grant_equivalent` sets its grant-equivalent total against the CRS grant equivalent at the same weight.

   The weighted comparators weigh the party's CRDF commitments and CRS records as `PROVIDER` does, by each agency's coefficients from `provider_coefficients` and the provider's for an agency without its own, and at Germany's coefficient for commitments and records significant on both objectives. Where the coefficients are case by case with no default, at the agency level as for France's AFD or the provider level as for the UK, Belgium, Finland, and the US, the activity counts at the coefficient the party's own BTR implies (`btr_coefficients`), else at `PROVIDER`'s 0.4 for Significant and 1.0 for Principal, as it does in `spending()`. The BTR coefficient leaves out activities whose BTR amount exceeds their CRS amount by more than 10%, such as the UK's USD 728m for BII's capital increase against USD 265m disbursed, so the UK's comparators weigh BII at the UK's significant coefficient like any other significant activity, and its ratio still shows the gap. A comparator weighed at a coefficient taken from the BTR rows it is set against agrees with them partly by construction, so read its ratio as a check on the links and the years more than on the coefficients. `party_weight` and `party_principal` show the provider's significant and principal coefficients, and `weight_source` names where they come from. `coefficient_basis` reads `provider` for a provider's flat coefficients, `btr` for a significant coefficient taken from the BTR, `default` for the default share of a provider that assesses case by case, and `case_by_case` where it gives neither. A party whose provider has no coefficients in the release weighs at `PROVIDER`'s coefficients throughout, with a null `coefficient_basis`.

   `crdf_method_share`, `crs_method_share`, and `crs_grant_equivalent_method_share` say how much of the CRDF, CRS disbursed, and CRS grant-equivalent comparators rests on `PROVIDER`'s own coefficients in place of the party's, stated or taken from its BTR. A share near 1 means the ratio compares the BTR with the method's assumption about the party, and a share near 0 that it compares with the party's own coefficients. France's and Canada's shares for 2021 and 2022 stay under 0.01: AFD assesses each activity case by case and takes the coefficients France's BTR implies. The US, whose BTR rows the release links to no CRS record, reads 1.0.

   The BTR side converts each amount from the party's own currency at the DAC exchange rate of the year, the rate the OECD side uses, and the row shows the rate the party's own USD figure implies. A row with `thin_comparator` true sets the BTR total against less than USD 10m, so its ratio says little, and a comparator of zero gives a null ratio.

3. Find where a gap comes from. `unfccc_only_usd` sums the BTR rows that no CRS record links to at a probability of 0.5 or more, split by the BTR funding source into ODA, other official flows, and the rest. `oecd_only_usd` sums the party-year's climate-marked CRS records of the row's flow that no BTR row links to, and is null for a year past the release's last complete CRS year. Read the two together: a large amount on both sides of one year usually means links the build missed, so an unlinked BTR row is not evidence that the OECD lacks the activity.

4. On a local build, list the rows behind a gap. `rows(provider=..., year=...)` returns every BTR row of that party and year with each CRS record linked to it, and prefixes the CRS facts with `crs_`. `unfccc_only` is true for a BTR row that no CRS record links to.

   ```python
   linked = reconciliation.rows(provider="Germany", year=2021)
   print(linked.select("row_id", "record_id", "unfccc_only").rows())
   ```

   ```output
   [('BTR-DEU-2021', 'DEU-B-R1', False), ('BTR-DEU-2021', 'DEU-B-R2', False)]
   ```

## What the comparison measures

A BTR row links to a CRS record at a probability of 0.5 or more. The share of a party's reported dollars that links is a link rate, and it says nothing about whether the two amounts agree. In the build behind the release, 61.5% of the dollars the BTR parties report link, and 49% of all links come from the BTR Splink model.

A party's BTR can leave out a programme that the OECD data counts as climate finance. Germany's BTR holds no rows for a Federal Ministry of Finance loan programme of about USD 1.25bn to 1.4bn a year, which the OECD marks significant on both objectives and Germany's coefficients count in full. In 2022 the programme lowers Germany's ratio from 0.90 to 0.79, and in 2021, when the CRDF carries it and the CRS does not, from 1.20 to 0.99, so the ratio moves with the programme. In a year the CRS carries such a programme, it shows in `oecd_only_usd`, and comparing the party's climate-marked flows in `release.table("total_flows")` by `agency_code` across years names the agency: Germany's Finance Ministry rises from under USD 0.05bn in 2021 to over USD 1.2bn in 2022.

A party's BTR can count its development finance institution at another stage than the OECD figures do. The CRS comparators count the capital a provider pays into the institution and leave out the investments it reports for memorandum, which `crs_memorandum_disbursed_at_party_weight` shows. Norway's BTR lists Norfund's investments one by one, so in 2021 its USD 699m of disbursements read 1.40 against a CRS comparator of USD 499m, and the memorandum column's USD 197m covers the gap. Switzerland's BTR claims the capital paid into SIFEM, as the comparator does. Sweden's claims USD 32m of capital paid into Swedfund in 2021, which the CRS gives no Rio marker, so Sweden reads 1.05 against a comparator that holds neither the capital nor Swedfund's investments.

Both sides read years as calendar years, as the workbooks and the CRS give them. Australia reports a July to June year, and Japan labels some titles by its fiscal year, so their years can shift by part of a year against the CRS.

`sources.json` lists each workbook the build read, with its party, the sheets read, and the rows kept and dropped by reason, such as blank amounts or a notation key like "NR". A party whose workbook gives no rows has none to compare.

## Verify it worked

Every party you asked for appears in `table["party"]`. A missing party has no BTR rows in the build, and a missing year has none for that year.

## Troubleshooting

- All comparators are null. The party reports no CRS data and has no DAC provider code.
- The ratio is far from 1. Check `thin_comparator`, then read `unfccc_only_usd` beside `oecd_only_usd`, and on a local build read `reconciliation.rows(provider=..., year=...)`.

## Next

- [Python API](../reference/python-api.md) lists every column of the reconciliation table.
- [About the methods](../explanation/methods.md) explains provider coefficients.

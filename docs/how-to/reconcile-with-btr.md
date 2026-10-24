# How to reconcile a party's BTR report with OECD data

Reconcile a party's Biennial Transparency Report (BTR) total against the OECD figures for the same party and year to find the projects the two disagree on, or to check whether a figure from `spending()` is close to what the donor told the UNFCCC. The release must hold BTR rows, which a build includes only when its configuration names a BTR archive.

## Before you start

Load a release. Parties are ISO 3 codes as the `btr` table holds them, such as `"DEU"`, `"NOR"`, and `"EUU"`.

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
        print(
            row["party"], row["year"], row["status"], round(row["btr_over_crdf_at_100"], 2)
        )
    ```

    ```output
    DEU 2021 disbursed 0.55
    DEU 2022 disbursed 0.88
    NOR 2021 disbursed 0.83
    NOR 2022 disbursed 1.67
    ```

    A party can report an activity as both committed and disbursed. Each row compares the party's main status for the year, which is the status carrying the most face value. `btr_face` is the BTR total for that status.

2. Pick the comparator that fits what the party reports. A party that reports commitments reads against `crdf_at_party_weight`, `crdf_at_100`, and `traced_climate_value`. A party that reports disbursements reads against `crs_disbursed_at_party_weight`. Each comes with its ratio, `btr_over_<comparator>`.

    - `crdf_at_100` is the party's CRDF commitments with Significant markers at 100%.
    - `crdf_at_party_weight` weights Significant markers at the party's own coefficient, `party_weight`, from the release's `provider_coefficients`. `weight_confirmed` says whether a reviewer confirmed that weight. A party without a weight has null weighted comparators.
    - `traced_climate_value` is the climate value that `method` gives the commitments the party disbursed.
    - `crs_disbursed_at_party_weight` is the year's CRS disbursements on Rio-marked records, weighted the same way.

3. Find the projects behind a gap. `rows(party, year)` returns every BTR row of that party and year with each CRS record linked to it, and prefixes the CRS facts with `crs_`. `unfccc_only` is true for a BTR row that no CRS record links to.

    ```python
    linked = reconciliation.rows("DEU", 2021)
    print(linked.select("row_id", "record_id", "unfccc_only").rows())
    ```

    ```output
    [('BTR-DEU-2021', 'DEU-B-R1', False), ('BTR-DEU-2021', 'DEU-B-R2', False)]
    ```

    The `unfccc_only_usd` column of the table sums the face value of those unlinked rows, showing how much of the gap lacks a matching CRS record.

## Verify it worked

Every party you asked for appears in `table["party"]`. A missing party has no BTR rows in the release, and a missing year has none for that year.

## Troubleshooting

- All comparators are null. The party reports no CRS data and has no DAC provider code.
- The ratio is far from 1. Read `rows(party, year)`. A large `unfccc_only_usd` means the party reports activities that no CRS record carries.

## Next

- [Python API](../reference/python-api.md) lists every column of the reconciliation table.
- [About the methods](../explanation/methods.md) explains party weights and the Significant coefficient.

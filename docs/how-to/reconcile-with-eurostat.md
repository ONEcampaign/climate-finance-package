# How to reconcile EU figures with Eurostat

Set each EU member state's, the European Commission's and the European Investment Bank's climate finance in the release against Eurostat's `sdg_13_50`, the figures the EU reports towards its international climate finance commitment, to find where the two disagree. Eurostat publishes a year before the members' Biennial Transparency Reports, so the comparison is the first check on a new year.

## Before you start

Load a release. The examples run on the synthetic example release, whose Eurostat lines are invented to suit its amounts. `cf.load()` opens the published release, which holds Eurostat's published figures.

```python
import climate_finance as cf

release = cf.load_example()
```

## Steps

1. Reconcile the lines and years you need. `providers=None` compares every line, and `years=None` every year from 2020 to 2024 the release holds Eurostat figures for. `providers=` names lines as `spending()` names providers: `"Germany"`, `"DEU"`, and `5` name Germany's line, `"EU Institutions"` both the Commission's and the EIB's, and `"European Commission"` or `"EIB"` one of them.

   ```python
   eurostat = release.reconcile_eurostat()
   for row in eurostat.table.iter_rows(named=True):
       print(
           row["geo"],
           row["year"],
           round(row["eurostat_eur"] / 1e6, 1),
           round(row["release_eur"] / 1e6, 1),
           round(row["gap_share"], 2),
       )
   ```

   ```output
   DE 2021 65.0 59.9 0.08
   DE 2022 38.0 42.4 -0.11
   EIB 2022 3.0 0.0 1.0
   FR 2021 40.0 40.2 -0.01
   FR 2022 25.0 17.5 0.3
   ```

   Each row is one Eurostat line and year, keyed by Eurostat's `geo` code. `EU27_2020`, the sum of the 27 members, has no row, since its members have theirs. Amounts are EUR at current prices, converted as `spending(currency="EUR")` converts them.

2. Read `gap_eur`, Eurostat's figure less the release's, and `gap_share`, the gap over Eurostat's figure. A positive gap means Eurostat counts more than the release. The example holds no EIB rows, so the EIB's whole line is a gap.

3. Split a member's gap by side. `release_bilateral_eur` is the member's bilateral commitments, and `release_multilateral_eur` the climate value of its core contributions to multilateral agencies, at each agency's climate share.

   ```python
   germany = release.reconcile_eurostat(providers="Germany", years=2022).table
   row = germany.row(0, named=True)
   print(
       row["basis"],
       round(row["release_bilateral_eur"] / 1e6, 1),
       round(row["release_multilateral_eur"] / 1e6, 1),
   )
   ```

   ```output
   bilateral_committed_multilateral_provided 22.7 19.6
   ```

   From 2022 the multilateral side counts what Germany's contributions disbursed. Counted as committed, as in 2021, the same contributions give EUR 23.6m.

## What the comparison measures

The release side is fixed, so the query takes no `method`. It weighs every row at `ONE`'s provider coefficients with EU Institutions counted as a provider, `cf.methods.ONE.replace(eu="provider")`, which the package names `query.EUROSTAT_METHOD`. Eurostat counts the EU budget and the European Development Fund under the Commission and the EIB's lending under the EIB, so members' contributions to all three count nothing in the members' lines, and the Commission's and the EIB's lines hold the money once.

`basis` follows Eurostat's metadata. For 2020 and 2021 it is `commitments`, and the release side counts members' core contributions as committed. From 2022 it is `bilateral_committed_multilateral_provided`, and the release side counts them as disbursed. Bilateral money counts as committed under both. Eurostat documents no basis before 2020, so the comparison starts there.

The Commission's line, `COMM`, holds EU Institutions agencies 1 and 2, the Commission's budget and the European Development Fund, which match it within 3% from 2020 to 2022. The EIB's line holds agency 3, whose own commitments count here although `spending()` leaves them out of the provider view under `eu="provider"`.

A release column is null for a year the release's tables hold no rows for. The tables are `commitments` for the bilateral side and `core_contributions` of the year's flow for the multilateral side.

In the build behind the release, the lines sum to EUR 31.73bn for 2024 in Eurostat and EUR 25.67bn in the release, a gap of EUR 6.06bn. Three lines carry most of it. The EIB reads EUR 0.60bn against Eurostat's EUR 2.40bn, because the release counts 29 of its 39 2024 CRDF rows as repeats of earlier commitments with the same project number and face value, and Eurostat counts them again. Germany reads EUR 1.62bn short of Eurostat's EUR 10.69bn, and Spain EUR 1.35bn short of EUR 1.73bn.

## Troubleshooting

- A `CoverageWarning` names a provider. Eurostat covers EU member states, the Commission and the EIB. A member's agency, such as KfW, has no line of its own, so name the member.
- A `CoverageWarning` names years. The comparison runs from 2020 to 2024, the years whose basis Eurostat documents, and only where the release holds Eurostat's figures.

## Next

- [How to reconcile a party's BTR report with OECD data](reconcile-with-btr.md) sets each party's own report against the release.
- [Python API](../reference/python-api.md) lists every column of the comparison.

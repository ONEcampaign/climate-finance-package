# How to reproduce the OECD's public climate finance total

Build the release's version of the figure the OECD reports for climate finance provided by developed countries: their bilateral finance plus the part of each development bank's and fund's climate outflows that the OECD attributes to them. Then split it into bilateral finance, multilateral development banks and multilateral climate funds, and set it against the OECD's total. [About the methods](../explanation/methods.md#attribution-by-outflows) explains the rule.

## Before you start

Load a release. The examples run on the synthetic example release, so their amounts are invented. It holds Table 8 shares for 2021 and 2022 only, and three institutions, all development banks, so it has no climate-fund row. `cf.load()` opens the published release, whose Table 8 shares cover 2013 to 2024.

```python
import polars as pl

import climate_finance as cf

release = cf.load_example()
year = 2022
```

## Steps

1. List the developed countries and the type of each institution. The release holds the countries in `developed_providers`, the OECD's Table 7. It holds the types in `outflow_shares`, which carries Table 8's "Type of institution" as `institution_type`: `development_bank` for the multilateral development banks and `climate_fund` for the multilateral climate funds.

   ```python
   developed = release.table("developed_providers").collect()["provider_code"].to_list()
   types = (
       release.table("outflow_shares")
       .select(
           via_provider_code="provider_code",
           agency_code="agency_code",
           year="year",
           institution_type="institution_type",
       )
       .collect()
   )
   print(len(developed), types["institution_type"].unique().to_list())
   ```

   ```output
   4 ['development_bank']
   ```

2. Run the provider view with `attribution="outflows"` for every provider, and keep the rows of the developed countries. Use `method="PROVIDER"`, which applies each donor's own marker coefficients. `flow="commitments"` gives the figure comparable with the OECD's, and `flow="disbursements"` the money paid in the year.

   ```python
   def oecd_rows(flow):
       spending = release.spending(
           years=year,
           method="PROVIDER",
           flow=flow,
           view="provider",
           attribution="outflows",
       )
       return spending.rows().filter(
           pl.col("provider_code").is_in(developed) | pl.col("provider_code").is_null()
       )
   ```

   The filter keeps rows with no provider. An institution that no developed country had funded by that year keeps its attributed share in rows with no provider, and the OECD's total counts them. `providers=developed` in the query would drop them.

3. Split each flow by where the money comes from. A bilateral row has no `via_provider_code`. An attributed row carries the institution it came through in that column, and the join gives it that institution's type. The EIB's rows carry EU Institutions' code 918 and agency 3, so the join keys on the agency too.

   ```python
   def split(rows):
       joined = rows.join(
           types,
           on=["via_provider_code", "agency_code", "year"],
           how="left",
           nulls_equal=True,
       )
       group = (
           pl.when(pl.col("via_provider_code").is_null())
           .then(pl.lit("bilateral"))
           .otherwise(pl.col("institution_type"))
       )
       return joined.group_by(group.alias("group")).agg(pl.col("value").sum())


   for flow in ["commitments", "disbursements"]:
       parts = split(oecd_rows(flow))
       print(flow, round(parts["value"].sum() / 1e6, 2))
       for row in parts.sort("value", descending=True).iter_rows(named=True):
           print("  ", row["group"], round(row["value"] / 1e6, 2))
   ```

   ```output
   commitments 166.21
      bilateral 94.9
      development_bank 71.31
   disbursements 88.47
      bilateral 53.02
      development_bank 35.45
   ```

   Divide by `1e9` for USD bn on the published release.

## Verify it worked

The total equals the query for the developed countries plus the rows with no provider.

```python
spending = release.spending(
    providers=developed,
    years=year,
    method="PROVIDER",
    flow="commitments",
    view="provider",
    attribution="outflows",
)
unshared = oecd_rows("commitments").filter(pl.col("provider_code").is_null())
print(
    round(spending.table["value"].sum() + unshared["value"].sum())
    == round(oecd_rows("commitments")["value"].sum())
)
```

```output
True
```

On the example release no institution lacks a developed contributor, so `unshared` is empty. On the 2026a release candidate it holds USD 0.04bn of 2024 commitments and USD 0.06bn of disbursements.

## The 2024 figures against the OECD's

On the 2026a release candidate, the steps above with `year = 2024` give these figures in USD bn. The OECD column is Table 1 of _Climate Finance Provided and Mobilised by Developed Countries in 2013-2024_ (May 2026).

| Source                         | OECD report | Release commitments | Release disbursements |
| ------------------------------ | ----------: | ------------------: | --------------------: |
| Bilateral                      |        43.9 |                36.1 |                  32.1 |
| Multilateral development banks |        50.5 |                47.6 |                  26.8 |
| Multilateral climate funds     |         7.2 |                 6.0 |                   1.6 |
| Total public                   |       101.6 |                89.7 |                  60.4 |

The OECD figure mostly counts commitments, so the commitments column is the comparison. It falls USD 11.9bn short: USD 7.8bn in bilateral finance, 2.9bn in the development banks and 1.2bn in the climate funds. The OECD reads bilateral finance from the reports donors send the UN, which hold amounts and climate shares the CRS does not, and the release reads the CRS alone, so the bilateral shortfall measures how far those reports go beyond the CRS. The release has no stated cause for the other two shortfalls.

The disbursements column is what was paid in 2024. It is two-thirds of the year's commitments, since payments in a year settle commitments from earlier years.

## Troubleshooting

- `CoverageWarning` that Table 8 gives no share for a year. A year without a share attributes no institution's outflows, so the total holds bilateral finance alone. The example release has shares for 2021 and 2022.
- A total USD 0.04bn to 0.06bn lower than this page's. The query named `providers=` with the developed countries and dropped the rows with no provider. Query every provider and filter the rows, as step 2 does.
- A different total under another method. The 2024 commitments are USD 87.8bn under `ONE`, 89.7bn under `PROVIDER`, and 108.8bn under `OECD`, which counts every marker at 100%. Name the method in the query, as the steps do.

## Next

- [About the methods](../explanation/methods.md#attribution-by-outflows) gives the rule, meaning which share each institution takes and how developed countries split it.
- [Python API](../reference/python-api.md) lists the arguments of `spending()` and the columns of `rows()`.

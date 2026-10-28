# How to compute one climate share across several keys

Compute the share of all CRS disbursements that is climate finance for several providers, recipients, or years together. `share_of_total()` returns one share per key. The share of the combined total is the sum of climate value over the sum of all disbursements, taken over every key that has disbursements, including the keys without a climate row. It differs from the average of the per-key shares.

## Before you start

Load a release. The examples take every recipient in 2021 to 2024 on the synthetic example release, so their amounts are invented. `cf.load()` opens the published release.

```python
import polars as pl

import climate_finance as cf

release = cf.load_example()
query = {
    "years": range(2021, 2025),
    "method": cf.methods.ONE,
    "flow": "disbursements",
}
result = release.spending(**query)
KEYS = ["provider_code", "recipient_code", "year", "bi_multi"]
```

A key is one provider, recipient, year, and Bi_Multi code. `result.table` holds a row for each key with climate value, untraced disbursement included, and no row for a key whose disbursements carry no Rio marker.

## Steps

1. Build the denominator from the release's `total_flows` table, which holds every CRS flow, climate or not. Take the rows of the query's flow, of the Bi_Multi codes its view counts, and of its providers, recipients, and years, and sum them by key. The recipient view counts Bi_Multi 1, 3, 7, and 8 for bilateral flows and 4 for multilateral outflows. Filter `total_flows` the same way `query` filters: add `pl.col("provider_code").is_in(...)` for `providers=` and `pl.col("recipient_code").is_in(...)` for `recipients=`.

   ```python
   totals = (
       release.table("total_flows")
       .filter(
           pl.col("flow") == "disbursements",
           pl.col("bi_multi").is_in([1, 3, 4, 7, 8]),
           pl.col("year").is_in(list(query["years"])),
       )
       .group_by(KEYS)
       .agg(total=pl.col("amount_usd").sum())
       .collect()
   )
   ```

2. Join the climate value to the denominator keys. A key with disbursements and no climate row gets a value of 0, so it stays in the denominator.

   ```python
   keyed = totals.join(
       result.table.select(*KEYS, "value"), on=KEYS, how="left"
   ).with_columns(pl.col("value").fill_null(0))
   print(keyed.height, keyed.filter(pl.col("value") == 0).height)
   ```

   ```output
   30 1
   ```

   The query returns 29 rows, and the denominator holds 30 keys. The missing key is Norway's USD 6.0m of 2023 disbursements to Kenya, none of it Rio-marked.

3. Divide the sums.

   ```python
   share = keyed["value"].sum() / keyed["total"].sum()
   print(round(share, 4))
   ```

   ```output
   0.1451
   ```

   Climate finance is 14.5% of the disbursements of these keys.

4. Compare it with the average of the per-key shares.

   ```python
   shares = result.share_of_total().table["value"]
   print(round(shares.mean(), 4))
   ```

   ```output
   0.1556
   ```

   The average gives a USD 4.8m key the same weight as a USD 200m key and leaves out the key without a row, so it reads 15.6%. The ratio of sums is the share of the combined disbursements.

## Convert to another currency or prices

A converted result multiplies each provider's amounts by the release's `conversion_factors` for the year, and the denominator takes the same factors. The table holds a provider's own series and, as a fallback, the DAC total under entity code 20001. For current prices, filter `base_year` to null; for constant prices, to the base year.

```python
eur = release.spending(**{**query, "currency": "EUR"})
factors = (
    release.table("conversion_factors")
    .filter(pl.col("currency") == "EUR", pl.col("base_year").is_null())
    .select("entity_code", "year", "factor")
    .collect()
)
own = factors.rename({"entity_code": "provider_code", "factor": "own"})
fallback = factors.filter(pl.col("entity_code") == 20001).select(
    "year", fallback="factor"
)
converted = (
    totals.join(own, on=["provider_code", "year"], how="left")
    .join(fallback, on="year", how="left")
    .with_columns(total=pl.col("total") * pl.coalesce("own", "fallback"))
)
print(round(eur.table["value"].sum() / converted["total"].sum(), 4))
```

```output
0.1447
```

The euro share differs from the dollar share because the factors vary by provider and year.

## Verify it worked

For each key with a climate row, the recipe's denominator gives the share `share_of_total()` returns, in dollars and in euros.

```python
for frame, shares in [
    (keyed, result.share_of_total()),
    (
        converted.join(eur.table.select(*KEYS, "value"), on=KEYS),
        eur.share_of_total(),
    ),
]:
    per_key = frame.join(shares.table.select(*KEYS, library="value"), on=KEYS)
    gap = (per_key["value"] / per_key["total"] - per_key["library"]).abs().max()
    print(gap < 1e-9)
```

```output
True
True
```

## Troubleshooting

- The ratio differs from `share_of_total()` for one key. The filters of `total_flows` differ from the query's. Match the flow, the Bi_Multi codes of the view, and every `providers=`, `recipients=`, and `years=` filter.
- `InvalidInputError` on `measure="share_of_total"`. A net flow takes no share, since the CRS totals hold no repayments. Run the gross flow.
- The provider view. Its totals count each EU Institutions channel under the method's EU setting and add attributed core contributions, which `total_flows` alone does not reproduce, so this recipe covers `view="recipient"`.
- `flow="commitments"`. The commitment totals count Bi_Multi 3, 7, and 8 as 1, so map those codes to 1 before the group-by.

## Next

- [Python API](../reference/python-api.md) describes `share_of_total()` and `measure`.
- [Release tables](../reference/release-tables.md#total_flows) lists the columns of `total_flows`.

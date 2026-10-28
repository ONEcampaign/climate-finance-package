# How to compute one climate share across several keys

Compute the share of all CRS flows that is climate finance for several providers, recipients, or years together. `result.combined_share()` divides the query's total climate value by the total of every flow in its scope, including the keys without a climate row. `share_of_total()` returns one share per key, and the average of those shares differs from the combined share.

## Before you start

Load a release and run the query. The examples take every recipient in 2021 to 2024 on the synthetic example release, so their amounts are invented. `cf.load()` opens the published release.

```python
import climate_finance as cf

release = cf.load_example()
query = {
    "years": range(2021, 2025),
    "method": cf.methods.ONE,
    "flow": "disbursements",
}
result = release.spending(**query)
```

## Steps

1. Take one share across the whole query. `combined_share()` returns one row per method. Each row carries the query's `flow`, `view`, `currency`, and `prices` as `result.table` holds them, so a saved share says what it measures, then the climate `value`, the `total` it divides by, and the `share`.

    ```python
    combined = result.combined_share()
    print(combined.columns)
    for row in combined.iter_rows(named=True):
        value, total = round(row["value"] / 1e6, 1), round(row["total"] / 1e6, 1)
        print(row["method"], row["currency"], value, total, round(row["share"], 4))
    ```

    ```output
    ['method', 'flow', 'view', 'currency', 'prices', 'value', 'total', 'share']
    ONE USD 288.3 1986.8 0.1451
    ```

    Climate finance is 14.5% of the disbursements of these providers and recipients. The total covers the query's flow, the Bi_Multi codes of its view, and every `providers=`, `recipients=`, and `years=` filter, at the query's currency and prices. It includes Norway's USD 6.0m of 2023 disbursements to Kenya, which carry no Rio marker and have no row in `result.table`.

2. Keep the key columns you want a share for. `by` names them, so `by="year"` gives one share per year.

    ```python
    by_year = result.combined_share(by="year")
    for year, share in by_year.select("year", "share").iter_rows():
        print(year, round(share, 4))
    ```

    ```output
    2021 0.1619
    2022 0.1403
    2023 0.1554
    2024 0.0986
    ```

    `by` takes the key columns of `result.table`: `year`, `provider_code`, `recipient_code`, `bi_multi`, and, for a query that names an agency, `agency_code` beside `provider_code`. The output puts the names of the code columns, such as `provider`, first.

3. Run several methods at once. Each method keeps its own share over the same total.

    ```python
    ranged = release.spending(**{**query, "method": cf.methods.RANGE})
    shares = ranged.combined_share().select("method", "share")
    for method, share in shares.iter_rows():
        print(method, round(share, 4))
    ```

    ```output
    ONE 0.1451
    OECD 0.1652
    PROVIDER 0.1525
    ```

The share is null where the total is zero, as for a query that returns no rows. Under `oda_only=True` the value counts ODA only and the total counts every flow code, so the share is ODA climate finance over all flows, as `share_of_total()` reads it.

## Why the average of per-key shares differs

`share_of_total()` divides each key by its own total. Their average gives a USD 4.8m key the same weight as a USD 200m key and leaves out the keys without a climate row.

```python
per_key = result.share_of_total().table["value"]
print(round(per_key.mean(), 4), round(combined["share"].item(), 4))
```

```output
0.1556 0.1451
```

The average reads 15.6%. The combined share, 14.5%, is the share of the combined disbursements.

## Verify it worked

Keeping every key column and dividing the sums gives the combined share again.

```python
keys = ["year", "provider_code", "recipient_code", "bi_multi"]
every_key = result.combined_share(by=keys)
print(round(every_key["value"].sum() / every_key["total"].sum(), 4))
```

```output
0.1451
```

## Troubleshooting

- `InvalidInputError` on a net flow. The CRS totals hold no repayments, so a net flow takes no share. Run the gross flow.
- `UnknownNameError` for a `by` column. `by` takes the code columns, such as `provider_code`, and the error suggests the closest.
- `InvalidInputError` for `by="agency_code"`. Agency codes repeat across providers, so keep `provider_code` with it.

## Next

- [Python API](../reference/python-api.md) describes `combined_share()`, `share_of_total()`, and `measure`.
- [Release tables](../reference/release-tables.md#total_flows) lists the columns of `total_flows`.

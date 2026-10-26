# How to compare gross and net climate finance

Subtract the principal and interest repaid on climate loans from a provider's disbursements, and check how much of its repayments the subtraction reaches. [About the methods](../explanation/methods.md#net-flows) explains how a repayment gets its loan's climate share.

## Before you start

Load a release. The examples compare France and Norway in 2021 to 2024 on the synthetic test release, so their amounts are invented.

```python
import polars as pl

import climate_finance as cf

release = cf.load()
query = {"providers": ["France", "Norway"], "years": range(2021, 2025)}
```

## Steps

1. Run the same query as a gross and a net flow, and set them side by side. `flow="net_disbursements"` subtracts principal, and `flow="net_transfers"` subtracts interest too.

    ```python
    gross = release.spending(**query)
    net = release.spending(**query, flow="net_disbursements")
    comparison = cf.compare(gross, net).table
    for provider, frame in comparison.group_by("provider", maintain_order=True):
        print(
            provider[0],
            round(frame["disbursements"].sum() / 1e6, 2),
            round(frame["net_disbursements"].sum() / 1e6, 2),
        )
    ```

    ```output
    France 2.64 1.92
    Norway 17.1 0.0
    ```

    A row in a year of repayments can read below zero. Norway's net flow in 2022 is USD -4.3m, as it repaid more that year than it disbursed to Bangladesh.

2. List the repayments behind a net figure. `rows(origin="repayment")` returns one row per repayment part, with a negative `value`, and `origin="interest"` the interest of a net transfer. `row_id` names the commitment whose climate share the repayment takes.

    ```python
    rows = net.rows(origin="repayment")
    by_year = rows.group_by("provider", "year").agg(pl.col("value").sum())
    for row in by_year.sort("provider", "year").iter_rows(named=True):
        print(row["provider"], row["year"], round(row["value"] / 1e6, 2))
    ```

    ```output
    France 2022 -0.2
    France 2023 -0.52
    Norway 2022 -10.0
    Norway 2023 -7.1
    ```

3. Check how much of the principal the subtraction reaches. `quality().repayments` holds each provider's principal by repayment year in `received_usd`, and `measured_share` is the part tied to a disbursement the release counts. The rest sits in the other `<basis>_usd` columns and enters no total.

    ```python
    repayments = release.quality(providers=query["providers"]).repayments
    for row in repayments.sort("provider", "year").iter_rows(named=True):
        print(
            row["provider"],
            row["year"],
            round(row["received_usd"] / 1e6, 1),
            round(row["measured_share"], 2),
        )
    ```

    ```output
    France 2022 0.5 1.0
    France 2023 1.3 1.0
    Norway 2022 10.0 1.0
    Norway 2023 10.4 0.68
    ```

    Norway's 2023 principal of USD 10.4m has a measured share of 0.68, so USD 3.3m of it repays loans that no total counts. A provider with a low measured share has a net figure that subtracts less than it repaid.

4. Read the interval. A net flow's `low` subtracts every attributed repayment and its `high` only those with a certain link to the loan, so `high` sits above `value` and is not the central figure.

    ```python
    table = net.table.group_by("provider").agg(pl.col("low", "value", "high").sum())
    for row in table.sort("provider").iter_rows(named=True):
        print(
            row["provider"],
            round(row["low"] / 1e6, 2),
            round(row["value"] / 1e6, 2),
            round(row["high"] / 1e6, 2),
        )
    ```

    ```output
    France -0.72 1.92 2.47
    Norway -17.1 0.0 17.95
    ```

## Verify it worked

The net `value` equals the gross `value` less the sum of the `repayment` rows, and with `flow="net_transfers"` less the `interest` rows too.

```python
repaid = net.rows(origin="repayment")["value"].sum()
print(round(gross.table["value"].sum() + repaid) == round(net.table["value"].sum()))
```

```output
True
```

## Troubleshooting

- A net flow equals the gross flow for a provider. The provider repaid nothing in the years queried, or `quality().repayments` shows its principal on bases that no total counts.
- `InvalidInputError` on `measure="share_of_total"` or a method with `relevance="jev"`. A net flow takes neither, since the CRS totals hold no repayments and Jev labels none. Run the gross flow for either.
- `net_transfers` equals `net_disbursements` for a development bank from 2019. The banks report no interest after 2018.

## Next

- [Low and high bounds](../explanation/low-and-high-bounds.md) explains why `high` is not the central value of a net flow.
- [Python API](../reference/python-api.md) lists the arguments of `spending()` and the columns of `quality().repayments`.

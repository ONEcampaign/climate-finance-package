# How to trace a figure

Trace a figure from `spending()` to the projects and links behind it, and save the query so anyone can rerun it. [How linking works](../explanation/how-linking-works.md) defines the link methods.

## Before you start

Load a release and run the query you want to trace. The examples follow Japan's 2023 disbursements.

```python
import polars as pl

import climate_finance as cf

snap = cf.load()
result = snap.spending(providers="Japan", years=[2023])
```

## Steps

1. List the projects behind the figure. `rows(origin="disbursement")` returns one row per CRS record allocated to a commitment. `row_id` names the CRDF commitment, and `value` is the row's weighted climate value.

    ```python
    rows = result.rows(origin="disbursement")
    by_project = rows.group_by("row_id", "title").agg(pl.col("value").sum())
    for row in by_project.sort("value", descending=True).iter_rows(named=True):
        print(row["row_id"], row["title"], round(row["value"] / 1e6, 1))
    ```

    ```output
    JPN-1 Climate project JPN-1 25.3
    JPN-3 Climate project JPN-3 15.0
    JPN-2 Climate project JPN-2 6.3
    ```

2. Split certain from estimated links. Each row's `low` column holds the part of its value that certain links carried by certain routes, so the rows' `low` sums to the table's `low`.

    ```python
    print(
        round(rows["low"].sum() / 1e6, 1),
        round(result.table.to_polars()["low"].sum() / 1e6, 1),
    )
    ```

    ```output
    12.0 12.0
    ```

    Group the rows by `link_method` to see which methods carry the rest, and read `link_probability` for the build's confidence in each link. `low` requires both a certain link and a certain route for the commitment's CRDF rows.

3. Check the status of each commitment. Group by `status` to see how much of the figure sits in fully and partly disbursed commitments.

    ```python
    by_status = rows.group_by("status").agg(pl.col("value").sum()).sort("status")
    for row in by_status.iter_rows(named=True):
        print(row["status"], round(row["value"] / 1e6, 1))
    ```

    ```output
    disbursed 6.3
    partly_disbursed 40.3
    ```

4. Read the method in words. Paste `result.explain()` under any published table. It names the method, flow, view, filters, units, and release.

5. Save the query. `result.spec` holds every argument. Store it as JSON beside the figure so anyone with the same release can reproduce the table.

    ```python
    saved = result.spec.model_dump_json()
    rerun = snap.spending(spec=cf.Spec.model_validate_json(saved))
    print(rerun.table.to_polars().equals(result.table.to_polars()))
    ```

    ```output
    True
    ```

    Record `result.meta.release` as well. Releases are versioned, and a later release can change a figure.

6. Express the figure as a share of all flows. `share_of_total()` divides every amount by all CRS flows with the same flow, year, provider, recipient, and Bi_Multi code.

    ```python
    shares = result.share_of_total().table.to_polars()
    print(shares.select("recipient_code", "value").rows())
    ```

    ```output
    [(248, 0.1), (666, 0.25)]
    ```

## Verify it worked

For a single method, the rows sum to the table total.

```python
print(
    round(result.rows()["value"].sum())
    == round(result.table.to_polars()["value"].sum())
)
```

```output
True
```

## Next

- [How to check link quality](check-link-quality.md) measures how much of a provider's money traces.
- [Python API](../reference/python-api.md) lists the columns `rows()` returns.

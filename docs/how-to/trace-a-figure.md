# How to trace a figure

Trace a figure from `spending()` to the projects and links behind it, and save the query so anyone can rerun it. [How linking works](../explanation/how-linking-works.md) defines the link methods.

## Before you start

Load a release and run the query you want to trace. The examples follow Japan's 2023 disbursements on the synthetic example release, so their amounts are invented. `cf.load()` opens the published release.

```python
import polars as pl

import climate_finance as cf

release = cf.load_example()
result = release.spending(providers="Japan", years=[2023])
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
        round(result.table["low"].sum() / 1e6, 1),
    )
    ```

    ```output
    12.0 12.0
    ```

    Group the rows by `link_method` to see which methods carry the rest. `link_probability` is 1.0 on a certain link, the model's score on a `classifier`, `classifier_jev`, or `splink_jev` link, and null on every other rule link. `low` requires both a certain link and a certain route for the commitment's CRDF rows.

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
    rerun = release.run(cf.Spec.model_validate_json(saved))
    print(rerun.table.equals(result.table))
    ```

    ```output
    True
    ```

    Record `result.meta.release` as well. Releases are versioned, and a later release can change a figure.

6. Express the figure as a share of all flows. `share_of_total()` divides every amount by all CRS flows with the same flow, year, provider, recipient, and Bi_Multi code.

    ```python
    shares = result.share_of_total().table
    print(shares.select("recipient_code", "value").rows())
    ```

    ```output
    [(248, 0.1), (666, 0.25)]
    ```

## Read a row whose markers differ

A disbursement row weighs by the Rio markers of its commitment, `rio_adaptation` and `rio_mitigation`, and keeps the CRS record's own markers beside them in `record_rio_adaptation` and `record_rio_mitigation`. `marker_source` says how the two relate. `components` rows weigh by the commitment's climate share, `commitment` rows have record markers equal to the commitment's, and `record_differs` rows carry other record markers. The commitment's marker counts in `value` for all three, so a `record_differs` row is climate finance because the donor marked the commitment, and the record may say otherwise. Read the source record before describing the project as climate action, especially where a later record of the line carries a marker lower than the commitment's.

```python
later = release.spending(providers="Japan", years=[2024]).rows(origin="disbursement")
lower = later.filter(pl.col("record_rio_mitigation") < pl.col("rio_mitigation"))
for row in lower.iter_rows(named=True):
    print(
        row["record_id"],
        row["marker_source"],
        row["rio_mitigation"],
        row["record_rio_mitigation"],
        round(row["value"] / 1e6, 1),
    )
```

```output
JPN-2-R2 record_differs 1 0 6.3
```

JPN-2's commitment marks mitigation significant (1), and its 2024 disbursement record marks it 0. The row still counts USD 6.3m, 40% of the USD 15.8m disbursed, under the commitment's marker. `result.rows(marker_source="record_differs")` lists every such row of a query, including first records that report no marker at all.

## Verify it worked

For a single method, the rows sum to the table total.

```python
print(round(result.rows()["value"].sum()) == round(result.table["value"].sum()))
```

```output
True
```

## Next

- [How to check link quality](check-link-quality.md) measures how much of a provider's money traces.
- [Python API](../reference/python-api.md) lists the columns `rows()` returns.

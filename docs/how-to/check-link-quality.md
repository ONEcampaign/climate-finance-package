# How to check link quality

Before you publish a figure for a provider, check how much of its climate finance traces to CRS records, how much rests on certain links, and how often the links were right. [Low and high bounds](../explanation/low-and-high-bounds.md) explains what the numbers mean.

## Before you start

Load a release. The examples check Germany and Japan on the synthetic test release, so their numbers are invented.

```python
import polars as pl

import climate_finance as cf

snap = cf.load()
quality = snap.quality(providers=["Germany", "Japan"])
```

## Steps

1. Read the link rate. `summary()` has one row per provider. `link_rate` is the share of the provider's climate value whose CRDF rows match a CRS commitment, and `certain_route_share` is the part whose CRDF rows all reach their CRS commitment through certain routes.

    ```python
    summary = quality.summary()
    for row in summary.iter_rows(named=True):
        print(row["provider_code"], row["link_rate"], row["certain_route_share"])
    ```

    ```output
    5 1.0 1.0
    701 1.0 0.8125
    ```

    A link rate counts the match to a CRS commitment, whether or not anything disbursed against it. A provider can read a `link_rate` of 1.0 and still hold every commitment in `not_traceable`, so read it beside the status split in the next step.

2. Read the status split. `summary()` holds one `<status>_share` column per status: `disbursed`, `partly_disbursed`, `no_disbursement_record`, `not_traceable`, and `pending_review`.

    ```python
    for row in summary.iter_rows(named=True):
        print(
            row["provider_code"],
            round(row["disbursed_share"], 2),
            round(row["partly_disbursed_share"], 2),
        )
    ```

    ```output
    5 0.62 0.38
    701 0.31 0.69
    ```

3. Find the certain share. `quality.table` carries, by provider and year, the climate value of disbursements in the high bound (`disbursed_climate_usd`) and in the low bound (`certain_disbursed_climate_usd`). Their ratio is the share of the provider's disbursements that rests on certain links, reached by certain routes.

    ```python
    table = quality.table.to_polars()
    certain = table.group_by("provider_code").agg(
        (
            pl.col("certain_disbursed_climate_usd").sum()
            / pl.col("disbursed_climate_usd").sum()
        ).alias("certain_share")
    )
    for row in certain.sort("provider_code").iter_rows(named=True):
        print(row["provider_code"], round(row["certain_share"], 2))
    ```

    ```output
    5 0.37
    701 0.09
    ```

4. Read the measured precision. `quality.precision` holds one row per provider that went through the release's blind sample. `estimate` is the share of allocated value whose link a sample found right, `lower` and `upper` give its interval, and `passed` shows whether the lower bound reached the release's threshold. `waived` is true where the release shipped despite a failure.

    ```python
    print(quality.precision.to_polars().columns)
    ```

    ```output
    ['provider_code', 'estimate', 'lower', 'upper', 'passed', 'waived', 'detail']
    ```

    Ungated releases, including the synthetic release, leave this table empty. For a published release, check that every provider you report has a row, that `passed` is true, and that `waived` is false. A wide interval means the provider has few labelled links, so its estimate is rough.

    A provider's estimate covers all its allocated value, certain and estimated links together. Each stratum's estimate and interval sit beside it in `snap.manifest.gates["precision"]`, under `strata`. Where a provider's figure rests on estimated links, read the `model` stratum as well as the provider's row.

5. Check what a provider view omits. For provider-view totals, `quality.eu_left_out` holds the value of EU Institutions agencies' own rows omitted from the total because members' contributions stand in for them. `quality.attribution` holds the value attributed through core contributions by share source.

    ```python
    print(quality.eu_left_out.to_polars().columns)
    ```

    ```output
    ['agency_code', 'channel_code', 'year', 'flow', 'value']
    ```

## Verify it worked

Every provider you report appears in `summary()` and, for a gated release, in `quality.precision`. `snap.manifest.gates` holds the gate rows that `cf-build release` recorded, and `snap.manifest.precision_gate_waived` is true when the release was packed with `--waive-precision`.

## Troubleshooting

- `precision` is empty. The release skipped gating, so treat link-based figures as unmeasured.
- A provider has a `summary()` row and no `precision` row. It has no allocated links, so the sample had nothing to draw.

## Next

- [How to trace a figure](trace-a-figure.md) lists the links behind a particular figure.
- [Release gates](../reference/release-gates.md) describes how the precision interval is built.

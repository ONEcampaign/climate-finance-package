# Migrating from 1.x

In climate-finance 1.2.3, `ClimateData` downloaded OECD files, matched them, and computed figures on your machine. In 2.0, a release holds the matched data, and one call to `Release.spending()` answers a question. [The 2.0.0 release notes](release-notes/2.0.0.md) explain why the numbers differ.

## Before you start

- Python 3.12 or later.
- `pip install climate-finance`. `result.table` is a polars DataFrame, and its `to_pandas()` needs pandas, which `pip install "climate-finance[pandas]"` adds. `ClimateData.get_data()` needs it too.
- Remove `set_climate_finance_data_path()` from your scripts. A release downloads to `~/.cache/climate-finance`. Set `CLIMATE_FINANCE_CACHE` or pass `cache_dir=` to `load()` to move it. Open a release folder on disk with `load(path=...)`.

## What still runs

`ClimateData` imports from `climate_finance` and runs, with a `DeprecationWarning`, until 3.0. It wraps 2.0 by calling `spending()` once per source and flow and returning pandas. The constructor, the `set_*` methods, `load_spending_data()`, and `get_data()` run. `get_data()` returns the 2.0 columns, so a script that reads `indicator` or `oecd_provider_code` raises `KeyError`. The examples on this page run on the synthetic test release, so their amounts are invented. `set_climate_finance_data_path` and `get_unfccc_data` import with the same warning. Calling them raises `NotImplementedError` and names their replacement. Calls with no 2.0 equivalent also raise `NotImplementedError` and state that none exists.

```python
from climate_finance import ClimateData

climate_data = ClimateData(years=range(2021, 2024), providers=[701, 5])
climate_data.load_spending_data(
    methodology="ONE", source="OECD_CRDF_CRS_ALLOCABLE", flows="gross_disbursements"
)
data = climate_data.get_data()
print(data.columns.tolist())
```

```output
['year', 'provider', 'recipient', 'method', 'provider_code', 'recipient_code', 'bi_multi', 'amount', 'adaptation', 'mitigation', 'cross_cutting', 'unsplit', 'value', 'low', 'high', 'untraced', 'no_grant_equivalent', 'flow', 'source']
```

The 1.x frame was long, with one row per `indicator`. The 2.0 frame is wide, with one column per objective. A script that reads these columns changes as follows:

| 1.x                                                                                                 | 2.0                                                                      |
| --------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| `oecd_provider_code`                                                                                | `provider_code`                                                          |
| `oecd_recipient_code`                                                                               | `recipient_code`                                                         |
| `provider`, `recipient`                                                                             | `provider`, `recipient`, the listing names of the codes                  |
| `year`                                                                                              | `year`                                                                   |
| `indicator` of `climate_adaptation`, `climate_mitigation`, or `climate_cross_cutting`, with `value` | the `adaptation`, `mitigation`, and `cross_cutting` columns              |
| `indicator` of `not_climate_relevant`                                                               | none, since 2.0 returns climate value only                               |
| `value` summed over the indicators                                                                  | `value`, the climate total, with `low`, `high`, and `untraced` beside it |
| project columns, such as `crs_id`, `project_title`, and the sector columns                          | `result.rows()`, which returns the project rows behind a table           |

Three columns have no 1.x counterpart: `bi_multi`, `method`, and `amount`, the unweighted face value. `get_data()` adds `flow` and `source`.

## Rewrite to the 2.0 API

The same query in 2.0 runs through `Release.spending()`.

```python
import climate_finance as cf

release = cf.load()
result = release.spending(
    providers=[701, 5],
    years=range(2021, 2024),
    method=cf.methods.ONE,
    flow="disbursements",
)
data = result.table.to_pandas()
print(len(data), "rows")
```

```output
10 rows
```

Any Arrow-aware library reads the frame without pandas, such as `pa.table(result.table)`.

| 1.x                                                                                                        | 2.0                                                                                                                                                                                                                                                                                      |
| ---------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `set_climate_finance_data_path(path)`                                                                      | `CLIMATE_FINANCE_CACHE`, `load(cache_dir=...)`, or `load(path=...)`                                                                                                                                                                                                                      |
| `ClimateData(years, providers, recipients)`                                                                | `release.spending(years=..., providers=..., recipients=...)`                                                                                                                                                                                                                             |
| `currency="CAN"`                                                                                           | `currency="CAD"`                                                                                                                                                                                                                                                                         |
| `prices="constant", base_year=2022`                                                                        | `prices=2022`                                                                                                                                                                                                                                                                            |
| `set_only_oda()`                                                                                           | `oda_only=True`                                                                                                                                                                                                                                                                          |
| `set_update_data()`                                                                                        | `load(refresh=True)`                                                                                                                                                                                                                                                                     |
| `methodology="ONE"` or `"OECD"`                                                                            | `method=cf.methods.ONE` or `cf.methods.OECD`                                                                                                                                                                                                                                             |
| `set_custom_spending_methodology((0.5, 1.0), highest_marker=True)`                                         | `method=cf.Method(significant=0.5, principal=1.0, highest_marker=True)`                                                                                                                                                                                                                  |
| `flows="gross_disbursements"`, `"commitments"`, `"grant_equivalent"`, `"net_disbursements"`                | `flow="disbursements"`, `"commitments"`, `"grant_equivalent"`, `"net_disbursements"`                                                                                                                                                                                                     |
| `source="OECD_CRS"`, `"OECD_CRS_ALLOCABLE"`, `"OECD_CRDF"`, `"OECD_CRDF_CRS"`, `"OECD_CRDF_CRS_ALLOCABLE"` | `view="recipient"`, the default. A release holds one traced source, which includes disbursements from development banks' own ledgers, so `"OECD_CRS"` can return more than the CRS alone. The `_ALLOCABLE` names return the same rows as their plain names, without their former filter. |
| `source="OECD_CRDF_DONOR"`                                                                                 | `view="provider"`                                                                                                                                                                                                                                                                        |
| `get_data()`                                                                                               | `result.table.to_pandas()`, or `pa.table(result.table)` for an Arrow-aware library                                                                                                                                                                                                       |
| `as_share_of_total()`, `convert_to_shares()`                                                               | `result.share_of_total()`                                                                                                                                                                                                                                                                |
| `available_providers()` and the `get_*_providers()` helpers                                                | `cf.providers.list()`, which has a `type` column                                                                                                                                                                                                                                         |
| `available_flows()`, `available_methodologies()`                                                           | `cf.flows.list()`, `cf.methods.list()`                                                                                                                                                                                                                                                   |
| `get_unfccc_data()`                                                                                        | `release.reconcile()`                                                                                                                                                                                                                                                                    |
| `get_multilateral_spending_shares()`, `load_multilateral_imputations_data()`                               | `spending(view="provider")`, with `method=cf.methods.OECD` for the OECD's imputed shares                                                                                                                                                                                                 |

1.x `net_disbursements` subtracted each climate row's own `usd_received` from its disbursement. 2.0 subtracts the principal repaid on the loans the disbursements count, at each loan's counted share, and `flow="net_transfers"` subtracts interest too, so totals differ from 1.x. The UNFCCC source of `ClimateData` has no equivalent. `release.reconcile()` compares BTR reports with OECD data.

## What else is different

- The result is a polars table, with `adaptation`, `mitigation`, `cross_cutting`, `unsplit`, `value`, `low`, `high`, `untraced`, and `no_grant_equivalent` columns. `result.rows()` returns the projects behind it.
- `value` is climate value only, in `ClimateData` too, and no row carries the 1.x indicator `not_climate_relevant`. A 1.x script that sums `value` over every indicator summed all development finance, and the 2.0 sum is climate value.
- A result carries its `Spec`. `release.run(result.spec)` reruns it.
- Names and codes resolve through `cf.providers` and `cf.recipients`. A misspelt name, or a code the listings lack, raises an error that suggests close matches.
- Building a release fetches OECD data, using `cf-build` and the `build` extra. 2.0 reads the built release. [cf-build](reference/cf-build.md) lists the commands.

## Verify it worked

Run the `ClimateData` call and its `spending()` rewrite for one provider and year on the same release. Their `value` columns match. A figure from 1.2.3 itself differs, for the reasons the release notes give.

## Next

- [Trace Japan's climate finance to Bangladesh](tutorial.md) walks through the 2.0 API from the start.
- [Python API](reference/python-api.md) lists every argument.

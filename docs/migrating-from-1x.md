# Migrating from 1.x

In climate-finance 1.2.3, `ClimateData` downloaded OECD files, matched them, and computed figures on your machine. In 2.0, a release holds the matched data, and one call to `Release.spending()` answers a question. [The 2.0.0 release notes](release-notes/2.0.0.md) explain why the numbers differ.

## Before you start

- Python 3.12 or later.
- `pip install climate-finance`. `result.table` is a polars DataFrame, and its `to_pandas()` needs pandas, which `pip install "climate-finance[pandas]"` adds. `ClimateData.get_data()` needs it too.
- A release downloads to `~/.cache/climate-finance`. Set `CLIMATE_FINANCE_CACHE_DIR` or pass `cache_dir=` to `load()` to move it. Open a release folder on disk with `load(path=...)`.

## What still runs

`ClimateData` imports from `climate_finance` and runs until 3.0, with a `ClimateFinanceDeprecationWarning`, a `DeprecationWarning` in `climate_finance.errors`. It wraps 2.0 by calling `spending()` once per source and flow and returning pandas. The constructor, the `set_*` methods, `load_spending_data()`, and `get_data()` run for every choice whose meaning 2.0 keeps. A choice whose meaning changed raises `RemovedError` naming the 2.0 call to use, as [Choices that changed meaning](#choices-that-changed-meaning) lists. `get_data()` returns the 2.0 columns, so a script that reads `indicator` or `oecd_provider_code` raises `KeyError`. The examples on this page run on the synthetic test release, so their amounts are invented.

`set_climate_finance_data_path(path)` runs with the same warning, and every later `ClimateData` downloads its release into `path`. Pass `release=` to point one `ClimateData` at a release you opened, such as `ClimateData(years=range(2021, 2024), release=cf.load(path="releases/2026a"))`.

```python
from climate_finance import ClimateData

climate_data = ClimateData(years=range(2021, 2024), providers=[701, 5])
climate_data.load_spending_data(
    methodology="ONE", source="OECD_CRDF_CRS", flows="gross_disbursements"
)
data = climate_data.get_data()
print(data.columns.tolist())
```

```output
['year', 'provider', 'recipient', 'method', 'provider_code', 'recipient_code', 'bi_multi', 'flow', 'view', 'currency', 'prices', 'measure', 'amount', 'adaptation', 'mitigation', 'cross_cutting', 'unsplit', 'untraced', 'no_grant_equivalent', 'no_adaptation_marker', 'value', 'low', 'high', 'source']
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
    untraced=False,
)
data = result.table.to_pandas()
print(len(data), "rows")
```

```output
10 rows
```

`untraced=False` keeps the disbursements traced to the CRDF's commitments, as `source="OECD_CRDF_CRS"` counted them. Leave it out to add the Rio-marked CRS disbursement traced to no commitment, which `spending()` counts by default. Any Arrow-aware library reads the frame without pandas, such as `pa.table(result.table)`.

| 1.x                                                                          | 2.0                                                                                                                                                                            |
| ---------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `set_climate_finance_data_path(path)`                                        | `load(cache_dir=...)`, `CLIMATE_FINANCE_CACHE_DIR`, or `load(path=...)`                                                                                                        |
| `ClimateData(years, providers, recipients)`                                  | `release.spending(years=..., providers=..., recipients=...)`                                                                                                                   |
| `currency="CAN"`                                                             | `currency="CAD"`                                                                                                                                                               |
| `prices="constant", base_year=2022`                                          | `prices=2022`                                                                                                                                                                  |
| `set_only_oda()`                                                             | `oda_only=True`                                                                                                                                                                |
| `set_update_data()`                                                          | `load(refresh=True)`                                                                                                                                                           |
| `methodology="ONE"` or `"OECD"`                                              | `method=cf.methods.ONE` or `cf.methods.OECD`                                                                                                                                   |
| `set_custom_spending_methodology((0.5, 1.0), highest_marker=True)`           | `method=cf.Method(significant=0.5, principal=1.0, highest_marker=True)`                                                                                                        |
| `flows="gross_disbursements"`, `"commitments"`, `"grant_equivalent"`         | `flow="disbursements"`, `"commitments"`, `"grant_equivalent"`                                                                                                                  |
| `source="OECD_CRDF_CRS"`, and `"OECD_CRDF"` with commitments                 | `view="recipient"`, the default, with `untraced=False`. The traced disbursements include those from development banks' own ledgers, which stand in for the banks' CRS records. |
| `source="OECD_CRDF_DONOR"` with commitments                                  | `view="provider"`, with `untraced=False`                                                                                                                                       |
| `get_data()`                                                                 | `result.table.to_pandas()`, or `pa.table(result.table)` for an Arrow-aware library                                                                                             |
| `as_share_of_total()`, `convert_to_shares()`                                 | `result.share_of_total()`                                                                                                                                                      |
| `available_providers()` and the `get_*_providers()` helpers                  | `cf.providers.list()`, which has a `type` column                                                                                                                               |
| `available_flows()`, `available_methodologies()`                             | `cf.flows.list()`, `cf.methods.list()`                                                                                                                                         |
| `get_unfccc_data()`                                                          | none for the Biennial Report years. `release.reconcile()` compares BTR totals, from 2021, with OECD figures.                                                                   |
| `get_multilateral_spending_shares()`, `load_multilateral_imputations_data()` | `spending(view="provider")`, with `method=cf.methods.OECD` for the OECD's imputed shares                                                                                       |

## Choices that changed meaning

These 1.x choices run in 2.0 with another meaning, so `ClimateData` refuses them with `RemovedError`. Its `removed` attribute names the choice, and its `replacement` names the 2.0 call.

| 1.x                                                                                | Why `ClimateData` refuses it                                                                                                        | 2.0                                                                                                                                        |
| ---------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `flows="net_disbursements"`                                                        | 1.x subtracted each climate row's own `usd_received`. 2.0 subtracts the principal repaid on each counted loan at its counted share. | `spending(flow="net_disbursements")`, or `flow="net_transfers"` to subtract interest too                                                   |
| `source="OECD_CRS"`, the default                                                   | 1.x counted every Rio-marked CRS record. 2.0 counts CRS records beside the CRDF's commitments.                                      | `load_spending_data(source="OECD_CRDF_CRS")`, or `spending()`, which adds the Rio-marked disbursement traced to no commitment              |
| `source="OECD_CRS_ALLOCABLE"`                                                      | 2.0 holds no allocable filter, and has no query of the CRS alone.                                                                   | as for `"OECD_CRS"`                                                                                                                        |
| `source="OECD_CRDF_CRS_ALLOCABLE"`                                                 | 2.0 holds no allocable filter.                                                                                                      | `load_spending_data(source="OECD_CRDF_CRS")`, or `spending(untraced=False)`                                                                |
| `source="OECD_CRDF"` or `"OECD_CRDF_DONOR"` with a flow other than `"commitments"` | 1.x held commitments only from the CRDF and returned no rows. 2.0 counts the flow.                                                  | `load_spending_data(source="OECD_CRDF_CRS")`, or `spending(flow=..., untraced=False)`, with `view="provider"` for the provider perspective |
| `source="UNFCCC"`, `get_unfccc_data()`                                             | 2.0 holds no Biennial Report data.                                                                                                  | `release.reconcile()`, for BTR totals from 2021                                                                                            |

## Removed modules

Importing a 1.x module raises `RemovedModuleError`, a `ModuleNotFoundError` that names what replaces it.

| 1.x module                                             | 2.0                                                                                      |
| ------------------------------------------------------ | ---------------------------------------------------------------------------------------- |
| `climate_finance.unfccc`                               | none for the Biennial Report data. `release.reconcile()` compares BTR totals from 2021.  |
| `climate_finance.core`                                 | `ClimateData` from `climate_finance.compat`, or `cf.load().spending()`                   |
| `climate_finance.oecd`                                 | a release, which holds the matched OECD data. `cf-build` fetches it.                     |
| `climate_finance.methodologies`                        | `method=` on `spending()`, with `cf.methods.ONE`, `cf.methods.OECD`, or `cf.Method(...)` |
| `climate_finance.config`                               | `load(cache_dir=...)`                                                                    |
| `climate_finance.common`, `climate_finance.validation` | none                                                                                     |

## What else is different

- The result is a polars table, with `adaptation`, `mitigation`, `cross_cutting`, `unsplit`, `value`, `low`, `high`, `untraced`, `no_grant_equivalent`, and `no_adaptation_marker` columns. `result.rows()` returns the projects behind it.
- `value` is climate value only, in `ClimateData` too, and no row carries the 1.x indicator `not_climate_relevant`. A 1.x script that sums `value` over every indicator summed all development finance, and the 2.0 sum is climate value.
- A result carries its `Spec`. `release.run(result.spec)` reruns it.
- Names and codes resolve through `cf.providers` and `cf.recipients`. A misspelt name, or a code the listings lack, raises an error that suggests close matches.
- Building a release fetches OECD data, using `cf-build` and the `build` extra. 2.0 reads the built release. [cf-build](reference/cf-build.md) lists the commands.

## Verify it worked

Run the `ClimateData` call and its `spending()` rewrite for one provider and year on the same release. Their `value` columns match. A figure from 1.2.3 itself differs, for the reasons the release notes give.

## Next

- [Trace Japan's climate finance to Bangladesh](tutorial.md) walks through the 2.0 API from the start.
- [Python API](reference/python-api.md) lists every argument.

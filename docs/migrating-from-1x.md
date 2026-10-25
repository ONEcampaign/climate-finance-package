# Migrating from 1.x

In climate-finance 1.2.3, `ClimateData` downloaded OECD files, matched them, and computed figures on your machine. In 2.0, a release holds the matched data, and one call to `Release.spending()` answers a question. [The 2.0.0 release notes](release-notes/2.0.0.md) explain why the numbers differ.

## Before you start

- Python 3.12 or later.
- `pip install climate-finance`. `Result.table.to_pandas()` needs pandas, which `pip install "climate-finance[compat]"` adds.
- Remove `set_climate_finance_data_path()` from your scripts. A release downloads to `~/.cache/climate-finance`. Set `CLIMATE_FINANCE_CACHE` or pass `cache_dir=` to `load()` to move it. Open a release folder on disk with `load(path=...)`.

## Keep 1.x code running

`ClimateData` imports from `climate_finance` and runs, with a `DeprecationWarning`, until 3.0. It wraps 2.0 by calling `spending()` once per source and flow and returning pandas. The examples on this page run on the synthetic test release, so their amounts are invented. `set_climate_finance_data_path` and `get_unfccc_data` import with the same warning. Calling them raises `NotImplementedError` and names their replacement. Calls with no 2.0 equivalent also raise `NotImplementedError` and state that none exists.

```python
from climate_finance import ClimateData

climate_data = ClimateData(years=range(2021, 2024), providers=[701, 5])
climate_data.load_spending_data(
    methodology="ONE", source="OECD_CRDF_CRS_ALLOCABLE", flows="gross_disbursements"
)
data = climate_data.get_data()
print(data.columns.tolist()[-6:])
```

```output
['value', 'low', 'high', 'untraced', 'flow', 'source']
```

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

| 1.x | 2.0 |
|---|---|
| `set_climate_finance_data_path(path)` | `CLIMATE_FINANCE_CACHE`, `load(cache_dir=...)`, or `load(path=...)` |
| `ClimateData(years, providers, recipients)` | `release.spending(years=..., providers=..., recipients=...)` |
| `currency="CAN"` | `currency="CAD"` |
| `prices="constant", base_year=2022` | `prices=2022` |
| `set_only_oda()` | `oda_only=True` |
| `set_update_data()` | `load(refresh=True)` |
| `methodology="ONE"` or `"OECD"` | `method=cf.methods.ONE` or `cf.methods.OECD` |
| `set_custom_spending_methodology((0.5, 1.0), highest_marker=True)` | `method=cf.Method(significant=0.5, principal=1.0, highest_marker=True)` |
| `flows="gross_disbursements"`, `"commitments"`, `"grant_equivalent"` | `flow="disbursements"`, `"commitments"`, `"grant_equivalent"` |
| `source="OECD_CRS"`, `"OECD_CRS_ALLOCABLE"`, `"OECD_CRDF"`, `"OECD_CRDF_CRS"`, `"OECD_CRDF_CRS_ALLOCABLE"` | `view="recipient"`, the default. A release holds one traced source, which includes disbursements from development banks' own ledgers, so `"OECD_CRS"` can return more than the CRS alone. The `_ALLOCABLE` names return the same rows as their plain names, without their former filter. |
| `source="OECD_CRDF_DONOR"` | `view="provider"` |
| `get_data()` | `result.table.to_pandas()`, `to_polars()`, or `to_arrow()` |
| `as_share_of_total()`, `convert_to_shares()` | `result.share_of_total()` |
| `available_providers()` and the `get_*_providers()` helpers | `cf.providers.list()`, which has a `type` column |
| `available_flows()`, `available_methodologies()` | `cf.flows.list()`, `cf.methods.list()` |
| `get_unfccc_data()` | `release.reconcile()` |
| `get_multilateral_spending_shares()`, `load_multilateral_imputations_data()` | `spending(view="provider")`, with `method=cf.methods.OECD` for the OECD's imputed shares |

Releases hold gross disbursements only, so `net_disbursements` has no 2.0 equivalent. The UNFCCC source of `ClimateData` has no equivalent either. `release.reconcile()` compares BTR reports with OECD data.

## What else is different

- The result is a polars table by default, with `adaptation`, `mitigation`, `cross_cutting`, `unsplit`, `value`, `low`, `high`, and `untraced` columns. `result.rows()` returns the projects behind it.
- `value` is climate value only, in `ClimateData` too, and no row carries the 1.x indicator `not_climate_relevant`. A 1.x script that sums `value` over every indicator gets a different number and no error.
- A result carries its `Spec`. `release.spending(spec=result.spec)` reruns it.
- Names and codes resolve through `cf.providers` and `cf.recipients`. A misspelt name raises an error that suggests close matches.
- Building a release fetches OECD data, using `cf-build` and the `build` extra. 2.0 reads the built release. [cf-build](reference/cf-build.md) lists the commands.

## Verify it worked

Run the `ClimateData` call and its `spending()` rewrite for one provider and year on the same release. Their `value` columns match. A figure from 1.2.3 itself differs, for the reasons the release notes give.

## Next

- [Trace Japan's climate finance to Bangladesh](tutorial.md) walks through the 2.0 API from the start.
- [Python API](reference/python-api.md) lists every argument.

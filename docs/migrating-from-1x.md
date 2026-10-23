# Migrating from 1.x

In climate-finance 1.2.3, `ClimateData` downloaded OECD files, matched them, and computed figures on your machine. In 2.0, a release holds the matched data, and one call to `Snapshot.spending()` answers a question. [The 2.0.0 release notes](release-notes/2.0.0.md) explain why the numbers differ.

## Before you start

- Python 3.12 or later.
- `pip install climate-finance`. `Result.table.to_pandas()` needs pandas, which `pip install "climate-finance[compat]"` adds.
- Remove `set_climate_finance_data_path()` from your scripts. A release downloads to `~/.cache/climate-finance`. Set `CLIMATE_FINANCE_CACHE` or pass `cache_dir=` to `load()` to move it. Open a release folder on disk with `load(path=...)`.

## Keep 1.x code running

`ClimateData`, `set_climate_finance_data_path`, and `get_unfccc_data` still import from `climate_finance`, with a `DeprecationWarning`, until 3.0. `ClimateData` is a wrapper that makes one `spending()` call per source and flow and returns pandas.

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
['unsplit', 'value', 'low', 'high', 'flow', 'source']
```

Calls with no 2.0 equivalent raise `NotImplementedError` that names the replacement, or says none exists.

## Rewrite to the 2.0 API

The same query in 2.0 runs through `Snapshot.spending()`.

```python
import climate_finance as cf

snap = cf.load()
result = snap.spending(
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
| `ClimateData(years, providers, recipients)` | `snap.spending(years=..., providers=..., recipients=...)` |
| `currency="CAN"` | `currency="CAD"` |
| `prices="constant", base_year=2022` | `prices=2022` |
| `set_only_oda()` | `oda_only=True` |
| `set_update_data()` | `load(refresh=True)` |
| `methodology="ONE"` or `"OECD"` | `method=cf.methods.ONE` or `cf.methods.OECD` |
| `set_custom_spending_methodology((0.5, 1.0), highest_marker=True)` | `method=cf.Method(significant=0.5, principal=1.0, highest_marker=True)` |
| `flows="gross_disbursements"`, `"commitments"`, `"grant_equivalent"` | `flow="disbursements"`, `"commitments"`, `"grant_equivalent"` |
| `source="OECD_CRS"`, `"OECD_CRS_ALLOCABLE"`, `"OECD_CRDF"`, `"OECD_CRDF_CRS"`, `"OECD_CRDF_CRS_ALLOCABLE"` | `view="recipient"`, the default. A release holds one traced source. |
| `source="OECD_CRDF_DONOR"` | `view="provider"` |
| `get_data()` | `result.table.to_pandas()`, `to_polars()`, or `to_arrow()` |
| `as_share_of_total()`, `convert_to_shares()` | `result.share_of_total()` |
| `available_providers()` and the `get_*_providers()` helpers | `cf.providers.list()`, which has a `type` column |
| `available_flows()`, `available_methodologies()` | `cf.flows.list()`, `cf.methods.list()` |
| `get_unfccc_data()` | `snap.reconcile()` |
| `get_multilateral_spending_shares()`, `load_multilateral_imputations_data()` | `spending(view="provider")`, with `method=cf.methods.OECD` for the OECD's imputed shares |

`net_disbursements` has no 2.0 equivalent, because releases hold gross disbursements only. The UNFCCC source of `ClimateData` has no equivalent either. `snap.reconcile()` compares BTR reports with OECD data.

## What else is different

- The result is a polars table by default, with `adaptation`, `mitigation`, `cross_cutting`, `unsplit`, `value`, `low`, and `high` columns. `result.rows()` returns the projects behind it.
- A result carries its `Spec`. `snap.spending(spec=result.spec)` reruns it.
- Names and codes resolve through `cf.providers` and `cf.recipients`. A misspelt name raises an error that suggests close matches.
- Building a release fetches OECD data, using `cf-build` and the `build` extra; 2.0 itself reads the built release. [cf-build](reference/cf-build.md) lists the commands.

## Verify it worked

Run the 1.x and 2.0 queries for one provider and year and compare `value`.

## Next

- [Trace Japan's climate finance to Bangladesh](tutorial.md) walks through the 2.0 API from the start.
- [Python API](reference/python-api.md) lists every argument.

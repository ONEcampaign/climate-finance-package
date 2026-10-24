[![pypi](https://img.shields.io/pypi/v/climate_finance.svg)](https://pypi.org/project/climate_finance/)
[![python](https://img.shields.io/pypi/pyversions/climate_finance.svg)](https://pypi.org/project/climate_finance/)

# The climate finance package

**climate-finance** is a Python package for international public climate finance. It returns figures such as the adaptation finance France disbursed to Kenya in 2021 in a few lines of code, under a stated methodology, and traces each figure to the OECD records behind it.

Climate finance data is messy and scattered across several websites. It took us months to learn which data to use and longer to clean it for the Climate Finance Files. The package ships that work as versioned releases built from the OECD's Creditor Reporting System (CRS) and Climate-related Development Finance database (CRDF), so researchers and campaigners can start from clean data and hold governments to their pledges.

## Install

```bash
pip install climate-finance
```

## Query a release

```python
import climate_finance as cf

snap = cf.load()
result = snap.spending(
    providers="France",
    recipients="Kenya",
    years=range(2018, 2023),
    method=cf.methods.ONE,
)
result.table.to_polars()
```

`load()` opens the latest release, or the one in `path=` for a release on disk. The result table has one row per year, provider, recipient, Bi_Multi code and method, with adaptation, mitigation, cross-cutting, `unsplit` and total climate `value` columns, and `low` and `high` bounds. `unsplit` holds attributed finance through agencies whose climate share has no split by objective, as the OECD gives before 2021. Convert it with `to_polars()`, `to_pandas()` or `to_arrow()`.

Each query takes these arguments:

- `method`: how Rio markers become climate value. `cf.methods.ONE` counts significant markers at 40% and principal markers at 100%, with each activity under its higher marker. `cf.methods.OECD` counts both at 100%. `cf.methods.PROVIDER` is ONE with each provider's own significant coefficient where the release has one. `cf.methods.RANGE` reports all three, and `low` and `high` become the lowest and highest value across them.
- `flow`: `"commitments"`, `"disbursements"` (the default) or `"grant_equivalent"`.
- `view`: `"recipient"` (the default) or `"provider"`, which adds core contributions to multilateral agencies in proportion to each agency's climate share.
- `currency` and `prices`: `"USD"`, `"EUR"`, `"GBP"` or `"CAD"`, at current prices or at a base year, converted with OECD DAC exchange rates and deflators.
- `oda_only`: keep official development assistance flows only.

Providers and recipients take names or DAC codes. `cf.providers`, `cf.recipients`, `cf.flows` and `cf.methods` list what is available, with `.list()`, `.info(name)` and `.search(text)`. A misspelt name raises an error that suggests close matches.

## Trace and compare

`result.rows()` returns the project rows behind the table, and `result.explain()` describes the method, flow, filters and units in plain language. `result.meta` records the release, package version and source versions. `result.share_of_total()` divides every amount by all CRS flows for the same key. `snap.quality()` reports how much climate value traces to CRS records, and `cf.compare(a, b)` sets several results side by side.

`result.spec` holds every argument as a frozen `Spec`, and `snap.spending(spec=result.spec)` reruns the query to an identical table.

## Migrating from 1.x

`ClimateData`, `set_climate_finance_data_path` and `get_unfccc_data` still import, with a `DeprecationWarning`, until 3.0. Each call without a 2.0 equivalent raises an error that names its replacement.

## Contact

We want this package to help others analyse climate finance data. If you want to collaborate, or have questions, please reach out.

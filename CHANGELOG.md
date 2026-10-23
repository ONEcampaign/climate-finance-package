# Changes to the climate_finance package

## 2.0.0
- Replaces matching on the user's machine with versioned releases. `climate_finance.load()` opens a release, and `Snapshot.spending()` returns climate finance with `low`, `value` and `high` columns, the rows behind each figure and an explanation of the method.
- Traces CRS disbursements to the CRDF commitments they pay, in any year, with the method of each link recorded. Certain links count in the low bound and estimated links in `value`. `Snapshot.quality()` reports link rates, commitment status and the measured precision of the links.
- Adds the methods `ONE`, `OECD`, `PROVIDER` and `RANGE`, `Method` for custom coefficients, a provider view that attributes core contributions to multilateral agencies, `Snapshot.reconcile()` for BTR reports and `cf-build` for building and releasing data.
- Withholds the Green Climate Fund's ledger rows from releases, as its terms bar redistribution. `commitments.withheld_usd` states their amount.
- Requires Python 3.12 or later.
- Deprecates `ClimateData`, `set_climate_finance_data_path` and `get_unfccc_data`, which import with a `DeprecationWarning` until 3.0. See `docs/migrating-from-1x.md` and `docs/release-notes/2.0.0.md`, which explains why totals differ from 1.x.

## 1.2.3 (2026-08-11)
- Declares `pydeflate`, `numpy` and `python-dateutil` as direct dependencies instead of relying on them arriving transitively.

## 1.2.2 (2025-12-15)
- Fixes indicator compatibility issue with oda-data >= 2.3.1 by switching to the new `core_multilateral_contributions_by_provider` function.
- Updates `oda-data` dependency to >= 2.3.1.
- Adds `KeysView` type support in `to_list_of_ints` helper function.
- Automatically extends years range when rolling years exceed the requested years.
- Changes default `rolling_years_spending` from 1 to 3 in `load_multilateral_imputations_data`.
- Removes deprecated `clean_multisystem_indicators` function.

## 1.2.1 (2025-12-04)
- Adds testing suite and updates project structure. It also fixes a publishing issue.

## 1.2 (2025-09-19)
- Updates to dependencies and core logic based on
changes to the underlying data structures from the OECD

## 1.1.2 (2025-03-20)
- Re releases 1.1.1 since the version on PyPI doesn't match the GitHub version.

## 1.1.1 (2024-06-13)
- Fixes a bug which prevented users from getting all providers by not specifying them (leaving `providers` as None).
- Fixes a bug where the data wouldn't necessarily be stored in the desired user-defined folder.
- Adds additional columns to the CRS used for analysis.

## 1.1.0 (2024-04-08)
- Adds functionality to accept "providers" as OECD codes, ISO3 codes, or provider names.
- Adds functionality to get the list of available providers (`ClimateData.available_providers()`).
- Adds functionality to get groups of providers (like DAC, non-DAC, etc.). See the 
  documentation for more information.

## 1.0.1 (2024-04-04)
- Fixes a bug where data was not being converted to other currencies when current prices
were selected.

## 1.0.0 (2024-04-03)
- First release on PyPI.
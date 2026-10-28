# Changes to the climate_finance package

## 2.0.0
- Replaces matching on the user's machine with versioned releases. `climate_finance.load()` opens a release, and `Release.spending()` returns climate finance with `low`, `value` and `high` columns, the rows behind each figure and an explanation of the method.
- Traces CRS disbursements to the CRDF commitments they pay, in any year, with the method of each link recorded. Certain links count in the low bound, and every link in `value`. `Release.quality()` reports link rates, commitment status and the measured precision of the links.
- Adds `cf.load_example()`, which opens a small synthetic release shipped in the wheel, with no download, so every example in the documentation runs after `pip install`.
- Adds the methods `ONE`, `OECD`, `PROVIDER` and `RANGE`, the bounds of Oxfam's Climate-Specific Net Assistance as `OXFAM_LOW`, `OXFAM_HIGH` and `OXFAM`, `Method` for custom coefficients, a provider view that attributes core contributions to multilateral agencies, `Release.reconcile()` for BTR reports and `cf-build` for building and releasing data.
- Adds `flow="net_disbursements"` and `flow="net_transfers"`, which subtract the principal and interest repaid on climate loans at the share each loan was counted under, and `Release.quality().repayments`, which reports the repayments no total counts. Their `high` bound subtracts only repayments with a certain link, so it sits above `value`.
- Values a grant (CRS finance type 110 or 210) the CRS gives no grant equivalent at face value under `flow="grant_equivalent"`, from the build, so every release table and `share_of_total()` agree. A loan or other record without a grant equivalent counts 0 in `value`, `low` and `high`, and the result's `no_grant_equivalent` column holds its climate value at face.
- Adds the build switch `jev_flags` and the release table `jev_flags`, which hold Jev's answers to four questions about each climate-marked CRS record, and `Release.jev_flags()`, which returns them with a `no_aim` and a `surprising` flag. The flags rank records for review and change no total.
- Ships each release's currency and price conversion factors in the release table `conversion_factors`, built from pydeflate's OECD DAC exchange rates and deflators, so a query converts offline and a converted figure changes only with a new release. pydeflate moves to the `build` extra.
- Withholds the Green Climate Fund's ledger rows from releases, as its terms bar redistribution. `commitments.withheld_usd` states their amount.
- Requires Python 3.12 or later.
- Adds the columns `flow`, `view`, `currency`, `prices`, `measure` and `no_adaptation_marker` to spending results, and `range_low` and `range_high` under several methods.
- Adds `Result.combined_share()`, which returns one climate share per method across a query's keys, or per the key columns `by` names. The share divides by every flow in the query's scope, including keys with no climate row.
- Adds `ClimateFinanceWarning` as the base of every warning, with `CoverageWarning`, `DoubleCountWarning` and `ClimateFinanceDeprecationWarning` beneath it, and a `code` on every error, such as `unknown_provider`, shown in brackets at the end of the message.
- Adds the `type` and `mdb` columns to `climate_finance.providers.list()`, the `type` column to `climate_finance.recipients.list()`, and the provider group `"World Bank"`, which names IBRD and IDA.
- Adds `cf-build publish`, which uploads a pack to the GitHub release tagged `data` and replaces `releases.json` there.
- Deprecates `ClimateData`, which runs with a `ClimateFinanceDeprecationWarning` until 3.0. `set_climate_finance_data_path` and `get_unfccc_data` import with the warning, and `get_unfccc_data` raises `RemovedError` naming `Release.reconcile()` when called. See `docs/migrating-from-1x.md` and `docs/release-notes/2.0.0.md`, which explains why totals differ from 1.x.
- Removes these 1.x calls. Each raises `RemovedError` naming its 2.0 replacement:
  - `ClimateData.get_multilateral_spending_shares`, `load_multilateral_imputations_data`, `as_share_of_total`, `convert_to_shares`, `available_methodologies`, `available_flows`, `available_sources`, `available_prices`, `available_currencies`, `available_providers`, `get_dac_providers`, `get_non_dac_providers`, `get_multilateral_providers` and `get_private_providers`.
  - `get_unfccc_data`, and `ClimateData.load_spending_data(source="UNFCCC")`.
  - `ClimateData.load_spending_data()` without `source=`, which 1.x read as `"OECD_CRS"`, and the sources `"OECD_CRS"`, `"OECD_CRS_ALLOCABLE"` and `"OECD_CRDF_CRS_ALLOCABLE"`.
  - `"OECD_CRDF"` and `"OECD_CRDF_DONOR"` with a flow other than `"commitments"`, and `flows="net_disbursements"`, which 2.0 nets by loan with `Release.spending(flow="net_disbursements")`.
  - The modules `climate_finance.common`, `config`, `core`, `methodologies`, `oecd`, `unfccc` and `validation`. Importing one raises `RemovedModuleError`, a `ModuleNotFoundError`.

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
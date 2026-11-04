# Changes to the climate_finance package

## 2.0.0

- Replaces matching on the user's machine with versioned releases. `climate_finance.load()` opens a release, and `Release.spending()` returns climate finance with `low`, `value` and `high` columns, the rows behind each figure and an explanation of the method.
- Traces CRS disbursements to the CRDF commitments they pay, in any year, with the method of each link recorded. Certain links count in the low bound, and every link in `value`. `Release.quality()` reports link rates, commitment status and the measured precision of the links.
- Adds `cf.load_example()`, which opens a small synthetic release shipped in the wheel, with no download, so every example in the documentation runs after `pip install`.
- Adds the methods `ONE`, `MARKERS`, `OECD` and `RANGE`, the bounds of Oxfam's Climate-Specific Net Assistance as `OXFAM_LOW`, `OXFAM_HIGH` and `OXFAM`, `Method` for custom coefficients, a provider view that attributes core contributions to multilateral agencies, `Release.reconcile()` for BTR reports and `cf-build` for building and releasing data. `ONE` takes each provider's own coefficients, and `MARKERS` counts significant markers at 40% and principal markers at 100% for every provider.
- Adds `flow="net_disbursements"` and `flow="net_transfers"`, which subtract the principal and interest repaid on climate loans at the share each loan was counted under, and `Release.quality().repayments`, which reports the repayments no total counts. Their `high` bound subtracts only repayments with a certain link, so it sits above `value`.
- Values a grant (CRS finance type 110 or 210) the CRS gives no grant equivalent at face value under `flow="grant_equivalent"`, from the build, so every release table and `share_of_total()` agree. A loan or other record without a grant equivalent counts 0 in `value`, `low` and `high`, and the result's `no_grant_equivalent` column holds its climate value at face.
- Adds the build switch `jev_flags` and the release table `jev_flags`, which hold Jev's answers to four questions about each climate-marked CRS record, and `Release.jev_flags()`, which returns them with a `no_aim` and a `surprising` flag. The flags rank records for review and change no total.
- Adds the release table `btr_coefficients`, which holds, for each provider and agency of a BTR party, the climate share its BTR reports on significant-marked and principal-marked activities, from the BTR years the release holds, with the linked CRS amount and components behind it, and the components left out for a BTR amount more than 10% above their CRS amount. `ONE` takes it, in every year, for a provider that assesses each activity case by case and states no coefficient, such as the UK and France's AFD, where it took 0.4 and 1.0. Releases move to data format 2; a format 1 release reads its stated coefficients alone.
- Reads BTR Table III.2, each party's contributions through multilateral channels, into the release tables `btr_multilateral`, one row per institution and row of the table, and `btr_multilateral_totals`, its rows summed by party, year, status and institution group. A group holds the CRS channel codes of one institution, such as IBRD and IDA for the World Bank, and `build/btr_institutions.toml` names the spellings parties use for it. The map groups 99% of the archive's 2021 and 2022 Table III.2 face value, and a name it leaves out sums into a row with no group. Both tables restate the amounts at DAC rates, and the published release ships the totals and leaves out the rows, as it does `btr`. `btr_institution_groups` lists each group's channel codes, and `btr_crs_earmarked` holds the bilateral climate-marked CRS amounts each BTR party delivers through those channels that no Table III.1 row links to, by agency, channel, flow and Rio markers. `sources.json` lists each workbook's Table III.2 sheets under `multilateral_workbooks`. Data format 3 gains the four tables. `Release.reconcile()` adds `Reconciliation.multilateral`, which sets each party's Table III.2 core and climate-specific amounts by institution group against its core contributions to the group's channels, their climate value under the method, and its earmarked bilateral money through them, with `table` unchanged.
- Adds the release table `eurostat_climate_finance`, which holds Eurostat's `sdg_13_50` (climate finance of the EU member states, the European Commission and the EIB, from 2014) in EUR by Eurostat geo code, with the DAC provider and agency each line stands for. `COMM` is EU Institutions (918), counted as agencies 1 and 2, and `EIB` is 918 agency 3. The build fetches the dataset into the raw manifest, stops on a geo code it has no provider for, and `sources.json` credits Eurostat under CC BY 4.0. Data format 3 gains the table.
- Adds `Release.reconcile_eurostat()`, which sets each Eurostat `sdg_13_50` line, a member state, the Commission or the EIB, against the release by year from 2020 to 2024 in EUR, and returns an `EurostatReconciliation`. The release side is fixed at `ONE`, the provider-coefficient rule, with EU Institutions as a provider: a member's bilateral commitments plus the climate value of its core contributions outside the EU budget, the EDF and the EIB, committed in 2020 and 2021 and disbursed from 2022 as Eurostat's basis changes, and the own commitments of EU Institutions agencies 1 and 2 for the Commission and agency 3 for the EIB. `basis` names each year's basis, and `gap_eur` and `gap_share` give Eurostat's figure less the release's. In the build behind the release the lines sum to EUR 31.73bn in Eurostat for 2024 against EUR 25.67bn in the release, and the EIB (EUR 1.80bn), Germany (EUR 1.62bn) and Spain (EUR 1.35bn) carry most of the gap.
- Adds the release gate `eurostat_reconciliation`, which runs `reconcile_eurostat()` on the release and fails a line-year from 2020 to 2024 whose gap is above EUR 100m and 10% of Eurostat's figure without a `[[eurostat_gaps]]` entry in `build.toml`, a recorded gap that moved by more than the larger of 5% of Eurostat's figure and EUR 100m, an entry whose line-year is now within tolerance, and a line the Eurostat reader knows (the 27 members, the Commission and the EIB) that `eurostat_climate_finance` lacks in a year from 2020 to 2024. `build.toml` records the 26 line-years over tolerance in the build behind the release, with the cause where one is traced: the EIB's 2021 and 2024 repeat rows, Germany's 2023 contribution to the IMF Resilience and Sustainability Trust and its 2024 falls at KfW and two federal ministries, Spain's 2024 other official flows without Rio markers, and Italy's 2022 JETP loans. A release without `eurostat_climate_finance` fails the gate.
- Ships each release's currency and price conversion factors in the release table `conversion_factors`, built from pydeflate's OECD DAC exchange rates and deflators, so a query converts offline and a converted figure changes only with a new release. pydeflate moves to the `build` extra.
- Counts a development finance institution's money once, at the stage its provider counts. The release tables carry the CRS private sector instrument flag as `psi_flag`, and no query, `total_flows` or `quality` counts a record the provider reports for memorandum (flags 4, 40, 41, 42 and 50), such as British International Investment's investments beside the UK's capital in it. ODA-only amounts are unchanged, and ODA-only shares of total move with their all-flows denominator. The build stops on a CRDF private sector instrument row (flow category 60) whose CRS record it cannot find. `Release.reconcile()` adds `crs_memorandum_disbursed_at_party_weight`, the memorandum records' disbursements at the party's weights, since Norway's BTR counts Norfund's investments where the CRS comparator counts Norway's capital in it. Releases move to data format 3, and the queries raise `FormatQueryError` on an older release, whose totals hold the memorandum records, while `Release.table()` still reads it.
- Carries the CRS flow category and type of finance on every commitment, disbursement and untraced row, as `flow_category` and `finance_t`, and on the rows of `Result.rows()`. A commitment takes both from its CRDF row, a disbursement or untraced row from its CRS record, and a bank-ledger or IATI fill from its commitment.
- Refuses to open a release of the current data format whose table lacks a column the schema declares, such as a pack built before `finance_t` was added, with a `ReleaseIntegrityError` naming each table and its missing columns. Before, the query filled the column with nulls.
- Spreads provider-view attributed finance over the climate outflows of the agency each core contribution went to, in the same year and flow, each disjoint part of the value (adaptation only, mitigation only, cross-cutting) over the agency's outflows of that part, so no spread row is negative. A contribution becomes one row per recipient and purpose the agency reached, with the agency in the new row column `via_provider_code`, so `recipients=` in the provider view keeps the attributed finance that reached them, where it dropped every attributed dollar before. Totals by provider and year are unchanged. A contribution with no outflows to spread over stays at no recipient, and `Release.quality().attribution` reports it as `unspread_usd`. `share_of_total()` gives attributed rows a null share, and `combined_share()` refuses a provider view with `by="recipient_code"` or with `recipients=`, since no CRS total holds attributed finance at a recipient. The International Investment Bank (channel 47146) and PIDG (channel 47086) join the channels whose agency share the build measures on their own commitments.
- Adds the release tables `outflow_shares` and `developed_providers`. `outflow_shares` holds, for each international institution and each year from 2013 to 2024, the share of its climate outflows that the OECD attributes to developed countries in Table 8 of _Climate Finance Provided and Mobilised by Developed Countries in 2013-2024_, with the column of the table each year takes and the reason. `developed_providers` lists the developed countries of the report's Table 7, which are the DAC members less Korea, plus Bulgaria, Croatia, Cyprus, Latvia, Malta, Romania, Liechtenstein and Monaco. Both compile from `build/oecd_attribution.yaml`, and `sources.json` credits the report.
- Adds `attribution="outflows"` to `Release.spending()` and `Spec` for the provider view, the OECD's rule for climate finance provided by developed countries. Each institution in `outflow_shares` counts its climate outflows, by recipient and purpose, times its Table 8 share for the year, split among the developed countries by their core contributions to it, summed from the release's first year. The EIB's share stays with EU Institutions, whose own rows count in full, and the listed institutions' own rows leave the view. An institution-year no developed country had contributed to keeps its share in rows with no provider, which `explain()` names. Rows carry the new columns `outflow_share` and `donor_share`, and result tables the new `attribution` column after `view`. The default, `"contributions"`, keeps the provider view as it was, and the recipient view refuses `"outflows"` with `InvalidInputError`. Years Table 8 covers none of, such as those before 2013, give a `CoverageWarning`.
- Adds the how-to `docs/how-to/reproduce-the-oecd-public-total.md`, which builds the OECD-comparable public climate finance figure from `attribution="outflows"` and the `developed_providers` table, in commitments and disbursements, and states the 2024 figures against the OECD's USD 101.6bn.
- Withholds the Green Climate Fund's ledger rows from releases, as its terms bar redistribution. `commitments.withheld_usd` states their amount.
- Requires Python 3.12 or later.
- Adds the columns `flow`, `view`, `attribution`, `currency`, `prices`, `measure` and `no_adaptation_marker` to spending results, and `range_low` and `range_high` under several methods.
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

# Changes to the climate_finance package

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
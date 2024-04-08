# Changes to the climate_finance package

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
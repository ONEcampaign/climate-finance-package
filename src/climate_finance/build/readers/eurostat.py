"""Eurostat's `sdg_13_50`, climate finance by EU member state, the Commission and the
EIB, as `sources.eurostat` saves it."""

from pathlib import Path

import polars as pl

from climate_finance.build.errors import BuildError
from climate_finance.schema import EIB_AGENCY, EU_INSTITUTIONS, EUROSTAT_CLIMATE_FINANCE

FILE = Path("eurostat/sdg_13_50.csv")
# The DAC provider code of each member state, by Eurostat's geo code (Greece is `EL`),
# and `(provider, agency)` of the other lines. `EU27_2020` is the sum of the 27
# members alone, so it has no provider; `COMM` has the provider but no single agency.
MEMBER_PROVIDERS = {
    "AT": 1,
    "BE": 2,
    "DK": 3,
    "FR": 4,
    "DE": 5,
    "IT": 6,
    "NL": 7,
    "PT": 9,
    "SE": 10,
    "FI": 18,
    "IE": 21,
    "LU": 22,
    "CY": 30,
    "EL": 40,
    "MT": 45,
    "ES": 50,
    "SI": 61,
    "HR": 62,
    "CZ": 68,
    "SK": 69,
    "BG": 72,
    "HU": 75,
    "PL": 76,
    "RO": 77,
    "EE": 82,
    "LV": 83,
    "LT": 84,
}
LINES: dict[str, tuple[int | None, int | None]] = {
    **{geo: (provider, None) for geo, provider in MEMBER_PROVIDERS.items()},
    "EU27_2020": (None, None),
    "COMM": (EU_INSTITUTIONS, None),
    "EIB": (EU_INSTITUTIONS, EIB_AGENCY),
}


def read_eurostat(path: Path) -> pl.DataFrame:
    """The `eurostat_climate_finance` table from the SDMX-CSV `download_climate_finance`
    saved.

    Args:
        path: The SDMX-CSV file.

    Returns:
        One row per geo and year with an observation, in EUR, sorted by the table's
        key.

    Raises:
        BuildError: when the file holds a geo code `LINES` lacks, a frequency other
            than annual or a unit other than millions of euro.
    """
    raw = pl.read_csv(path, infer_schema=False)
    if unknown := sorted(set(raw["geo"]) - LINES.keys()):
        raise BuildError(f"{path.name} has geo codes without a provider: {unknown}")
    if (units := set(raw["unit"])) != {"MIO_EUR"} or set(raw["freq"]) != {"A"}:
        raise BuildError(f"{path.name} is not annual, in million euro: {units}")
    return (
        raw.filter(pl.col("OBS_VALUE").is_not_null())
        .select(
            geo=pl.col("geo"),
            provider_code=pl.col("geo").replace_strict(
                {geo: pair[0] for geo, pair in LINES.items()}, return_dtype=pl.Int32
            ),
            agency_code=pl.col("geo").replace_strict(
                {geo: pair[1] for geo, pair in LINES.items()}, return_dtype=pl.Int32
            ),
            year=pl.col("TIME_PERIOD").cast(pl.Int16),
            # Eurostat gives millions to two decimals, so whole euro is exact.
            value_eur=(pl.col("OBS_VALUE").cast(pl.Float64) * 1e6).round(0),
            obs_flag=pl.col("OBS_FLAG"),
            last_update=pl.col("LAST UPDATE").str.to_date("%d/%m/%y %H:%M:%S"),
        )
        .select(list(EUROSTAT_CLIMATE_FINANCE.columns))
        .sort(EUROSTAT_CLIMATE_FINANCE.sort_key)
    )

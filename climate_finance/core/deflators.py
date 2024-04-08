import pandas as pd
from pydeflate import set_pydeflate_path, deflate, exchange

from climate_finance.common.schema import ClimateSchema
from climate_finance.config import ClimateDataPath
from climate_finance.core.enums import ValidPrices, ValidCurrencies

set_pydeflate_path(ClimateDataPath.raw_data)


def oecd_deflator(
    data: pd.DataFrame,
    prices: ValidPrices,
    target_currency: ValidCurrencies,
    base_year: int | None = None,
) -> pd.DataFrame:
    """"""

    if prices == "constant":
        return deflate(
            df=data,
            base_year=base_year,
            deflator_source="oecd_dac",
            deflator_method="dac_deflator",
            exchange_source="oecd_dac",
            source_currency="USA",
            target_currency=target_currency.name,
            id_column=ClimateSchema.PROVIDER_CODE,
            id_type="DAC",
            date_column=ClimateSchema.YEAR,
            source_column=ClimateSchema.VALUE,
            target_column=ClimateSchema.VALUE,
        ).assign(currency=target_currency.name, prices="constant")

    return exchange(
        df=data,
        source_currency="USA",
        target_currency=target_currency.name,
        rates_source="oecd_dac",
        id_column=ClimateSchema.PROVIDER_CODE,
        id_type="DAC",
        date_column=ClimateSchema.YEAR,
        value_column=ClimateSchema.VALUE,
        target_column=ClimateSchema.VALUE,
    ).assign(currency=target_currency.name, prices="current")


def imf_deflator(
    data: pd.DataFrame,
    prices: ValidPrices,
    target_currency: ValidCurrencies,
    base_year: int | None = None,
) -> pd.DataFrame:
    """"""

    if prices == "constant":
        return deflate(
            df=data,
            base_year=base_year,
            deflator_source="imf",
            deflator_method="gdp",
            exchange_source="imf",
            source_currency="USA",
            target_currency=target_currency.name,
            id_column=ClimateSchema.PROVIDER_ISO_CODE,
            id_type="ISO3",
            date_column=ClimateSchema.YEAR,
            source_column=ClimateSchema.VALUE,
            target_column=ClimateSchema.VALUE,
        ).assign(currency=target_currency.name, prices="constant")

    return exchange(
        df=data,
        source_currency="USA",
        target_currency=target_currency.name,
        rates_source="imf",
        id_column=ClimateSchema.PROVIDER_ISO_CODE,
        id_type="ISO3",
        date_column=ClimateSchema.YEAR,
        value_column=ClimateSchema.VALUE,
        target_column=ClimateSchema.VALUE,
    ).assign(currency=target_currency.name, prices="current")


def world_bank_deflator(
    data: pd.DataFrame,
    prices: ValidPrices,
    target_currency: ValidCurrencies,
    base_year: int | None = None,
) -> pd.DataFrame:
    """"""

    if prices == "constant":
        return deflate(
            df=data,
            base_year=base_year,
            deflator_source="wb",
            deflator_method="gdp_linked",
            exchange_source="wb",
            source_currency="USA",
            target_currency=target_currency.name,
            id_column=ClimateSchema.PROVIDER_ISO_CODE,
            id_type="ISO3",
            date_column=ClimateSchema.YEAR,
            source_column=ClimateSchema.VALUE,
            target_column=ClimateSchema.VALUE,
        ).assign(currency=target_currency.name, prices="constant")

    return exchange(
        df=data,
        source_currency="USA",
        target_currency=target_currency.name,
        rates_source="wb",
        id_column=ClimateSchema.PROVIDER_ISO_CODE,
        id_type="ISO3",
        date_column=ClimateSchema.YEAR,
        value_column=ClimateSchema.VALUE,
        target_column=ClimateSchema.VALUE,
    ).assign(currency=target_currency.name, prices="current")

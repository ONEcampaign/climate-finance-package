import pandas as pd
from oda_data import ODAData, set_data_path

from climate_finance.common.analysis_tools import filter_providers
from climate_finance.common.schema import (
    ClimateSchema,
    MULTISYSTEM_INDICATORS,
    CRS_MAPPING,
    MULTISYSTEM_COLUMNS,
)
from climate_finance.config import ClimateDataPath
from climate_finance.oecd.cleaning_tools.tools import (
    convert_flows_millions_to_units,
    channel_codes_to_names,
    clean_multisystem_indicators,
    key_crs_columns_to_str,
)

set_data_path(ClimateDataPath.raw_data)


def remap_select_channels_at_spending_level(df: pd.DataFrame) -> pd.DataFrame:
    # corrected_mapping = {
    #     44003: 44002,
    #     46016: 46015,
    #     46017: 46015,
    #     46018: 46015,
    #     46019: 46015,
    # }
    #
    # df[ClimateSchema.CHANNEL_CODE] = (
    #     df[ClimateSchema.CHANNEL_CODE]
    #     .map(corrected_mapping)
    #     .fillna(df[ClimateSchema.CHANNEL_CODE])
    # )

    names = (
        df.filter([ClimateSchema.CHANNEL_CODE, ClimateSchema.CHANNEL_NAME])
        .drop_duplicates()
        .set_index(ClimateSchema.CHANNEL_CODE)[ClimateSchema.CHANNEL_NAME]
        .to_dict()
    )

    df[ClimateSchema.CHANNEL_NAME] = df[ClimateSchema.CHANNEL_CODE].map(names)

    df = (
        df.groupby(
            [c for c in df.columns if c != ClimateSchema.VALUE],
            dropna=False,
            observed=True,
        )
        .sum(numeric_only=True)
        .reset_index()
        .astype({ClimateSchema.CHANNEL_CODE: "int64[pyarrow]"})
    )

    return df


def get_multilateral_contributions(
    start_year: int,
    end_year: int,
    provider_code: list[str] | str | int | None = None,
) -> pd.DataFrame:
    """Get the multilateral contributions data from the OECD.

    This script also handles cleaning and reshaping the data.

    Args:
        start_year (int, optional): The start year
        end_year (int, optional): The end year
        provider_code (list[str] | str, optional): The provider code(s) to filter the data by.

    """

    # Create an ODAData object, for the years selected
    oda = ODAData(years=range(start_year, end_year + 1), include_names=True)

    # Load the right indicator for commitments or disbursements
    oda.load_indicator(indicators=list(MULTISYSTEM_INDICATORS))

    # Get all the data that has been loaded. Clean the dataframe.
    data = oda.get_data().pipe(_clean_multi_contributions)

    data = filter_providers(data=data, provider_codes=provider_code)

    return data.reset_index(drop=True)


if __name__ == "__main__":
    df = get_multilateral_contributions(start_year=2018, end_year=2021).pipe(
        remap_select_channels_at_spending_level
    )

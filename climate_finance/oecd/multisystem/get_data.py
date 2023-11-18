import pandas as pd
from oda_data import ODAData, set_data_path

from climate_finance.config import ClimateDataPath
from climate_finance.oecd.cleaning_tools.schema import (
    ClimateSchema,
    MULTISYSTEM_INDICATORS,
    CRS_MAPPING,
)
from climate_finance.oecd.cleaning_tools.tools import (
    convert_flows_millions_to_units,
    get_crs_official_mapping,
)

set_data_path(ClimateDataPath.raw_data)


def _clean_multi_contributions(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the multilateral contributions dataframe

    Converts to units, renames and keeps only relevant columns

    Args:
        df (pd.DataFrame): The dataframe to clean.
        flow_type (str): The flow type (disbursements or commitments).

    """

    # define the columns to keep and their new names
    columns = {
        "year": ClimateSchema.YEAR,
        "indicator": ClimateSchema.FLOW_TYPE,
        "donor_code": ClimateSchema.PROVIDER_CODE,
        "donor_name": ClimateSchema.PROVIDER_NAME,
        "channel_code": ClimateSchema.CHANNEL_CODE,
        "channel_name": ClimateSchema.CHANNEL_NAME,
        "value": ClimateSchema.VALUE,
    }

    # Get the CRS channel names (mapped by channel code)
    channel_mapping = (
        get_crs_official_mapping()
        .rename(columns=CRS_MAPPING)
        .set_index(ClimateSchema.CHANNEL_CODE)[ClimateSchema.CHANNEL_NAME]
        .to_dict()
    )

    return (
        df.rename(columns=CRS_MAPPING)
        .pipe(
            convert_flows_millions_to_units, flow_columns=[ClimateSchema.VALUE]
        )  # convert to millions
        .assign(
            indicator=lambda d: d.indicator.map(
                MULTISYSTEM_INDICATORS
            ),  # rename indicator
            channel_name=lambda d: d[ClimateSchema.CHANNEL_CODE].map(
                channel_mapping
            ),  # map channel name
        )
        .rename(columns=columns)  # rename columns
        .filter(columns.values(), axis=1)  # keep only relevant columns
        .groupby(
            [c for c in columns.values() if c != ClimateSchema.VALUE],
            as_index=False,
            dropna=False,
            observed=True,
        )
        .sum()  # summarise the data
    )


def get_multilateral_contributions(
    start_year: int = 2019,
    end_year: int = 2021,
) -> pd.DataFrame:
    """Get the multilateral contributions data from the OECD.

    This script also handles cleaning and reshaping the data.

    Args:
        start_year (int, optional): The start year. Defaults to 2019.
        end_year (int, optional): The end year. Defaults to 2021.

    """

    # Create an ODAData object, for the years selected
    oda = ODAData(years=range(start_year, end_year + 1), include_names=True)

    # Load the right indicator for commitments or disbursements
    oda.load_indicator(indicators=list(MULTISYSTEM_INDICATORS))

    # Get all the data that has been loaded. Clean the dataframe.
    data = oda.get_data().pipe(_clean_multi_contributions)

    return data

import pandas as pd
from oda_data import ODAData, set_data_path

from climate_finance.config import ClimateDataPath
from climate_finance.oecd.cleaning_tools.tools import (
    convert_flows_millions_to_units,
    get_crs_official_mapping,
)

set_data_path(ClimateDataPath.raw_data)

MULTISYSTEM_INDICATORS: dict = {
    "multisystem_multilateral_contributions_disbursement_gross": "usd_disbursement",
    "multisystem_multilateral_contributions_commitments_gross": "usd_commitment",
}


def _clean_multi_contributions(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the multilateral contributions dataframe

    Converts to units, renames and keeps only relevant columns

    Args:
        df (pd.DataFrame): The dataframe to clean.
        flow_type (str): The flow type (disbursements or commitments).

    """

    # define the columns to keep and their new names
    columns = {
        "year": "year",
        "indicator": "flow_type",
        "donor_code": "oecd_donor_code",
        "donor_name": "oecd_donor_name",
        "channel_code": "oecd_channel_code",
        "channel_name": "oecd_channel_name",
        "value": "value",
    }

    channel_mapping = (
        get_crs_official_mapping().set_index("channel_code")["channel_name"].to_dict()
    )

    return (
        df.pipe(convert_flows_millions_to_units, flow_columns=["value"])
        .assign(
            indicator=lambda d: d.indicator.map(MULTISYSTEM_INDICATORS),
            channel_name=lambda d: d.channel_code.map(channel_mapping),
        )
        .rename(columns=columns)
        .filter(columns.values(), axis=1)
        .groupby(
            [c for c in columns.values() if c != "value"],
            as_index=False,
            dropna=False,
            observed=True,
        )
        .sum()
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

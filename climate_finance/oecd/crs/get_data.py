import pandas as pd
from oda_data import read_crs, set_data_path, download_crs

from climate_finance.common.analysis_tools import (
    add_net_disbursement,
    check_provider_codes_type,
)
from climate_finance.common.schema import ClimateSchema
from climate_finance.config import ClimateDataPath
from climate_finance.oecd.cleaning_tools.settings import (
    relevant_crs_columns,
    all_flow_columns,
)
from climate_finance.oecd.cleaning_tools.tools import (
    convert_flows_millions_to_units,
    rename_crs_columns,
    keep_only_allocable_aid,
    key_crs_columns_to_str,
    fix_crs_year_encoding,
    clean_adaptation_and_mitigation_columns,
)

set_data_path(ClimateDataPath.raw_data)


def read_clean_crs(years: list[int] | range) -> pd.DataFrame:
    """Helper function to get a copy of the CRS with clean column names
    and correct data types.

    Args:
        years (list[int]): The years to read.

    Returns:
        pd.DataFrame: A dataframe with clean column names and correct data types.

    """
    return (
        read_crs(years=years)
        .pipe(rename_crs_columns)
        .pipe(key_crs_columns_to_str)
        .pipe(fix_crs_year_encoding)
    )


def get_crs(
    start_year: int,
    end_year: int,
    groupby: list = None,
    provider_code: list[str] | str | None = None,
    force_update: bool = False,
) -> pd.DataFrame:
    """
    Fetches bilateral spending data for a given flow type and time period.

    Args:

        start_year (int, optional): The starting year for data extraction. Defaults to 2019.
        end_year (int, optional): The ending year for data extraction. Defaults to 2020.
        groupby: The columns to group by to aggregate/summarize the data.
        provider_code (list[str] | str, optional): The party code(s) to filter the data by.
        force_update (bool, optional): If True, the data is updated from the source.
        Defaults to False.

    Returns:
        pd.DataFrame: A dataframe containing bilateral spending data for
        the specified flow type and time period.
    """
    # Study years
    years = range(start_year, end_year + 1)

    # Check if data should be forced to update
    if force_update:
        download_crs(years=years)

    # get relevant columns plus flow modality
    columns = relevant_crs_columns() + [ClimateSchema.FLOW_MODALITY]

    # get flow columns
    flow_columns = all_flow_columns()

    # set the right grouper
    if groupby is None:
        groupby = columns

    # check that groupby is unique and includes flow_type
    groupby = list(dict.fromkeys(groupby + [ClimateSchema.FLOW_TYPE]))

    # Read CRS and rename columns
    crs = read_clean_crs(years=years)

    # Filter by provider code
    if provider_code is not None:
        provider_code = check_provider_codes_type(provider_codes=provider_code)
        crs = crs.loc[lambda d: d[ClimateSchema.PROVIDER_CODE].isin(provider_code)]

    # Add net disbursement
    crs = crs.pipe(add_net_disbursement)

    crs = (
        crs.filter(columns + flow_columns, axis=1)  # Keep only relevant columns
        .pipe(clean_adaptation_and_mitigation_columns)
        .pipe(convert_flows_millions_to_units, flow_columns=flow_columns)
        .filter(items=groupby + flow_columns)
        .melt(
            id_vars=[
                c for c in groupby if c in crs.columns and c != ClimateSchema.FLOW_TYPE
            ],
            value_vars=flow_columns,
            var_name=ClimateSchema.FLOW_TYPE,
            value_name=ClimateSchema.VALUE,
        )
        .groupby(by=groupby, dropna=False, observed=True)[ClimateSchema.VALUE]
        .sum()
        .reset_index()
        .loc[lambda d: d[ClimateSchema.VALUE] != 0]
        .reset_index(drop=True)
    )

    return crs


def get_crs_allocable_spending(
    start_year: int = 2019,
    end_year: int = 2020,
    provider_code: list[str] | str | None = None,
    force_update: bool = False,
) -> pd.DataFrame:
    """
    Fetches bilateral spending data for a given flow type and time period.

    Args:
        start_year (int, optional): The starting year for data extraction. Defaults to 2019.
        end_year (int, optional): The ending year for data extraction. Defaults to 2020.
        provider_code (list[str] | str, optional): The provider code(s) to filter the data by.
        force_update (bool, optional): If True, the data is updated from the source.
        Defaults to False.

    Returns:
        pd.DataFrame: A dataframe containing bilateral spending data for
        the specified flow type and time period.
    """
    crs = get_crs(
        start_year=start_year,
        end_year=end_year,
        provider_code=provider_code,
        force_update=force_update,
    )

    crs = crs.pipe(keep_only_allocable_aid)

    return crs.reset_index(drop=True)

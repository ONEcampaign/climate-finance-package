import pandas as pd
from oda_data import read_crs, set_data_path, download_crs

from climate_finance.config import ClimateDataPath
from climate_finance.oecd.cleaning_tools.tools import convert_flows_millions_to_units
from climate_finance.oecd.cleaning_tools.schema import CRS_MAPPING, CrsSchema

set_data_path(ClimateDataPath.raw_data)


def _keep_only_allocable_aid(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters the dataframe to retain only specific aid types considered allocable.

    Args:
        df (pd.DataFrame): The input dataframe with aid data.

    Returns:
        pd.DataFrame: A dataframe containing only the rows with allocable aid types."""

    aid_types = ["A02", "B01", "B03", "B04", "C01", "D01", "D02", "E01"]
    return df.loc[lambda d: d[CrsSchema.FLOW_MODALITY].isin(aid_types)].reset_index(
        drop=True
    )


def _get_relevant_crs_columns() -> list:
    """
    Fetches the list of relevant columns from the CRS data for data extraction.

    Returns:
        list: A list of column names considered relevant for data extraction."""

    return [
        CrsSchema.YEAR,
        CrsSchema.PARTY_CODE,
        CrsSchema.PARTY_NAME,
        CrsSchema.AGENCY_NAME,
        CrsSchema.RECIPIENT_CODE,
        CrsSchema.RECIPIENT_NAME,
        CrsSchema.FLOW_CODE,
        CrsSchema.FLOW_NAME,
        CrsSchema.SECTOR_CODE,
        CrsSchema.SECTOR_NAME,
        CrsSchema.PURPOSE_CODE,
        CrsSchema.PURPOSE_NAME,
        CrsSchema.PROJECT_TITLE,
        CrsSchema.CRS_ID,
        CrsSchema.PROJECT_ID,
        CrsSchema.PROJECT_DESCRIPTION,
        CrsSchema.FINANCE_TYPE,
        CrsSchema.MITIGATION,
        CrsSchema.ADAPTATION,
    ]


def _rename_crs_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renames certain columns in the CRS dataframe.

    Args:
        df: A dataframe containing the CRS data.

    Returns:
        A dataframe with renamed columns.
    """

    return df.rename(columns=CRS_MAPPING)


def _get_flow_columns() -> list:
    """
    Fetches the list of flow columns from the CRS data for data extraction.

    Returns:
        list: A list of column names considered relevant for data extraction.

    """
    return [
        CrsSchema.USD_COMMITMENT,
        CrsSchema.USD_DISBURSEMENT,
        CrsSchema.USD_RECEIVED,
        CrsSchema.USD_GRANT_EQUIV,
        CrsSchema.USD_NET_DISBURSEMENT,
    ]


def _set_crs_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sets the data types for columns in the CRS dataframe.

    Args:
        df (pd.DataFrame): The input dataframe with CRS data.

    Returns:
        pd.DataFrame: The dataframe with specified column data types set."""

    return df.astype(
        {
            CrsSchema.PARTY_CODE: "Int32",
            CrsSchema.YEAR: "Int32",
            CrsSchema.PARTY_NAME: "str",
            CrsSchema.RECIPIENT_NAME: "str",
            CrsSchema.RECIPIENT_CODE: "Int32",
            CrsSchema.AGENCY_NAME: "str",
            CrsSchema.FLOW_NAME: "str",
            CrsSchema.FLOW_CODE: "Int32",
            CrsSchema.MITIGATION: "str",
            CrsSchema.ADAPTATION: "str",
        }
    )


def _replace_missing_climate_with_zero(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Replaces missing values in a specified column with an empty string.

    Args:
        df (pd.DataFrame): The input dataframe with CRS data.
        column (str): The name of the column in which to replace missing values.

    Returns:
        pd.DataFrame: The dataframe with missing values in the specified column
        replaced by an empty string.
    """

    return df.assign(**{column: lambda d: d[column].replace("nan", "0")})


def _add_net_disbursement(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a column with net disbursement values.
    Args:
        df: A dataframe with a usd_disbursement and a usd_received column.

    Returns:
        A dataframe with a usd_net_disbursement column added.

    """
    return df.assign(
        **{
            CrsSchema.USD_NET_DISBURSEMENT: lambda d: d[
                CrsSchema.USD_DISBURSEMENT
            ].fillna(0)
            - d[CrsSchema.USD_RECEIVED].fillna(0)
        }
    )


def get_crs_allocable_spending(
    start_year: int = 2019, end_year: int = 2020, force_update: bool = False
) -> pd.DataFrame:
    """
    Fetches bilateral spending data for a given flow type and time period.

    Args:
        start_year (int, optional): The starting year for data extraction. Defaults to 2019.
        end_year (int, optional): The ending year for data extraction. Defaults to 2020.
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

    # get relevant columns
    columns = _get_relevant_crs_columns()

    # get flow columns
    flow_columns = _get_flow_columns()

    # Pipeline
    crs = (
        read_crs(years=years)  # Read CRS data
        .pipe(_rename_crs_columns)  # Rename columns for consistency
        .pipe(_keep_only_allocable_aid)  # Keep only allocable aid types
        .pipe(_add_net_disbursement)  # Add net disbursement column
        .filter(columns + flow_columns, axis=1)  # Keep only relevant columns
        .assign(
            year=lambda d: d[CrsSchema.YEAR]
            .astype("str")
            .str.replace("\ufeff", "", regex=True)
        )  # fix year
        .pipe(_set_crs_data_types)  # Set data types
        .pipe(_replace_missing_climate_with_zero, column=CrsSchema.MITIGATION)
        .pipe(_replace_missing_climate_with_zero, column=CrsSchema.ADAPTATION)
        .groupby(columns, as_index=False, dropna=False, observed=True)[flow_columns]
        .sum()
        .pipe(convert_flows_millions_to_units, flow_columns=flow_columns)
        .melt(
            id_vars=columns,
            value_vars=flow_columns,
            var_name=CrsSchema.FLOW_TYPE,
            value_name=CrsSchema.VALUE,
        )
        .loc[lambda d: d[CrsSchema.VALUE] != 0]
        .reset_index(drop=True)
        .astype({CrsSchema.MITIGATION: "Int16", CrsSchema.ADAPTATION: "Int16"})
    )

    return crs

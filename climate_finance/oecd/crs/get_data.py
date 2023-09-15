import pandas as pd
from oda_data import read_crs, set_data_path

from climate_finance.config import ClimateDataPath
from climate_finance.oecd.cleaning_tools.tools import convert_flows_millions_to_units

set_data_path(ClimateDataPath.raw_data)


def _keep_only_allocable_aid(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters the dataframe to retain only specific aid types considered allocable.

    Args:
        df (pd.DataFrame): The input dataframe with aid data.

    Returns:
        pd.DataFrame: A dataframe containing only the rows with allocable aid types."""

    aid_types = ["A02", "B01", "B03", "B04", "C01", "D01", "D02", "E01"]
    return df.query(f"aid_t in {aid_types}").reset_index(drop=True)


def _get_relevant_crs_columns() -> list:
    """
    Fetches the list of relevant columns from the CRS data for data extraction.

    Returns:
        list: A list of column names considered relevant for data extraction."""

    return [
        "year",
        "donor_code",
        "donor_name",
        "agency_name",
        "recipient_code",
        "recipient_name",
        "flow_code",  # number
        "flow_name",  # name of flow (like OOF or grant for example)
        "finance_t",
        "climate_mitigation",
        "climate_adaptation",
    ]


def _rename_crs_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renames certain columns in the CRS dataframe.

    Args:
        df: A dataframe containing the CRS data.

    Returns:
        A dataframe with renamed columns.
    """

    names = {
        "donor_code": "oecd_donor_code",
        "donor_name": "oecd_donor_name",
        "recipient_code": "oecd_recipient_code",
        "recipient_name": "oecd_recipient_name",
        "agency_name": "oecd_agency_name",
    }

    return df.rename(columns=names)


def _get_flow_columns() -> list:
    """
    Fetches the list of flow columns from the CRS data for data extraction.

    Returns:
        list: A list of column names considered relevant for data extraction.

    """
    return [
        "usd_commitment",
        "usd_disbursement",
        "usd_received",
        "usd_grant_equiv",
        "usd_net_disbursement",
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
            "donor_code": "Int32",
            "donor_name": "str",
            "recipient_name": "str",
            "recipient_code": "Int32",
            "agency_name": "str",
            "flow_name": "str",
            "flow_code": "Int32",
            "climate_mitigation": "str",
            "climate_adaptation": "str",
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
        usd_net_disbursement=lambda d: d.usd_disbursement.fillna(0)
        - d.usd_received.fillna(0)
    )


def get_crs_allocable_spending(
    start_year: int = 2019, end_year: int = 2020
) -> pd.DataFrame:
    """
    Fetches bilateral spending data for a given flow type and time period.

    Args:
        start_year (int, optional): The starting year for data extraction. Defaults to 2019.
        end_year (int, optional): The ending year for data extraction. Defaults to 2020.

    Returns:
        pd.DataFrame: A dataframe containing bilateral spending data for
        the specified flow type and time period.
    """

    # get relevant columns
    columns = _get_relevant_crs_columns()

    # get flow columns
    flow_columns = _get_flow_columns()

    # Study years
    years = range(start_year, end_year + 1)

    # Pipeline
    crs = (
        read_crs(years=years)  # Read CRS data
        .pipe(_keep_only_allocable_aid)  # Keep only allocable aid types
        .pipe(_add_net_disbursement)  # Add net disbursement column
        .filter(columns + flow_columns, axis=1)  # Keep only relevant columns
        .pipe(_set_crs_data_types)  # Set data types
        .pipe(_replace_missing_climate_with_zero, column="climate_mitigation")
        .pipe(_replace_missing_climate_with_zero, column="climate_adaptation")
        .groupby(columns, as_index=False, dropna=False)[flow_columns]
        .sum()
        .pipe(convert_flows_millions_to_units, flow_columns=flow_columns)
        .melt(
            id_vars=columns,
            value_vars=flow_columns,
            var_name="flow_type",
            value_name="value",
        )
        .loc[lambda d: d.value != 0]
        .pipe(_rename_crs_columns)
        .reset_index(drop=True)
    )

    return crs

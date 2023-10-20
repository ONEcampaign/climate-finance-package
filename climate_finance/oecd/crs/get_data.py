import pandas as pd
from oda_data import read_crs, set_data_path, download_crs, ODAData

from climate_finance.config import ClimateDataPath
from climate_finance.oecd.cleaning_tools.tools import (
    convert_flows_millions_to_units,
    rename_crs_columns,
    set_crs_data_types,
)
from climate_finance.oecd.cleaning_tools.schema import CrsSchema

set_data_path(ClimateDataPath.raw_data)


def keep_only_allocable_aid(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters the dataframe to retain only specific aid types considered allocable.

    Args:
        df (pd.DataFrame): The input dataframe with aid data.

    Returns:
        pd.DataFrame: A dataframe containing only the rows with allocable aid types."""

    aid_types = [
        "A02",
        "B01",
        "B03",
        "B031",
        "B032",
        "B033",
        "B04",
        "C01",
        "D01",
        "D02",
        "E01",
    ]
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
        CrsSchema.AGENCY_CODE,
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


def get_crs(
    start_year: int,
    end_year: int,
    groupby: list = None,
    party_code: list[str] | str | None = None,
    force_update: bool = False,
) -> pd.DataFrame:
    """
    Fetches bilateral spending data for a given flow type and time period.

    Args:

        start_year (int, optional): The starting year for data extraction. Defaults to 2019.
        end_year (int, optional): The ending year for data extraction. Defaults to 2020.
        groupby: The columns to group by to aggregate/summarize the data.
        party_code (list[str] | str, optional): The party code(s) to filter the data by.
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
    columns = _get_relevant_crs_columns() + [CrsSchema.FLOW_MODALITY]

    # get flow columns
    flow_columns = _get_flow_columns()

    # set the right grouper
    if groupby is None:
        groupby = columns

    # Pipeline
    crs = read_crs(years=years).pipe(rename_crs_columns)  # Read CRS data

    if party_code is not None:
        if isinstance(party_code, str):
            party_code = [party_code]
        crs = crs.loc[lambda d: d[CrsSchema.PARTY_CODE].isin(party_code)]

    crs = crs.pipe(_add_net_disbursement)

    crs = (
        crs.filter(columns + flow_columns, axis=1)  # Keep only relevant columns
        .assign(
            year=lambda d: d[CrsSchema.YEAR]
            .astype("str")
            .str.replace("\ufeff", "", regex=True)
        )  # fix year
        .pipe(set_crs_data_types)  # Set data types
        .pipe(_replace_missing_climate_with_zero, column=CrsSchema.MITIGATION)
        .pipe(_replace_missing_climate_with_zero, column=CrsSchema.ADAPTATION)
        .astype({CrsSchema.MITIGATION: "Int16", CrsSchema.ADAPTATION: "Int16"})
        .pipe(convert_flows_millions_to_units, flow_columns=flow_columns)
        .filter(items=groupby + flow_columns)
        .melt(
            id_vars=[c for c in groupby if c in crs.columns],
            value_vars=flow_columns,
            var_name=CrsSchema.FLOW_TYPE,
            value_name=CrsSchema.VALUE,
        )
        .groupby(by=groupby, dropna=False, observed=True)[CrsSchema.VALUE]
        .sum()
        .reset_index()
        .loc[lambda d: d[CrsSchema.VALUE] != 0]
        .reset_index(drop=True)
    )

    return crs


def get_crs_allocable_spending(
    start_year: int = 2019,
    end_year: int = 2020,
    force_update: bool = False,
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
    crs = get_crs(
        start_year=start_year,
        end_year=end_year,
        force_update=force_update,
    )

    crs = crs.pipe(keep_only_allocable_aid)

    return crs.reset_index(drop=True)


def get_crs_allocable_to_total_ratio(
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

    simpler_columns = [
        CrsSchema.YEAR,
        CrsSchema.PARTY_CODE,
        CrsSchema.PARTY_NAME,
        CrsSchema.FLOW_MODALITY,
        CrsSchema.FLOW_TYPE,
    ]

    # Pipeline
    crs = get_crs(
        start_year=start_year,
        end_year=end_year,
        force_update=force_update,
        groupby=simpler_columns,
    )

    total = (
        crs.copy()
        .assign(**{CrsSchema.FLOW_MODALITY: "total"})
        .groupby(
            simpler_columns + [CrsSchema.FLOW_TYPE],
            as_index=False,
            dropna=False,
            observed=True,
        )
        .sum(numeric_only=True)
    )

    allocable = (
        crs.pipe(keep_only_allocable_aid)
        .assign(**{CrsSchema.FLOW_MODALITY: "bilateral_allocable"})
        .groupby(
            simpler_columns + [CrsSchema.FLOW_TYPE],
            as_index=False,
            dropna=False,
            observed=True,
        )
        .sum(numeric_only=True)
    )

    data = pd.concat([allocable, total], ignore_index=True)

    data = (
        data.pivot(
            index=[
                c
                for c in data.columns
                if c not in [CrsSchema.VALUE, CrsSchema.FLOW_MODALITY]
            ],
            columns=CrsSchema.FLOW_MODALITY,
            values=CrsSchema.VALUE,
        )
        .reset_index()
        .assign(allocable_share=lambda d: d.bilateral_allocable / d.total)
    )

    return data

import pandas as pd
from oda_data import read_crs, set_data_path, download_crs

from climate_finance.common.analysis_tools import add_net_disbursement
from climate_finance.config import ClimateDataPath
from climate_finance.oecd.cleaning_tools.settings import (
    relevant_crs_columns,
    all_flow_columns,
)
from climate_finance.oecd.cleaning_tools.tools import (
    convert_flows_millions_to_units,
    rename_crs_columns,
    set_crs_data_types,
    keep_only_allocable_aid,
    replace_missing_climate_with_zero,
)
from climate_finance.common.schema import ClimateSchema

set_data_path(ClimateDataPath.raw_data)


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

    # get relevant columns
    columns = relevant_crs_columns() + [ClimateSchema.FLOW_MODALITY]

    # get flow columns
    flow_columns = all_flow_columns()

    # set the right grouper
    if groupby is None:
        groupby = columns

    # check that groupby is unique and includes flow_type
    groupby = list(dict.fromkeys(groupby + [ClimateSchema.FLOW_TYPE]))

    # Pipeline
    crs = read_crs(years=years).pipe(rename_crs_columns)  # Read CRS data

    if provider_code is not None:
        if isinstance(provider_code, str):
            provider_code = [provider_code]
        crs = crs.loc[lambda d: d[ClimateSchema.PROVIDER_CODE].isin(provider_code)]

    crs = crs.pipe(add_net_disbursement)

    crs = (
        crs.filter(columns + flow_columns, axis=1)  # Keep only relevant columns
        .assign(
            year=lambda d: d[ClimateSchema.YEAR]
            .astype("str")
            .str.replace("\ufeff", "", regex=True)
        )  # fix year
        .pipe(set_crs_data_types)  # Set data types
        .pipe(replace_missing_climate_with_zero, column=ClimateSchema.MITIGATION)
        .pipe(replace_missing_climate_with_zero, column=ClimateSchema.ADAPTATION)
        .astype({ClimateSchema.MITIGATION: "Int16", ClimateSchema.ADAPTATION: "Int16"})
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
    crs = get_crs(start_year=start_year, end_year=end_year, force_update=force_update)

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
        ClimateSchema.YEAR,
        ClimateSchema.PROVIDER_CODE,
        ClimateSchema.AGENCY_CODE,
        ClimateSchema.FLOW_MODALITY,
        ClimateSchema.FLOW_TYPE,
    ]

    # Pipeline
    crs = get_crs(start_year=start_year, end_year=end_year, groupby=simpler_columns,
                  force_update=force_update)

    total = (
        crs.copy()
        .assign(**{ClimateSchema.FLOW_MODALITY: "total"})
        .groupby(
            simpler_columns,
            dropna=False,
            observed=True,
        )
        .sum(numeric_only=True)
        .reset_index()
    )

    allocable = (
        crs.pipe(keep_only_allocable_aid)
        .assign(**{ClimateSchema.FLOW_MODALITY: "bilateral_allocable"})
        .groupby(
            simpler_columns,
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
                if c not in [ClimateSchema.VALUE, ClimateSchema.FLOW_MODALITY]
            ],
            columns=ClimateSchema.FLOW_MODALITY,
            values=ClimateSchema.VALUE,
        )
        .reset_index()
        .assign(allocable_share=lambda d: (d.bilateral_allocable / d.total).fillna(0))
    )

    return data

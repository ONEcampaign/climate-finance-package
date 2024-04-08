import numpy as np
import pandas as pd
from oda_data.clean_data.channels import clean_string

from climate_finance.common.schema import ClimateSchema
from climate_finance.methodologies.multilateral.multilateral_spending_data import (
    get_multilateral_spending_data,
    add_crs_data,
)
from climate_finance.oecd.cleaning_tools.tools import idx_to_str

CRDF_IDX = [
    ClimateSchema.YEAR,
    ClimateSchema.PROVIDER_CODE,
    ClimateSchema.AGENCY_CODE,
    ClimateSchema.PROVIDER_TYPE,
    ClimateSchema.CRS_ID,
    ClimateSchema.PROJECT_ID,
    ClimateSchema.PROJECT_TITLE,
    ClimateSchema.RECIPIENT_CODE,
    ClimateSchema.CONCESSIONALITY,
    ClimateSchema.CLIMATE_OBJECTIVE,
    ClimateSchema.CHANNEL_CODE_DELIVERY,
    ClimateSchema.PURPOSE_CODE,
    ClimateSchema.FLOW_NAME,
    ClimateSchema.FLOW_MODALITY,
    ClimateSchema.FINANCIAL_INSTRUMENT,
    ClimateSchema.FINANCE_TYPE,
    ClimateSchema.FLOW_TYPE,
]

CRDF_VALUES = [
    ClimateSchema.ADAPTATION_VALUE,
    ClimateSchema.MITIGATION_VALUE,
    ClimateSchema.CROSS_CUTTING_VALUE,
]


def group_and_summarize(df: pd.DataFrame) -> pd.DataFrame:
    """
    Group by the dataframe and summarize it.

    Args:
        df (pd.DataFrame): The input dataframe.

    Returns:
        pd.DataFrame: Grouped and summarized dataframe.
    """

    idx = [c for c in df.columns if c not in CRDF_VALUES and c in CRDF_IDX]

    df = idx_to_str(df, idx=idx)

    df = (
        df.groupby(by=idx, observed=True, dropna=False)
        .agg("sum", numeric_only=True)
        .reset_index()
        .filter(items=idx + CRDF_VALUES)
    )

    return df


def summarise_by_row(df: pd.DataFrame) -> pd.DataFrame:
    """
    Summarize the dataframe row-wise.

    Args:
        df (pd.DataFrame): The input dataframe.

    Returns:
        pd.DataFrame: Row-wise summarized dataframe.
    """
    group_by_cols = [
        c
        for c in df.columns
        if c
        not in [
            "climate_adaptation_value",
            "climate_mitigation_value",
            "overlap_commitment_current",
            "commitment_climate_share",
            "climate_finance_value",
            "gender",
        ]
    ]

    if not group_by_cols:
        return df

    df = (
        df.groupby(by=group_by_cols, observed=True)
        .agg(
            {
                "climate_adaptation_value": "sum",
                "climate_mitigation_value": "sum",
                "overlap_commitment_current": "sum",
            }
        )
        .reset_index()
    )

    return df


def subtract_overlaps_by_project(df: pd.DataFrame) -> pd.DataFrame:
    """
    Subtract overlaps by project.

    Args:
        df (pd.DataFrame): The input dataframe.

    Returns:
        pd.DataFrame: Dataframe with values calculated based on conditions.
    """

    # subtract cross-cutting from adaptation and mitigation
    df[ClimateSchema.ADAPTATION_VALUE] = (
        df[ClimateSchema.ADAPTATION_VALUE] - df[ClimateSchema.CROSS_CUTTING_VALUE]
    )
    df[ClimateSchema.MITIGATION_VALUE] = (
        df[ClimateSchema.MITIGATION_VALUE] - df[ClimateSchema.CROSS_CUTTING_VALUE]
    )

    return df


def clean_component(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process the given dataframe and return a dataframe that with clean climate components.

    Args:
        df (pd.DataFrame): The input dataframe.

    Returns:
        pd.DataFrame: The processed dataframe.
    """

    if ClimateSchema.PROJECT_TITLE in df.columns:
        df[ClimateSchema.PROJECT_TITLE] = clean_string(df[ClimateSchema.PROJECT_TITLE])

    return df.pipe(group_and_summarize).pipe(subtract_overlaps_by_project)


CLIMATE_COLS = [
    ClimateSchema.MITIGATION_VALUE,
    ClimateSchema.ADAPTATION_VALUE,
    ClimateSchema.CROSS_CUTTING_VALUE,
]


def _validate_missing_climate_cols(data: pd.DataFrame) -> pd.DataFrame:
    missing_climate = [c for c in CLIMATE_COLS if c not in data.columns]

    if len(missing_climate) > 0:
        for c in missing_climate:
            data[c] = np.nan

    return data


def add_climate_total_column(data: pd.DataFrame) -> pd.DataFrame:
    data[ClimateSchema.CLIMATE_UNSPECIFIED] = (
        data[ClimateSchema.ADAPTATION_VALUE].fillna(0)
        + data[ClimateSchema.MITIGATION_VALUE].fillna(0)
        + data[ClimateSchema.CROSS_CUTTING_VALUE].fillna(0)
    )

    return data


def one_multilateral_spending(
    start_year: int,
    end_year: int,
    provider_code: list[str] | str | int | None = None,
    force_update: bool = False,
) -> pd.DataFrame:
    """
    Compute the rolling totals or shares of climate finance for the multilateral spending data.

    This is done using ONE's methodology. This methodology gets the spending data (aggregated
    by purpose, country, flow_type), applies the highest marker methodology, and then computes
    the rolling totals or shares (based on the window size).

    Args:
        start_year: The start year of the data.
        end_year: The end year of the data.
        rolling_window: The window size for the rolling totals or shares (in years).
        provider_code: The list of parties to filter the data by.
        force_update: Whether to force update the data.

    Returns:
        pd.DataFrame: The rolling totals or shares of climate finance for the multilateral
        spending data.

    """
    data = (
        get_multilateral_spending_data(
            start_year=start_year,
            end_year=end_year,
            provider_code=provider_code,
            force_update=force_update,
        )
        .loc[lambda d: d[ClimateSchema.PROVIDER_TYPE] != "DAC member"]
        .pipe(clean_component)
    )

    data = data.pipe(add_crs_data)

    data = data.pipe(_validate_missing_climate_cols).pipe(add_climate_total_column)

    return data

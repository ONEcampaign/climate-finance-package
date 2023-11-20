import pandas as pd

from climate_finance.common.schema import ClimateSchema
from climate_finance.oecd.cleaning_tools.tools import idx_to_str
from climate_finance.unfccc.cleaning_tools.channels import clean_string

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

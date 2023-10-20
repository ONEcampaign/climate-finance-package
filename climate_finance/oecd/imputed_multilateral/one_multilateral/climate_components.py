import numpy as np
import pandas as pd

from climate_finance.oecd.cleaning_tools.schema import CrsSchema
from climate_finance.oecd.cleaning_tools.tools import idx_to_str, set_crs_data_types

CRDF_IDX = [
    CrsSchema.YEAR,
    CrsSchema.PARTY_CODE,
    CrsSchema.AGENCY_CODE,
    CrsSchema.CRS_ID,
    CrsSchema.PROJECT_ID,
    CrsSchema.RECIPIENT_CODE,
    CrsSchema.CONCESSIONALITY,
    CrsSchema.CLIMATE_OBJECTIVE,
    CrsSchema.CHANNEL_CODE_DELIVERY,
    CrsSchema.PURPOSE_CODE,
    CrsSchema.FLOW_MODALITY,
    CrsSchema.FINANCIAL_INSTRUMENT,
    CrsSchema.FINANCE_TYPE,
    CrsSchema.FLOW_TYPE,
]

CRDF_VALUES = [
    CrsSchema.ADAPTATION_VALUE,
    CrsSchema.MITIGATION_VALUE,
    CrsSchema.CROSS_CUTTING_VALUE,
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
        df.groupby(by=idx, observed=True)
        .agg("sum", numeric_only=True)
        .reset_index()
        .filter(items=idx + CRDF_VALUES)
        .pipe(set_crs_data_types)
        .groupby(by=idx, observed=True)
        .agg("sum", numeric_only=True)
        .reset_index()
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
    df[CrsSchema.ADAPTATION_VALUE] = (
        df[CrsSchema.ADAPTATION_VALUE] - df[CrsSchema.CROSS_CUTTING_VALUE]
    )
    df[CrsSchema.MITIGATION_VALUE] = (
        df[CrsSchema.MITIGATION_VALUE] - df[CrsSchema.CROSS_CUTTING_VALUE]
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

    return df.pipe(group_and_summarize).pipe(subtract_overlaps_by_project)

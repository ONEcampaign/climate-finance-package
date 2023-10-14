import numpy as np
import pandas as pd

from climate_finance.oecd.cleaning_tools.schema import CrsSchema


def group_and_summarize(df: pd.DataFrame) -> pd.DataFrame:
    """
    Group by the dataframe and summarize it.

    Args:
        df (pd.DataFrame): The input dataframe.

    Returns:
        pd.DataFrame: Grouped and summarized dataframe.
    """

    exclude_cols = [
        "share",
        "value",
        "total_value",
        CrsSchema.ADAPTATION_VALUE,
        CrsSchema.MITIGATION_VALUE,
        CrsSchema.CROSS_CUTTING_VALUE,
        "overlap_commitment_current",
        "climate_finance_value",
        "commitment_climate_share",
    ]

    # Store numeric types
    original_types = {k: v for k, v in df.dtypes.to_dict().items() if v == "Int32"}

    # Convert all columns to string
    df = df.astype({k: "str" for k in df.columns if k not in exclude_cols})

    df = (
        df.groupby(by=[c for c in df.columns if c not in exclude_cols], observed=True)
        .sum(numeric_only=True)
        .reset_index()
        .replace("<NA>", np.nan)
        .astype(original_types)
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

    return (
        df.pipe(group_and_summarize)
        .pipe(summarise_by_row)
        .pipe(subtract_overlaps_by_project)
    )

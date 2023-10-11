import numpy as np
import pandas as pd


def add_rounded_total(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a rounded total column to the dataframe.

    Args:
        df (pd.DataFrame): The input dataframe.

    Returns:
        pd.DataFrame: Dataframe with the added rounded_total column.
    """
    return df.assign(
        rounded_total=lambda d: round(d.total_value / 100, 0).astype("Int64")
    )


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicates from the dataframe in two passes.

    Args:
        df (pd.DataFrame): The input dataframe.

    Returns:
        pd.DataFrame: Dataframe with duplicates removed.
    """
    exclude_cols = ["share", "value", "total_value"]

    df = (
        df.sort_values(by=["value"])
        .drop_duplicates(
            subset=[c for c in df.columns if c not in exclude_cols],
            keep="first",
        )
        .reset_index(drop=True)
    )

    return df


def group_and_summarize(df: pd.DataFrame) -> pd.DataFrame:
    """
    Group by the dataframe and summarize it.

    Args:
        df (pd.DataFrame): The input dataframe.

    Returns:
        pd.DataFrame: Grouped and summarized dataframe.
    """

    exclude_cols = ["share", "value", "total_value"]

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


def pivot_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot the dataframe based on the 'indicator' column and 'value'.

    Args:
        df (pd.DataFrame): The input dataframe.

    Returns:
        pd.DataFrame: Pivoted dataframe.
    """
    index_cols = [c for c in df.columns if c not in ["indicator", "value"]]

    df = df.pivot(index=index_cols, columns="indicator", values="value").reset_index()

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
            "Adaptation",
            "Mitigation",
            "Cross-cutting",
            "share",
            "total_value",
            "rounded_total",
        ]
    ]

    if not group_by_cols:
        return df

    df = (
        df.groupby(by=group_by_cols, observed=True)
        .agg(
            {
                "Adaptation": "sum",
                "Mitigation": "sum",
                "Cross-cutting": "sum",
                "share": "max",
                "total_value": "max",
                "rounded_total": "max",
            }
        )
        .reset_index()
    )

    return df


def calculate_values_based_on_conditions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate values based on certain conditions and masks.

    Args:
        df (pd.DataFrame): The input dataframe.

    Returns:
        pd.DataFrame: Dataframe with values calculated based on conditions.
    """
    df = df.fillna(0)

    # Check if adaptation and mitigation are equal
    mask_adaptation_mitigation_equal = df["Adaptation"].round(0) == df[
        "Mitigation"
    ].round(0)

    # Check if cross-cutting is present
    mask_cross_cutting_present = df["Cross-cutting"] > 0

    # Check if cross-cutting should be used. Only used when cross-cutting is present
    # and adaptation and mitigation are equal
    mask_use_cross_cutting = (
        mask_cross_cutting_present & mask_adaptation_mitigation_equal
    )

    # Check if adaptation is higher than mitigation
    mask_adaptation_higher = df["Adaptation"] > df["Mitigation"]

    # Identify cross cutting present and adaptation and mitigation are not equal
    mask_cross_cutting_not_equal = (
        mask_cross_cutting_present & ~mask_adaptation_mitigation_equal
    )

    # Calculate values based on conditions
    cross_cutting_values = df["Cross-cutting"]
    adaptation_higher_values = df["Adaptation"] + df["Mitigation"] - df["Cross-cutting"]
    mitigation_higher_values = df["Mitigation"] + df["Adaptation"] - df["Cross-cutting"]

    # Set values based on conditions
    df["value"] = np.where(
        mask_use_cross_cutting,  # Cross-cutting should be used
        cross_cutting_values,  # Use cross-cutting values
        np.where(  # Otherwise check if adaptation is higher than mitigation
            mask_adaptation_higher,  # Adaptation is higher than mitigation
            adaptation_higher_values,  # Use adaptation_higher_values
            mitigation_higher_values,  # Otherwise use mitigation_higher_values
        ),
    )

    # Set indicator based on conditions
    df["indicator"] = np.where(
        mask_use_cross_cutting,  # Cross-cutting should be used
        "Cross-cutting",  # Use Cross-cutting
        np.where(
            mask_adaptation_higher,  # Adaptation is higher than mitigation
            "Adaptation",  # Use Adaptation
            "Mitigation",  # Otherwise use Mitigation
        ),
    )

    return df


def cleanup_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleanup the dataframe by dropping unnecessary columns.

    Args:
        df (pd.DataFrame): The input dataframe.

    Returns:
        pd.DataFrame: Cleaned up dataframe.
    """
    return df.drop(
        columns=["Adaptation", "Mitigation", "Cross-cutting", "rounded_total"]
    )


def clean_marker(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process the given dataframe and return a dataframe that highlights the highest marker.

    Args:
        df (pd.DataFrame): The input dataframe.

    Returns:
        pd.DataFrame: The processed dataframe.
    """

    return (
        df.pipe(add_rounded_total)
        .pipe(remove_duplicates)
        .pipe(group_and_summarize)
        .pipe(pivot_dataframe)
        .pipe(summarise_by_row)
        .pipe(calculate_values_based_on_conditions)
        .pipe(cleanup_dataframe)
    )

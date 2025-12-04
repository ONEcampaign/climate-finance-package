import numpy as np
import pandas as pd

from climate_finance.common.schema import ClimateSchema


def filter_climate_data(df, highest_marker: bool = True):
    """
    Args:
        df: pandas DataFrame containing climate data.
        highest_marker: Whether to use the highest marker value.

    Returns:
        pandas DataFrame: A filtered DataFrame containing climate data where either
        the 'MITIGATION' or 'ADAPTATION' column has a value greater than 0, and where
        the 'MITIGATION' and 'ADAPTATION' values are not equal.
    """
    # Climate is where adaptation OR mitigation is larger than 0
    # and where adaptation and mitigation are not equal
    data = df.copy(deep=True).loc[
        lambda d: (d[ClimateSchema.MITIGATION] > 0) | (d[ClimateSchema.ADAPTATION] > 0)
    ]

    if highest_marker:
        data = data.loc[
            lambda d: d[ClimateSchema.MITIGATION] != d[ClimateSchema.ADAPTATION]
        ]

    return data


def apply_highest_marker(df):
    """
    Applies the highest marker value to the given dataframe.

    Args:
        df: The dataframe to apply the highest marker value to.

    Returns:
        The modified dataframe with the highest marker value applied.
    """

    # Select the highest marker and assign it to the indicator column
    df[ClimateSchema.INDICATOR] = np.where(
        df[ClimateSchema.MITIGATION] > df[ClimateSchema.ADAPTATION],
        ClimateSchema.MITIGATION,
        ClimateSchema.ADAPTATION,
    )

    # Select the highest marker value and assign it to the level column
    df[ClimateSchema.LEVEL] = np.where(
        df[ClimateSchema.MITIGATION] > df[ClimateSchema.ADAPTATION],
        df[ClimateSchema.MITIGATION],
        df[ClimateSchema.ADAPTATION],
    )

    # Drop the mitigation and adaptation columns
    return df.drop(columns=[ClimateSchema.MITIGATION, ClimateSchema.ADAPTATION])


def reshape_individual_markers(df):
    """
    Reshapes the marker level of the given dataframe.

    Args:
        df (pandas.DataFrame): The input dataframe.

    Returns:
        pandas.DataFrame: The reshaped dataframe with marker level information.
    """

    # Create a dataframe with only adaptation data
    adaptation = (
        df.loc[lambda d: d[ClimateSchema.ADAPTATION] > 0]
        .assign(**{ClimateSchema.INDICATOR: ClimateSchema.ADAPTATION})
        .rename(columns={ClimateSchema.ADAPTATION: ClimateSchema.LEVEL})
        .drop(columns=[ClimateSchema.MITIGATION])
    )

    # Create a dataframe with only mitigation data
    mitigation = (
        df.loc[lambda d: d[ClimateSchema.MITIGATION] > 0]
        .assign(**{ClimateSchema.INDICATOR: ClimateSchema.MITIGATION})
        .rename(columns={ClimateSchema.MITIGATION: ClimateSchema.LEVEL})
        .drop(columns=[ClimateSchema.ADAPTATION])
    )

    # Concatenate the two dataframes
    return pd.concat([adaptation, mitigation], ignore_index=True)


def apply_coefficients(df, percentage_significant, percentage_principal):
    """
    Args:
        df: The dataframe containing climate data.
        percentage_significant: The percentage by which to multiply the climate values
                               for levels below 2.
        percentage_principal: The percentage by which to multiply the climate values
                              for level 2.

    Returns:
        The modified dataframe with the updated climate values.
    """

    # Apply coefficients to 'significant' data
    df.loc[
        lambda d: (d[ClimateSchema.LEVEL] < 2),
        ClimateSchema.VALUE,
    ] *= percentage_significant

    # Apply coefficients to 'principal' data
    df.loc[
        lambda d: (d[ClimateSchema.LEVEL] == 2),
        ClimateSchema.VALUE,
    ] *= percentage_principal

    return df


def filter_cross_cutting_data(df, cross_cutting_threshold, highest_marker):
    # Filter for data where both mitigation and adaptation are larger than the threshold
    data = df.copy(deep=True)[
        (df[ClimateSchema.MITIGATION] > cross_cutting_threshold)
        & (df[ClimateSchema.ADAPTATION] > cross_cutting_threshold)
    ]

    # If highest_marker is True, filter for data where mitigation and adaptation are equal
    if highest_marker:
        data = data.loc[
            lambda d: d[ClimateSchema.MITIGATION] == d[ClimateSchema.ADAPTATION]
        ]

    return data


def process_cross_cutting_data(
    df: pd.DataFrame,
    cross_cutting_threshold: int = 0,
    percentage_significant: float = 0.4,
    percentage_principal: float = 1.0,
    highest_marker: bool = True,
) -> pd.DataFrame:
    """
    Get cross cutting data. This is data where both climate mitigation and climate
    adaptation are larger than 0 and equal to each other.

    Args:
        df: A dataframe containing the CRS data.
        cross_cutting_threshold: The threshold for the cross cutting indicator. The
        default is 0, which means that both climate mitigation and climate adaptation
        must be larger than 0.
        percentage_significant: The percentage of the activity that is considered
        climate relevant when the marker is 1. The default is 0.4.
        percentage_principal: The percentage of the activity that is considered
        climate relevant when the marker is 2. The default is 1.0.
        highest_marker: Whether to use the highest marker value.

    Returns:
        A dataframe with cross cutting data. The data is assigned the indicator
        'climate_cross_cutting'.

    """
    # Get cross cutting data
    cross_cutting = filter_cross_cutting_data(
        df=df,
        cross_cutting_threshold=cross_cutting_threshold,
        highest_marker=highest_marker,
    )

    # Assign indicator
    cross_cutting = cross_cutting.assign(
        **{ClimateSchema.INDICATOR: ClimateSchema.CROSS_CUTTING}
    )

    # Assign level
    cross_cutting = cross_cutting.assign(
        **{ClimateSchema.LEVEL: cross_cutting[ClimateSchema.MITIGATION]}
    )

    # Drop mitigation and adaptation columns
    cross_cutting = cross_cutting.drop(
        columns=[ClimateSchema.MITIGATION, ClimateSchema.ADAPTATION]
    )

    # Apply coefficients
    cross_cutting = apply_coefficients(
        df=cross_cutting,
        percentage_significant=percentage_significant,
        percentage_principal=percentage_principal,
    )
    return cross_cutting

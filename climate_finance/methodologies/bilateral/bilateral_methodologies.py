import numpy as np
import pandas as pd

from climate_finance.common.schema import (
    ClimateSchema,
    OECD_CLIMATE_INDICATORS,
    CRS_CLIMATE_COLUMNS,
)


def _melt_crs_climate_indicators_oecd(
    df: pd.DataFrame, climate_indicators: list
) -> pd.DataFrame:
    """
    Melt the dataframe to get the indicators as a column
    Args:
        df: A dataframe containing the CRS data.
        climate_indicators: A list of climate indicators to melt.

    Returns:
        A dataframe with melted climate indicators.
    """

    # get all columns except the indicators
    melted_cols = [c for c in df.columns if c not in climate_indicators]

    # melt the dataframe to get the indicators as a column
    melted_df = df.melt(
        id_vars=melted_cols,
        value_vars=climate_indicators,
        var_name=ClimateSchema.INDICATOR,
        value_name="indicator_value",
    )
    # keep only where the indicator value is larger than 0
    return melted_df.loc[lambda d: d.indicator_value > 0].drop(
        columns=["indicator_value"]
    )


def melt_crs_climate_indicators_highest_marker(
    df: pd.DataFrame,
    climate_indicators: list,
    percentage_significant: float = 0.4,
    percentage_principal: float = 1.0,
) -> pd.DataFrame:
    """
    Melt the dataframe to get the indicators as a column
    Args:
        df: A dataframe containing the CRS data.
        climate_indicators: A list of climate indicators to melt.
        percentage_significant: The percentage of the activity that is considered
        climate relevant when the marker is 1. The default is 0.4.
        percentage_principal: The percentage of the activity that is considered
        climate relevant when the marker is 2. The default is 1.0.
        calculating values. Defaults to True.

    Returns:
        A dataframe with melted climate indicators.
    """
    # Climate is where adaptation OR mitigation is larger than 0
    climate_df = df.copy(deep=True).loc[
        lambda d: (d[ClimateSchema.MITIGATION] > 0) | (d[ClimateSchema.ADAPTATION] > 0)
    ]

    # apply coefficient
    climate_df.loc[
        lambda d: (d[ClimateSchema.MITIGATION] < 2) & (d[ClimateSchema.ADAPTATION] < 2),
        ClimateSchema.VALUE,
    ] *= percentage_significant

    climate_df.loc[
        lambda d: (d[ClimateSchema.MITIGATION] == 2)
        | (d[ClimateSchema.ADAPTATION] == 2),
        ClimateSchema.VALUE,
    ] *= percentage_principal

    # Drop cross-cutting
    climate_df = climate_df.loc[
        lambda d: (d[ClimateSchema.MITIGATION] != d[ClimateSchema.ADAPTATION])
    ]

    # Select the highest marker
    climate_df[ClimateSchema.INDICATOR] = np.where(
        climate_df[ClimateSchema.MITIGATION] > climate_df[ClimateSchema.ADAPTATION],
        ClimateSchema.MITIGATION,
        ClimateSchema.ADAPTATION,
    )

    # get all columns except the indicators
    melted_cols = [c for c in df.columns if c not in climate_indicators]

    return climate_df.filter(melted_cols + [ClimateSchema.INDICATOR])


def get_cross_cutting_data_oecd(
    df: pd.DataFrame, cross_cutting_threshold: int = 0
) -> pd.DataFrame:
    """
    Get cross cutting data. This is data where both climate mitigation and climate
    adaptation are larger than 0.

    Args:
        df: A dataframe containing the CRS data.
        cross_cutting_threshold: The threshold for the cross cutting indicator. The
        default is 0, which means that both climate mitigation and climate adaptation
        must be larger than 0.

    Returns:
        A dataframe with cross cutting data. The data is assigned the indicator
        'climate_cross_cutting'.

    """
    return (
        df[
            (df[ClimateSchema.MITIGATION] > cross_cutting_threshold)
            & (df[ClimateSchema.ADAPTATION] > cross_cutting_threshold)
        ]
        .copy()
        .assign(**{ClimateSchema.INDICATOR: ClimateSchema.CROSS_CUTTING})
        .drop(columns=[ClimateSchema.MITIGATION, ClimateSchema.ADAPTATION])
    )


def get_cross_cutting_data_one(
    df: pd.DataFrame,
    cross_cutting_threshold: int = 0,
    percentage_significant: float = 0.4,
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

    Returns:
        A dataframe with cross cutting data. The data is assigned the indicator
        'climate_cross_cutting'.

    """
    cross_cutting = (
        df[
            (df[ClimateSchema.MITIGATION] > cross_cutting_threshold)
            & (df[ClimateSchema.ADAPTATION] > cross_cutting_threshold)
            & (df[ClimateSchema.MITIGATION] == df[ClimateSchema.ADAPTATION])
        ]
        .copy()
        .assign(**{ClimateSchema.INDICATOR: ClimateSchema.CROSS_CUTTING})
    )

    # apply coefficient
    cross_cutting.loc[
        lambda d: (d[ClimateSchema.MITIGATION] < 2) & (d[ClimateSchema.ADAPTATION] < 2),
        ClimateSchema.VALUE,
    ] *= percentage_significant

    cross_cutting = cross_cutting.drop(
        columns=[ClimateSchema.MITIGATION, ClimateSchema.ADAPTATION]
    )
    return cross_cutting


def _get_not_climate_relevant_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Get data that is not climate relevant. This is data where both climate mitigation
    and climate adaptation are 0.

    Args:
        df: A dataframe containing the CRS data.

    Returns:
        A dataframe with data that is not climate relevant. The data is assigned the
        indicator 'not_climate_relevant'.

    """
    return (
        df[(df[ClimateSchema.MITIGATION] == 0) & (df[ClimateSchema.ADAPTATION] == 0)]
        .copy()
        .assign(**{ClimateSchema.INDICATOR: ClimateSchema.NOT_CLIMATE})
        .drop(columns=[ClimateSchema.MITIGATION, ClimateSchema.ADAPTATION])
    )


def _combine_clean_sort(dfs: list[pd.DataFrame], sort_cols: list[str]) -> pd.DataFrame:
    """
    Combine, clean and sort the dataframes. Climate indicators are mapped to their
    full names, defined in OECD_CLIMATE_INDICATORS.

    Args:
        dfs: A list of dataframes to combine.
        sort_cols: A list of columns to sort the dataframe by.

    Returns:
        A dataframe with the combined dataframes, cleaned and sorted.

    """
    return (
        pd.concat(dfs, ignore_index=True)
        .assign(
            **{
                ClimateSchema.INDICATOR: lambda d: d[ClimateSchema.INDICATOR].map(
                    OECD_CLIMATE_INDICATORS
                )
            }
        )
        .sort_values(by=sort_cols)
        .reset_index(drop=True)
    )


def base_oecd_transform_markers_into_indicators(
    df: pd.DataFrame, **kwargs
) -> pd.DataFrame:
    """
    Transform the CRS markers into climate indicators. The CRS markers are transformed
    into the following climate indicators:
    - Adaptation
    - Mitigation
    - Cross-cutting
    - Not climate relevant

    This transformation is based on the following rules:
    - Adaptation: any activity where climate_adaptation is larger than 0
    - Mitigation: any activity where climate_mitigation is larger than 0
    - Cross-cutting: any activity where both climate_adaptation and climate_mitigation
    are larger than 0
    - Not climate relevant: any activity where both climate_adaptation and
    climate_mitigation are 0

    This method leads to double counting if it is simply aggregated to get a total.

    Args:
        df: A dataframe containing the CRS data.

    Returns:
        A dataframe with the CRS data transformed into climate indicators.
    """

    # Melt the dataframe to get the indicators as a column
    melted_df = _melt_crs_climate_indicators_oecd(
        df, climate_indicators=CRS_CLIMATE_COLUMNS
    )

    # Get cross_cutting data
    cross_cutting_df = get_cross_cutting_data_oecd(df, cross_cutting_threshold=0)

    # Get not climate relevant data
    not_climate_df = _get_not_climate_relevant_data(df)

    # combine the two dataframes
    combined_df = _combine_clean_sort(
        dfs=[melted_df, cross_cutting_df, not_climate_df],
        sort_cols=[c for c in df.columns if c not in CRS_CLIMATE_COLUMNS],
    )

    return combined_df


def highest_marker_transform_markers_into_indicators(
    df: pd.DataFrame,
    percentage_significant: float = 0.4,
    percentage_principal: float = 1.0,
) -> pd.DataFrame:
    """
    Transform the CRS markers into climate indicators. The CRS markers are transformed
    into the following climate indicators:
    - Adaptation
    - Mitigation
    - Cross-cutting
    - Not climate relevant

    This transformation is based on the following rules:
    - The 'highest marker' is used to determine the climate indicator. The highest
    marker is defined as the marker with the highest value between climate_adaptation
    and climate_mitigation.
    - Where both climate_adaptation and climate_mitigation are 0, the marker is
    'not_climate_relevant'.
    - Where both climate_adaptation and climate_mitigation are 2, the marker is
    'cross_cutting'.
    - When the marker is 1, only 40% of the activity is considered climate relevant.
    - When the marker is 2, 100% of the activity is considered climate relevant.

    This method avoids double counting when aggregating and reduces the inflation
    inherent in the marker system.

    Args:
        df: A dataframe containing the CRS data.
        percentage_significant: The percentage of the activity that is considered
        climate relevant when the marker is 1. The default is 0.4.
        percentage_principal: The percentage of the activity that is considered
        climate relevant when the marker is 2. The default is 1.0.

    Returns:
        A dataframe with the CRS data transformed into climate indicators.
    """

    # Melt the dataframe to get the indicators as a column
    melted_df = melt_crs_climate_indicators_highest_marker(
        df=df,
        climate_indicators=CRS_CLIMATE_COLUMNS,
        percentage_significant=percentage_significant,
        percentage_principal=percentage_principal,
    )

    # Get cross_cutting data
    cross_cutting_df = get_cross_cutting_data_one(df, cross_cutting_threshold=0)

    # Get not climate relevant data
    not_climate_df = _get_not_climate_relevant_data(df)

    # combine the two dataframes
    combined_df = _combine_clean_sort(
        dfs=[melted_df, cross_cutting_df, not_climate_df],
        sort_cols=[c for c in df.columns if c not in CRS_CLIMATE_COLUMNS],
    )

    return combined_df


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


def process_crs_climate_indicators(
    df: pd.DataFrame,
    percentage_significant: float = 0.4,
    percentage_principal: float = 1.0,
    highest_marker: bool = True,
) -> pd.DataFrame:
    """
    Args:
        df: A pandas DataFrame containing climate indicator data.

        percentage_significant: How much of the value of a 'significant' project to count.
                                 Default value is 0.4.

        percentage_principal: How much of the value of a 'principal' project to count.
                                Default value is 1.0.

        highest_marker: Whether to use the highest marker value.

    Returns:
        A pandas DataFrame with the processed climate indicator data.

    """

    # Keep only climate-relevant, non-cross-cutting data
    climate_df = filter_climate_data(df, highest_marker=highest_marker)

    # If highest_marker is True, apply the highest marker value to the dataframe.
    if highest_marker:
        climate_df = apply_highest_marker(climate_df)
    # Otherwise, reshape the marker level of the dataframe.
    else:
        climate_df = reshape_individual_markers(climate_df)

    # Apply coefficients to the dataframe
    climate_df = apply_coefficients(
        df=climate_df,
        percentage_significant=percentage_significant,
        percentage_principal=percentage_principal,
    )

    return climate_df


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


def process_not_climate_relevant(df) -> pd.DataFrame:
    """
    Get data that is not climate relevant. This is data where both climate mitigation
    and climate adaptation are 0.

    Args:
        df: A dataframe containing the CRS data.

    Returns:
        A dataframe with data that is not climate relevant. The data is assigned the
        indicator 'not_climate_relevant'.

    """

    return (
        df.copy(deep=True)[
            (df[ClimateSchema.MITIGATION] == 0) & (df[ClimateSchema.ADAPTATION] == 0)
        ]
        .assign(
            **{ClimateSchema.INDICATOR: ClimateSchema.NOT_CLIMATE},
            **{ClimateSchema.LEVEL: 0}
        )
        .drop(columns=[ClimateSchema.MITIGATION, ClimateSchema.ADAPTATION])
    )


def transform_markers_into_indicators(
    df: pd.DataFrame,
    percentage_significant: float = 0.4,
    percentage_principal: float = 1.0,
    highest_marker: bool = True,
) -> pd.DataFrame:
    """
    Transform the CRS markers into climate indicators. The CRS markers are transformed
    into the following climate indicators:
    - Adaptation
    - Mitigation
    - Cross-cutting
    - Not climate relevant

    This transformation is based on the following rules:
    - The 'highest marker' is used to determine the climate indicator. The highest
    marker is defined as the marker with the highest value between climate_adaptation
    and climate_mitigation.
    - Where both climate_adaptation and climate_mitigation are 0, the marker is
    'not_climate_relevant'.
    - Where both climate_adaptation and climate_mitigation are 2, the marker is
    'cross_cutting'.
    - When the marker is 1, only 40% of the activity is considered climate relevant.
    - When the marker is 2, 100% of the activity is considered climate relevant.

    This method avoids double counting when aggregating and reduces the inflation
    inherent in the marker system.

    Args:
        df: A dataframe containing the CRS data.
        percentage_significant: The percentage of the activity that is considered
        climate relevant when the marker is 1. The default is 0.4.
        percentage_principal: The percentage of the activity that is considered
        climate relevant when the marker is 2. The default is 1.0.
        highest_marker: Whether to use the highest marker value.

    Returns:
        A dataframe with the CRS data transformed into climate indicators.
    """

    df = df.query("flow_type == 'usd_disbursement'")

    climate_df = process_crs_climate_indicators(
        df=df,
        percentage_significant=percentage_significant,
        percentage_principal=percentage_principal,
        highest_marker=highest_marker,
    )

    cross_cutting = process_cross_cutting_data(
        df=df,
        cross_cutting_threshold=0,
        percentage_significant=percentage_significant,
        percentage_principal=percentage_principal,
        highest_marker=highest_marker,
    )

    not_climate = process_not_climate_relevant(df=df)

    # combine the two dataframes
    combined_df = _combine_clean_sort(
        dfs=[climate_df, cross_cutting, not_climate],
        sort_cols=[c for c in df.columns if c not in CRS_CLIMATE_COLUMNS],
    )

    return combined_df

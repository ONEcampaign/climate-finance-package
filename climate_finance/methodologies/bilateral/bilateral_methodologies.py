import numpy as np
import pandas as pd

from climate_finance.common.schema import (
    ClimateSchema,
    CRS_CLIMATE_COLUMNS,
)
from climate_finance.methodologies.spending.crs import _combine_clean_sort


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

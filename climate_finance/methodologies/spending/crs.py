import pandas as pd

from climate_finance.common.schema import (
    ClimateSchema,
    OECD_CLIMATE_INDICATORS,
    CRS_CLIMATE_COLUMNS,
)
from climate_finance.methodologies.spending.tools import (
    filter_climate_data,
    apply_highest_marker,
    reshape_individual_markers,
    apply_coefficients,
    process_cross_cutting_data,
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

import pandas as pd

OECD_CLIMATE_INDICATORS: dict[str, str] = {
    "climate_adaptation": "Adaptation",
    "climate_mitigation": "Mitigation",
    "climate_cross_cutting": "Cross-cutting",
    "not_climate_relevant": "Not climate relevant",
}


def _melt_crs_climate_indicators(
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
        var_name="indicator",
        value_name="indicator_value",
    )
    # keep only where the indicator value is larger than 0
    return melted_df.loc[lambda d: d.indicator_value > 0].drop(
        columns=["indicator_value"]
    )


def _get_cross_cutting_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Get cross cutting data. This is data where both climate mitigation and climate
    adaptation are larger than 0.

    Args:
        df: A dataframe containing the CRS data.

    Returns:
        A dataframe with cross cutting data. The data is assigned the indicator
        'climate_cross_cutting'.

    """
    return (
        df[(df["climate_mitigation"] > 0) & (df["climate_adaptation"] > 0)]
        .copy()
        .assign(indicator="climate_cross_cutting")
        .drop(columns=["climate_mitigation", "climate_adaptation"])
    )


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
        df[(df["climate_mitigation"] == 0) & (df["climate_adaptation"] == 0)]
        .copy()
        .assign(indicator="not_climate_relevant")
        .drop(columns=["climate_mitigation", "climate_adaptation"])
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
        .assign(indicator=lambda d: d.indicator.map(OECD_CLIMATE_INDICATORS))
        .sort_values(by=sort_cols)
        .reset_index(drop=True)
    )


def base_oecd_transform_markers_into_indicators(df: pd.DataFrame) -> pd.DataFrame:
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
    # melt the dataframe to get the indicators as a column
    climate_indicators = ["climate_mitigation", "climate_adaptation"]

    # Melt the dataframe to get the indicators as a column
    melted_df = _melt_crs_climate_indicators(df, climate_indicators)

    # Get cross_cutting data
    cross_cutting_df = _get_cross_cutting_data(df)

    # Get not climate relevant data
    not_climate_df = _get_not_climate_relevant_data(df)

    # combine the two dataframes
    combined_df = _combine_clean_sort(
        dfs=[melted_df, cross_cutting_df, not_climate_df],
        sort_cols=[c for c in df.columns if c not in climate_indicators],
    )

    return combined_df

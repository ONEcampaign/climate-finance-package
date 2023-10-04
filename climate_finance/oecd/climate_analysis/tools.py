import numpy as np
import pandas as pd

from climate_finance.config import logger
from climate_finance.oecd.cleaning_tools.schema import OECD_CLIMATE_INDICATORS, CrsSchema

MULTILATERAL_ID_COLUMNS: list[str] = [
    CrsSchema.YEAR,
    CrsSchema.CHANNEL_CODE,
    CrsSchema.CHANNEL_NAME,
    CrsSchema.FLOW_TYPE,
    CrsSchema.MULTILATERAL_TYPE,
    CrsSchema.REPORTING_METHOD,
    CrsSchema.CONVERGED_REPORTING,
]


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
        var_name=CrsSchema.INDICATOR,
        value_name="indicator_value",
    )
    # keep only where the indicator value is larger than 0
    return melted_df.loc[lambda d: d.indicator_value > 0].drop(
        columns=["indicator_value"]
    )


def _melt_multilateral_climate_indicators(
    df: pd.DataFrame, climate_indicators: list
) -> pd.DataFrame:
    """
    Melt the dataframe to get the indicators as a column
    Args:
        df: A dataframe containing the multilateral climate data.
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
        var_name=CrsSchema.INDICATOR,
        value_name=CrsSchema.VALUE,
    )
    # keep only where the indicator value is larger than 0
    return melted_df.dropna(subset=[CrsSchema.VALUE]).reset_index(drop=True)


def _filter_multilateral_indicators_total(
    df: pd.DataFrame, climate_indicators: list
) -> pd.DataFrame:
    """
    Filter the data to include the 'total' indicators

    Args:
        df: A dataframe containing the multilateral climate data.
        climate_indicators: A list of climate indicators keep.

    Returns:
        A dataframe with the multilateral climate data filtered to only include the
        requested indicators.

    """
    return df.filter(MULTILATERAL_ID_COLUMNS + climate_indicators)


def _remove_climate_unspecified(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[
        lambda d: ~(
            (d[CrsSchema.YEAR] >= 2021) & (d[CrsSchema.INDICATOR] == "climate_total")
        )
    ]


def _add_not_climate_relevant(df: pd.DataFrame) -> pd.DataFrame:
    df = df.assign(
        **{
            CrsSchema.NOT_CLIMATE: lambda d: (
                d[CrsSchema.CLIMATE_UNSPECIFIED]
                / (1 - d[CrsSchema.CLIMATE_UNSPECIFIED_SHARE])
            ).replace([np.inf, -np.inf], np.nan),
            f"{CrsSchema.NOT_CLIMATE}_share": lambda d: 1
            - d[CrsSchema.CLIMATE_UNSPECIFIED_SHARE],
        }
    )
    return df


def get_cross_cutting_data(df: pd.DataFrame) -> pd.DataFrame:
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
        df[(df[CrsSchema.MITIGATION] > 0) & (df[CrsSchema.ADAPTATION] > 0)]
        .copy()
        .assign(**{CrsSchema.INDICATOR: CrsSchema.CROSS_CUTTING})
        .drop(columns=[CrsSchema.MITIGATION, CrsSchema.ADAPTATION])
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
        df[(df[CrsSchema.MITIGATION] == 0) & (df[CrsSchema.ADAPTATION] == 0)]
        .copy()
        .assign(**{CrsSchema.INDICATOR: CrsSchema.NOT_CLIMATE})
        .drop(columns=[CrsSchema.MITIGATION, CrsSchema.ADAPTATION])
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


def check_and_filter_parties(
    df: pd.DataFrame, party: list[str] | str | None, party_col: str = "party"
) -> pd.DataFrame:
    """
    Check that the requested parties are in the CRS data and filter the data to only
    include the requested parties. If party is None, return the original dataframe.

    Args:
        df: A dataframe containing the CRS data.
        party: A list of parties to filter the data to.
        party_col: The column containing the parties.

    Returns:
        A dataframe with the CRS data filtered to only include the requested parties.
        If party is None, return the original dataframe.

    """

    # Validate the party argument
    if isinstance(party, str):
        party = [party]

    if party is not None:
        # Check that the requested parties are in the CRS data
        missing_party = set(party) - set(df[party_col].unique())
        # Log a warning if any of the requested parties are not in the CRS data
        if len(missing_party) > 0:
            logger.warning(
                f"The following parties are not found in CRS data:\n{missing_party}"
            )
        # Filter the data to only include the requested parties
        return df.loc[lambda d: d[party_col].isin(party)]

    # if Party is None, return the original dataframe
    return df


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
    climate_indicators = [CrsSchema.MITIGATION, CrsSchema.ADAPTATION]

    # Melt the dataframe to get the indicators as a column
    melted_df = _melt_crs_climate_indicators(df, climate_indicators)

    # Get cross_cutting data
    cross_cutting_df = get_cross_cutting_data(df)

    # Get not climate relevant data
    not_climate_df = _get_not_climate_relevant_data(df)

    # combine the two dataframes
    combined_df = _combine_clean_sort(
        dfs=[melted_df, cross_cutting_df, not_climate_df],
        sort_cols=[c for c in df.columns if c not in climate_indicators],
    )

    return combined_df


def _oecd_multilateral_agency_helper(
    df: pd.DataFrame, climate_indicators: dict
) -> pd.DataFrame:
    """
    Helper function for the OECD multilateral agency data. This function is used to
    transform the multilateral agency data into climate indicators.

    Args:
        df: A dataframe containing the multilateral agency data.
        climate_indicators: A dictionary of climate indicators to keep.

    Returns:
        A dataframe with the multilateral agency data transformed into climate
        indicators.

    """

    # calculate not climate relevant
    df = _add_not_climate_relevant(df)

    # Filter the data to include the 'total' indicators
    data = _filter_multilateral_indicators_total(
        df=df, climate_indicators=list(climate_indicators)
    )

    # Melt the dataframe to get the indicators as a column
    data = _melt_multilateral_climate_indicators(
        df=data, climate_indicators=list(climate_indicators)
    )

    # Map indicator names
    data = data.assign(indicator=lambda d: d.indicator.map(climate_indicators))

    # Remove unspecified from 2021 (given that there is a detailed breakdown)
    data = _remove_climate_unspecified(data)

    # Map indicator names
    return data.assign(indicator=lambda d: d.indicator.map(OECD_CLIMATE_INDICATORS))


def base_oecd_multilateral_agency_total(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the multilateral agency data into climate indicators (total
    flow figures). The multilateral agency data is transformed into the following
    climate indicators:
    - Adaptation
    - Mitigation
    - Cross-cutting
    - Not climate relevant

    Args:
        df: A dataframe containing the multilateral agency data.

    Returns:
        A dataframe with the multilateral agency data transformed into climate
        indicators (total flow figures).

    """
    climate_indicators = {
        "oecd_climate_total": CrsSchema.CLIMATE_UNSPECIFIED,
        "oecd_mitigation": CrsSchema.MITIGATION,
        "oecd_adaptation": CrsSchema.ADAPTATION,
        "oecd_cross_cutting": CrsSchema.CROSS_CUTTING,
        "not_climate_relevant": CrsSchema.NOT_CLIMATE,
    }

    return _oecd_multilateral_agency_helper(df, climate_indicators)


def base_oecd_multilateral_agency_share(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the multilateral agency data into climate indicators (share of total
    flow figures). The multilateral agency data is transformed into the following
    climate indicators:
    - Adaptation
    - Mitigation
    - Cross-cutting
    - Not climate relevant

    Args:
        df: A dataframe containing the multilateral agency data.

    Returns:
        A dataframe with the multilateral agency data transformed into climate
        indicators (share of total flow figures).

    """

    climate_indicators = {
        "oecd_climate_total_share": CrsSchema.CLIMATE_UNSPECIFIED,
        "oecd_mitigation_share": CrsSchema.MITIGATION,
        "oecd_adaptation_share": CrsSchema.ADAPTATION,
        "oecd_cross_cutting_share": CrsSchema.CROSS_CUTTING,
        "not_climate_relevant_share": CrsSchema.NOT_CLIMATE,
    }

    return _oecd_multilateral_agency_helper(df, climate_indicators).assign(
        **{CrsSchema.FLOW_TYPE: lambda d: d[CrsSchema.FLOW_TYPE] + "_share"}
    )

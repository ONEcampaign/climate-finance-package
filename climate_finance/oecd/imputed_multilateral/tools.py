import numpy as np
import pandas as pd

from climate_finance.config import logger
from climate_finance.oecd.cleaning_tools.schema import (
    OECD_CLIMATE_INDICATORS,
    CrsSchema,
)
from climate_finance.oecd.cleaning_tools.tools import idx_to_str, set_crs_data_types

MULTILATERAL_ID_COLUMNS: list[str] = [
    CrsSchema.YEAR,
    CrsSchema.CHANNEL_CODE,
    CrsSchema.CHANNEL_NAME,
    CrsSchema.FLOW_TYPE,
    CrsSchema.MULTILATERAL_TYPE,
    CrsSchema.REPORTING_METHOD,
    CrsSchema.CONVERGED_REPORTING,
]


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


def summarise_by_party_idx(
    data: pd.DataFrame, idx: list[str], by_indicator: bool = False
) -> pd.DataFrame:
    grouper = [CrsSchema.PARTY_CODE] + idx

    if by_indicator:
        grouper += [CrsSchema.INDICATOR]

    grouper = [c for c in grouper if c in data.columns]

    grouper = list(dict.fromkeys(grouper))

    data = data.pipe(idx_to_str, idx=grouper)

    return (
        data.groupby(grouper, observed=True)[CrsSchema.VALUE]
        .sum()
        .reset_index()
        .pipe(set_crs_data_types)
    )


def compute_rolling_sum(
    group,
    start_year: int,
    end_year: int,
    window: int = 2,
    values: list[str] = None,
    agg: str = "sum",
    include_yearly_total: bool = True,
):
    if values is None:
        values = [CrsSchema.VALUE]

    if include_yearly_total:
        values += ["yearly_total"]
        values = list(dict.fromkeys(values))

    all_years = range(start_year, end_year + 1)

    # 2. Reindex the group using the complete range of years
    group = group.set_index(CrsSchema.YEAR).reindex(all_years)

    group[values] = group[values].fillna(0)

    group[values] = group[values].rolling(window=window).agg(agg).fillna(group[values])

    group = group.dropna(subset=[CrsSchema.PARTY_CODE])

    return group.reset_index(drop=False)


def merge_total(
    data: pd.DataFrame, totals: pd.DataFrame, idx: list[str]
) -> pd.DataFrame:
    # Make sure index is valid
    idx = [
        c
        for c in idx
        if c in totals.columns
        and c
        not in [
            CrsSchema.PARTY_NAME,
            CrsSchema.RECIPIENT_NAME,
            CrsSchema.SECTOR_NAME,
            CrsSchema.SECTOR_CODE,
            CrsSchema.PURPOSE_NAME,
            CrsSchema.FLOW_NAME,
        ]
    ]

    # Transform idx columns to string
    data = data.pipe(idx_to_str, idx=idx)
    totals = totals.pipe(idx_to_str, idx=idx)

    data = data.merge(totals, on=idx, how="left", suffixes=("", "_crs")).pipe(
        set_crs_data_types
    )
    return data.drop(columns=[c for c in data.columns if c.endswith("_crs")])


def log_notes(df: pd.DataFrame) -> None:
    """
    Log the latest update date from the notes sheet.
    Args:
        df: The notes sheet.

    Returns:
        None
    """

    logger.info(f"{df.iloc[1].values[0]}")

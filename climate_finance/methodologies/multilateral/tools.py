import numpy as np
import pandas as pd

from climate_finance.common.schema import (
    ClimateSchema,
    MULTILATERAL_IMPUTATIONS_ID_COLUMNS,
)
from climate_finance.config import logger
from climate_finance.oecd.cleaning_tools.tools import idx_to_str


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

    climate_indicators = [c for c in climate_indicators if c in df.columns]

    # melt the dataframe to get the indicators as a column
    melted_df = df.melt(
        id_vars=melted_cols,
        value_vars=climate_indicators,
        var_name=ClimateSchema.INDICATOR,
        value_name=ClimateSchema.VALUE,
    )
    # keep only where the indicator value is larger than 0
    return melted_df.dropna(subset=[ClimateSchema.VALUE]).reset_index(drop=True)


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

    return df.filter(MULTILATERAL_IMPUTATIONS_ID_COLUMNS + climate_indicators)


def _remove_climate_unspecified(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[
        lambda d: ~(
            (d[ClimateSchema.YEAR].astype("Int16") >= 2021)
            & (
                (d[ClimateSchema.INDICATOR] == "climate_total")
                | (d[ClimateSchema.INDICATOR] == "climate_total_share")
            )
        )
    ]


def _add_not_climate_relevant(df: pd.DataFrame) -> pd.DataFrame:
    df = df.assign(
        **{
            ClimateSchema.NOT_CLIMATE: lambda d: (
                d[ClimateSchema.CLIMATE_UNSPECIFIED]
                / (1 - d[ClimateSchema.CLIMATE_UNSPECIFIED_SHARE])
            ).replace([np.inf, -np.inf], np.nan),
            f"{ClimateSchema.NOT_CLIMATE}_share": lambda d: 1
            - d[ClimateSchema.CLIMATE_UNSPECIFIED_SHARE],
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

    df = df.rename(columns=climate_indicators)

    # calculate not climate relevant
    df = _add_not_climate_relevant(df)

    # Filter the data to include the 'total' indicators
    data = _filter_multilateral_indicators_total(
        df=df, climate_indicators=list(climate_indicators.values())
    )

    # Melt the dataframe to get the indicators as a column
    data = _melt_multilateral_climate_indicators(
        df=data, climate_indicators=list(climate_indicators.values())
    )

    # Remove unspecified from 2021 (given that there is a detailed breakdown)
    data = _remove_climate_unspecified(data)

    # Map indicator names
    return data


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

    indicators = {
        "climate_total": ClimateSchema.CLIMATE_UNSPECIFIED,
        "oecd_climate_total_share": ClimateSchema.CLIMATE_UNSPECIFIED_SHARE,
        "oecd_mitigation_share": f"{ClimateSchema.MITIGATION}_share",
        "oecd_adaptation_share": f"{ClimateSchema.ADAPTATION}_share",
        "oecd_cross_cutting_share": f"{ClimateSchema.CROSS_CUTTING}_share",
    }

    return _oecd_multilateral_agency_helper(df, indicators)


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
        "oecd_climate_total_share": ClimateSchema.CLIMATE_UNSPECIFIED,
        "oecd_mitigation_share": ClimateSchema.MITIGATION,
        "oecd_adaptation_share": ClimateSchema.ADAPTATION,
        "oecd_cross_cutting_share": ClimateSchema.CROSS_CUTTING,
        "not_climate_relevant_share": ClimateSchema.NOT_CLIMATE,
    }

    return _oecd_multilateral_agency_helper(df, climate_indicators).assign(
        **{ClimateSchema.FLOW_TYPE: lambda d: d[ClimateSchema.FLOW_TYPE] + "_share"}
    )


def summarise_by_party_idx(
    data: pd.DataFrame, idx: list[str], by_indicator: bool = False
) -> pd.DataFrame:
    grouper = [ClimateSchema.PROVIDER_CODE] + idx

    if by_indicator:
        grouper += [ClimateSchema.INDICATOR]

    grouper = [c for c in grouper if c in data.columns]

    grouper = list(dict.fromkeys(grouper))

    data = data.pipe(idx_to_str, idx=grouper)

    return (
        data.groupby(grouper, observed=True, dropna=False)[ClimateSchema.VALUE]
        .sum()
        .reset_index()
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
        values = [ClimateSchema.VALUE]

    if include_yearly_total:
        values += ["yearly_total"]
        values = list(dict.fromkeys(values))

    all_years = range(start_year, end_year + 1)

    # 2. Reindex the group using the complete range of years
    group = group.set_index(ClimateSchema.YEAR).reindex(all_years)

    group[values] = group[values].fillna(0)

    group[values] = group[values].rolling(window=window).agg(agg).fillna(group[values])

    group = group.dropna(subset=[ClimateSchema.PROVIDER_CODE])

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
            ClimateSchema.PROVIDER_NAME,
            ClimateSchema.RECIPIENT_NAME,
            ClimateSchema.SECTOR_NAME,
            ClimateSchema.SECTOR_CODE,
            ClimateSchema.PURPOSE_NAME,
            ClimateSchema.FLOW_NAME,
        ]
    ]

    # Transform idx columns to string
    data = data.pipe(idx_to_str, idx=idx)
    totals = totals.pipe(idx_to_str, idx=idx)

    data = data.merge(totals, on=idx, how="left", suffixes=("", "_crs"))

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


def crdf_multilateral_provider_codes() -> list[str]:
    return [
        "990",
        "909",
        "915",
        "976",
        "913",
        "914",
        "918",
        "901",
        "905",
        "903",
        "1024",
        "1019",
        "1015",
        "1016",
        "1011",
        "1313",
        "988",
        "981",
        "910",
        "906",
    ]

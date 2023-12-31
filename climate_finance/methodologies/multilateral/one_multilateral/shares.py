import pandas as pd

from climate_finance.common.analysis_tools import (
    keep_commitments_and_disbursements_only,
)
from climate_finance.common.schema import (
    ClimateSchema,
    CLIMATE_VALUES,
    CLIMATE_VALUES_TO_NAMES,
)
from climate_finance.config import logger, ClimateDataPath
from climate_finance.methodologies.bilateral.tools import (
    rio_markers_multi_codes,
    remove_private_and_not_climate_relevant,
)
from climate_finance.methodologies.multilateral.one_multilateral.climate_components import (
    one_multilateral_spending,
)
from climate_finance.oecd.cleaning_tools.tools import idx_to_str
from climate_finance.oecd.crs.get_data import get_crs_allocable_spending, get_crs
from climate_finance.oecd.get_oecd_data import get_oecd_bilateral


def high_confidence_multilateral_crdf_providers() -> list:
    return [
        "990",
        "909",
        "915",
        "976",
        "901",
        "905",
        "1024",
        "1015",
        "1011",
        "1016",
        "1313",
        "988",
        "981",
        "910",
        "906",
    ]


def _pivot_indicators_as_columns(data: pd.DataFrame, idx: list[str]) -> pd.DataFrame:
    return data.pivot(
        index=idx, columns=ClimateSchema.INDICATOR, values=ClimateSchema.VALUE
    ).reset_index()


def get_crs_spending_totals(
    start_year: int,
    end_year: int,
    allocable_only=True,
    provider_code: list[str] | str | None = None,
    groupby: list[str] | None = None,
) -> pd.DataFrame:
    if allocable_only:
        data = get_crs_allocable_spending(
            start_year=start_year, end_year=end_year, provider_code=provider_code
        )
    else:
        data = get_crs(
            start_year=start_year, end_year=end_year, provider_code=provider_code
        )

    if groupby is not None:
        idx = [c for c in groupby if c in data.columns]

        data = (
            data.groupby(idx, observed=True, dropna=False)[ClimateSchema.VALUE]
            .sum()
            .reset_index()
        )

    return data


def get_mutlilateral_climate_spending_for_imputations(
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    # Define Rio multilaterals
    multi_rio = rio_markers_multi_codes()

    # Define CRDF multilaterals
    valid_crdf_multi = high_confidence_multilateral_crdf_providers()

    # Get CRS data for rio.
    rio_data = (
        get_oecd_bilateral(
            start_year=start_year,
            end_year=end_year,
            provider_code=multi_rio,
            methodology="one_bilateral",
        )
        .pipe(remove_private_and_not_climate_relevant)
        .pipe(keep_commitments_and_disbursements_only)
    )

    crdf_data = one_multilateral_spending(
        start_year=start_year, end_year=end_year, provider_code=valid_crdf_multi
    )

    crdf_data = crdf_data.melt(
        id_vars=[
            c
            for c in crdf_data
            if c not in CLIMATE_VALUES + [ClimateSchema.CLIMATE_UNSPECIFIED]
        ],
        var_name=ClimateSchema.INDICATOR,
    ).assign(
        **{
            ClimateSchema.INDICATOR: lambda d: d[ClimateSchema.INDICATOR]
            .map(CLIMATE_VALUES_TO_NAMES)
            .fillna(d[ClimateSchema.INDICATOR])
        }
    )

    data = (
        pd.concat([rio_data, crdf_data], ignore_index=True)
        .filter([c for c in rio_data if c in crdf_data])
        .loc[lambda d: d[ClimateSchema.INDICATOR] != ClimateSchema.CLIMATE_UNSPECIFIED]
    )

    return data


def rolling_values(
    group,
    start_year: int,
    end_year: int,
    window: int = 2,
    values: list[str] = None,
    agg: str = "mean",
):
    if values is None:
        values = [ClimateSchema.VALUE]

    all_years = [str(c) for c in range(start_year, end_year + 1)]

    # 2. Reindex the group using the complete range of years
    group = group.set_index(ClimateSchema.YEAR).reindex(all_years)

    group[values] = group[values].fillna(0)

    group[[f"{c}_rolling" for c in values]] = (
        group[values].rolling(window=window).agg(agg).fillna(group[values])
    )

    group = group.dropna(subset=[ClimateSchema.CHANNEL_CODE])

    return group.reset_index(drop=False)


def map_imputations_channels(data: pd.DataFrame) -> pd.DataFrame:
    # read mapping data
    mapping = pd.read_csv(
        ClimateDataPath.crs_channel_mapping,
        dtype={
            ClimateSchema.AGENCY_CODE: "Int32",
            ClimateSchema.PROVIDER_CODE: "Int32",
            ClimateSchema.CHANNEL_CODE: "Int32",
        },
    )

    # fill gaps in agencies
    data[ClimateSchema.AGENCY_CODE] = data[ClimateSchema.AGENCY_CODE].fillna("0")

    # Drop existing channel codes and names, if present
    data = data.filter(
        [
            c
            for c in data.columns
            if c not in [ClimateSchema.CHANNEL_CODE, ClimateSchema.CHANNEL_NAME]
        ]
    )

    # idx
    idx = [ClimateSchema.PROVIDER_CODE, ClimateSchema.AGENCY_CODE]

    mapping = mapping.pipe(idx_to_str, idx=idx)
    data = data.pipe(idx_to_str, idx=idx)

    # map
    data = data.merge(mapping, on=idx, how="outer", indicator=True)

    # log missing
    only_data = data[lambda d: d._merge == "left_only"].drop_duplicates(subset=idx)
    only_mapping = data[lambda d: d._merge == "right_only"][
        ClimateSchema.CHANNEL_CODE
    ].unique()

    logger.debug(
        f"{len(only_data)} providers in spending data but not in channel mapping"
    )
    logger.debug(
        f"{len(only_mapping)} providers in channel mapping but not in spending data"
    )

    data = data.drop(columns=["_merge"])

    return data


def summarise_by_imputation_channels(data: pd.DataFrame) -> pd.DataFrame:
    return (
        data.groupby(
            [
                c
                for c in data.columns
                if c
                not in [
                    ClimateSchema.VALUE,
                    ClimateSchema.AGENCY_CODE,
                    ClimateSchema.AGENCY_NAME,
                    ClimateSchema.PROVIDER_CODE,
                    ClimateSchema.PROVIDER_NAME,
                ]
            ],
            observed=True,
            dropna=False,
        )
        .sum(numeric_only=True)
        .reset_index()
    )


def prep_multilateral_spending_data(
    data: pd.DataFrame, groupby: list[str] | None = None
) -> pd.DataFrame:
    """Prepare data for multilateral agencies."""

    spending = data.pipe(map_imputations_channels).pipe(
        summarise_by_imputation_channels
    )

    # create a new groupby
    groupby = [
        c
        for c in groupby
        if c
        not in [
            ClimateSchema.PROVIDER_CODE,
            ClimateSchema.PROVIDER_NAME,
            ClimateSchema.AGENCY_NAME,
            ClimateSchema.AGENCY_CODE,
        ]
    ]

    spending = (
        spending.groupby(groupby, observed=True, dropna=False)
        .sum(numeric_only=True)
        .reset_index()
        .dropna(subset=[ClimateSchema.YEAR])
    )

    return spending


def read_overall_spending_by(
    start_year: int,
    end_year: int,
    groupby: list[str] = None,
    provider_code: list[str] | str | None = None,
) -> pd.DataFrame:
    if groupby is None:
        groupby = [
            ClimateSchema.YEAR,
            ClimateSchema.PROVIDER_CODE,
            ClimateSchema.AGENCY_CODE,
            ClimateSchema.PROVIDER_NAME,
            ClimateSchema.AGENCY_NAME,
            ClimateSchema.FLOW_TYPE,
        ]

    spending = get_crs_spending_totals(
        start_year=start_year,
        end_year=end_year,
        allocable_only=False,
        provider_code=provider_code,
        groupby=groupby,
    ).pipe(keep_commitments_and_disbursements_only)

    return spending


def prep_overall_spending_data(data: pd.DataFrame) -> pd.DataFrame:
    return data.pipe(map_imputations_channels).pipe(summarise_by_imputation_channels)


def merge_climate_and_spending_data(
    climate_data: pd.DataFrame, overall_spending_data: pd.DataFrame, idx: list[str]
) -> pd.DataFrame:
    shared_idx = [c for c in idx if c in climate_data and c in overall_spending_data]
    overall_idx = [c for c in idx if c in overall_spending_data]
    climate_idx = [c for c in idx if c in climate_data]

    overall_spending_data = (
        overall_spending_data.pipe(idx_to_str, idx=overall_idx)
        .groupby(overall_idx, observed=True, dropna=False)
        .sum(numeric_only=True)
        .reset_index()
    )
    climate_data = (
        climate_data.pipe(idx_to_str, idx=climate_idx)
        .groupby(climate_idx, observed=True, dropna=False)
        .sum(numeric_only=True)
        .reset_index()
    )

    merged_data = overall_spending_data.merge(
        climate_data, on=shared_idx, how="inner", suffixes=("", "_climate")
    )

    return merged_data


def smooth_values(
    data: pd.DataFrame,
    idx: list[str],
    values: list[str],
    window: int = 2,
) -> pd.DataFrame:
    """"""

    data = data.sort_values([ClimateSchema.YEAR, ClimateSchema.CHANNEL_CODE])

    start_year = int(data[ClimateSchema.YEAR].min())
    end_year = int(data[ClimateSchema.YEAR].max())

    data = data.groupby(
        [c for c in idx if c != ClimateSchema.YEAR],
        observed=True,
        group_keys=False,
    ).apply(
        rolling_values,
        window=window,
        start_year=start_year,
        end_year=end_year,
        values=values,
        agg="mean",
    )

    return data


def multilateral_shares_pipeline(
    start_year: int, end_year: int, groupby: list[str], window=2
) -> pd.DataFrame:
    if ClimateSchema.CHANNEL_CODE not in groupby:
        groupby.append(ClimateSchema.CHANNEL_CODE)

    climate_spending = get_mutlilateral_climate_spending_for_imputations(
        start_year=start_year, end_year=end_year
    )

    climate_providers = climate_spending[ClimateSchema.PROVIDER_CODE].unique().tolist()

    climate_spending = climate_spending.pipe(
        prep_multilateral_spending_data, groupby=groupby
    )

    overall_spending = read_overall_spending_by(
        start_year=start_year, end_year=end_year, provider_code=climate_providers
    ).pipe(prep_overall_spending_data)

    combined = merge_climate_and_spending_data(
        climate_data=climate_spending,
        overall_spending_data=overall_spending,
        idx=groupby,
    )

    combined_smooth = combined.pipe(
        smooth_values,
        idx=groupby,
        values=[c for c in combined if c not in groupby],
        window=window,
    )

    # add shares
    combined_smooth[ClimateSchema.CLIMATE_SHARE] = (
        combined_smooth[f"{ClimateSchema.VALUE}_climate"]
        / combined_smooth[ClimateSchema.VALUE]
    )

    combined_smooth[ClimateSchema.CLIMATE_SHARE_ROLLING] = (
        combined_smooth[f"{ClimateSchema.VALUE}_climate_rolling"]
        / combined_smooth[f"{ClimateSchema.VALUE}_rolling"]
    )

    return combined_smooth


if __name__ == "__main__":
    climate_shares = multilateral_shares_pipeline(
        start_year=2013,
        end_year=2021,
        groupby=[
            "year",
            "oecd_channel_code",
            "flow_type",
            "indicator",
            "oecd_recipient_code",
            "purpose_code",
        ],
    )

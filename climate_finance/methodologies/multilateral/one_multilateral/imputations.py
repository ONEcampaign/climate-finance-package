import pandas as pd

from climate_finance.common.schema import (
    ClimateSchema,
    VALUE_COLUMNS,
    CLIMATE_VALUES,
)
from climate_finance.config import ClimateDataPath, logger
from climate_finance.oecd.cleaning_tools.tools import idx_to_str
from climate_finance.oecd.crs.get_data import (
    get_crs_allocable_spending,
    get_crs,
)
from climate_finance.common.analysis_tools import (
    get_crs_allocable_to_total_ratio,
    keep_commitments_and_disbursements_only,
)
from climate_finance.methodologies.multilateral.one_multilateral.shares import (
    one_multilateral_spending,
)
from climate_finance.methodologies.multilateral.tools import compute_rolling_sum
from climate_finance.oecd.multisystem.get_data import get_multilateral_contributions
from climate_finance.unfccc.download.pre_process import map_channel_names_to_oecd_codes

ONE_IMPUTATIONS_IDX = [
    ClimateSchema.YEAR,
    ClimateSchema.CHANNEL_CODE,
    ClimateSchema.FLOW_TYPE,
]

ONE_CONTRIBUTIONS_IDX = [
    ClimateSchema.YEAR,
    ClimateSchema.FLOW_TYPE,
    ClimateSchema.CHANNEL_CODE,
]

ONE_IMPUTATIONS_SPENDING_IDX = [
    ClimateSchema.YEAR,
    ClimateSchema.PROVIDER_CODE,
    ClimateSchema.PROVIDER_NAME,
    ClimateSchema.AGENCY_NAME,
    ClimateSchema.FLOW_TYPE,
]

ONE_IMPUTATIONS_OUTPUT_COLUMNS = [
    ClimateSchema.YEAR,
    ClimateSchema.PROVIDER_CODE,
    ClimateSchema.PROVIDER_NAME,
    ClimateSchema.CHANNEL_CODE,
    ClimateSchema.CHANNEL_NAME,
    ClimateSchema.RECIPIENT_CODE,
    ClimateSchema.RECIPIENT_NAME,
    ClimateSchema.SECTOR_CODE,
    ClimateSchema.SECTOR_NAME,
    ClimateSchema.PURPOSE_CODE,
    ClimateSchema.PURPOSE_NAME,
    ClimateSchema.FINANCE_TYPE,
    ClimateSchema.FLOW_CODE,
    ClimateSchema.FLOW_TYPE,
    *VALUE_COLUMNS,
]


def _filter_merge_transform_provider_data(
    spending_data: pd.DataFrame,
    contributions_data: pd.DataFrame,
    provider: str | int,
    idx: list[str],
) -> pd.DataFrame:
    # filter for the specific provider
    filtered_multi = contributions_data.loc[
        lambda d: d[ClimateSchema.PROVIDER_CODE] == provider
    ]

    # Merge the spending data
    combined = (
        spending_data.merge(
            filtered_multi, on=idx, how="left", suffixes=("_spending", "")
        )
        .dropna(subset=[f"{ClimateSchema.PROVIDER_CODE}"])
        .reset_index(drop=True)
    )

    # Define columns to transform to amounts
    amounts = [
        ClimateSchema.MITIGATION_VALUE,
        ClimateSchema.ADAPTATION_VALUE,
        ClimateSchema.CROSS_CUTTING_VALUE,
        ClimateSchema.CLIMATE_UNSPECIFIED,
    ]

    # Compute the amounts, based on shares
    for col in amounts:
        combined[col] = combined[col] * combined[ClimateSchema.VALUE]

    return combined.loc[lambda d: d["climate_total"] > 0]


def _correct_for_allocable_share(imputations_data: pd.DataFrame) -> pd.DataFrame:
    # Allocable index
    allocable_idx = [
        ClimateSchema.YEAR,
        ClimateSchema.PROVIDER_CODE,
        ClimateSchema.AGENCY_CODE,
        ClimateSchema.FLOW_TYPE,
    ]

    # Imputations index
    imputations_idx = [
        ClimateSchema.YEAR,
        f"{ClimateSchema.PROVIDER_CODE}_spending",
        ClimateSchema.AGENCY_CODE,
        ClimateSchema.FLOW_TYPE,
    ]

    # Get allocable ratios
    allocable_ratios = get_crs_allocable_to_total_ratio(
        start_year=int(imputations_data[ClimateSchema.YEAR].min()),
        end_year=int(imputations_data[ClimateSchema.YEAR].max()),
    ).filter(allocable_idx + ["allocable_share"])

    # Set index to str
    allocable_ratios = allocable_ratios.pipe(idx_to_str, idx=allocable_idx)
    imputations_data = imputations_data.pipe(idx_to_str, idx=imputations_idx)

    # Merge the datasets
    merged_data = imputations_data.merge(
        allocable_ratios,
        left_on=imputations_idx,
        right_on=allocable_idx,
        how="left",
        suffixes=("", "_allocable"),
    )

    for column in [
        ClimateSchema.ADAPTATION_VALUE,
        ClimateSchema.MITIGATION_VALUE,
        ClimateSchema.CROSS_CUTTING_VALUE,
        ClimateSchema.CLIMATE_UNSPECIFIED,
    ]:
        merged_data[column] = merged_data[column] * merged_data["allocable_share"]

    return merged_data


def convert_to_imputations(
    multi_spending: pd.DataFrame,
    multi_contributions: pd.DataFrame,
    groupby: list[str] = None,
) -> pd.DataFrame:
    # Get allocable ratios

    if groupby is None:
        groupby = ONE_IMPUTATIONS_IDX

    idx = list(
        set(multi_spending.columns) & set(multi_contributions.columns) & set(groupby)
    )
    multi_spending = multi_spending.pipe(idx_to_str, idx=idx)
    multi_contributions = multi_contributions.pipe(idx_to_str, idx=idx)

    # List to store individual provider data
    dfs = []

    # Loop through each provider
    for provider in multi_contributions[ClimateSchema.PROVIDER_CODE].unique():
        dfs.append(
            _filter_merge_transform_provider_data(
                spending_data=multi_spending,
                contributions_data=multi_contributions,
                provider=provider,
                idx=idx,
            )
        )

    imputations_data = pd.concat(dfs, ignore_index=True)

    # Clean
    imputations_data = imputations_data.rename(
        columns={
            ClimateSchema.PROVIDER_TYPE: "channel_type",
            ClimateSchema.VALUE: "total_contribution",
        }
    ).filter(ONE_IMPUTATIONS_OUTPUT_COLUMNS)

    return imputations_data


def _get_unique_agencies_df(data: pd.DataFrame) -> pd.DataFrame:
    idx = [
        ClimateSchema.PROVIDER_CODE,
        ClimateSchema.PROVIDER_NAME,
        ClimateSchema.AGENCY_NAME,
    ]
    return (
        data.drop_duplicates(idx)
        .filter(idx)
        .dropna(subset=[ClimateSchema.PROVIDER_CODE])
    )


def _map_agency_then_party_to_channel_code(spending_data: pd.DataFrame) -> pd.DataFrame:
    # Get unique agencies as a dataframe
    unique_agencies = _get_unique_agencies_df(spending_data)

    # Try a first map through agencies
    first_pass = unique_agencies.pipe(
        map_channel_names_to_oecd_codes, channel_names_column=ClimateSchema.AGENCY_NAME
    ).rename(
        columns={ClimateSchema.CHANNEL_CODE: f"{ClimateSchema.CHANNEL_CODE}_agency"}
    )

    # Try a second map through parties
    second_pass = first_pass.pipe(
        map_channel_names_to_oecd_codes,
        channel_names_column=ClimateSchema.PROVIDER_NAME,
    )

    # Assign the channel code to the original dataframe, filling gaps
    return second_pass.assign(
        **{
            ClimateSchema.CHANNEL_CODE: lambda d: d[
                f"{ClimateSchema.CHANNEL_CODE}_agency"
            ].fillna(d[ClimateSchema.CHANNEL_CODE])
        }
    ).filter(
        [
            ClimateSchema.PROVIDER_CODE,
            ClimateSchema.AGENCY_NAME,
            ClimateSchema.CHANNEL_CODE,
        ]
    )


def _summarise_spending_by_idx(data: pd.DataFrame, idx: list[str]) -> pd.DataFrame:
    return (
        data.pipe(idx_to_str, idx=idx)
        .groupby(idx, observed=True, dropna=False)[ClimateSchema.VALUE]
        .sum()
        .reset_index()
        .loc[
            lambda d: d.flow_type.isin(
                [ClimateSchema.USD_DISBURSEMENT, ClimateSchema.USD_COMMITMENT]
            )
        ]
    )


def get_multilateral_spending_totals(
    start_year: int, end_year: int, party_code: list[str] = None
) -> pd.DataFrame:
    # Get total yearly spend
    total_spend = get_crs_allocable_spending(
        start_year=start_year, end_year=end_year
    ).pipe(_summarise_spending_by_idx, idx=ONE_IMPUTATIONS_SPENDING_IDX)

    # Filter for specific party code(s)
    if party_code is not None:
        total_spend = total_spend.loc[
            lambda d: d[ClimateSchema.PROVIDER_CODE].isin(party_code)
        ]

    # Get unique agencies and codes
    codes = _map_agency_then_party_to_channel_code(total_spend)

    # idx
    idx = [ClimateSchema.PROVIDER_CODE, ClimateSchema.AGENCY_NAME]

    # Set index to str
    codes = codes.pipe(idx_to_str, idx=idx)
    total_spend = total_spend.pipe(idx_to_str, idx=idx)

    # Merge spending and agencies data
    total_spend = total_spend.merge(codes, on=idx, how="left")

    return total_spend


def calculate_rolling_contributions(
    contributions_data: pd.DataFrame, window: int = 2, agg: str = "mean"
) -> pd.DataFrame:
    return (
        contributions_data.sort_values([ClimateSchema.YEAR])
        .groupby(
            [c for c in contributions_data.columns if c not in [ClimateSchema.YEAR]],
            observed=True,
            group_keys=False,
        )
        .apply(
            compute_rolling_sum,
            window=window,
            values=[ClimateSchema.VALUE],
            include_yearly_total=False,
            agg=agg,
        )
        .reset_index(drop=True)
    )


def get_imputations_by_provider_and_channel(
    start_year: int,
    end_year: int,
    rolling_window_spending: int = 2,
    rolling_agg: str = "mean",
    groupby: list[str] | None = None,
) -> pd.DataFrame:
    """
    Pipeline to get overall imputations based on a provider and a
    multilateral channel.

    A start and end year must be specified.
    The rolling window is 2 years, to match OECD practise. Setting
    a window of 1 would be equivalent to doing no smoothing.

    The aggregation operation matters a lot. In the default case, a 'mean'
    means that years with no flows will be filled with zeros. For example,
    the flow for Y if Y is 0 and Y-1 is 100 would become 50.

    The groupby parameter allows for convenient aggregations at a different level
    from the default. The aggregation must at least include the provider and channel
    codes.

    Args:
        start_year: The starting year for the data
        end_year: The end year for the data
        rolling_window_spending: How many years should be included as the 'window' in
        rolling calculations.
        rolling_agg: the aggregation function. It defaults to mean
        groupby: To use a different aggregation from the default.
    Returns:
        A pandas dataframe with the imputations data in USD units.

    """
    # Yearly multilateral spending
    spending = one_multilateral_spending(
        start_year=start_year,
        end_year=end_year,
        rolling_window=rolling_window_spending,
        agg=rolling_agg,
        groupby=groupby,
    ).pipe(
        map_channel_names_to_oecd_codes,
        channel_names_column=ClimateSchema.PROVIDER_NAME,
    )

    contributions = get_multilateral_contributions(
        start_year=start_year, end_year=end_year
    )

    imputed_data = convert_to_imputations(
        multi_spending=spending, multi_contributions=contributions
    )

    return imputed_data.reset_index(drop=True)


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

    group = group.dropna(subset=[ClimateSchema.PROVIDER_CODE])

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
    data[ClimateSchema.AGENCY_NAME] = data[ClimateSchema.AGENCY_NAME].fillna("0")

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


def read_summarise_multilateral_climate_spending(
    groupby: list[str] = None,
) -> pd.DataFrame:
    spending = pd.read_feather(
        ClimateDataPath.raw_data / "one_multilateral_spending.feather"
    )

    if groupby is None:
        groupby = [
            ClimateSchema.YEAR,
            ClimateSchema.PROVIDER_CODE,
            ClimateSchema.PROVIDER_NAME,
            ClimateSchema.AGENCY_NAME,
            ClimateSchema.AGENCY_CODE,
            ClimateSchema.FLOW_TYPE,
            ClimateSchema.CHANNEL_CODE,
            ClimateSchema.CHANNEL_NAME,
        ]

    spending = (
        spending.groupby(
            groupby,
            observed=True,
            dropna=False,
        )[CLIMATE_VALUES + ["climate_total"]]
        .sum()
        .reset_index()
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


if __name__ == "__main__":
    climate_spending = (
        read_summarise_multilateral_climate_spending()
        .pipe(map_imputations_channels)
        .pipe(summarise_by_imputation_channels)
    )
    #
    # overall_spending = read_overall_spending_by(
    #     start_year=2013,
    #     end_year=2021,
    #     provider_code=climate_spending.oecd_provider_code.unique(),
    # )
    #
    # overall = overall_spending.pipe(map_imputations_channels).pipe(
    #     summarise_by_imputation_channels
    # )
    #
    # idx = ["year", "oecd_channel_code", "flow_type"]
    #
    # combined = overall.pipe(idx_to_str, idx=idx).merge(
    #     climate_spending.pipe(idx_to_str, idx=idx),
    #     on=idx,
    #     how="inner",
    #     suffixes=("", "_climate"),
    #     # indicator=True,
    # )
    #
    # combined_climate_r = (
    #     combined.sort_values([ClimateSchema.YEAR, ClimateSchema.PROVIDER_CODE])
    #     .groupby(
    #         [
    #             ClimateSchema.PROVIDER_CODE,
    #             ClimateSchema.PROVIDER_NAME,
    #             ClimateSchema.FLOW_TYPE,
    #         ],
    #         observed=True,
    #         group_keys=False,
    #     )
    #     .apply(
    #         rolling_values,
    #         window=2,
    #         start_year=2013,
    #         end_year=2021,
    #         values=CLIMATE_VALUES + ["climate_total", "value"],
    #         agg="mean",
    #     )
    # )

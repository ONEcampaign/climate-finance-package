import pandas as pd

from climate_finance.common.schema import (
    ClimateSchema,
    VALUE_COLUMNS,
)
from climate_finance.oecd.cleaning_tools.tools import idx_to_str, set_crs_data_types
from climate_finance.oecd.crs.get_data import (
    get_crs_allocable_spending,
)
from climate_finance.common.analysis_tools import get_crs_allocable_to_total_ratio
from climate_finance.methodologies.imputed_multilateral.one_multilateral.shares import (
    one_multilateral_spending,
)
from climate_finance.methodologies.imputed_multilateral.tools import compute_rolling_sum
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

    imputations_data = pd.concat(dfs, ignore_index=True).pipe(
        _correct_for_allocable_share
    )

    # Clean
    imputations_data = (
        imputations_data.rename(
            columns={
                ClimateSchema.PROVIDER_TYPE: "channel_type",
                ClimateSchema.VALUE: "total_contribution",
            }
        )
        .pipe(set_crs_data_types)
        .filter(ONE_IMPUTATIONS_OUTPUT_COLUMNS)
    )

    return imputations_data


def _get_unique_agencies_df(data: pd.DataFrame) -> pd.DataFrame:
    idx = [ClimateSchema.PROVIDER_CODE, ClimateSchema.PROVIDER_NAME, ClimateSchema.AGENCY_NAME]
    return data.drop_duplicates(idx).filter(idx).dropna(subset=[ClimateSchema.PROVIDER_CODE])


def _map_agency_then_party_to_channel_code(spending_data: pd.DataFrame) -> pd.DataFrame:
    # Get unique agencies as a dataframe
    unique_agencies = _get_unique_agencies_df(spending_data)

    # Try a first map through agencies
    first_pass = unique_agencies.pipe(
        map_channel_names_to_oecd_codes, channel_names_column=ClimateSchema.AGENCY_NAME
    ).rename(columns={ClimateSchema.CHANNEL_CODE: f"{ClimateSchema.CHANNEL_CODE}_agency"})

    # Try a second map through parties
    second_pass = first_pass.pipe(
        map_channel_names_to_oecd_codes, channel_names_column=ClimateSchema.PROVIDER_NAME
    )

    # Assign the channel code to the original dataframe, filling gaps
    return second_pass.assign(
        **{
            ClimateSchema.CHANNEL_CODE: lambda d: d[
                f"{ClimateSchema.CHANNEL_CODE}_agency"
            ].fillna(d[ClimateSchema.CHANNEL_CODE])
        }
    ).filter([ClimateSchema.PROVIDER_CODE, ClimateSchema.AGENCY_NAME, ClimateSchema.CHANNEL_CODE])


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
        .pipe(set_crs_data_types)
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

    return total_spend.pipe(set_crs_data_types)


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
    ).pipe(map_channel_names_to_oecd_codes, channel_names_column=ClimateSchema.PROVIDER_NAME)

    contributions = get_multilateral_contributions(
        start_year=start_year, end_year=end_year
    )

    imputed_data = convert_to_imputations(
        multi_spending=spending, multi_contributions=contributions
    )

    return imputed_data.reset_index(drop=True)

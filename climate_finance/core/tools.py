import pandas as pd
from bblocks import convert_id
from oda_data.clean_data.channels import add_multi_channel_codes
from oda_data.clean_data.schema import OdaSchema
from thefuzz import process

from climate_finance.common.schema import (
    ClimateSchema,
    CRS_MAPPING,
    OECD_CLIMATE_INDICATORS,
)
from climate_finance.config import ClimateDataPath, logger
from climate_finance.oecd.cleaning_tools.tools import (
    convert_flows_millions_to_units,
    clean_multisystem_indicators,
    channel_codes_to_names,
)


def oecd_flow_mapping(flow: str) -> str:
    """Map the flow type to the OECD CRS flow type.

    Args:
        flow: A string specifying the flow type.

    Returns:
        str: The mapped flow type.

    """
    return {
        "gross_disbursements": "usd_disbursement",
        "commitments": "usd_commitment",
        "grant_equivalent": "usd_grant_equiv",
        "net_disbursements": "usd_net_disbursement",
    }[flow]


def standard_shareby(data: pd.DataFrame) -> list[str]:
    """Returns a list of column names (based on the provided data), which
     is a very aggregate way to group the data.

    Args:
        data: A pandas DataFrame.

    Returns:
        list[str]: A list of column names.

    """
    return [
        c
        for c in data.columns
        if c
        in [
            ClimateSchema.YEAR,
            ClimateSchema.PROVIDER_CODE,
            ClimateSchema.PROVIDER_NAME,
            ClimateSchema.AGENCY_CODE,
            ClimateSchema.AGENCY_NAME,
            ClimateSchema.FINANCE_TYPE,
            "source",
            ClimateSchema.PRICES,
            ClimateSchema.CURRENCY,
        ]
    ]


def groupby_none(data: pd.DataFrame) -> list[str]:
    """Returns a list of columns (excluding the value column).
    It's purpose is to keep the current level of detail in a groupby operation

    Args:
        data: A pandas DataFrame.


    Returns:
        list[str]: A list of column names.

    """
    return [c for c in data.columns if c != ClimateSchema.VALUE]


def groupby_sum(data: pd.DataFrame, groupby: list[str]) -> pd.DataFrame:
    """Sums the value column by the groupby columns.

    Args:
        data: A pandas DataFrame.
        groupby: A list of strings specifying the columns to group by.

    Returns:
        pd.DataFrame: The summed data.
    """

    groupby = [c for c in groupby if c in data.columns]

    return data.groupby(groupby, observed=True, dropna=False, as_index=False)[
        ClimateSchema.VALUE
    ].sum()


def rolling_value_sum(
    data: pd.DataFrame, groupby: list[str], rolling_years: int
) -> pd.DataFrame:
    """Sums the value column by the groupby columns and a rolling year window.

    Args:
        data: A pandas DataFrame.
        groupby: A list of strings specifying the columns to group by.
        rolling_years: An integer specifying the number of years to use for the rolling
        total share

    Returns:
        pd.DataFrame: The summed data.
    """

    # Add the rolling sum
    data[ClimateSchema.VALUE] = (
        data.fillna({ClimateSchema.VALUE: 0})
        .groupby(
            [c for c in groupby if c != ClimateSchema.YEAR],
            observed=True,
            dropna=False,
            group_keys=False,
            as_index=False,
        )[ClimateSchema.VALUE]
        .rolling(
            rolling_years, min_periods=1
        )  # min periods are 1 to avoid Nans in sparse data (when very granular)
        .sum()[ClimateSchema.VALUE]
    )

    data = data.loc[
        lambda d: d[ClimateSchema.YEAR]
        >= d[ClimateSchema.YEAR].min() - 1 + rolling_years
    ]

    return data


def share_of_shareby(data: pd.DataFrame, shareby: list[str]) -> pd.DataFrame:
    """Calculate the share of the shareby columns
    Args:
        data: pd.DataFrame: The data to calculate the share of the shareby columns
        shareby: list[str]: A list of strings specifying the columns to calculate the shares by.

    Returns:
        pd.DataFrame: The data with the shares of the shareby columns.

    """
    # Calculate the shares
    data[ClimateSchema.VALUE] = data[ClimateSchema.VALUE] / data.groupby(
        shareby, observed=True, dropna=False, group_keys=False
    )[ClimateSchema.VALUE].transform("sum")

    return data


def data_to_share(
    data: pd.DataFrame, groupby: list[str], shareby: list[str], rolling_years: int
) -> pd.DataFrame:
    """
    Convert the loaded data to shares of total spending. The shares are calculated
    based on the rolling total of the spending for the specified years.

    Args:
        data: a pandas DataFrame with the loaded data.
        groupby: a list of strings specifying the columns to group by.
        shareby: a list of strings specifying the columns to calculate the shares by.
        rolling_years: an integer specifying the number of years to use for the rolling
        total share

    Returns:
        pd.DataFrame: The data with the shares of total spending.
    """

    # First get the data at the right level of aggregation
    data = groupby_sum(data=data, groupby=groupby)

    # Second transform the value column into a rolling sum by the number of periods
    data = rolling_value_sum(data=data, groupby=groupby, rolling_years=rolling_years)

    # Calculate the shares
    data = share_of_shareby(data=data, shareby=shareby)

    return data


def filter_flows(data: pd.DataFrame, flows: list[str]) -> pd.DataFrame:
    """Filter the data to include only the specified flows.

    Args:
        data: A pandas DataFrame.
        flows: A list of strings specifying the flows to include.

    Returns:
        pd.DataFrame: The filtered data.

    """
    # convert the flow string the user passes into the OECD CRS flow type
    flows = [oecd_flow_mapping(flow) for flow in flows]

    # filter the data
    return data.loc[data[ClimateSchema.FLOW_TYPE].isin(flows)]


def clean_multi_contributions(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the multilateral contributions dataframe

    Converts to units, renames and keeps only relevant columns

    Args:
        df (pd.DataFrame): The dataframe to clean.
        flow_type (str): The flow type (disbursements or commitments).

    """

    # rename columns
    df = df.rename(columns=CRS_MAPPING)

    # convert to millions
    df = convert_flows_millions_to_units(df, flow_columns=[ClimateSchema.VALUE])

    # rename indicators
    df = clean_multisystem_indicators(df)

    # map channel names
    df = channel_codes_to_names(df)

    # keep only relevant columns
    df = df.filter(
        [
            ClimateSchema.YEAR,
            ClimateSchema.PROVIDER_CODE,
            ClimateSchema.CHANNEL_CODE,
            ClimateSchema.CHANNEL_NAME,
            ClimateSchema.FLOW_TYPE,
            ClimateSchema.CURRENCY,
            ClimateSchema.PRICES,
            ClimateSchema.VALUE,
        ]
    )

    # clean output
    df = (
        df.groupby(
            by=[c for c in df.columns if c != ClimateSchema.VALUE],
            dropna=False,
            observed=True,
        )
        .sum(numeric_only=True)
        .reset_index()
    )

    return df


def align_oda_data_names(
    data: pd.DataFrame, to_climate_names: bool = False
) -> pd.DataFrame:
    """Align the column names of the ODA data to the climate finance data.

    Args:
        data: A pandas DataFrame.
        to_climate_names: A boolean specifying whether to convert the column names
        back to climate finance names.


    Returns:
        pd.DataFrame: The aligned data.

    """

    names = {
        ClimateSchema.PROVIDER_NAME: OdaSchema.PROVIDER_NAME,
        ClimateSchema.PROVIDER_CODE: OdaSchema.PROVIDER_CODE,
        ClimateSchema.AGENCY_NAME: OdaSchema.AGENCY_NAME,
        ClimateSchema.AGENCY_CODE: OdaSchema.AGENCY_CODE,
        ClimateSchema.CHANNEL_CODE: OdaSchema.CHANNEL_CODE,
        ClimateSchema.CHANNEL_NAME: "name",
    }

    if to_climate_names:
        names = {v: k for k, v in names.items()}

    # check that all the columns are in the data
    names = {k: v for k, v in names.items() if k in data.columns}

    return data.rename(columns=names)


def remove_channel_name_from_spending_data(spending_data: pd.DataFrame) -> pd.DataFrame:
    """Remove the channel name column and group the data by remaining columns
    in order to have 1 line per matched provider

    Args:
        spending_data: The Pandas DataFrame with the spending data.

    """

    # Remove channel name from spending data
    spending_data = spending_data.drop(columns=[ClimateSchema.CHANNEL_NAME])

    spending_data = (
        spending_data.groupby(
            [c for c in spending_data.columns if c not in [ClimateSchema.VALUE]],
            observed=True,
            dropna=False,
        )[ClimateSchema.VALUE]
        .sum()
        .reset_index()
    )

    return spending_data


def merge_spending_and_contributions(
    spending_data: pd.DataFrame, contributions_data: pd.DataFrame
) -> pd.DataFrame:
    """Merge the spending and contributions data for each provider

    Args:
        spending_data: A pandas DataFrame containing the spending data.
        contributions_data: A pandas DataFrame containing the contributions data.

    Returns:
        pd.DataFrame: The merged data.


    """

    # Define an index
    idx = [
        c for c in spending_data if c in contributions_data and c != ClimateSchema.VALUE
    ]

    # Create a list to store the individual data for providers
    combined = []

    for provider in contributions_data[ClimateSchema.PROVIDER_CODE].unique():
        contrib_ = contributions_data.loc[
            lambda d: d[ClimateSchema.PROVIDER_CODE] == provider
        ]
        merged_ = contrib_.merge(
            spending_data, on=idx, how="inner", suffixes=("_inflow", "_spending_share")
        )
        combined.append(merged_)

    return pd.concat(combined, ignore_index=True)


def calculate_imputations(data: pd.DataFrame) -> pd.DataFrame:
    """Calculate imputations using a dataframe which contains inflow and spending shares
    data.

    Args:
            data: A pandas DataFrame containing inflow and spending shares data.

    Returns:
            pd.DataFrame: The imputed data.
    """

    # Calculate the imputations
    data[ClimateSchema.VALUE] = (
        data[ClimateSchema.VALUE + "_inflow"]
        * data[ClimateSchema.VALUE + "_spending_share"]
    )

    # drop the inflow and spending share columns
    data = data.drop(
        [ClimateSchema.VALUE + "_inflow", ClimateSchema.VALUE + "_spending_share"],
        axis=1,
    )

    return data


def get_oecd_classification() -> dict:
    """Read the OECD CRS classification and return it as a dictionary."""

    df = pd.read_csv(ClimateDataPath.scripts / "core" / "oecd_classification.csv")

    types = {}

    for dt in df["type"].unique():
        types[dt] = df.loc[df["type"] == dt].set_index("code")["name"].to_dict()

    return types


def get_available_providers(include_private: bool = False) -> dict:
    """return a list of available providers"""

    providers = get_oecd_classification()

    available = (
        providers["DAC member"]
        | providers["Non-DAC member"]
        | providers["Multilateral donor"]
    )

    if include_private:
        available |= providers["Private donor"]

    return available


def fuzzy_match_provider(providers: list[str], options: dict) -> list:
    results = []
    providers_list = list(options)

    for user_input in providers:
        # Finding the best match for the user_input in the list of countries
        best_match = process.extractOne(user_input, providers_list)
        # best_match is a tuple like ('Country Name', score)
        if best_match:
            if best_match[1] < 89:
                logger.info(f"No match found for {user_input}")
                continue
            # Find the key in the dictionary corresponding to the matched country
            match_key = options[best_match[0]]
            results.append(match_key)

    return results


def match_providers(providers: str | list[str]) -> list[int]:
    """"""
    # Silence the country_converter logger
    import logging

    cc_loger = logging.getLogger("country_converter")
    cc_loger.setLevel(logging.CRITICAL)

    # Ensure user_inputs is always a list to simplify processing
    if not isinstance(providers, list):
        providers = [providers]

    # Get the available providers
    options = {v: k for k, v in get_available_providers(include_private=True).items()}

    # Attempt to match the providers to the DAC codes
    match = convert_id(
        series=pd.Series(providers, index=providers),
        from_type="ISO3" if all(len(item) == 3 for item in providers) else "regex",
        to_type="DACcode",
        not_found=pd.NA,
    ).astype("int32[pyarrow]")

    # if missing, try fuzzy matching
    if match.isna().any():
        # Get the missing providers
        missing = match[match.isna()].index.tolist()
        # Fuzzy match the missing providers
        results = fuzzy_match_provider(providers=missing, options=options)
    else:
        results = []

    # Combine the matches
    all_matches = match[match.notna()].tolist() + results

    return all_matches


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


def alignment_pipeline(data: pd.DataFrame) -> pd.DataFrame:
    """Pipeline to align the data to the climate schema.
    Args:
        data: A raw dataframe to be aligned to the climate schema.

    Returns:
        pd.DataFrame: The aligned data.

    """
    return (
        data.pipe(align_oda_data_names)
        .pipe(add_multi_channel_codes)
        .pipe(align_oda_data_names, to_climate_names=True)
        .pipe(remove_channel_name_from_spending_data)
    )


def subtract_cross_cutting(data: pd.DataFrame) -> pd.DataFrame:
    """Subtract cross-cutting data from Adaptation and Mitigation data."""

    climate_columns = [
        ClimateSchema.ADAPTATION,
        ClimateSchema.MITIGATION,
        ClimateSchema.CROSS_CUTTING,
    ]

    datap = (
        data.pivot(
            index=[c for c in data.columns if c not in ["indicator", "value"]],
            columns="indicator",
            values="value",
        )
        .fillna({c: 0 for c in climate_columns})
        .reset_index()
    )

    # subtract cross-cutting from adaptation and mitigation
    datap[ClimateSchema.ADAPTATION] = (
        datap[ClimateSchema.ADAPTATION] - datap[ClimateSchema.CROSS_CUTTING]
    )

    datap[ClimateSchema.MITIGATION] = (
        datap[ClimateSchema.MITIGATION] - datap[ClimateSchema.CROSS_CUTTING]
    )

    data = datap.melt(
        id_vars=[c for c in datap.columns if c not in climate_columns],
        var_name=ClimateSchema.INDICATOR,
        value_name=ClimateSchema.VALUE,
    )

    return data


def compute_non_climate_from_climate_and_total(
    climate_df: pd.DataFrame, total_df: pd.DataFrame
) -> pd.DataFrame:
    """Compute the non-climate finance from the climate and total finance data.

    Non-climate finance is computed as the total finance minus the climate finance.
    The data is grouped by the columns that are common to both the climate and total
    finance data.

    Args:
        climate_df: A pandas DataFrame containing the climate finance data.
        total_df: A pandas DataFrame containing the total finance data.

    Returns:
        pd.DataFrame: The non-climate finance data.

    """

    join_by = [c for c in climate_df.columns if c in total_df.columns and c != "value"]

    data = (
        climate_df.pipe(groupby_sum, groupby=join_by)
        .merge(total_df, on=join_by, how="outer", suffixes=("", "_crs"))
        .fillna({ClimateSchema.VALUE: 0})
        .assign(
            **{
                ClimateSchema.VALUE: lambda x: x[f"{ClimateSchema.VALUE}_crs"]
                - x[ClimateSchema.VALUE]
            }
        )
        .dropna(subset=ClimateSchema.CHANNEL_CODE)
        .drop(columns=[ClimateSchema.VALUE + "_crs"])
    )

    data[ClimateSchema.INDICATOR] = "Not climate relevant"

    return data

import pandas as pd

from climate_finance.common.schema import ClimateSchema
from climate_finance.config import logger
from climate_finance.oecd.cleaning_tools.tools import keep_only_allocable_aid


def pivot_by_modality(data: pd.DataFrame) -> pd.DataFrame:
    """
    Args:
        data (pd.DataFrame): The input dataframe to pivot.

    Returns:
        pd.DataFrame: The pivoted dataframe, with modality values as
         columns and the corresponding values as the cell values.

    """
    return data.pivot(
        index=[
            c
            for c in data.columns
            if c not in [ClimateSchema.VALUE, ClimateSchema.FLOW_MODALITY]
        ],
        columns=ClimateSchema.FLOW_MODALITY,
        values=ClimateSchema.VALUE,
    ).reset_index()


def add_allocable_share(data: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a column with the allocable share of the total.
    Args:
        data: A dataframe with a bilateral_allocable and a total column.

    Returns:
        A dataframe with a allocable_share column added.

    """
    return data.assign(
        **{
            ClimateSchema.ALLOCABLE_SHARE: lambda d: (
                d.bilateral_allocable / d.total
            ).fillna(0)
        }
    )


def to_list_of_ints(value):
    """
    Convert the given value to a list of strings.

    Args:
        value: The value to be converted.

    Returns:
        A list of strings where each item is converted to its string representation.

    """
    if isinstance(value, (str, int)):
        return [int(value)]
    elif isinstance(value, list):
        return [int(item) if isinstance(item, float) else int(item) for item in value]
    else:
        return value


def check_codes_type(
    codes: list[str | int] | str | int | None,
) -> list[str] | None:
    """
    Checks that the provider codes are of the right type.
    Args:
        codes (list[str] | str | None): The provider codes to check.
    Returns:
        list[str] | None: The provider codes if they are of the right type.
    """
    if codes is None:
        return None
    if isinstance(codes, float):
        raise TypeError(f"Codes must be integers")

    codes = to_list_of_ints(codes)

    if not all(isinstance(code, int) for code in codes):
        try:
            codes = [int(code) for code in codes]
        except ValueError:
            raise TypeError(f"Codes must all be integers")
    return codes


def filter_providers(
    data: pd.DataFrame, provider_codes: list[int] | int
) -> pd.DataFrame:
    """
    Check that the requested providers are in the data and filter the data to only
    include the requested providers. If party is None, return the original dataframe.

    Args:
        data: A dataframe containing the CRS data.
        provider_codes: A list of parties to filter the data to.

    Returns:
        A dataframe with the CRS data filtered to only include the requested parties.
        If party is None, return the original dataframe.

    """

    # Validate the provider argument
    provider_codes = check_codes_type(provider_codes)
    if provider_codes is None:
        return data

    # Check that the requested providers are in the CRS data
    missing_providers = set(provider_codes) - set(
        data[ClimateSchema.PROVIDER_CODE].unique()
    )
    # Log a warning if any of the requested providers are not in the CRS data
    if len(missing_providers) > 0:
        logger.warning(
            f"The following parties are not found in CRS data:\n{missing_providers}"
        )
    # Filter the data to only include the requested providers
    return data.loc[lambda d: d[ClimateSchema.PROVIDER_CODE].isin(provider_codes)]


def filter_recipients(data: pd.DataFrame, recipient_codes: list[int]) -> pd.DataFrame:
    """
    Check that the requested providers are in the data and filter the data to only
    include the requested providers. If party is None, return the original dataframe.

    Args:
        data: A dataframe containing the CRS data.
        recipient_codes: A list of recipients to filter the data.

    Returns:
        A dataframe with the CRS data filtered to only include the requested parties.
        If party is None, return the original dataframe.

    """

    # Validate the provider argument
    recipient_codes = check_codes_type(recipient_codes)
    if recipient_codes is None:
        return data

    # Check that the requested recipients are in the CRS data
    missing_recipients = set(recipient_codes) - set(
        data[ClimateSchema.RECIPIENT_CODE].unique()
    )
    # Log a warning if any of the requested recipients are not in the data
    if len(missing_recipients) > 0:
        logger.warning(
            f"The following recipients are not found in the data:\n{recipient_codes}"
        )
    # Filter the data to only include the requested providers
    return data.loc[lambda d: d[ClimateSchema.RECIPIENT_CODE].isin(recipient_codes)]


def add_net_disbursement(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a column with net disbursement values.
    Args:
        df: A dataframe with a usd_disbursement and a usd_received column.

    Returns:
        A dataframe with a usd_net_disbursement column added.

    """
    return df.assign(
        **{
            ClimateSchema.USD_NET_DISBURSEMENT: lambda d: d[
                ClimateSchema.USD_DISBURSEMENT
            ].fillna(0)
            - d[ClimateSchema.USD_RECEIVED].fillna(0)
        }
    )


def get_crs_allocable_to_total_ratio(full_crs: pd.DataFrame) -> pd.DataFrame:
    """
    Fetches bilateral spending data for a given flow type and time period.

    Args: full_crs (pd.DataFrame): The full clean CRS data, not filtered for allocable.

    Returns:
        pd.DataFrame: A dataframe containing bilateral spending data for
        the specified flow type and time period.
    """

    simpler_columns = [
        ClimateSchema.YEAR,
        ClimateSchema.PROVIDER_CODE,
        ClimateSchema.AGENCY_CODE,
        ClimateSchema.FLOW_MODALITY,
        ClimateSchema.FLOW_TYPE,
    ]

    # Calculate the total
    total = (
        full_crs.copy(deep=True)
        .assign(**{ClimateSchema.FLOW_MODALITY: "total"})
        .groupby(simpler_columns, dropna=False, observed=True)
        .sum(numeric_only=True)
        .reset_index()
    )

    # Calculate the allocable
    allocable = (
        full_crs.pipe(keep_only_allocable_aid)
        .assign(**{ClimateSchema.FLOW_MODALITY: "bilateral_allocable"})
        .groupby(simpler_columns, dropna=False, observed=True)
        .sum(numeric_only=True)
        .reset_index()
    )

    # Combine the data
    data = pd.concat([allocable, total], ignore_index=True)

    data = data.pipe(pivot_by_modality).pipe(add_allocable_share)

    return data


def keep_commitments_and_disbursements_only(data: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only commitments and disbursements from the CRS data.
    Args:
        data: A dataframe with a flow_type column.

    Returns:
        A dataframe with only commitments and disbursements.

    """
    keep = [ClimateSchema.USD_COMMITMENT, ClimateSchema.USD_DISBURSEMENT]
    return data.loc[lambda d: d[ClimateSchema.FLOW_TYPE].isin(keep)]

from typing import KeysView

import pandas as pd

from climate_finance.common.schema import ClimateSchema
from climate_finance.config import logger


def to_list_of_ints(value):
    """
    Convert the given value to a list of strings.

    Args:
        value: The value to be converted.

    Returns:
        A list of strings where each item is converted to its string representation.

    """
    if isinstance(value, KeysView):
        value = list(value)
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
        raise TypeError("Codes must be integers")

    codes = to_list_of_ints(codes)

    if not all(isinstance(code, int) for code in codes):
        try:
            codes = [int(code) for code in codes]
        except ValueError:
            raise TypeError("Codes must all be integers")
    return codes


def get_providers_filter(
    provider_codes: list[int], provider_column: str = "donor_code"
) -> tuple:
    provider_code = check_codes_type(codes=provider_codes)
    return provider_column, "in", provider_code


def get_recipients_filter(
    recipient_codes: list[int], recipient_column: str = "recipient_code"
) -> tuple:

    recipient_code = check_codes_type(codes=recipient_codes)
    return recipient_column, "in", recipient_code


def check_missing(data: pd.DataFrame, column: str, codes: list) -> None:
    if codes is None:
        return

    codes = check_codes_type(codes=codes)

    missing_codes = set(codes) - set(data[column].unique())
    if len(missing_codes) > 0:
        logger.warning(
            f"The following {column}s are not found in the data:\n{missing_codes}"
        )


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

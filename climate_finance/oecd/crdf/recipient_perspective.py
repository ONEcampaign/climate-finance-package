from pathlib import Path

import pandas as pd
from oda_data import set_data_path

from climate_finance.common.analysis_tools import filter_providers, filter_recipients
from climate_finance.common.schema import ClimateSchema
from climate_finance.config import ClimateDataPath, logger
from climate_finance.oecd.cleaning_tools.tools import (
    fix_crdf_provider_names_columns,
    fix_crdf_recipient_errors,
    assign_usd_commitments_as_flow_type,
)
from climate_finance.oecd.crdf.tools import (
    download_file,
    load_or_download,
)
from oda_data.clean_data.channels import clean_string

FILE_PATH: Path = (
    ClimateDataPath.raw_data / "oecd_climate_recipient_perspective.feather"
)

set_data_path(ClimateDataPath.raw_data)

BASE_URL: str = "https://webfs.oecd.org/climate/RecipientPerspective/CRDF-RP-2000-"

UNIQUE_INDEX = [
    ClimateSchema.YEAR,
    ClimateSchema.PROVIDER_CODE,
    ClimateSchema.AGENCY_CODE,
    ClimateSchema.CRS_ID,
    ClimateSchema.PROJECT_ID,
    ClimateSchema.RECIPIENT_CODE,
    ClimateSchema.PURPOSE_CODE,
]

MULTI_COLUMNS: list = [
    ClimateSchema.YEAR,
    ClimateSchema.PROVIDER_TYPE,
    ClimateSchema.PROVIDER_NAME,
    ClimateSchema.PROVIDER_DETAILED,
    ClimateSchema.PROVIDER_CODE,
    ClimateSchema.AGENCY_CODE,
    ClimateSchema.AGENCY_NAME,
    ClimateSchema.CRS_ID,
    ClimateSchema.PROJECT_ID,
    ClimateSchema.RECIPIENT_CODE,
    ClimateSchema.RECIPIENT_NAME,
    ClimateSchema.RECIPIENT_REGION,
    ClimateSchema.RECIPIENT_INCOME,
    ClimateSchema.CONCESSIONALITY,
    ClimateSchema.CHANNEL_CODE_DELIVERY,
    ClimateSchema.CHANNEL_NAME_DELIVERY,
    ClimateSchema.SECTOR_NAME,
    ClimateSchema.PURPOSE_CODE,
    ClimateSchema.PURPOSE_NAME,
    ClimateSchema.FLOW_MODALITY,
    ClimateSchema.FINANCIAL_INSTRUMENT,
    ClimateSchema.FINANCE_TYPE,
    ClimateSchema.PROJECT_TITLE,
    ClimateSchema.PROJECT_DESCRIPTION,
    ClimateSchema.GENDER,
    ClimateSchema.INDICATOR,
    ClimateSchema.FLOW_TYPE,
    ClimateSchema.VALUE,
    ClimateSchema.TOTAL_VALUE,
    ClimateSchema.SHARE,
]


def get_recipient_perspective(
    start_year: int,
    end_year: int,
    provider_code: int | list[int] | str | None = None,
    recipient_code: int | list[int] | str | None = None,
    force_update: bool = False,
) -> pd.DataFrame:
    """
    Get the provider perspective data from the OECD website. The data is read or downloaded
    and then reshaped to be in a 'longer' format where the different types of climate
    finance are indicators.

    Args:
        start_year: The start year that should be covered in the data
        end_year: The end year that should be covered in the data
        provider_code: Optionally, specify one or more provider. If not specified, all
        providers are included.
        recipient_code: Optionally, specify one or more recipient. If not specified, all
        recipients are included.
        force_update: If True, the data is updated from the source. This can potentially
        overwrite any data that has been downloaded to the 'raw_data' folder.

    Returns:

    """
    # Study years
    years = [y for y in range(start_year, end_year + 1)]

    # Check if data should be forced to update
    if force_update:
        download_file(base_url=BASE_URL, save_to_path=FILE_PATH)

    # Try to load file
    df = load_or_download(base_url=BASE_URL, save_to_path=FILE_PATH)

    # Filter for years
    df = df.loc[lambda d: d[ClimateSchema.YEAR].isin(years)]

    # filter for providers (if needed)
    df = filter_providers(data=df, provider_codes=provider_code)

    # filter for recipients (if needed)
    df = filter_recipients(data=df, recipient_codes=recipient_code)

    # Rename provider columns
    df = df.pipe(fix_crdf_provider_names_columns)

    # Fix errors in recipient code
    df = df.pipe(fix_crdf_recipient_errors)

    # Clean long description
    df[ClimateSchema.PROJECT_DESCRIPTION] = clean_string(
        df[ClimateSchema.PROJECT_DESCRIPTION]
    )

    # Add flow type
    df = df.pipe(assign_usd_commitments_as_flow_type)

    return df


if __name__ == "__main__":
    df = get_recipient_perspective(2019, 2021)

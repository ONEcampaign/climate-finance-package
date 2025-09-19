from pathlib import Path

import pandas as pd

from climate_finance.common.analysis_tools import (
    get_providers_filter,
    get_recipients_filter,
    check_missing,
)
from climate_finance.common.schema import ClimateSchema
from climate_finance.config import ClimateDataPath
from climate_finance.core.tools import get_cross_cutting_data_oecd
from climate_finance.oecd.cleaning_tools.tools import (
    clean_crdf_columns,
)
from climate_finance.oecd.crdf.tools import (
    download_file,
    get_marker_data,
    load_or_download,
)

FILE_PATH: Path = ClimateDataPath.raw_data / "oecd_climate_provider_perspective.parquet"
BASE_URL: str = (
    "https://webfs.oecd.org/climate/DonorPerspective/CRDF-DP-all%20years-2012-"
)


def _get_and_remove_multilateral(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Get the multilateral data and remove it from the dataframe.
    This function returns the original dataframe without the multilateral data and the
    multilateral data separately.

    Args:
        df: The dataframe to get the multilateral data from.

    Returns:
        The dataframe without the multilateral data and the multilateral data (as a tuple)

    """
    # Create a mask for the multilateral data
    mask = (df[ClimateSchema.MITIGATION] == 99) | (df[ClimateSchema.ADAPTATION] == 99)

    # Create a dataframe with the multilateral data
    multilateral = (
        df.loc[mask]
        .copy()
        .assign(**{ClimateSchema.INDICATOR: ClimateSchema.CLIMATE_UNSPECIFIED})
        .rename(columns={ClimateSchema.CLIMATE_FINANCE_VALUE: "value"})
    )

    # Remove the multilateral data from the dataframe
    df = df.loc[~mask]

    return df, multilateral


def get_provider_perspective(
    start_year: int,
    end_year: int,
    provider_code: str | int | list[str | int] | None = None,
    recipient_code: str | int | list[str | int] | None = None,
    force_update: bool = False,
) -> pd.DataFrame:
    """
    Get the provider perspective data from the OECD website. The data is read or downloaded
    and then reshaped to be in a 'longer' format where the different types of climate
    finance are indicators.

    Args:
        start_year: The start year that should be covered in the data
        end_year: The end year that should be covered in the data
        provider_code: Optionally, specify one or more providers. If not specified, all
        providers are included.
        recipient_code: Optionally, specify one or more recipients. If not specified, all
        recipients are included.
        force_update: If True, the data is updated from the source. This can potentially
        overwrite any data that has been downloaded to the 'raw_data' folder.

    Returns:

    """
    # Study years
    years = [str(y) for y in range(start_year, end_year + 1)]

    filters = []
    # Provider and recipient filters
    if provider_code is not None:
        filters.append(
            get_providers_filter(provider_code, provider_column="provider_code")
        )
    if recipient_code is not None:
        filters.append(
            get_recipients_filter(recipient_code, recipient_column="recipient_code")
        )

    # Provider and recipient filters
    filters.append(["year", "in", years])

    # Check if data should be forced to update
    if force_update:
        download_file(base_url=BASE_URL, save_to_path=FILE_PATH)

    # Try to load file
    df = load_or_download(base_url=BASE_URL, save_to_path=FILE_PATH, filters=filters)

    # Check if there are missing values from filter
    check_missing(df, ClimateSchema.PROVIDER_CODE, provider_code)
    check_missing(df, ClimateSchema.RECIPIENT_CODE, recipient_code)
    check_missing(df, ClimateSchema.YEAR, years)

    # get a multilateral df and remove multilateral from the main df
    df, multilateral = _get_and_remove_multilateral(df)

    # get cross cutting values
    cross_cutting = df.pipe(get_cross_cutting_data_oecd).rename(
        columns={ClimateSchema.CROSS_CUTTING_VALUE: ClimateSchema.VALUE}
    )

    # Get adaptation
    adaptation = get_marker_data(df, marker=ClimateSchema.ADAPTATION)

    # Get mitigation
    mitigation = get_marker_data(df, marker=ClimateSchema.MITIGATION)

    # Clean the different dataframes
    dfs = [
        clean_crdf_columns(d)
        for d in [adaptation, mitigation, multilateral, cross_cutting]
    ]

    # Merge the dataframes
    data = pd.concat(dfs, ignore_index=True)

    return data

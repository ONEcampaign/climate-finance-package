from pathlib import Path

import pandas as pd

from climate_finance.config import ClimateDataPath
from climate_finance.oecd.cleaning_tools.schema import CrsSchema
from climate_finance.oecd.climate_related_activities.tools import (
    download_file,
    rename_marker_columns,
    marker_columns_to_numeric,
    get_marker_data,
    load_or_download,
    clean_columns,
)
from climate_finance.oecd.imputed_multilateral.tools import check_and_filter_parties
from climate_finance.oecd.methodologies.bilateral_methodologies import (
    get_cross_cutting_data_oecd,
)

FILE_PATH: Path = ClimateDataPath.raw_data / "oecd_climate_provider_perspective.feather"
BASE_URL: str = "https://webfs.oecd.org/climate/DonorPerspective/CRDF-DP-2012-"


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
    mask = (df[CrsSchema.MITIGATION] == 99) | (df[CrsSchema.ADAPTATION] == 99)

    # Create a dataframe with the multilateral data
    multilateral = (
        df.loc[mask]
        .copy()
        .assign(**{CrsSchema.INDICATOR: CrsSchema.CLIMATE_UNSPECIFIED})
        .rename(columns={CrsSchema.CLIMATE_FINANCE_VALUE: "value"})
    )

    # Remove the multilateral data from the dataframe
    df = df.loc[~mask]

    return df, multilateral


def get_provider_perspective(
    start_year: int,
    end_year: int,
    party: str | list[str] | None = None,
    force_update: bool = False,
) -> pd.DataFrame:
    """
    Get the provider perspective data from the OECD website. The data is read or downloaded
    and then reshaped to be in a 'longer' format where the different types of climate
    finance are indicators.

    Args:
        start_year: The start year that should be covered in the data
        end_year: The end year that should be covered in the data
        party: Optionally, specify one or more parties. If not specified, all
        parties are included.
        force_update: If True, the data is updated from the source. This can potentially
        overwrite any data that has been downloaded to the 'raw_data' folder.

    Returns:

    """
    # Study years
    years = range(start_year, end_year + 1)

    # Check if data should be forced to update
    if force_update:
        download_file(base_url=BASE_URL, save_to_path=FILE_PATH)

    # Try to load file
    df = load_or_download(base_url=BASE_URL, save_to_path=FILE_PATH)

    # Rename markers
    df = rename_marker_columns(df)
    # Convert markers to multilateral
    df = marker_columns_to_numeric(df)

    # get a multilateral df and remove multilateral from the main df
    df, multilateral = _get_and_remove_multilateral(df)

    # get cross cutting values
    cross_cutting = get_cross_cutting_data_oecd(df).rename(
        columns={CrsSchema.CROSS_CUTTING_VALUE: CrsSchema.VALUE}
    )

    # Get adaptation
    adaptation = get_marker_data(df, marker=CrsSchema.ADAPTATION)

    # Get mitigation
    mitigation = get_marker_data(df, marker=CrsSchema.MITIGATION)

    # Clean the different dataframes
    dfs = [
        clean_columns(d) for d in [adaptation, mitigation, multilateral, cross_cutting]
    ]

    # Merge the dataframes
    data = pd.concat(dfs, ignore_index=True)

    # filter for years
    data = data.loc[lambda d: d[CrsSchema.YEAR].isin(years)]

    # Check parties
    data = check_and_filter_parties(data, party=party)

    return data


if __name__ == "__main__":
    df = get_provider_perspective(2019, 2020, force_update=True)

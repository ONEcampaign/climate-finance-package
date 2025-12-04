import pandas as pd

from climate_finance.unfccc.download.get_data import (
    get_unfccc_bilateral as unfccc_bilateral_from_interface,
    get_unfccc_multilateral as unfccc_multilateral_from_interface,
    get_unfccc_summary as unfccc_summary_from_interface,
)


def bilateral_unfccc(
    start_year: int,
    end_year: int,
    party: list[str] | str | None = None,
    br: list[int] | int | None = None,
    update_data: bool = False,
    data_source: str = "data_interface",
) -> pd.DataFrame:
    """
    This function can be used to get the UNFCCC bilateral data. This data can
    come from the UNFCCC data interface or from the BR files (TODO).

    It returns a dataframe based on the different arguments passed to it.

    Args:
        start_year: The start year that should be covered in the data
        end_year: The end year that should be covered in the data
        party: Optionally, specify one or more parties. If not specified, all
        parties are included.
        br: Optionally, specify one or more BRs. If not specified, all previously
        downloaded BRs are included. If none have been downloaded, or if a data
        update is requested without specifying which BRs to download, BRs 3-5 are
        downloaded.
        update_data: If True, the data is updated from the source. This can potentially
        overwrite any data that has been downloaded to the 'unfccc_data_interface' folder.
        data_source: The source of the data. Currently, only 'data_interface' is supported.

    Returns:
        df (pd.DataFrame): The UNFCCC bilateral data.

    """

    # Check that the data source requested is valid
    if data_source not in ["data_interface", "br_files"]:
        raise ValueError("data_source must be either 'data_interface' or 'br_files'")

    # Get the right function to get the data, based on the data source requested
    if data_source == "data_interface":
        bilateral_function = unfccc_bilateral_from_interface
    else:
        raise NotImplementedError("Accessing data from BR files is not yet supported")

    # Get the data
    data = bilateral_function(
        start_year=start_year,
        end_year=end_year,
        party=party,
        br=br,
        force_download=update_data,
    )

    return data


def multilateral_unfccc(
    start_year: int,
    end_year: int,
    party: list[str] | str | None = None,
    br: list[int] | int | None = None,
    update_data: bool = False,
    data_source: str = "data_interface",
) -> pd.DataFrame:
    """
    This function can be used to get the UNFCCC multilateral data. This data can
    come from the UNFCCC data interface or from the BR files (TODO).

    It returns a dataframe based on the different arguments passed to it.

    Args:
        start_year: The start year that should be covered in the data
        end_year: The end year that should be covered in the data
        party: Optionally, specify one or more parties. If not specified, all
        parties are included.
        br: Optionally, specify one or more BRs. If not specified, all previously
        downloaded BRs are included. If none have been downloaded, or if a data
        update is requested without specifying which BRs to download, BRs 3-5 are
        downloaded.
        update_data: If True, the data is updated from the source. This can potentially
        overwrite any data that has been downloaded to the 'unfccc_data_interface' folder.
        data_source: The source of the data. Currently, only 'data_interface' is supported.

    Returns:
        df (pd.DataFrame): The UNFCCC multilateral data.
    """

    # Check that the data source requested is valid
    if data_source not in ["data_interface", "br_files"]:
        raise ValueError("data_source must be either 'data_interface' or 'br_files'")

    # Get the right function to get the data, based on the data source requested
    if data_source == "data_interface":
        multilateral_function = unfccc_multilateral_from_interface
    else:
        raise NotImplementedError("Accessing data from BR files is not yet supported")

    # Get the data
    data = multilateral_function(
        start_year=start_year,
        end_year=end_year,
        party=party,
        br=br,
        force_download=update_data,
    )

    return data


def summary_unfccc(
    start_year: int,
    end_year: int,
    party: list[str] | str | None = None,
    br: list[int] | int | None = None,
    update_data: bool = False,
    data_source: str = "data_interface",
) -> pd.DataFrame:
    """
    This function can be used to get the UNFCCC summary data. This data can
    come from the UNFCCC data interface or from the BR files (TODO).

    It returns a dataframe based on the different arguments passed to it.

    Args:
        start_year: The start year that should be covered in the data
        end_year: The end year that should be covered in the data
        party: Optionally, specify one or more parties. If not specified, all
        parties are included.
        br: Optionally, specify one or more BRs. If not specified, all previously
        downloaded BRs are included. If none have been downloaded, or if a data
        update is requested without specifying which BRs to download, BRs 3-5 are
        downloaded.
        update_data: If True, the data is updated from the source. This can potentially
        overwrite any data that has been downloaded to the 'unfccc_data_interface' folder.
        data_source: The source of the data. Currently, only 'data_interface' is supported.

    Returns:
        df (pd.DataFrame): The UNFCCC summary data.
    """

    # Check that the data source requested is valid
    if data_source not in ["data_interface", "br_files"]:
        raise ValueError("data_source must be either 'data_interface' or 'br_files'")

    # Get the right function to get the data, based on the data source requested
    if data_source == "data_interface":
        summary_function = unfccc_summary_from_interface
    else:
        raise NotImplementedError("Accessing data from BR files is not yet supported")

    # Get the data
    data = summary_function(
        start_year=start_year,
        end_year=end_year,
        party=party,
        br=br,
        force_download=update_data,
    )

    return data

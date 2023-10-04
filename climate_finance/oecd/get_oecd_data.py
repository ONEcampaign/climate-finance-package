import pandas as pd

from climate_finance.oecd.climate_analysis.tools import (
    base_oecd_transform_markers_into_indicators,
    check_and_filter_parties,
    base_oecd_multilateral_agency_total,
    base_oecd_multilateral_agency_share,
)
from climate_finance.oecd.cleaning_tools.schema import CRS_MAPPING
from climate_finance.oecd.crs.get_data import get_crs_allocable_spending
from climate_finance.oecd.imputations.get_data import (
    get_oecd_multilateral_climate_imputations,
)

BILATERAL_CLIMATE_METHODOLOGY: dict[str, callable] = {
    "oecd_bilateral": base_oecd_transform_markers_into_indicators
}

MULTILATERAL_CLIMATE_METHODOLOGY_DONOR: dict[str, callable] = {
    "oecd_multilateral_agency_total": base_oecd_multilateral_agency_total,
    "oecd_multilateral_agency_share": base_oecd_multilateral_agency_share,
}


def get_oecd_bilateral(
    start_year: int,
    end_year: int,
    party: list[str] | str | None = None,
    update_data: bool = False,
    methodology: str = "oecd_bilateral",
) -> pd.DataFrame:
    """
    Get bilateral climate finance data from the OECD CRS data. The user can specify
    which methodology to use to transform the markers into indicators.

    In this context, bilateral means any donor/party that appears on the CRS data. This
    means 'bilateral' as in the control of the donor/party, not a core contribution to
    a multilateral organisation (for example).

    The data in this context is 'bilateral allocable'. Refer to the OECD methodology
    for more information on what this means.

    Args:
        start_year: The start year that should be covered in the data
        end_year: The end year that should be covered in the data
        party: Optionally, specify one or more parties. If not specified, all
        parties are included.
        update_data: If True, the data is updated from the source. This can potentially
        overwrite any data that has been downloaded to the 'raw_data' folder.
        methodology: The methodology to use to transform the markers into indicators.
        The default is 'oecd_bilateral', which is the methodology used by the OECD.

    Returns:
        df (pd.DataFrame): The OECD bilateral climate finance data.

    """
    # Check that the methodology requested is valid
    if methodology not in BILATERAL_CLIMATE_METHODOLOGY:
        raise ValueError(
            f"Methodology must be one of {list(BILATERAL_CLIMATE_METHODOLOGY)}"
        )

    # Get the CRS data
    data = get_crs_allocable_spending(
        start_year=start_year, end_year=end_year, force_update=update_data
    )

    # Filter the data to only include the requested parties
    data = check_and_filter_parties(data, party)

    # Transform the markers into indicators
    data = BILATERAL_CLIMATE_METHODOLOGY[methodology](data)

    return data


def get_oecd_multilateral(
    start_year: int,
    end_year: int,
    oecd_channel_name: list[str] | str | None = None,
    update_data: bool = False,
    methodology: str = "oecd_multilateral_agency_total",
) -> pd.DataFrame:
    """
    Get multilateral climate finance data from the OECD multilateral files published by
    the OECD. The user can specify which methodology to use to transform the data into
    indicators.

    In this context, multilateral means any donor/party that _does not_ appear on the
    CRS data, and/or that appears on the OECD's dataset on multilateral climate finance.

    This function provides convenient access to the data in the OECD's multilateral
    climate finance dataset. Refer to the OECD methodology for more information on what
    it covers.

    Args:
        start_year: The start year that should be covered in the data
        end_year: The end year that should be covered in the data
        party: Optionally, specify one or more parties. If not specified, all
        parties are included. Only multilateral parties are valid in this context.
        update_data: If True, the data is updated from the source. This can potentially
        overwrite any data that has been downloaded to the 'raw_data' folder.
        methodology: The methodology to use to transform the data into indicators.

    Returns:
        df (pd.DataFrame): The OECD multilateral climate finance data.

    """

    # Check that the methodology requested is valid
    if methodology not in MULTILATERAL_CLIMATE_METHODOLOGY_DONOR:
        raise ValueError(
            f"Methodology must be one of {list(MULTILATERAL_CLIMATE_METHODOLOGY_DONOR)}"
        )

    # Get the Imputations data
    data = get_oecd_multilateral_climate_imputations(
        start_year=start_year, end_year=end_year, force_update=update_data
    ).rename(columns=CRS_MAPPING)

    # Filter the data to only include the requested parties
    data = check_and_filter_parties(
        data, oecd_channel_name, party_col="oecd_channel_name"
    )

    # Transform the markers into indicators
    data = MULTILATERAL_CLIMATE_METHODOLOGY_DONOR[methodology](data)

    return data

import pandas as pd

from climate_finance.config import logger
from climate_finance.oecd.climate_analysis.tools import (
    base_oecd_transform_markers_into_indicators,
)
from climate_finance.oecd.crs.get_data import get_crs_allocable_spending

BILATERAL_CLIMATE_METHODOLOGY: dict[str, callable] = {
    "oecd_bilateral": base_oecd_transform_markers_into_indicators
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

    # Validate the party argument
    if isinstance(party, str):
        party = [party]

    # Get the CRS data
    data = get_crs_allocable_spending(
        start_year=start_year, end_year=end_year, force_update=update_data
    )

    if party is not None:
        # Check that the requested parties are in the CRS data
        missing_party = set(party) - set(data.oecd_donor_name.unique())
        # Log a warning if any of the requested parties are not in the CRS data
        if len(missing_party) > 0:
            logger.warning(
                f"The following parties are not found in CRS data:\n{missing_party}"
            )
        # Filter the data to only include the requested parties
        data = data.loc[lambda d: d.oecd_donor_name.isin(party)]

    # Transform the markers into indicators
    data = BILATERAL_CLIMATE_METHODOLOGY[methodology](data)

    return data

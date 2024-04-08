import pandas as pd

from climate_finance.common.analysis_tools import (
    keep_commitments_and_disbursements_only,
)
from climate_finance.common.schema import (
    CLIMATE_VALUES,
    ClimateSchema,
    CLIMATE_VALUES_TO_NAMES,
)
from climate_finance.methodologies.bilateral.tools import (
    rio_markers_multi_codes,
    remove_private_and_not_climate_relevant,
)
from climate_finance.methodologies.multilateral.one_multilateral.climate_components import (
    one_multilateral_spending,
)

from climate_finance.oecd.get_oecd_data import get_oecd_bilateral


def high_confidence_multilateral_crdf_providers() -> list:
    return [
        "990",
        "909",
        "915",
        "976",
        "901",
        "905",
        "1024",
        "1015",
        "1011",
        "1016",
        "1313",
        "988",
        "981",
        "910",
        "906",
    ]


def prep_crdf_data_totals(crdf_data: pd.DataFrame) -> pd.DataFrame:
    """Prepares the CRDF data for imputation. It basically reshapes it
    to match the format of the CRS data"""

    # A list of all variables that aren't values
    id_vars = [
        c
        for c in crdf_data
        if c not in CLIMATE_VALUES + [ClimateSchema.CLIMATE_UNSPECIFIED]
    ]

    # Melt the data to generate an indicator column
    crdf_data = crdf_data.melt(
        id_vars=id_vars, var_name=ClimateSchema.INDICATOR
    ).assign(
        **{
            ClimateSchema.INDICATOR: lambda d: d[ClimateSchema.INDICATOR]
            .map(CLIMATE_VALUES_TO_NAMES)
            .fillna(d[ClimateSchema.INDICATOR])
        }
    )

    return crdf_data


def get_mutlilateral_climate_spending_for_imputations(
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    """Get spending totals for each multilateral provider for the given years.

    Args:
        start_year (int): The start year of the data.
        end_year (int): The end year of the data.

    Returns:
        pd.DataFrame: A dataframe with the spending totals for each multilateral provider.
        Only commitments and disbursements are included.

    """

    # First need to define the different multilateral providers since
    # the data comes from different places

    # Define Rio multilaterals
    multi_rio = rio_markers_multi_codes()

    # Define CRDF multilaterals
    valid_crdf_multi = high_confidence_multilateral_crdf_providers()

    # Get CRS data for rio.
    rio_data = (
        get_oecd_bilateral(
            start_year=start_year,
            end_year=end_year,
            provider_code=multi_rio,
            methodology="one_bilateral",
        )
        .pipe(remove_private_and_not_climate_relevant)
        .pipe(keep_commitments_and_disbursements_only)
    )

    # Get CRDF data for CRDF multilaterals
    crdf_data = one_multilateral_spending(
        start_year=start_year,
        end_year=end_year,
        provider_code=valid_crdf_multi,
    ).pipe(prep_crdf_data_totals)

    # Combine the data
    data = (
        pd.concat([rio_data, crdf_data], ignore_index=True)
        .filter([c for c in rio_data if c in crdf_data])
        .loc[lambda d: d[ClimateSchema.INDICATOR] != ClimateSchema.CLIMATE_UNSPECIFIED]
    )

    return crdf_data


if __name__ == "__main__":
    df = get_mutlilateral_climate_spending_for_imputations(
        start_year=2020,
        end_year=2021,
    )

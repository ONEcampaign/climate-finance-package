import pandas as pd
from oda_data import donor_groupings, set_data_path

from climate_finance.common.schema import ClimateSchema
from climate_finance.config import ClimateDataPath

set_data_path(ClimateDataPath.raw_data)

RIO_MULTI = [
    "Adaptation Fund",
    "Council of Europe Development Bank",
    "Food and Agriculture Organisation",
    "Global Environment Facility",
    "Nordic Development Fund",
    "EU Institutions",
]


def rio_markers_multi_codes() -> list[str]:
    """Return a list of multilateral organisation codes that use the Rio markers"""
    rio_multi = [
        str(k) for k, v in donor_groupings()["multilateral"].items() if v in RIO_MULTI
    ]

    if len(rio_multi) != len(RIO_MULTI):
        raise ValueError("Not all multilaterals were matched to a code")

    return rio_multi


def rio_markers_bilat_codes() -> list[str]:
    """Return a list of bilateral organisation codes that use the Rio markers"""
    return [str(p) for p in list(donor_groupings()["all_bilateral"])]


def rio_markers_all_codes() -> list[str]:
    """Return a list of all organisation codes that use the Rio markers"""
    return list(set(rio_markers_bilat_codes() + rio_markers_multi_codes()))


def remove_private_development_finance(data: pd.DataFrame) -> pd.DataFrame:
    """Remove private development finance from the CRS data"""
    return data.loc[
        lambda d: (d[ClimateSchema.FLOW_NAME] != "Private Development Finance")
    ]


def remove_climate_not_relevant(data: pd.DataFrame) -> pd.DataFrame:
    """Remove climate not relevant from the CRS data"""
    return data.loc[lambda d: (d[ClimateSchema.INDICATOR] != "Not climate relevant")]


def remove_private_and_not_climate_relevant(data: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the raw bilateral data to retain only the rows that are relevant for the
    climate finance analysis.

    Args:
        data: The raw bilateral data

    Returns:
        A cleaned dataframe.
    """
    return data.pipe(remove_private_development_finance).pipe(
        remove_climate_not_relevant
    )


if __name__ == "__main__":
    rmm = rio_markers_multi_codes()
    rmb = rio_markers_bilat_codes()
    rma = rio_markers_all_codes()

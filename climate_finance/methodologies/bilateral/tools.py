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


def non_rio_markers() -> list[str]:
    return [
        str(c)
        for c in donor_groupings()["all_official"]
        if str(c) not in rio_markers_all_codes()
    ]


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


def crdf_rio_providers() -> list[str]:
    return [
        "801",
        "1",
        "2",
        "301",
        "3",
        "18",
        "4",
        "5",
        "21",
        "7",
        "8",
        "9",
        "50",
        "10",
        "11",
        "12",
        "701",
        "40",
        "820",
        "918",
        "302",
        "6",
        "742",
        "576",
        "1012",
        "22",
        "104",
        "61",
        "68",
        "1011",
        "20",
        "811",
        "988",
        "76",
        "69",
        "77",
        "1016",
        "84",
        "1614",
        "1602",
        "1616",
        "1313",
        "1608",
        "1610",
        "1643",
        "1604",
        "1603",
        "1635",
        "1640",
        "1615",
        "1632",
        "1617",
        "1618",
        "1627",
        "1626",
        "1642",
        "1619",
        "83",
        "1611",
        "1628",
        "1624",
        "1601",
        "1607",
        "1638",
        "1606",
        "1631",
        "1634",
        "1623",
        "1609",
        "906",
        "1013",
        "932",
        "75",
        "1629",
        "1637",
        "611",
        "1646",
        "1620",
        "82",
        "1612",
        "613",
        "1613",
        "1644",
        "1647",
        "70",
    ]


def crdf_rio_providers_official() -> list[str]:
    return [
        "801",
        "1",
        "2",
        "301",
        "3",
        "18",
        "4",
        "5",
        "21",
        "7",
        "8",
        "9",
        "50",
        "10",
        "11",
        "12",
        "701",
        "40",
        "820",
        "918",
        "302",
        "6",
        "742",
        "576",
        "1012",
        "22",
        "104",
        "61",
        "68",
        "1011",
        "20",
        "811",
        "988",
        "76",
        "69",
        "77",
        "1016",
        "84",
        "1313",
        "83",
        "906",
        "1013",
        "932",
        "75",
        "611",
        "82",
        "613",
        "70",
    ]


if __name__ == "__main__":
    rmm = rio_markers_multi_codes()
    rmb = rio_markers_bilat_codes()
    rma = rio_markers_all_codes()
    nrm = non_rio_markers()

    crdfr = crdf_rio_providers()
    crdfr_official = crdf_rio_providers_official()

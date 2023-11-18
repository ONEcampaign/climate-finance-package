from unittest.mock import patch

import pandas as pd
from pandas._testing import assert_frame_equal

from climate_finance.oecd.cleaning_tools.tools import (
    rename_crs_columns,
    set_crs_data_types, keep_only_allocable_aid,
)
from climate_finance.oecd.crs.get_data import (
    _add_net_disbursement,
    _get_flow_columns,
    _get_relevant_crs_columns,
    _replace_missing_climate_with_zero,
    get_crs_allocable_spending,
)


def test_keep_only_allocable_aid():
    # Create a DataFrame with different aid types
    df = pd.DataFrame(
        {
            "modality": [
                "A02",
                "B01",
                "B03",
                "B04",
                "C01",
                "D01",
                "D02",
                "E01",
                "F01",
                "G01",
            ],
            "value": list(range(10)),
        }
    )

    # Call the function
    result = keep_only_allocable_aid(df)

    # Check if the resulting DataFrame only contains the rows with allocable aid types
    assert len(result) == 8
    assert all(
        result["modality"].isin(
            ["A02", "B01", "B03", "B04", "C01", "D01", "D02", "E01"]
        )
    )


def test_get_relevant_crs_columns():
    assert isinstance(_get_relevant_crs_columns(), list)


def test_rename_crs_columns():
    # Create a DataFrame with the original column names
    df = pd.DataFrame(
        {
            "donor_code": [1, 2, 3],
            "donor_name": ["donor1", "donor2", "donor3"],
            "recipient_code": [4, 5, 6],
            "recipient_name": ["recipient1", "recipient2", "recipient3"],
            "agency_name": ["agency1", "agency2", "agency3"],
        }
    )

    # Call the function
    result = rename_crs_columns(df)

    # Check if the resulting DataFrame has the renamed columns
    expected_columns = [
        "oecd_party_code",
        "party",
        "oecd_recipient_code",
        "recipient",
        "agency",
    ]
    assert list(result.columns) == expected_columns


def test_get_flow_columns():
    assert isinstance(_get_flow_columns(), list)


def test_set_crs_data_types():
    # Create a DataFrame with incorrect data types
    df = pd.DataFrame(
        {
            "year": ["2019", "2020", "2021"],
            "oecd_party_code": ["1", "2", "3"],
            "party": [4, 5, 6],
            "recipient": [7, 8, 9],
            "oecd_recipient_code": ["10", "11", "12"],
            "agency": [13, 14, 15],
            "flow_name": [16, 17, 18],
            "flow_code": ["19", "20", "21"],
            "climate_mitigation": [22, 23, 24],
            "climate_adaptation": [25, 26, 27],
        }
    )

    # Call the function
    result = set_crs_data_types(df)

    # Check if the resulting DataFrame has the correct data types
    expected_dtypes = {
        "year": "Int32",
        "oecd_donor_code": "Int32",
        "party": "object",
        "recipient": "object",
        "oecd_recipient_code": "Int32",
        "agency_name": "object",
        "flow_name": "object",
        "flow_code": "Int32",
        "climate_mitigation": "object",
        "climate_adaptation": "object",
    }
    assert result.year.dtype == expected_dtypes["year"]


def test_replace_missing_climate_with_zero():
    # Create a DataFrame with missing values
    df = pd.DataFrame(
        {
            "climate_mitigation": ["1", "nan", "nan"],
            "climate_adaptation": ["1", "nan", "nan"],
        }
    )

    # Call the function
    result = _replace_missing_climate_with_zero(df, "climate_mitigation")

    # Check if the resulting DataFrame has no missing values in the specified column
    assert result["climate_mitigation"].isnull().sum() == 0
    assert (result["climate_mitigation"] == "0").any()


def test_add_net_disbursement():
    # Create a DataFrame with disbursement and received columns
    df = pd.DataFrame(
        {
            "usd_disbursement": [100, 200, 300],
            "usd_received": [50, 100, 150],
        }
    )

    # Call the function
    result = _add_net_disbursement(df)

    # Check if the resulting DataFrame has a correct net disbursement column
    expected = pd.DataFrame(
        {
            "usd_disbursement": [100, 200, 300],
            "usd_received": [50, 100, 150],
            "usd_net_disbursement": [50, 100, 150],
        }
    )
    pd.testing.assert_frame_equal(result, expected)

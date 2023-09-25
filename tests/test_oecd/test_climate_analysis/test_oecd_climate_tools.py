import pandas as pd

from climate_finance.oecd.climate_analysis.tools import (
    _melt_crs_climate_indicators,
    _get_cross_cutting_data,
    _get_not_climate_relevant_data,
    _combine_clean_sort,
    check_and_filter_parties,
    base_oecd_transform_markers_into_indicators,
)


def test_melt_crs_climate_indicators():
    df = pd.DataFrame(
        {
            "climate_adaptation": [0, 1, 2],
            "climate_mitigation": [2, 0, 1],
            "other_column": ["a", "b", "c"],
        }
    )
    climate_indicators = ["climate_adaptation", "climate_mitigation"]
    result = _melt_crs_climate_indicators(df, climate_indicators).reset_index(drop=True)
    expected = pd.DataFrame(
        {
            "other_column": ["b", "c", "a", "c"],
            "indicator": [
                "climate_adaptation",
                "climate_adaptation",
                "climate_mitigation",
                "climate_mitigation",
            ],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


def test_get_cross_cutting_data():
    df = pd.DataFrame(
        {
            "climate_adaptation": [0, 1, 2],
            "climate_mitigation": [2, 0, 1],
            "other_column": ["a", "b", "c"],
        }
    )
    result = _get_cross_cutting_data(df).reset_index(drop=True)
    expected = pd.DataFrame(
        {"other_column": ["c"], "indicator": ["climate_cross_cutting"]}
    )
    pd.testing.assert_frame_equal(result, expected)


def test_get_not_climate_relevant_data():
    df = pd.DataFrame(
        {
            "climate_adaptation": [0, 0, 1],
            "climate_mitigation": [2, 0, 0],
            "other_column": ["a", "b", "c"],
        }
    )
    result = _get_not_climate_relevant_data(df).reset_index(drop=True)
    expected = pd.DataFrame(
        {"other_column": ["b"], "indicator": ["not_climate_relevant"]}
    )
    pd.testing.assert_frame_equal(result, expected)


def test_combine_clean_sort():
    # Create first dataframe
    df1 = pd.DataFrame(
        {
            "other_column": ["a", "b"],
            "indicator": ["climate_adaptation", "climate_mitigation"],
        }
    )

    # Create second dataframe
    df2 = pd.DataFrame({"other_column": ["c"], "indicator": ["climate_cross_cutting"]})

    # Create list of dataframes
    dfs = [df1, df2]

    # Create list of columns to sort by
    sort_cols = ["other_column"]

    # Run function
    result = _combine_clean_sort(dfs, sort_cols).reset_index(drop=True)

    # Create expected dataframe
    expected = pd.DataFrame(
        {
            "other_column": ["a", "b", "c"],
            "indicator": ["Adaptation", "Mitigation", "Cross-cutting"],
        }
    )

    # Check that the result is as expected
    pd.testing.assert_frame_equal(result, expected)


def test_check_and_filter_parties():
    df = pd.DataFrame(
        {
            "oecd_donor_name": ["party1", "party2", "party3"],
            "other_column": ["a", "b", "c"],
        }
    )

    # Test when party is a string
    party = "party1"
    result = check_and_filter_parties(df, party).reset_index(drop=True)
    expected = pd.DataFrame({"oecd_donor_name": ["party1"], "other_column": ["a"]})
    pd.testing.assert_frame_equal(result, expected)

    # Test when party is a list
    party = ["party1", "party3"]
    result = check_and_filter_parties(df, party).reset_index(drop=True)
    expected = pd.DataFrame(
        {"oecd_donor_name": ["party1", "party3"], "other_column": ["a", "c"]}
    )
    pd.testing.assert_frame_equal(result, expected)

    # Test when party is None
    party = None
    result = check_and_filter_parties(df, party).reset_index(drop=True)
    pd.testing.assert_frame_equal(result, df)

    # Test when party is not found in the dataframe
    party = ["party4"]
    result = check_and_filter_parties(df, party).reset_index(drop=True)
    expected = pd.DataFrame({"oecd_donor_name": [], "other_column": []}).astype(
        {"oecd_donor_name": "str", "other_column": "str"}
    )
    pd.testing.assert_frame_equal(result, expected)


def test_base_oecd_transform_markers_into_indicators():
    df = pd.DataFrame(
        {
            "climate_adaptation": [0, 0, 2],
            "climate_mitigation": [2, 0, 1],
            "other_column": ["a", "b", "c"],
        }
    )
    result = base_oecd_transform_markers_into_indicators(df).reset_index(drop=True)
    expected = pd.DataFrame(
        {
            "other_column": ["a", "b", "c", "c", "c"],
            "indicator": [
                "Mitigation",
                "Not climate relevant",
                "Mitigation",
                "Adaptation",
                "Cross-cutting",
            ],
        }
    )
    pd.testing.assert_frame_equal(result, expected)

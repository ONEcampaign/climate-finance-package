from unittest.mock import patch

import pandas as pd
import pytest

from climate_finance import config
from climate_finance.unfccc.cleaning_tools.channels import (
    _apply_fuzzy_match,
    _clean_names_for_regex,
    _direct_match_name,
    _fuzzy_match_name,
    _generate_regex_full_lookahead,
    _generate_regex_full_word,
    _generate_regex_partial_lookahead,
    _regex_map_names_to_codes,
    _regex_match_channel_name_to_code,
    add_channel_names,
    channel_to_code,
    clean_string,
    generate_channel_mapping_dictionary,
    get_crs_official_mapping,
    match_names_direct_and_fuzzy,
    match_names_regex,
    raw_data_to_unique_channels,
    regex_to_code_dictionary,
)

MockChannels = df = pd.DataFrame(
    {
        "clean_channel": [
            "UNFCCC",
            "World Bank IBRD",
            "IBRD",
            "IDA channel name",
            "African Development Fund (ADF)",
            "African Development bank",
        ],
        "channel_code": [pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA],
    }
)


def test_raw_data_to_unique_channels():
    # Define mock data
    raw_data = pd.DataFrame({"channel": ["name1", "name2", "name2"]})
    channel_names_column = "channel"

    # Call the function and check results
    result = raw_data_to_unique_channels(raw_data, channel_names_column)
    expected_result = pd.DataFrame(
        {
            "channel": [
                "name1",
                "name2",
            ],
            "clean_channel": ["name1", "name2"],
        }
    )
    pd.testing.assert_frame_equal(result, expected_result)


@patch(
    "climate_finance.unfccc.cleaning_tools.channels.get_crs_official_mapping",
    return_value=pd.DataFrame(
        {"channel_name": ["name1", "name2", "name3"], "channel_code": [1, 2, 3]}
    ),
)
def test_channel_to_code(mock_get_crs_official_mapping):
    # Define mock data
    map_to = "channel_name"

    # Call the function and check results
    result = channel_to_code(map_to)
    expected_result = {"name1": 1, "name2": 2, "name3": 3}
    assert result == expected_result

    # Check if the mocked functions were called
    mock_get_crs_official_mapping.assert_called()

    # Test with invalid map_to
    with pytest.raises(ValueError):
        channel_to_code("invalid")


@patch("pandas.read_csv")
def test_get_crs_official_mapping(mock_read_csv):
    # Assume
    mock_df = pd.DataFrame()
    mock_read_csv.return_value = mock_df
    expected_path = (
        config.ClimateDataPath.oecd_cleaning_tools / "crs_channel_mapping.csv"
    )

    # Act
    result = get_crs_official_mapping()

    # Assert
    mock_read_csv.assert_called_once_with(expected_path)
    assert isinstance(result, pd.DataFrame)
    pd.testing.assert_frame_equal(result, mock_df)


def test_clean_string():
    assert clean_string("HELLO") == "hello"
    assert clean_string("   HELLO   ") == "hello"
    assert clean_string("HELLO WORLD!") == "hello world"
    assert clean_string("Hello, World!") == "hello world"
    assert clean_string("Hello,    World!") == "hello world"
    assert clean_string("") == ""


def test_direct_match_name():
    # Define mock data
    df = pd.DataFrame({"clean_channel": ["name1", "name2", "name3"]})
    channels_dict = {"name1": 1, "name2": 2, "name3": 3}

    # Call the function
    result = _direct_match_name(df, channels_dict)

    # Define expected result
    expected_result = pd.DataFrame(
        {"clean_channel": ["name1", "name2", "name3"], "channel_code": [1, 2, 3]}
    ).astype({"channel_code": "Int32"})

    # Check if the result matches the expected result
    pd.testing.assert_frame_equal(result, expected_result)


def test_fuzzy_match_name():
    # Define mock data
    channels_dict = {"name 1": 1, "name_2": 2, "Name3": 3}

    # Call the function and check results
    assert _fuzzy_match_name("name1", channels_dict) == 1
    assert _fuzzy_match_name("name2", channels_dict) == 2
    assert _fuzzy_match_name("name3", channels_dict) == 3
    assert _fuzzy_match_name("name4", channels_dict) is pd.NA
    assert _fuzzy_match_name("nam", channels_dict) is pd.NA


def test_apply_fuzzy_match():
    channel_names = channel_to_code(map_to="channel_name")
    en_acronyms = channel_to_code(map_to="en_acronym")

    mapping_dictionaries = [(channel_names, 90), (en_acronyms, 95)]

    # Call the function
    result = _apply_fuzzy_match(MockChannels, mapping_dictionaries)

    # Define expected result
    expected_result = pd.DataFrame(
        {
            "clean_channel": [
                "UNFCCC",
                "World Bank IBRD",
                "IBRD",
                "IDA channel name",
                "African Development Fund (ADF)",
                "African Development bank",
            ],
            "channel_code": [41316, pd.NA, 44001, pd.NA, 46003, 46002],
        }
    )

    # Check if the result matches the expected result
    pd.testing.assert_frame_equal(result, expected_result)


@patch(
    "climate_finance.unfccc.cleaning_tools.channels._direct_match_name",
    return_value=pd.DataFrame(
        {"clean_channel": ["name1", "name2", "name3"], "channel_code": [1, 2, 3]}
    ),
)
@patch(
    "climate_finance.unfccc.cleaning_tools.channels._apply_fuzzy_match",
    return_value=pd.DataFrame(
        {"clean_channel": ["name1", "name2", "name3"], "channel_code": [1, 2, 3]}
    ),
)
def test_match_names_direct_and_fuzzy(mock_direct_match, mock_fuzzy_match):
    # Call the function
    result = match_names_direct_and_fuzzy(MockChannels)

    # Check if the mocked functions were called
    mock_direct_match.assert_called()
    mock_fuzzy_match.assert_called()

    # Define expected result
    expected_result = pd.DataFrame(
        {
            "clean_channel": ["name1", "name2", "name3"],
            "channel_code": [1, 2, 3],
            "mapped_name": [pd.NA, pd.NA, pd.NA],
        }
    )

    # Check if the result matches the expected result
    pd.testing.assert_frame_equal(result, expected_result)


def test_clean_names_for_regex():
    assert _clean_names_for_regex("HELLO") == "hello"
    assert _clean_names_for_regex("   HELLO   ") == "hello"
    assert _clean_names_for_regex("HELLO and WORLD!") == "hello world"
    assert _clean_names_for_regex("Hello, the World!") == "hello world"
    assert _clean_names_for_regex("Hello,   on World!") == "hello world"
    assert _clean_names_for_regex("") == ""
    assert _clean_names_for_regex(pd.NA) == ""
    assert _clean_names_for_regex("Hello, for Of on World! ") == "hello world"


def test_generate_regex_partial_lookahead():
    assert (
        _generate_regex_partial_lookahead(
            ["long", "agency", "name", "odd", "number", "words", "count"]
        )
        == "(?=.*long)(?=.*agency)(?=.*name)(?=.*odd).*"
    )

    assert (
        _generate_regex_partial_lookahead(["hello", "world", "even", "test"])
        == "(?=.*hello)(?=.*world)(?=.*even).*"
    )

    assert (
        _generate_regex_partial_lookahead(["hello", "world"])
        == "(?=.*hello)(?=.*world).*"
    )
    assert _generate_regex_partial_lookahead(["hello"]) == "(?=.*hello).*"
    assert _generate_regex_partial_lookahead([]) == ".*"


def test_generate_regex_full_lookahead():
    assert (
        _generate_regex_full_lookahead(["hello", "world", "even", "test"])
        == "(?=.*hello)(?=.*world)(?=.*even)(?=.*test).*"
    )
    assert _generate_regex_full_lookahead(["hello"]) == "(?=.*hello).*"
    assert _generate_regex_full_lookahead([]) == ".*"


def test_generate_regex_full_word():
    assert _generate_regex_full_word(["hello", "world"]) == r"\bhello\b"
    assert _generate_regex_full_word(["hello"]) == r"\bhello\b"
    assert _generate_regex_full_word([]) == "no_match__"


def test_regex_map_names_to_codes():
    # Define mock data
    d_ = pd.DataFrame(
        {
            "channel_name": ["agency name very long many words", "short", ""],
            "channel_code": [1, 2, 3],
        }
    )
    names_column = "channel_name"

    # Call the function and check results
    result = _regex_map_names_to_codes(d_, names_column, regex_type="full_look_ahead")
    expected_result = {
        "(?=.*agency)(?=.*name)(?=.*very)(?=.*long)(?=.*many)(?=.*words).*": 1,
        "(?=.*short).*": 2,
        ".*": 3,
    }
    assert result == expected_result

    result = _regex_map_names_to_codes(
        d_, names_column, regex_type="partial_look_ahead"
    )
    expected_result = {
        "(?=.*agency)(?=.*name)(?=.*very)(?=.*long).*": 1,
        "(?=.*short).*": 2,
        ".*": 3,
    }
    assert result == expected_result

    result = _regex_map_names_to_codes(d_, names_column, regex_type="full_word")
    expected_result = {r"\bagency\b": 1, r"\bshort\b": 2, r"no_match__": 3}
    assert result == expected_result

    # Test with empty DataFrame
    d_ = pd.DataFrame({"channel_name": [], "channel_code": []})
    result = _regex_map_names_to_codes(d_, names_column, regex_type="full_word")
    assert result == {}

    # Test with invalid regex_type
    with pytest.raises(ValueError):
        _regex_map_names_to_codes(d_, names_column, regex_type="invalid")


@patch(
    "climate_finance.unfccc.cleaning_tools.channels._regex_map_names_to_codes",
    return_value={"regex": 1},
)
def test_regex_to_code_dictionary(mock_regex_map):
    # Define mock data
    channels = pd.DataFrame(
        {
            "channel_name": [
                "agency channel name 1",
                "Second agency",
                "A third agency",
            ],
            "en_acronym": ["acronym1", "acronym2", "acronym3"],
        }
    )
    names_column = "channel_name"

    # Call the function and check results
    result = regex_to_code_dictionary(channels, names_column)

    mock_regex_map.assert_called()

    assert isinstance(result, dict)


def test_regex_match_channel_name_to_code():
    # Define mock data
    regex_dict = {r"\bword\b": 1}

    # Call the function and check results
    assert _regex_match_channel_name_to_code("this has a word here", regex_dict) == 1
    assert _regex_match_channel_name_to_code("non-match", regex_dict) is pd.NA


@patch(
    "climate_finance.unfccc.cleaning_tools.channels.channel_to_code",
    return_value={"channel_name": 1},
)
def test_add_channel_names(mock_channel_to_code):
    # Define mock data
    df = pd.DataFrame({"channel_code": [1, 2, 3]})
    codes_column = "channel_code"
    target_column = "mapped_name"

    # Call the function
    result = add_channel_names(df, codes_column, target_column)

    # Check if the mocked function was called
    mock_channel_to_code.assert_called()

    # Define expected result
    expected_result = pd.DataFrame(
        {"channel_code": [1, 2, 3], "mapped_name": ["channel_name", pd.NA, pd.NA]}
    )

    # Check if the result matches the expected result
    pd.testing.assert_frame_equal(result, expected_result)


@patch(
    "climate_finance.unfccc.cleaning_tools.channels._regex_match_channel_name_to_code",
    return_value=1,
)
@patch(
    "climate_finance.unfccc.cleaning_tools.channels.add_channel_names",
    return_value=pd.DataFrame(
        {"clean_channel": ["name1", "name2", "name3"], "channel_code": [1, 2, 3]}
    ),
)
def test_match_names_regex(mock_regex_match, mock_add_channel_names):
    # Define mock data
    df = pd.DataFrame(
        {
            "clean_channel": ["name1", "name2", "name3"],
            "channel_code": [pd.NA, pd.NA, pd.NA],
        }
    )
    regex_dict = {"regex": 1}
    column = "clean_channel"

    # Call the function
    result = match_names_regex(df, regex_dict, column)

    # Check if the mocked functions were called
    mock_regex_match.assert_called()
    mock_add_channel_names.assert_called()

    # Define expected result
    expected_result = pd.DataFrame(
        {"clean_channel": ["name1", "name2", "name3"], "channel_code": [1, 2, 3]}
    )

    # Check if the result matches the expected result
    pd.testing.assert_frame_equal(result, expected_result)


@patch(
    "climate_finance.unfccc.cleaning_tools.channels.raw_data_to_unique_channels",
    return_value=pd.DataFrame({"channel": ["name1", "name2", "name3"]}),
)
@patch(
    "climate_finance.unfccc.cleaning_tools.channels.match_names_direct_and_fuzzy",
    return_value=pd.DataFrame(
        {"channel": ["name1", "name2", "name3"], "channel_code": [1, 2, 3]}
    ),
)
@patch(
    "climate_finance.unfccc.cleaning_tools.channels.get_crs_official_mapping",
    return_value=pd.DataFrame(
        {"channel_name": ["name1", "name2", "name3"], "channel_code": [1, 2, 3]}
    ),
)
@patch(
    "climate_finance.unfccc.cleaning_tools.channels.regex_to_code_dictionary",
    return_value={"regex": 1},
)
@patch(
    "climate_finance.unfccc.cleaning_tools.channels.match_names_regex",
    return_value=pd.DataFrame(
        {
            "oecd_channel_name": ["name1", "name2", "name3"],
            "channel_code": [1, 2, 3],
        }
    ),
)
def test_generate_channel_mapping_dictionary(
    mock_raw_data_to_unique_channels,
    mock_match_names_direct_and_fuzzy,
    mock_get_crs_official_mapping,
    mock_regex_to_code_dictionary,
    mock_match_names_regex,
):
    # Define mock data
    raw_data = pd.DataFrame(
        {"oecd_channel_name": ["name1", "name2", "name3"], "channel_code": [1, 2, 3]}
    )
    channel_names_column = "oecd_channel_name"
    export_missing_path = None

    # Call the function
    result = generate_channel_mapping_dictionary(
        raw_data, channel_names_column, export_missing_path
    )

    # Check if the mocked functions were called
    mock_raw_data_to_unique_channels.assert_called()
    mock_match_names_direct_and_fuzzy.assert_called()
    mock_get_crs_official_mapping.assert_called()
    mock_regex_to_code_dictionary.assert_called()
    mock_match_names_regex.assert_called()

    # Define expected result
    expected_result = {"name1": 1, "name2": 2, "name3": 3}

    # Check if the result matches the expected result
    assert result == expected_result

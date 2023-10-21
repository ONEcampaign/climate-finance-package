import re
import string

import pandas as pd
from thefuzz import process

from climate_finance.oecd.cleaning_tools.tools import get_crs_official_mapping

ADDITIONAL_PATTERNS: dict[str, int] = {
    r"\bworld bank\b": 44000,
    r"\bcbit\b": 47044,
    r"\bleast developed countries fund\b": 47129,
    r"(?=.*\biisd\b)(?=.*\biidd\b)(?=.*\binstitut\b)": 21039,
    r"(?=.*\bmontreal protocol\b).*": 47078,
    r"(?=.*\binteramerican\b)(?=.*\biic\b).*": 46012,
    r"\bafrican union\b": 47005,
    r"\bibrdesmap\b": 44001,
    r"\bkyoto protocol\b": 41316,
    r"consultative group of international agricultural research": 47015,
    r"\b[a-zA-Z]{2,}ifc\b": 44004,
    r"\bconservation international\b": 21063,
    r"food and agriculture organisation": 41301,
    r"\bcif\b": 47134,
    r"\bseforall\b": 21508,
    r"\bunisdr\b": 41315,
    r"\bgems\b": 41116,
    r"\biaea\b": 41312,
    r"(?=.*\biaea\b)(?=.*\bmandatory\b)": 41107,
    r"(?=.*\biaea\b)(?=.*\btechnical\b)": 47078,
    r"\bworld bank agreed\b": 44000,
    r"\binteramerican investment corporation\b": 46012,
    r"\bmultilateral investment fund\b": 46012,
    r"\bmultilateral fund\b": 47078,
    r"\bccac\b": 41116,
    r"(?=.*\bhaut\b)(?=.*\bcommissariat\b)(?=.*\bréfugiés\b)": 41121,
    r"\bworld health organization\b": 41307,
    r"\bclimate investment funds\b": 47134,
    r"\bcommonwealth small states\b": 47028,
    r"\bctcn\b": 41316,
    r"\bsdg \b": 41402,
    r"\bmontreal fund\b": 47078,
    r"\bspecial climate change fund\b": 47130,
    r"\devlopment programme\b": 41114,
    r"\bfund for special operations (fso)\b": 46013,
}


def clean_string(text: str) -> str:
    """Clean the text by removing punctuation (converted to spaces),
     converting to lowercase, removing unnecessary spacing.

    Args:
        text (str): The text to be cleaned.

    Returns:
        str: The cleaned text.

    """
    # Ensure that the text is a string
    text = str(text)

    # Convert to lowercase
    text = text.lower()

    # Remove punctuation (change to spaces)
    text = text.translate(
        str.maketrans(string.punctuation, " " * len(string.punctuation))
    )

    # Replace multiple spaces with a single space
    text = re.sub("\s+", " ", text)

    # Remove leading and trailing spaces
    text = text.strip()

    return text


def raw_data_to_unique_channels(
    raw_data: pd.DataFrame, channel_names_column: str
) -> pd.DataFrame:
    """Get a dataframe of unique channel names from the raw data.

    Args:
        raw_data (pd.DataFrame): The raw data.
        channel_names_column (str): The column containing the channel names.

    Returns:
        pd.DataFrame: A dataframe of unique channel names.

    """

    return (  # Get the channel names column, clean the strings, and drop duplicates
        raw_data.filter([channel_names_column], axis=1)
        .assign(clean_channel=lambda d: d[channel_names_column].apply(clean_string))
        .drop_duplicates(subset=[channel_names_column])
    )


def channel_to_code(map_to: str = "channel_name") -> dict[str, int]:
    """Get a dictionary mapping channel names to channel codes.

    Args:
        map_to (str, optional): The column to map to. Defaults to "channel_name".
        Other options are "en_acronym" and "fr_acronym".

    Returns:
        dict: A dictionary mapping channel names (or acronyms) to channel codes.

    """
    # Check that map_to is valid
    if map_to not in ["en_acronym", "fr_acronym", "channel_name"]:
        raise ValueError(
            "map_to must be one of 'en_acronym', 'fr_acronym', 'channel_name'"
        )

    # Get the CRS mapping data, filter the desired column, and drop duplicates
    mapping_data = (
        get_crs_official_mapping()
        .assign(channel_name=lambda d: d.channel_name.apply(clean_string))
        .drop_duplicates(subset=[map_to, "channel_code"])
        .dropna(subset=[map_to])
        .sort_values(by=[map_to])
        .drop_duplicates(subset=[map_to], keep="last")
    )

    # Convert to dictionary and return
    return mapping_data.set_index(map_to)["channel_code"].to_dict()


def _direct_match_name(
    df: pd.DataFrame,
    channels_dict: dict[str, int],
    names_column: str = "clean_channel",
    target_column: str = "channel_code",
):
    """Directly match channel names to channel codes.

    Args:
        df (pd.DataFrame): The dataframe to match.
        channels_dict (dict[str, int]): A dictionary mapping channel names to channel codes.
        names_column (str, optional): The column containing the channel names. Defaults
        to "clean_channel".
        target_column (str, optional): The column to map to. Defaults to "channel_code".

    Returns:
        pd.DataFrame: The dataframe with the channel codes added.
    """

    return df.assign(
        **{target_column: lambda d: d[names_column].map(channels_dict).astype("Int32")}
    )


def _fuzzy_match_name(name: str, channels_dict: dict[str, int], tolerance: int = 90):
    """Fuzzy match a channel name to a channel code.

    Args:
        name (str): The channel name to match.
        channels_dict (dict[str, int]): A dictionary mapping channel names to channel codes.
        tolerance (int, optional): The tolerance for the fuzzy match. Defaults to 90.

    Returns:
        int: The channel code if the match is above the tolerance, otherwise NA.
    """

    # Do a fuzzy match for the name based on the dictionary keys
    match = process.extractOne(name, channels_dict.keys())

    # If the match is above the tolerance, return the channel code
    if match[1] > tolerance:
        return channels_dict[match[0]]
    # Otherwise, return NA
    else:
        return pd.NA


def _apply_fuzzy_match(
    df: pd.DataFrame,
    mapping_dictionaries: list[tuple[dict[str, int], int]],
    names_column: str = "clean_channel",
):
    """Apply a fuzzy match the names in a dataframe using one or more mapping
    dictionaries.

    Args:
        df (pd.DataFrame): The dataframe to match.
        mapping_dictionaries (list[tuple[dict[str, int], int]]): A list of tuples
        containing the mapping dictionary and the tolerance for the fuzzy match.
        names_column (str, optional): The column containing the channel names. Defaults
        to "clean_channel".

    Returns:
        pd.DataFrame: The dataframe with the channel codes added.
    """

    # Apply the fuzzy match for each dictionary and tolerance
    for dictionary, tolerance in mapping_dictionaries:
        df["fuzzy_mapping"] = df[names_column].apply(
            _fuzzy_match_name, channels_dict=dictionary, tolerance=tolerance
        )

        # Fill in the channel codes where there is a match
        df["channel_code"] = df["channel_code"].fillna(df["fuzzy_mapping"])

    # Drop the fuzzy mapping column and return dataframe
    return df.drop(columns=["fuzzy_mapping"])


def match_names_direct_and_fuzzy(channels: pd.DataFrame) -> pd.DataFrame:
    """Match channel names to channel codes using a direct match and a fuzzy match.

    Args:
        channels (pd.DataFrame): A dataframe of unique channel names.

    Returns:
        # pd.DataFrame: A dataframe of unique channel names with channel codes added.
    """
    # Get the channel mappings as names, English acronyms, and French acronyms
    channel_names = channel_to_code(map_to="channel_name")
    en_acronyms = channel_to_code(map_to="en_acronym")
    fr_acronyms = channel_to_code(map_to="fr_acronym")

    # The first part of the strategy is to attempt a direct match
    df_direct = _direct_match_name(df=channels, channels_dict=channel_names)

    # The second part of the strategy is to attempt a fuzzy match with names,
    # English acronyms, and French acronyms
    mapping_dictionaries = [(channel_names, 90), (en_acronyms, 95), (fr_acronyms, 95)]

    # Apply the fuzzy match
    df_fuzzy = _apply_fuzzy_match(
        df_direct,
        mapping_dictionaries=mapping_dictionaries,
        names_column="clean_channel",
    )

    # Map the channel names to the channel codes
    df_fuzzy["mapped_name"] = df_fuzzy["channel_code"].map(
        {v: k for k, v in channel_names.items()}
    )

    return df_fuzzy


def _clean_names_for_regex(name: str) -> str:
    """Clean the given text by removing non-alphabetic characters,
    converting to lower case, and removing unnecessary spaces.

    Args:
        name (str): The text to clean.

    Returns:
        str: The cleaned text.

    """

    # If the name is NA, return an empty string
    if pd.isna(name):
        return ""

    # keep only alphabetic characters and spaces
    name = re.sub(r"[^a-zA-Z\s]", "", name)

    # Convert to lower case and remove unnecessary spaces
    name = name.lower()
    name = re.sub(r"\s+", " ", name).strip()

    # Remove common words
    for word in ["the", "of", "for", "at", "on", "in", "and", "to", "or", "a", "e"]:
        name = name.replace(f" {word} ", " ")
    return name


def _generate_regex_partial_lookahead(words: list[str]) -> str:
    """Generate a regular expression that matches any string
    containing at least half of the given words, in any order.

    Args:
        words (list[str]): A list of words to match.

    Returns:
        str: The regular expression.
    """

    # The number of words required to match
    num_required = len(words) // 2 + 1

    # Generate the lookaheads
    lookaheads = [f"(?=.*{word})" for word in words[:num_required]]

    # Return the regular expression
    return "".join(lookaheads) + ".*"


def _generate_regex_full_lookahead(words: list[str]) -> str:
    """Generate a regular expression that matches any string
    containing the given words, in any order.

    Args:
        words (list[str]): A list of words to match.

    Returns:
        str: The regular expression.
    """
    # Generate the lookaheads
    lookaheads = [f"(?=.*{word})" for word in words]

    # Return the regular expression
    return "".join(lookaheads) + ".*"


def _generate_regex_full_word(words: list[str]) -> str:
    """Generate a regular expression that matches any string
    containing at least half of the given words, in any order.

    Args:
        words (list[str]): A list of words to match.

    Returns:
        str: The regular expression.
    """

    # If there are more than 1 word, generate a regex only for the first word
    if len(words) > 0:
        return rf"\b{words[0]}\b"
    # If there are no words, return a regex that will never match
    else:
        return "no_match__"


def _regex_map_names_to_codes(
    d_: pd.DataFrame, names_column: str, regex_type: str = "look_ahead"
) -> dict:
    """Helper function to generate a dictionary mapping channel names to channel codes.

    Args:
        d_ (pd.DataFrame): The dataframe with the channel names.
        names_column (str): The column containing the channel names.
        regex_type (str, optional): The type of regular expression to use.
        Defaults to "look_ahead".

    Raises:
        ValueError: If regex_type is not one of
        "look_ahead", "full_word", or "full_look_ahead".

    Returns:
        dict: A dictionary mapping channel names to channel codes.
    """

    # Validate the regex type
    if regex_type == "partial_look_ahead":
        regex_func = _generate_regex_partial_lookahead
    elif regex_type == "full_word":
        regex_func = _generate_regex_full_word
    elif regex_type == "full_look_ahead":
        regex_func = _generate_regex_full_lookahead
    else:
        raise ValueError("regex_type must be one of 'look_ahead', 'full_word'")

    # Clean the channel names
    d_[names_column] = d_[names_column].apply(_clean_names_for_regex)

    # Split the channel names into words
    d_["channel_words"] = d_[names_column].apply(lambda x: x.split())

    # Generate the regular expression
    d_["regex"] = d_["channel_words"].apply(regex_func)

    # Return the dictionary. It is sorted by length of the regex
    return {
        k: v
        for k, v in sorted(
            d_.set_index("regex")["channel_code"].to_dict().items(),
            key=lambda item: -len(item[0]),
        )
    }


def regex_to_code_dictionary(
    channels: pd.DataFrame, names_column: str
) -> dict[str, int]:
    """Generate a dictionary mapping channel names to channel codes

    Args:
        channels (pd.DataFrame): A dataframe containing the channel names and codes.
        names_column (str): The column containing the channel names.

    Returns:
        dict[str, int]: A dictionary mapping channel names to channel codes.

    """

    # Clean the channel names
    channels = channels.assign(
        clean_channel=lambda d: d[names_column].apply(clean_string)
    )

    # Clean the english acronyms
    channels["en_acronym"] = (
        channels["en_acronym"]
        .apply(clean_string)
        .str.replace(" ", "")
        .replace("nan", pd.NA)
    )

    # Generate the regex dictionary for english acronyms.
    english_acronyms = _regex_map_names_to_codes(
        channels, names_column="en_acronym", regex_type="full_word"
    )

    # Generate the regex dictionary for full channel names.
    full_channel_names = _regex_map_names_to_codes(
        channels, names_column="clean_channel", regex_type="full_look_ahead"
    )

    # Generate the regex dictionary for partial channel names.
    partial_channel_names = _regex_map_names_to_codes(
        channels, names_column="clean_channel", regex_type="partial_look_ahead"
    )

    # Return a single dictionary with all the regexes: english acronyms,
    # full channel names, partial channel names, and additional patterns (manual)
    return (
        english_acronyms
        | full_channel_names
        | partial_channel_names
        | ADDITIONAL_PATTERNS
    )


def _regex_match_channel_name_to_code(channel: str, regex_dict: dict):
    """Helper function to match a channel name to a channel code.

    Args:
        channel (str): The channel name.
        regex_dict (dict): The dictionary of regular expressions of channel names
        to channel codes.

    Returns:
        int: The channel code.
    """

    # Iterate over the regular expressions
    for regex, code in regex_dict.items():
        # If the channel name matches the regular expression, return the code
        if re.search(re.compile(regex), channel):
            return code
    # If no match is found, return NA
    return pd.NA


def add_channel_names(
    df: pd.DataFrame,
    codes_column: str = "channel_code",
    target_column: str = "mapped_name",
) -> pd.DataFrame:
    """Add a column with the channel names.

    Args:
        df (pd.DataFrame): The dataframe containing the channel codes.
        codes_column (str, optional): The column containing the channel codes.
        Defaults to "channel_code".
        target_column (str, optional): The column to add the channel names to.
        Defaults to "mapped_name".

    Returns:
        pd.DataFrame: The dataframe with the channel names.
    """
    # Get a dictionary with channel codes to channel names
    channel_names = channel_to_code(map_to="channel_name")

    # Map the channel codes to channel names
    df[target_column] = df[codes_column].map({v: k for k, v in channel_names.items()})

    return df


def match_names_regex(
    df: pd.DataFrame, regex_dict: dict[str, int], column: str = "clean_channel"
) -> pd.DataFrame:
    """Match channel names to channel codes using regular expressions.

    Args:
        df (pd.DataFrame): The dataframe containing the channel names.
        regex_dict (dict[str, int]): The dictionary of regular expressions of channel names
        to channel codes.
        column (str, optional): The column containing the channel names.
        Defaults to "clean_channel".

    Returns:
        pd.DataFrame: The dataframe with the channel codes.

    """

    # Create a column which maps the channel names to channel codes using regular expressions
    df["regex_mapped_channel"] = df[column].apply(
        lambda x: _regex_match_channel_name_to_code(x, regex_dict=regex_dict)
    )

    # Fill missing values in channel_code with the regex_mapped_channel
    df["channel_code"] = df["channel_code"].fillna(df["regex_mapped_channel"])

    # Map the channel codes to channel
    df = add_channel_names(
        df=df, codes_column="channel_code", target_column="mapped_name"
    )

    return df


def generate_channel_mapping_dictionary(
    raw_data: pd.DataFrame,
    channel_names_column: str,
    export_missing_path: str | None = None,
) -> dict[str, int]:
    """Generates a dictionary of channel names to channel codes, using the raw data.
    If export_missing_path is provided, it will export a csv of the missing channels
    to the specified path.

    Args:
        raw_data (pd.DataFrame): The raw data containing the channel names.
        channel_names_column (str): The column containing the channel names.
        export_missing_path (str, optional): The path to export the missing channels to.
        Defaults to None.

    Returns:
        dict[str, int]: A dictionary mapping channel names to channel codes.
    """

    # Create a dataframe with clean versions of the channel names
    df_clean_channels = raw_data_to_unique_channels(
        raw_data=raw_data, channel_names_column=channel_names_column
    )
    # Match channel names to the official channel mapping, directly and using fuzzy matching.
    df_mapped = match_names_direct_and_fuzzy(df_clean_channels)

    # Create a dictionary of official channel names to channel codes, using regex.
    # Full names, acronyms and the additional patterns are used.
    # Load the official channel mapping
    crs_mapping = get_crs_official_mapping()

    # Generate the regex dictionary
    name_regex_dict = regex_to_code_dictionary(crs_mapping, names_column="channel_name")

    # Match the channel names to the regex dictionary
    df_mapped = match_names_regex(df_mapped, name_regex_dict, column="clean_channel")

    if export_missing_path:
        df_mapped.query("channel_code.isna()").to_csv(export_missing_path, index=False)

    return (
        df_mapped.dropna(subset=["channel_code"])
        .set_index(channel_names_column)["channel_code"]
        .to_dict()
    )

import numpy as np
import pandas as pd
from oda_data import donor_groupings, set_data_path

from climate_finance.config import ClimateDataPath
from climate_finance.common.schema import CRS_MAPPING, CRS_TYPES, ClimateSchema

set_data_path(ClimateDataPath.raw_data)


def flag_oda(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a column to the DataFrame that identifies Official Development Assistance (ODA)
    and non-ODA flows.

    Args:
        df (pd.DataFrame): The DataFrame to process.

    Returns:
        pd.DataFrame: The DataFrame with an additional 'oda' column
        indicating ODA and non-ODA flows.
    """

    mapping = {
        14: "ODA",
        30: "ODA",
    }
    df["oda"] = df.flow_code.map(mapping).fillna("non-ODA")

    return df


def remove_missing_climate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows where both climate adaptation and mitigation values are missing.

    Args:
        df (pd.DataFrame): The DataFrame to process.

    Returns:
        pd.DataFrame: The DataFrame with rows removed where both adaptation and
        mitigation are missing.
    """
    return df.replace(to_replace="", value=pd.NA).dropna(
        subset=["climate_adaptation", "climate_mitigation"], how="all"
    )


def _climate_mapping(row: pd.Series) -> str:
    """
    Helper function to map the climate adaptation and mitigation codes of a row
    to a single formatted string.

    Args:
        row (pd.Series): The row of data containing climate codes.

    Returns:
        str: A string representation of the climate codes.
    """

    # Fill NaN with "0"
    row = row.fillna("0")

    # Check if values are in the expected range
    valid_values = ["0", "1", "2"]
    if (
        row.climate_mitigation not in valid_values
        or row.climate_adaptation not in valid_values
    ):
        return "Other"

    # Format and return the string
    return f"{row.climate_mitigation}M {row.climate_adaptation}A"


def map_climate_string(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map the climate adaptation and mitigation codes in the DataFrame to a
    single column of formatted strings.

    Args:
        df (pd.DataFrame): The DataFrame to process.

    Returns:
        pd.DataFrame: The DataFrame with an additional 'climate' column
        containing the formatted climate strings.
    """
    return df.assign(climate=lambda d: d.apply(_climate_mapping, axis=1))


def _oecd_climate_methodology_mapping(row: pd.Series) -> pd.Series:
    """Reduces the 8 possible climate combinations to 3 categories using the
    OECD methodology, which produces an overlap. The total is calculated by
    subtracting the overlap from the sum of adaptation and mitigation.

    Helper function to apply the OECD climate methodology on a row to categorize
    climate combinations.

    Args:
    row (pd.Series): The row of data containing climate values.

    Returns:
    pd.Series: The row with new columns representing OECD methodology categories.

    """

    mapping = {
        "adaptation": ["0M 2A", "0M 1A", "1M 2A", "2M 1A", "1M 1A", "2M 2A"],
        "mitigation": ["2M 0A", "1M 0A", "2M 1A", "1M 1A", "1M 2A", "2M 2A"],
        "overlap": ["2M 1A", "1M 2A", "2M 2A", "1M 1A"],
    }

    # Directly assign values to the new columns based on the row's climate combination
    row["oecd_adaptation"] = row.value if row.climate in mapping["adaptation"] else 0
    row["oecd_mitigation"] = row.value if row.climate in mapping["mitigation"] else 0
    row["oecd_overlap"] = row.value if row.climate in mapping["overlap"] else 0

    # Compute the total value
    row["oecd_climate_total"] = (
        row.oecd_adaptation + row.oecd_mitigation - row.oecd_overlap
    )

    return row


def apply_oecd_climate_methodology(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply the OECD climate methodology on the entire DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to process.

    Returns:
        pd.DataFrame: The DataFrame with new columns representing OECD methodology categories.
    """
    return df.apply(_oecd_climate_methodology_mapping, axis=1)


def coefficient_mapping() -> dict:
    """
    Provide a mapping of coefficients for each climate combination.

    Returns:
        dict: A dictionary mapping each climate combination to its respective coefficient.
    """
    return {
        # Full coefficients
        "2M 0A": 1,
        "0M 2A": 1,
        "2M 1A": 1,
        "1M 2A": 1,
        "2M 2A": 1,  # 40% coefficients
        "1M 1A": 0.4,
        "0M 1A": 0.4,
        "1M 0A": 0.4,  # 0% coefficients
        "0M 0A": 0,
    }


def to_all_recipients(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate bilateral spending data to all recipients.

    Args:
        df (pd.DataFrame): The DataFrame containing spending data.

    Returns:
        pd.DataFrame: The DataFrame aggregated to 'All Developing Countries, Total'.
    """
    return (
        df.groupby(
            [
                c
                for c in df.columns
                if c not in ["recipient_code", "recipient_name", "value"]
            ],
            as_index=False,
            dropna=False,
        )["value"]
        .sum()
        .assign(recipient_name="All Developing Countries, Total")
    )


def keep_dac_only(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter the DataFrame to retain only rows corresponding to DAC members.

    Args:
        df (pd.DataFrame): The DataFrame to filter.

    Returns:
        pd.DataFrame: The filtered DataFrame containing only DAC members.
    """
    # List of DAC members with Lithuania added
    dac = list(donor_groupings()["dac_members"].keys()) + [84]

    return df.query(f"donor_code in {dac}").reset_index(drop=True)


def keep_multilaterals_only(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter the DataFrame to retain only rows corresponding to multilateral donors.

    Args:
        df (pd.DataFrame): The DataFrame to filter.

    Returns:
        pd.DataFrame: The filtered DataFrame containing only multilateral donors.
    """

    # Get multilateral donors
    multilaterals = donor_groupings()["multilateral"]

    return df.query(f"donor_code in {list(multilaterals)}").reset_index(drop=True)


def add_channel_names(
    df: pd.DataFrame, codes_col: str = "oecd_channel_code", target_col: str = "channel"
) -> pd.DataFrame:
    """
    Add channel names to the DataFrame based on the channel codes.

    Args:
        df (pd.DataFrame): The DataFrame to process.
        codes_col (str, optional): The column name containing channel codes.
        Defaults to 'oecd_channel_code'.
        target_col (str, optional): The target column name to store channel names.
        Defaults to 'channel'.

    Returns:
        pd.DataFrame: The DataFrame with an additional column containing channel names.
    """
    from oda_data import set_data_path, read_multisystem

    set_data_path(ClimateDataPath.raw_data)

    code2name = (
        read_multisystem(years=[2015, 2018, 2021])
        .filter(["channel_name_e", "channel_code"], axis=1)
        .drop_duplicates()
        .set_index("channel_code")["channel_name_e"]
        .to_dict()
    )

    return df.assign(**{target_col: lambda d: d[codes_col].map(code2name)})


def add_dac_donor_names(
    df: pd.DataFrame, codes_col: str = "donor_code", target_col: str = "donor_name"
) -> pd.DataFrame:
    """
    Add donor names to the DataFrame based on the donor codes.

    Args:
        df (pd.DataFrame): The DataFrame to process.
        codes_col (str, optional): The column name containing donor codes.
        Defaults to 'donor_code'.
        target_col (str, optional): The target column name to store donor names.
        Defaults to 'donor_name'.

    Returns:
        pd.DataFrame: The DataFrame with an additional column containing donor names.
    """
    return df.assign(
        **{target_col: lambda d: d[codes_col].map(donor_groupings()["all_official"])}
    )


def get_crs_official_mapping() -> pd.DataFrame:
    """Get the CRS official mapping file."""
    return pd.read_csv(ClimateDataPath.oecd_cleaning_tools / "crs_channel_mapping.csv")


def convert_flows_millions_to_units(df: pd.DataFrame, flow_columns) -> pd.DataFrame:
    """
    Converts flow values from millions to units.
    Args:
        df: A dataframe containing the data and the columns on the flow_columns list.
        flow_columns: A list of column names containing flow values in millions.

    Returns:
        A dataframe with flow values converted from millions to units.

    """
    # Convert flow values from millions to units
    for column in flow_columns:
        df[column] = df[column] * 1e6

    return df


def rename_crs_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renames certain columns in the CRS dataframe.

    Args:
        df: A dataframe containing the CRS data.

    Returns:
        A dataframe with renamed columns.
    """

    return df.rename(columns=CRS_MAPPING)


def set_crs_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sets the data types for columns in the CRS dataframe.

    Args:
        df (pd.DataFrame): The input dataframe with CRS data.

    Returns:
        pd.DataFrame: The dataframe with specified column data types set."""

    # check columns

    types = {k: v for k, v in CRS_TYPES.items() if k in df.columns}

    replacements = {
        "<NA>": np.nan,
        "nan": np.nan,
        "Data only reported in the CRDF as commitments": np.nan,
        "Unspecified": np.nan,
    }

    return df.replace(to_replace=replacements).astype(types)


def idx_to_str(df: pd.DataFrame, idx: list[str]) -> pd.DataFrame:
    """
    Converts the index of a dataframe to a column of strings.

    Args:
        df (pd.DataFrame): The input dataframe.
        idx (list[str]): The list of index names to convert to strings.

    Returns:
        pd.DataFrame: The dataframe with index converted to a column of strings.
    """

    return df.astype({c: "str" for c in idx if c in df.columns})


def keep_only_allocable_aid(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters the dataframe to retain only specific aid types considered allocable.

    Args:
        df (pd.DataFrame): The input dataframe with aid data.

    Returns:
        pd.DataFrame: A dataframe containing only the rows with allocable aid types."""

    aid_types = [
        "A02",
        "B01",
        "B03",
        "B031",
        "B032",
        "B033",
        "B04",
        "C01",
        "D01",
        "D02",
        "E01",
    ]
    return df.loc[lambda d: d[ClimateSchema.FLOW_MODALITY].isin(aid_types)].reset_index(
        drop=True
    )


def replace_missing_climate_with_zero(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Replaces missing values in a specified column with an empty string.

    Args:
        df (pd.DataFrame): The input dataframe with CRS data.
        column (str): The name of the column in which to replace missing values.

    Returns:
        pd.DataFrame: The dataframe with missing values in the specified column
        replaced by an empty string.
    """

    return df.assign(**{column: lambda d: d[column].replace("nan", "0")})

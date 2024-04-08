import numpy as np
import pandas as pd
from oda_data import set_data_path, ODAData
from pandas._typing import MergeHow, Suffixes

from climate_finance.common.schema import (
    CRS_MAPPING,
    CRS_TYPES,
    ClimateSchema,
    CRS_CLIMATE_COLUMNS,
    OECD_CLIMATE_INDICATORS,
    MULTISYSTEM_INDICATORS,
)
from climate_finance.config import ClimateDataPath
from climate_finance.oecd.cleaning_tools.settings import relevant_crs_columns

set_data_path(ClimateDataPath.raw_data)


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

    return df.astype({c: "string[pyarrow]" for c in idx if c in df.columns})


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


def key_crs_columns_to_str(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure right data types are set"""

    columns = relevant_crs_columns() + [
        ClimateSchema.FLOW_TYPE,
        ClimateSchema.FLOW_MODALITY,
        ClimateSchema.PROVIDER_TYPE,
        f"{ClimateSchema.PROVIDER_NAME}_short",
    ]

    key_crs_columns = set(
        c
        for c in columns
        if c not in [ClimateSchema.ADAPTATION, ClimateSchema.MITIGATION]
        and c in df.columns
    )

    climate_columns = [c for c in CRS_CLIMATE_COLUMNS if c in df.columns]

    return df.astype({c: "str" for c in key_crs_columns}).astype(
        {c: "Int16" for c in climate_columns}
    )


def fix_crs_year_encoding(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        **{
            ClimateSchema.YEAR: lambda d: d[ClimateSchema.YEAR]
            .astype("str")
            .str.replace("\ufeff", "", regex=True)
        }
    )


def clean_adaptation_and_mitigation_columns(df: pd.DataFrame) -> pd.DataFrame:
    df[CRS_CLIMATE_COLUMNS] = df[CRS_CLIMATE_COLUMNS].fillna(0)

    return df


def rename_crdf_marker_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename the marker columns to more standard names.

    Args:
        df: The dataframe to rename the columns for.

    Returns:
        The dataframe with the columns renamed.

    """
    # rename marker columns
    markers = {
        "adaptation_objective_applies_to_rio_marked_data_only": ClimateSchema.ADAPTATION,
        "mitigation_objective_applies_to_rio_marked_data_only": ClimateSchema.MITIGATION,
        "adaptation_related_development_finance_commitment_current": ClimateSchema.ADAPTATION_VALUE,
        "mitigation_related_development_finance_commitment_current": ClimateSchema.MITIGATION_VALUE,
    }

    return df.rename(columns=markers)


def marker_columns_to_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert the marker columns to numeric.

    The markers are converted to numeric values according to the following mapping:
    - Principal: 2
    - Significant: 1
    - Not targeted/Not screened: 0
    - Imputed multilateral contributions: 99

    Args:
        df: The dataframe to convert the marker columns for.

    Returns:
        The dataframe with the marker columns converted to numeric.

    """
    # markers to numeric
    markers_numeric = {
        "Principal": "2",
        "Significant": "1",
        "Not targeted/Not screened": "0",
        "Imputed multilateral contributions": "99",
        "Climate components": "100",
    }

    # Identify the marker columns
    marker_columns = [ClimateSchema.ADAPTATION, ClimateSchema.MITIGATION]

    # Convert the marker columns to numeric
    df[marker_columns] = (
        df[marker_columns].replace(markers_numeric).astype("int16[pyarrow]")
    )

    return df


def clean_crdf_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the columns of the dataframe. This means removing columns that are not needed
    and renaming the index columns. The "commitments" flow information is also added.

    Args:
        df: The dataframe to clean the columns for.

    Returns:
        The dataframe with the columns cleaned.

    """
    to_drop = [
        ClimateSchema.CLIMATE_OBJECTIVE,
        ClimateSchema.ADAPTATION,
        ClimateSchema.ADAPTATION_VALUE,
        ClimateSchema.MITIGATION,
        ClimateSchema.MITIGATION_VALUE,
        ClimateSchema.CLIMATE_FINANCE_VALUE,
        ClimateSchema.CROSS_CUTTING_VALUE,
        ClimateSchema.COMMITMENT_CLIMATE_SHARE,
    ]

    return (
        df.filter([c for c in df.columns if c not in to_drop])
        .assign(indicator=lambda d: d.indicator.map(OECD_CLIMATE_INDICATORS))
        .assign(flow_type=ClimateSchema.USD_COMMITMENT)
    )


def fix_crdf_provider_names_columns(
    data: pd.DataFrame,
) -> pd.DataFrame:
    return data.rename(
        columns={
            ClimateSchema.PROVIDER_NAME: f"{ClimateSchema.PROVIDER_NAME}_short",
            ClimateSchema.PROVIDER_DETAILED: ClimateSchema.PROVIDER_NAME,
        }
    ).astype({ClimateSchema.PROVIDER_NAME: "string[pyarrow]"})


def fix_crdf_recipient_errors(data: pd.DataFrame) -> pd.DataFrame:
    return data.replace({ClimateSchema.RECIPIENT_CODE: {9998: 998}})


def assign_usd_commitments_as_flow_type(data: pd.DataFrame) -> pd.DataFrame:
    return data.assign(
        **{ClimateSchema.FLOW_TYPE: ClimateSchema.USD_COMMITMENT}
    ).astype({ClimateSchema.FLOW_TYPE: "string[pyarrow]"})


def convert_thousands_to_units(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert the columns that are in thousands to units.

    Args:
        df: The dataframe to convert the columns for.

    Returns:
        The dataframe with the columns converted.

    """

    # Identify the columns that are in thousands
    usd_thousands_cols = df.columns[df.columns.str.contains("_usd_thousand")]

    # For each column, convert to units
    for col in usd_thousands_cols:
        df[col] *= 1e3

    # Rename the columns
    df = df.rename(
        columns={col: col.replace("_usd_thousand", "") for col in usd_thousands_cols}
    )

    return df


def set_crdf_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Set the data types for the dataframe.
    Args:
        df: The dataframe to set the data types for.

    Returns:
        The dataframe with the data types set.

    """
    # Convert int column to "Int32" or similar
    int_data_types = {
        "year": "int16[pyarrow]",
        "provider_code": "int16[pyarrow]",
        "agency_code": "int16[pyarrow]",
        "recipient_code": "int32[pyarrow]",
        "channel_of_delivery_code": "int32[pyarrow]",
        "purpose_code": "int32[pyarrow]",
        "type_of_finance": "int32[pyarrow]",
        "coal_related_financing": "int32[pyarrow]",
    }

    # Convert float columns to "float64" or similar
    float_data_types = {
        "adaptation_related_development_finance_commitment_current": "float[pyarrow]",
        "mitigation_related_development_finance_commitment_current": "float[pyarrow]",
        "overlap_commitment_current": "float[pyarrow]",
        "climate_related_development_finance_commitment_current": "float[pyarrow]",
        "share_of_the_underlying_commitment_when_available": "float[pyarrow]",
    }

    # Set data types by column
    for col in df.columns:
        df[col] = df[col].astype(
            (int_data_types | float_data_types).get(col, "string[pyarrow]")
        )

    return df.reset_index(drop=True)


def clean_raw_crdf(data: pd.DataFrame) -> pd.DataFrame:
    """Cleans an individual dataframe from the imputed multilateral shares file.

    Args:
        data (pd.DataFrame): Dataframe to clean.

    """

    # convert all column names to lower case and remove spaces and special characters
    data.columns = (
        data.columns.str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
        .str.replace(r"[°(),%]", "", regex=True)
        .str.replace(r"_{2,}", "_", regex=True)
    )

    # Drop any columns that contain an integer
    data = data.drop(columns=data.columns[data.columns.str.contains(r"\d")])

    # Convert thousands to units
    data = convert_thousands_to_units(data)

    # fix year column
    data = data.pipe(fix_crs_year_encoding)

    # Convert data types
    data = set_crdf_data_types(data)

    return data


def get_crs_channel_code2name_mapping() -> dict:
    return (
        get_crs_official_mapping()
        .rename(columns=CRS_MAPPING)
        .set_index(ClimateSchema.CHANNEL_CODE)[ClimateSchema.CHANNEL_NAME]
        .to_dict()
    )


def channel_codes_to_names(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        **{
            ClimateSchema.CHANNEL_NAME: lambda d: d[ClimateSchema.CHANNEL_CODE].map(
                get_crs_channel_code2name_mapping()
            )
        }
    )


def clean_multisystem_indicators(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        **{
            ClimateSchema.FLOW_TYPE: lambda d: d[ClimateSchema.INDICATOR].map(
                MULTISYSTEM_INDICATORS
            )
        }
    )


def _fill_missing_by_type(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return series.fillna(-100)
    if pd.api.types.is_string_dtype(series):
        return series.fillna("MISSING_DATA")


def _filled_to_missing_by_type(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return series.replace(-100, np.nan)
    if pd.api.types.is_string_dtype(series):
        return series.replace("MISSING_DATA", np.nan, regex=False)


def merge_with_missing(
    left: pd.DataFrame,
    right: pd.DataFrame,
    on: list[str] | None = None,
    how: MergeHow = "inner",
    left_on: list[str] = None,
    right_on: list[str] = None,
    suffixes: Suffixes = ("_left", "_right"),
    indicator: str | bool = False,
    validate: str | None = None,
) -> pd.DataFrame:
    if on is not None:
        left_on = on
        right_on = on

    for column in left_on:
        left[column] = _fill_missing_by_type(left[column])

    for column in right_on:
        right[column] = _fill_missing_by_type(right[column])

    data = pd.merge(
        left,
        right,
        how=how,
        left_on=left_on,
        right_on=right_on,
        suffixes=suffixes,
        indicator=indicator,
        validate=validate,
    )

    for column in left_on:
        data[column] = _filled_to_missing_by_type(data[column])

    for column in right_on:
        data[column] = _filled_to_missing_by_type(data[column])

    return data


def multi_flows_to_indicators(flows: list[str] | str) -> list[str]:
    """Transform a list of requested flows into a list of indicators."""

    # If string make list
    if isinstance(flows, str):
        flows = [flows]

    indicators = []

    for flow in flows:
        # Verify flow type
        if flow == "gross_disbursements":
            indicators.append("_disbursements_gross")
        elif flow == "commitments":
            indicators.append("_commitments_gross")
        else:
            raise ValueError("Only gross disbursements and commitments are accepted")

    # Clean indicator
    indicators = [
        f"multisystem_multilateral_contributions{indicator}" for indicator in indicators
    ]

    return indicators


def get_contributions_data(
    providers: list[str] | str,
    recipients: list[str] | str,
    years: list[int] | int,
    currency: str,
    prices: str,
    base_year: int | None,
    indicators: list[str],
) -> pd.DataFrame:
    """Use ODA data to get contributions data"""

    # if "disbursements" in the indicators, the 's' is removed
    indicators = [
        indicator.replace("disbursements", "disbursement") for indicator in indicators
    ]

    # create an instance of ODAData with the relevant settings
    contributions = ODAData(
        donors=providers,
        recipients=recipients,
        years=years,
        currency=currency,
        prices=prices,
        base_year=base_year,
    )

    # Load the indicators and get the data
    contributions_data = contributions.load_indicator(indicators=indicators).get_data()

    return contributions_data

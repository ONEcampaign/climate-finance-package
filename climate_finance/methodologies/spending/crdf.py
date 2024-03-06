import pandas as pd

from climate_finance.common.schema import (
    CRDF_VALUES,
    ClimateSchema,
    CLIMATE_VALUES_TO_NAMES,
)
from climate_finance.methodologies.spending.crs import (
    transform_markers_into_indicators,
)
from oda_data.clean_data.channels import clean_string

VALUES = CRDF_VALUES + [ClimateSchema.CLIMATE_FINANCE_VALUE]


def subtract_overlaps_by_project(df: pd.DataFrame) -> pd.DataFrame:
    """
    Subtract overlaps by project.

    Args:
        df (pd.DataFrame): The input dataframe.

    Returns:
        pd.DataFrame: Dataframe with values calculated based on conditions.
    """

    # subtract cross-cutting from adaptation and mitigation
    df[ClimateSchema.ADAPTATION_VALUE] = (
        df[ClimateSchema.ADAPTATION_VALUE] - df[ClimateSchema.CROSS_CUTTING_VALUE]
    )
    df[ClimateSchema.MITIGATION_VALUE] = (
        df[ClimateSchema.MITIGATION_VALUE] - df[ClimateSchema.CROSS_CUTTING_VALUE]
    )

    return df


def melt_crdf_values(df: pd.DataFrame, values: list[str] | None) -> pd.DataFrame:
    """
    Args:
        df: A pandas DataFrame containing the data to be melted.
        values: A list of column names to be melted.

    Returns:
        A pandas DataFrame with the melted data.
    """

    df = df.melt(
        id_vars=[c for c in df.columns if c not in values],
        value_vars=values,
        var_name=ClimateSchema.INDICATOR,
        value_name=ClimateSchema.VALUE,
    )

    return df


def clean_crdf_markers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a value column by adding adaptation + mitigation and subtracting cross-cutting.

    Args:
        df: A pandas DataFrame containing the CRDF markers data.

    Returns:
        A new pandas DataFrame with the CRDF markers data cleaned.

    """
    # Create a value column by adding adaptation + mitigation and subtracting cross-cutting
    df[ClimateSchema.VALUE] = (
        df[[ClimateSchema.ADAPTATION_VALUE, ClimateSchema.MITIGATION_VALUE]].sum(axis=1)
        - df[ClimateSchema.CROSS_CUTTING_VALUE]
    )

    # Drop the columns that are no longer needed

    df = df.filter(
        [
            c
            for c in df.columns
            if c not in CRDF_VALUES + [ClimateSchema.CLIMATE_FINANCE_VALUE]
        ]
    )

    return df


def process_climate_components(
    df: pd.DataFrame, highest_marker: bool = True
) -> pd.DataFrame:
    """
    Args:
        df: A pandas DataFrame containing the climate components data.
        highest_marker: A boolean flag indicating whether to subtract overlaps by project
        based on the highest marker.

    Returns:
        A pandas DataFrame with the processed climate components data.

    """

    # If highest marker is selected, the overlaps become 'cross-cutting'
    # Otherwise they are just 'overlaps' (to be subtracted from totals)
    if highest_marker:
        df = subtract_overlaps_by_project(df)

    # Drop the total value column
    df = df.filter(
        [c for c in df.columns if c not in [ClimateSchema.CLIMATE_FINANCE_VALUE]]
    )

    # Melt the data to have climate values as 'value' and an indicator column
    df = melt_crdf_values(df, values=CRDF_VALUES)

    # Rename the indicators to match the standard names used elsewhere
    df = df.assign(indicator=lambda d: d.indicator.map(CLIMATE_VALUES_TO_NAMES))

    # Drop climate adaptation and mitigation columns, and the share column
    df = df.drop(columns=[ClimateSchema.ADAPTATION, ClimateSchema.MITIGATION])

    return df


def drop_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop the names of the columns in the dataframe.

    Args:
        df (pd.DataFrame): The input dataframe.

    Returns:
        pd.DataFrame: The dataframe with the names of the columns dropped.
    """

    # List of all the columns that include names
    names = [
        ClimateSchema.RECIPIENT_NAME,
        ClimateSchema.RECIPIENT_REGION,
        ClimateSchema.CLIMATE_OBJECTIVE,
        ClimateSchema.SECTOR_NAME,
        ClimateSchema.PURPOSE_NAME,
        ClimateSchema.CHANNEL_NAME_DELIVERY,
    ]

    return df.filter([c for c in df.columns if c not in names])


def clean_string_cols(df: pd.DataFrame, cols: list[str] | None) -> pd.DataFrame:
    """
    Cleans string columns in a pandas DataFrame. It removes special characters
    and converts the string to lower case.

    Args:
        df (pd.DataFrame): The DataFrame containing the string columns.
        cols (list[str] | None): The list of column names to be cleaned.
            If None, only the column 'ClimateSchema.PROJECT_TITLE'
            will be cleaned. Default is None.

    Returns:
        pd.DataFrame: The DataFrame with the cleaned string columns.

    """
    if cols is None:
        cols = [ClimateSchema.PROJECT_TITLE]

    for column in cols:
        if column in df.columns:
            df[column] = clean_string(df[column])

    return df


def group_and_summarize(df: pd.DataFrame) -> pd.DataFrame:
    """
    Group the data in order to have one row per project.

    Args:
        df: pd.DataFrame - The input DataFrame containing the data to be grouped and summarized.

    Returns:
        pd.DataFrame - The resulting DataFrame with grouped and summarized data.
    """
    # Identify all non 'value' columns
    idx = [c for c in df.columns if c not in VALUES]

    # Valid values
    values = [c for c in df.columns if c in VALUES]

    # Group so that each project has only one row
    df = df.groupby(by=idx, observed=True, dropna=False)[values].sum().reset_index()

    return df


def split_into_markers_and_components(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split the dataframe into markers and components.

    Args:
        df (pd.DataFrame): The input dataframe.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: The markers and components dataframes.
    """

    # Markers when adaptation or mitigation is > 2
    markers = df.loc[
        lambda d: (d[ClimateSchema.ADAPTATION] <= 2)
        | (d[ClimateSchema.MITIGATION] <= 2)
    ].copy(deep=True)

    # Components when adaptation or mitigation are 100
    components = df.loc[
        lambda d: (d[ClimateSchema.ADAPTATION] == 100)
        | (d[ClimateSchema.MITIGATION] == 100)
    ].copy(deep=True)

    return markers, components


def transform_crdf_into_indicators(
    df: pd.DataFrame,
    percentage_significant: float | int = 1,
    percentage_principal: float | int = 1,
    highest_marker: bool = True,
) -> pd.DataFrame:
    """Transforms the CRDF data into climate indicators

    The marker data is treated just as the CRS data. The climate components data is
    processed differently, since there are no levels of marking.

    Args:
        df: A DataFrame containing the CRDF data.
        percentage_significant: The percentage of the activity that is considered
            climate relevant when the marker is 1. The default is 1.0.
        percentage_principal: The percentage of the activity that is considered
            climate relevant when the marker is 2. The default is 1.0.
        highest_marker: Whether to use the highest marker value.

    Returns:
        A DataFrame containing the transformed indicators data.
    """

    # Prepare the CRDF data
    df = (
        df.pipe(drop_names)
        .pipe(clean_string_cols, cols=["project_title", "description"])
        .pipe(group_and_summarize)
        .drop(columns=[ClimateSchema.COMMITMENT_CLIMATE_SHARE])
    )

    # Split into data that uses markers and data that uses climate components
    markers, components = split_into_markers_and_components(df)

    # Process the markers data
    markers = markers.pipe(clean_crdf_markers).pipe(
        transform_markers_into_indicators,
        percentage_significant=percentage_significant,
        percentage_principal=percentage_principal,
        highest_marker=highest_marker,
    )

    # Process the climate components data
    components = components.pipe(
        process_climate_components, highest_marker=highest_marker
    )

    # Combine the data
    data = pd.concat([markers, components], ignore_index=True)

    return data

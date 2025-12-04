from collections import defaultdict

import pandas as pd

from climate_finance.common.schema import ClimateSchema


def schema_types() -> dict:
    """
    Returns a dictionary of schema types. Invalid columns are assumed to be strings.
    By default, pyarrow types are used for all columns.

    Returns:
        dict: A dictionary mapping attribute names to their corresponding data types.

    """
    types = {
        ClimateSchema.YEAR: "int16[pyarrow]",
        ClimateSchema.PROVIDER_CODE: "int16[pyarrow]",
        ClimateSchema.PROVIDER_NAME: "string[pyarrow]",
        ClimateSchema.PROVIDER_TYPE: "string[pyarrow]",
        ClimateSchema.PROVIDER_DETAILED: "string[pyarrow]",
        ClimateSchema.AGENCY_CODE: "int16[pyarrow]",
        ClimateSchema.AGENCY_NAME: "string[pyarrow]",
        ClimateSchema.CRS_ID: "string[pyarrow]",
        ClimateSchema.PROJECT_ID: "string[pyarrow]",
        ClimateSchema.RECIPIENT_CODE: "int16[pyarrow]",
        ClimateSchema.RECIPIENT_NAME: "string[pyarrow]",
        ClimateSchema.RECIPIENT_REGION: "string[pyarrow]",
        ClimateSchema.RECIPIENT_REGION_CODE: "int16[pyarrow]",
        ClimateSchema.RECIPIENT_INCOME: "string[pyarrow]",
        ClimateSchema.FLOW_MODALITY: "string[pyarrow]",
        ClimateSchema.ALLOCABLE_SHARE: "float64[pyarrow]",
        ClimateSchema.CONCESSIONALITY: "string[pyarrow]",
        ClimateSchema.FINANCIAL_INSTRUMENT: "string[pyarrow]",
        ClimateSchema.FLOW_TYPE: "string[pyarrow]",
        ClimateSchema.FINANCE_TYPE: "string[pyarrow]",
        ClimateSchema.CHANNEL_NAME_DELIVERY: "string[pyarrow]",
        ClimateSchema.CHANNEL_CODE_DELIVERY: "int32[pyarrow]",
        ClimateSchema.CHANNEL_CODE: "int32[pyarrow]",
        ClimateSchema.CHANNEL_NAME: "string[pyarrow]",
        ClimateSchema.ADAPTATION: "int16[pyarrow]",
        ClimateSchema.MITIGATION: "int16[pyarrow]",
        ClimateSchema.CROSS_CUTTING: "int16[pyarrow]",
        ClimateSchema.ADAPTATION_VALUE: "float64[pyarrow]",
        ClimateSchema.MITIGATION_VALUE: "float64[pyarrow]",
        ClimateSchema.CROSS_CUTTING_VALUE: "float64[pyarrow]",
        ClimateSchema.CLIMATE_OBJECTIVE: "string[pyarrow]",
        ClimateSchema.CLIMATE_FINANCE_VALUE: "float64[pyarrow]",
        ClimateSchema.COMMITMENT_CLIMATE_SHARE: "float64[pyarrow]",
        ClimateSchema.NOT_CLIMATE: "float64[pyarrow]",
        ClimateSchema.CLIMATE_UNSPECIFIED: "float64[pyarrow]",
        ClimateSchema.CLIMATE_UNSPECIFIED_SHARE: "float64[pyarrow]",
        ClimateSchema.PURPOSE_CODE: "int32[pyarrow]",
        ClimateSchema.PURPOSE_NAME: "string[pyarrow]",
        ClimateSchema.SECTOR_CODE: "int32[pyarrow]",
        ClimateSchema.SECTOR_NAME: "string[pyarrow]",
        ClimateSchema.PROJECT_TITLE: "string[pyarrow]",
        ClimateSchema.PROJECT_DESCRIPTION: "string[pyarrow]",
        ClimateSchema.PROJECT_DESCRIPTION_SHORT: "string[pyarrow]",
        ClimateSchema.GENDER: "string[pyarrow]",
        ClimateSchema.INDICATOR: "string[pyarrow]",
        ClimateSchema.VALUE: "float64[pyarrow]",
        ClimateSchema.TOTAL_VALUE: "float64[pyarrow]",
        ClimateSchema.SHARE: "float64[pyarrow]",
        ClimateSchema.CLIMATE_SHARE: "float64[pyarrow]",
        ClimateSchema.CLIMATE_SHARE_ROLLING: "float64[pyarrow]",
        ClimateSchema.FLOW_CODE: "int32[pyarrow]",
        ClimateSchema.FLOW_NAME: "string[pyarrow]",
        ClimateSchema.CATEGORY: "int16[pyarrow]",
        ClimateSchema.USD_COMMITMENT: "float64[pyarrow]",
        ClimateSchema.USD_DISBURSEMENT: "float64[pyarrow]",
        ClimateSchema.USD_RECEIVED: "float64[pyarrow]",
        ClimateSchema.USD_GRANT_EQUIV: "float64[pyarrow]",
        ClimateSchema.USD_NET_DISBURSEMENT: "float64[pyarrow]",
        ClimateSchema.REPORTING_METHOD: "string[pyarrow]",
        ClimateSchema.MULTILATERAL_TYPE: "string[pyarrow]",
        ClimateSchema.CONVERGED_REPORTING: "string[pyarrow]",
        ClimateSchema.COAL_FINANCING: "string[pyarrow]",
        ClimateSchema.LEVEL: "string[pyarrow]",
    }

    return defaultdict(lambda: "string[pyarrow]", types)


def set_default_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Set the types of the columns in the dataframe.

    Args:
        df (pd.DataFrame): The input dataframe.

    Returns:
        pd.DataFrame: The dataframe with the types set.

    """
    default_types = schema_types()

    converted_types = {c: default_types[c] for c in df.columns}

    return df.astype(converted_types)

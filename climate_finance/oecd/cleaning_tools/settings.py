from climate_finance.common.schema import ClimateSchema


def relevant_crs_columns() -> list:
    """
    Fetches the list of relevant columns from the CRS data for data extraction.

    Returns:
        list: A list of column names considered relevant for data extraction."""

    return [
        ClimateSchema.YEAR,
        ClimateSchema.PROVIDER_CODE,
        ClimateSchema.PROVIDER_NAME,
        ClimateSchema.AGENCY_NAME,
        ClimateSchema.AGENCY_CODE,
        ClimateSchema.RECIPIENT_CODE,
        ClimateSchema.RECIPIENT_NAME,
        ClimateSchema.FLOW_CODE,
        ClimateSchema.FLOW_NAME,
        ClimateSchema.SECTOR_CODE,
        ClimateSchema.SECTOR_NAME,
        ClimateSchema.PURPOSE_CODE,
        ClimateSchema.PURPOSE_NAME,
        ClimateSchema.PROJECT_TITLE,
        ClimateSchema.CRS_ID,
        ClimateSchema.PROJECT_ID,
        ClimateSchema.PROJECT_DESCRIPTION,
        ClimateSchema.FINANCE_TYPE,
        ClimateSchema.MITIGATION,
        ClimateSchema.ADAPTATION,
    ]


def all_flow_columns() -> list:
    """
    Fetches the list of flow columns from the CRS data for data extraction.

    Returns:
        list: A list of column names considered relevant for data extraction.

    """
    return [
        ClimateSchema.USD_COMMITMENT,
        ClimateSchema.USD_DISBURSEMENT,
        ClimateSchema.USD_RECEIVED,
        ClimateSchema.USD_GRANT_EQUIV,
        ClimateSchema.USD_NET_DISBURSEMENT,
    ]

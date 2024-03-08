from climate_finance.common.schema import (
    ClimateSchema,
)

ONE_IMPUTATIONS_IDX = [
    ClimateSchema.YEAR,
    ClimateSchema.CHANNEL_CODE,
    ClimateSchema.FLOW_TYPE,
]

ONE_CONTRIBUTIONS_IDX = [
    ClimateSchema.YEAR,
    ClimateSchema.FLOW_TYPE,
    ClimateSchema.CHANNEL_CODE,
]

ONE_IMPUTATIONS_SPENDING_IDX = [
    ClimateSchema.YEAR,
    ClimateSchema.PROVIDER_CODE,
    ClimateSchema.PROVIDER_NAME,
    ClimateSchema.AGENCY_NAME,
    ClimateSchema.FLOW_TYPE,
]

ONE_IMPUTATIONS_OUTPUT_COLUMNS = [
    ClimateSchema.YEAR,
    ClimateSchema.PROVIDER_CODE,
    ClimateSchema.PROVIDER_NAME,
    ClimateSchema.CHANNEL_CODE,
    ClimateSchema.CHANNEL_NAME,
    ClimateSchema.RECIPIENT_CODE,
    ClimateSchema.RECIPIENT_NAME,
    ClimateSchema.SECTOR_CODE,
    ClimateSchema.SECTOR_NAME,
    ClimateSchema.PURPOSE_CODE,
    ClimateSchema.PURPOSE_NAME,
    ClimateSchema.FINANCE_TYPE,
    ClimateSchema.FLOW_CODE,
    ClimateSchema.FLOW_TYPE,
    f"imputed_{ClimateSchema.VALUE}",
    f"imputed_{ClimateSchema.VALUE}_rolling",
]

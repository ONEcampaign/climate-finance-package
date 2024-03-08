from dataclasses import dataclass


@dataclass
class ClimateSchema:
    YEAR: str = "year"
    PROVIDER_CODE: str = "oecd_provider_code"
    PROVIDER_ISO_CODE: str = "iso3_provider_code"
    PROVIDER_NAME: str = "provider"
    PROVIDER_TYPE: str = "provider_type"
    PROVIDER_DETAILED: str = "provider_detailed"
    AGENCY_CODE: str = "oecd_agency_code"
    AGENCY_NAME: str = "agency_name"
    CRS_ID: str = "crs_id"
    PROJECT_ID: str = "project_id"
    RECIPIENT_CODE: str = "oecd_recipient_code"
    RECIPIENT_NAME: str = "recipient"
    RECIPIENT_REGION: str = "recipient_region"
    RECIPIENT_REGION_CODE: str = "oecd_recipient_region_code"
    RECIPIENT_INCOME: str = "oecd_recipient_income"
    FLOW_MODALITY: str = "modality"
    ALLOCABLE_SHARE: str = "allocable_share"
    CONCESSIONALITY: str = "concessionality"
    FINANCIAL_INSTRUMENT: str = "financial_instrument"
    FLOW_TYPE: str = "flow_type"
    FINANCE_TYPE: str = "type_of_finance"
    CHANNEL_NAME_DELIVERY: str = "oecd_channel_name_delivery"
    CHANNEL_CODE_DELIVERY: str = "oecd_channel_code_delivery"
    CHANNEL_CODE: str = "oecd_channel_code"
    CHANNEL_NAME: str = "oecd_channel_name"
    ADAPTATION: str = "climate_adaptation"
    MITIGATION: str = "climate_mitigation"
    CROSS_CUTTING: str = "climate_cross_cutting"
    ADAPTATION_VALUE: str = "climate_adaptation_value"
    MITIGATION_VALUE: str = "climate_mitigation_value"
    CROSS_CUTTING_VALUE: str = "overlap_commitment_current"
    CLIMATE_OBJECTIVE: str = "climate_objective"
    CLIMATE_FINANCE_VALUE: str = "climate_finance_value"
    COMMITMENT_CLIMATE_SHARE: str = "commitment_climate_share"
    NOT_CLIMATE: str = "not_climate_relevant"
    CLIMATE_UNSPECIFIED: str = "climate_total"
    CLIMATE_UNSPECIFIED_SHARE: str = "climate_total_share"
    PURPOSE_CODE: str = "purpose_code"
    SECTOR_CODE: str = "sector_code"
    PURPOSE_NAME: str = "purpose_name"
    SECTOR_NAME: str = "sector_name"
    PROJECT_TITLE: str = "project_title"
    PROJECT_DESCRIPTION: str = "description"
    PROJECT_DESCRIPTION_SHORT: str = "short_description"
    GENDER: str = "gender"
    INDICATOR: str = "indicator"
    VALUE: str = "value"
    TOTAL_VALUE: str = "total_value"
    SHARE: str = "share"
    CLIMATE_SHARE: str = "climate_share"
    CLIMATE_SHARE_ROLLING: str = "climate_share_rolling"
    FLOW_CODE: str = "flow_code"
    FLOW_NAME: str = "flow_name"
    CATEGORY: str = "category"
    USD_COMMITMENT: str = "usd_commitment"
    USD_DISBURSEMENT: str = "usd_disbursement"
    USD_RECEIVED: str = "usd_received"
    USD_GRANT_EQUIV: str = "usd_grant_equiv"
    USD_NET_DISBURSEMENT: str = "usd_net_disbursement"
    REPORTING_METHOD: str = "reporting_method"
    MULTILATERAL_TYPE: str = "multilateral_type"
    CONVERGED_REPORTING: str = "converged_reporting"
    COAL_FINANCING: str = "coal_related_financing"
    LEVEL: str = "level"
    PRICES: str = "prices"
    CURRENCY: str = "currency"


CRS_MAPPING: dict[str, str] = {
    "donor_code": ClimateSchema.PROVIDER_CODE,
    "donor_name": ClimateSchema.PROVIDER_NAME,
    "provider": ClimateSchema.PROVIDER_NAME,
    "provider_type": ClimateSchema.PROVIDER_TYPE,
    "provider_detailed": ClimateSchema.PROVIDER_DETAILED,
    "provider_code": ClimateSchema.PROVIDER_CODE,
    "agency_code": ClimateSchema.AGENCY_CODE,
    "agency_name": ClimateSchema.AGENCY_NAME,
    "extending_agency": ClimateSchema.AGENCY_NAME,
    "crs_identification_n": ClimateSchema.CRS_ID,
    "donor_project_n": ClimateSchema.PROJECT_ID,
    "project_number": ClimateSchema.PROJECT_ID,
    "recipient_code": ClimateSchema.RECIPIENT_CODE,
    "recipient_name": ClimateSchema.RECIPIENT_NAME,
    "region_name": ClimateSchema.RECIPIENT_REGION,
    "region_code": ClimateSchema.RECIPIENT_REGION_CODE,
    "incomegoup_name": ClimateSchema.RECIPIENT_INCOME,
    "recipient_income_group_oecd_classification": ClimateSchema.RECIPIENT_INCOME,
    "development_cooperation_modality": ClimateSchema.FLOW_MODALITY,
    "aid_t": ClimateSchema.FLOW_MODALITY,
    "financial_instrument": ClimateSchema.FINANCIAL_INSTRUMENT,
    "type_of_finance": ClimateSchema.FINANCE_TYPE,
    "finance_t": ClimateSchema.FINANCE_TYPE,
    "channel_of_delivery": ClimateSchema.CHANNEL_NAME_DELIVERY,
    "channel_of_delivery_code": ClimateSchema.CHANNEL_CODE_DELIVERY,
    "channel_code": ClimateSchema.CHANNEL_CODE,
    "channel_name": ClimateSchema.CHANNEL_NAME,
    "adaptation_objective_applies_to_rio_marked_data_only": ClimateSchema.ADAPTATION,
    "mitigation_objective_applies_to_rio_marked_data_only": ClimateSchema.MITIGATION,
    "adaptation_related_development_finance_commitment_current": ClimateSchema.ADAPTATION_VALUE,
    "mitigation_related_development_finance_commitment_current": ClimateSchema.MITIGATION_VALUE,
    "climate_objective_applies_to_rio_marked_data_only_or_climate_component": ClimateSchema.CLIMATE_OBJECTIVE,
    "climate_related_development_finance_commitment_current": ClimateSchema.CLIMATE_FINANCE_VALUE,
    "share_of_the_underlying_commitment_when_available": ClimateSchema.COMMITMENT_CLIMATE_SHARE,
    "overlap_commitment_current": ClimateSchema.CROSS_CUTTING_VALUE,
    "sector_detailed": ClimateSchema.SECTOR_NAME,
    "sub_sector": ClimateSchema.PURPOSE_NAME,
    "purpose_code": ClimateSchema.PURPOSE_CODE,
    "sector_code": ClimateSchema.SECTOR_CODE,
    "project_title": ClimateSchema.PROJECT_TITLE,
    "long_description": ClimateSchema.PROJECT_DESCRIPTION,
    "short_description": ClimateSchema.PROJECT_DESCRIPTION_SHORT,
    "flow_code": ClimateSchema.FLOW_CODE,
    "flow_name": ClimateSchema.FLOW_NAME,
    "gender": ClimateSchema.GENDER,
    "category": ClimateSchema.CATEGORY,
    "usd_commitment": ClimateSchema.USD_COMMITMENT,
    "usd_disbursement": ClimateSchema.USD_DISBURSEMENT,
    "usd_received": ClimateSchema.USD_RECEIVED,
    "usd_grant_equiv": ClimateSchema.USD_GRANT_EQUIV,
    "usd_net_disbursement": ClimateSchema.USD_NET_DISBURSEMENT,
    "oecd_climate_total": ClimateSchema.CLIMATE_UNSPECIFIED,
    "reporting_type": ClimateSchema.REPORTING_METHOD,
    "type": ClimateSchema.MULTILATERAL_TYPE,
    "converged_reporting": ClimateSchema.CONVERGED_REPORTING,
    "coal_related_financing": ClimateSchema.COAL_FINANCING,
    "flow_type": ClimateSchema.FLOW_TYPE,
    "type_of_flow": ClimateSchema.FLOW_TYPE,
}

OECD_CLIMATE_INDICATORS: dict[str, str] = {
    ClimateSchema.ADAPTATION: "Adaptation",
    ClimateSchema.MITIGATION: "Mitigation",
    ClimateSchema.CROSS_CUTTING: "Cross-cutting",
    ClimateSchema.NOT_CLIMATE: "Not climate relevant",
    ClimateSchema.CLIMATE_UNSPECIFIED: "Climate unspecified",
}

OECD_IMPUTED_CLIMATE_INDICATORS: dict[str, str] = {
    "oecd_climate_total": ClimateSchema.CLIMATE_UNSPECIFIED,
    "oecd_mitigation": ClimateSchema.MITIGATION,
    "oecd_adaptation": ClimateSchema.ADAPTATION,
    "oecd_cross_cutting": ClimateSchema.CROSS_CUTTING,
    "not_climate_relevant": ClimateSchema.NOT_CLIMATE,
}

MULTILATERAL_IMPUTATIONS_ID_COLUMNS: list[str] = [
    ClimateSchema.YEAR,
    ClimateSchema.CHANNEL_CODE,
    ClimateSchema.CHANNEL_NAME,
    ClimateSchema.FLOW_TYPE,
    ClimateSchema.MULTILATERAL_TYPE,
    ClimateSchema.REPORTING_METHOD,
    ClimateSchema.CONVERGED_REPORTING,
]

MULTISYSTEM_INDICATORS: dict = {
    "multisystem_multilateral_contributions_disbursement_gross": ClimateSchema.USD_DISBURSEMENT,
    "multisystem_multilateral_contributions_commitments_gross": ClimateSchema.USD_COMMITMENT,
}

CRS_CLIMATE_COLUMNS: list[str] = [
    ClimateSchema.ADAPTATION,
    ClimateSchema.MITIGATION,
]

VALUE_COLUMNS: list[str] = [
    ClimateSchema.ADAPTATION_VALUE,
    ClimateSchema.MITIGATION_VALUE,
    ClimateSchema.CROSS_CUTTING_VALUE,
    ClimateSchema.CLIMATE_FINANCE_VALUE,
    ClimateSchema.COMMITMENT_CLIMATE_SHARE,
    ClimateSchema.CLIMATE_UNSPECIFIED,
    ClimateSchema.USD_COMMITMENT,
    ClimateSchema.USD_DISBURSEMENT,
    ClimateSchema.USD_RECEIVED,
    ClimateSchema.USD_GRANT_EQUIV,
    ClimateSchema.USD_NET_DISBURSEMENT,
    ClimateSchema.VALUE,
    ClimateSchema.TOTAL_VALUE,
    ClimateSchema.SHARE,
]

CRS_TYPES = {
    ClimateSchema.YEAR: "Int32",
    ClimateSchema.PROVIDER_CODE: "Int32",
    ClimateSchema.PROVIDER_NAME: "category",
    ClimateSchema.RECIPIENT_NAME: "category",
    ClimateSchema.RECIPIENT_CODE: "Int32",
    ClimateSchema.AGENCY_NAME: "str",
    ClimateSchema.AGENCY_CODE: "Int32",
    ClimateSchema.FLOW_NAME: "str",
    ClimateSchema.FLOW_CODE: "Int32",
    ClimateSchema.MITIGATION: "str",
    ClimateSchema.ADAPTATION: "str",
    ClimateSchema.PURPOSE_CODE: "Int32",
    ClimateSchema.SECTOR_CODE: "Int32",
    ClimateSchema.FINANCE_TYPE: "Int32",
}

CLIMATE_VALUES = [
    ClimateSchema.ADAPTATION_VALUE,
    ClimateSchema.MITIGATION_VALUE,
    ClimateSchema.CROSS_CUTTING_VALUE,
]

CLIMATE_VALUES_TO_NAMES: dict[str, str] = {
    ClimateSchema.ADAPTATION_VALUE: ClimateSchema.ADAPTATION,
    ClimateSchema.MITIGATION_VALUE: ClimateSchema.MITIGATION,
    ClimateSchema.CROSS_CUTTING_VALUE: ClimateSchema.CROSS_CUTTING,
}

MULTISYSTEM_COLUMNS: list[str] = [
    ClimateSchema.YEAR,
    ClimateSchema.PROVIDER_CODE,
    ClimateSchema.PROVIDER_NAME,
    ClimateSchema.FLOW_TYPE,
    ClimateSchema.CHANNEL_CODE,
    ClimateSchema.CHANNEL_NAME,
    ClimateSchema.VALUE,
]

MAIN_FLOWS: list[str] = [
    ClimateSchema.USD_COMMITMENT,
    ClimateSchema.USD_DISBURSEMENT,
]

CRDF_VALUES = [
    ClimateSchema.ADAPTATION_VALUE,
    ClimateSchema.MITIGATION_VALUE,
    ClimateSchema.CROSS_CUTTING_VALUE,
]

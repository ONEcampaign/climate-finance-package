from dataclasses import dataclass


@dataclass
class CrsSchema:
    YEAR: str = "year"
    PARTY_CODE: str = "oecd_party_code"
    PARTY_NAME: str = "party"
    PARTY_TYPE: str = "party_type"
    PARTY_DETAILED: str = "party_detailed"
    AGENCY_CODE: str = "oecd_agency_code"
    AGENCY_NAME: str = "agency"
    CRS_ID: str = "crs_id"
    PROJECT_ID: str = "project_id"
    RECIPIENT_CODE: str = "oecd_recipient_code"
    RECIPIENT_NAME: str = "recipient"
    RECIPIENT_REGION: str = "recipient_region"
    RECIPIENT_REGION_CODE: str = "oecd_recipient_region_code"
    RECIPIENT_INCOME: str = "oecd_recipient_income"
    FLOW_MODALITY: str = "modality"
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


CRS_MAPPING: dict[str, str] = {
    "donor_code": CrsSchema.PARTY_CODE,
    "donor_name": CrsSchema.PARTY_NAME,
    "provider": CrsSchema.PARTY_NAME,
    "provider_type": CrsSchema.PARTY_TYPE,
    "provider_detailed": CrsSchema.PARTY_DETAILED,
    "provider_code": CrsSchema.PARTY_CODE,
    "agency_code": CrsSchema.AGENCY_CODE,
    "agency_name": CrsSchema.AGENCY_NAME,
    "extending_agency": CrsSchema.AGENCY_NAME,
    "crs_identification_n": CrsSchema.CRS_ID,
    "donor_project_n": CrsSchema.PROJECT_ID,
    "project_number": CrsSchema.PROJECT_ID,
    "recipient_code": CrsSchema.RECIPIENT_CODE,
    "recipient_name": CrsSchema.RECIPIENT_NAME,
    "region_name": CrsSchema.RECIPIENT_REGION,
    "region_code": CrsSchema.RECIPIENT_REGION_CODE,
    "incomegoup_name": CrsSchema.RECIPIENT_INCOME,
    "recipient_income_group_oecd_classification": CrsSchema.RECIPIENT_INCOME,
    "development_cooperation_modality": CrsSchema.FLOW_MODALITY,
    "aid_t": CrsSchema.FLOW_MODALITY,
    "financial_instrument": CrsSchema.FINANCIAL_INSTRUMENT,
    "type_of_finance": CrsSchema.FINANCE_TYPE,
    "finance_t": CrsSchema.FINANCE_TYPE,
    "channel_of_delivery": CrsSchema.CHANNEL_NAME_DELIVERY,
    "channel_of_delivery_code": CrsSchema.CHANNEL_CODE_DELIVERY,
    "channel_code": CrsSchema.CHANNEL_CODE,
    "channel_name": CrsSchema.CHANNEL_NAME,
    "adaptation_objective_applies_to_rio_marked_data_only": CrsSchema.ADAPTATION,
    "mitigation_objective_applies_to_rio_marked_data_only": CrsSchema.MITIGATION,
    "adaptation_related_development_finance_commitment_current": CrsSchema.ADAPTATION_VALUE,
    "mitigation_related_development_finance_commitment_current": CrsSchema.MITIGATION_VALUE,
    "climate_objective_applies_to_rio_marked_data_only_or_climate_component": CrsSchema.CLIMATE_OBJECTIVE,
    "climate_related_development_finance_commitment_current": CrsSchema.CLIMATE_FINANCE_VALUE,
    "share_of_the_underlying_commitment_when_available": CrsSchema.COMMITMENT_CLIMATE_SHARE,
    "overlap_commitment_current": CrsSchema.CROSS_CUTTING_VALUE,
    "sector_detailed": CrsSchema.SECTOR_NAME,
    "sub_sector": CrsSchema.PURPOSE_NAME,
    "purpose_code": CrsSchema.PURPOSE_CODE,
    "sector_code": CrsSchema.SECTOR_CODE,
    "project_title": CrsSchema.PROJECT_TITLE,
    "long_description": CrsSchema.PROJECT_DESCRIPTION,
    "short_description": CrsSchema.PROJECT_DESCRIPTION_SHORT,
    "flow_code": CrsSchema.FLOW_CODE,
    "flow_name": CrsSchema.FLOW_NAME,
    "gender": CrsSchema.GENDER,
    "category": CrsSchema.CATEGORY,
    "usd_commitment": CrsSchema.USD_COMMITMENT,
    "usd_disbursement": CrsSchema.USD_DISBURSEMENT,
    "usd_received": CrsSchema.USD_RECEIVED,
    "usd_grant_equiv": CrsSchema.USD_GRANT_EQUIV,
    "usd_net_disbursement": CrsSchema.USD_NET_DISBURSEMENT,
    "oecd_climate_total": CrsSchema.CLIMATE_UNSPECIFIED,
    "reporting_type": CrsSchema.REPORTING_METHOD,
    "type": CrsSchema.MULTILATERAL_TYPE,
    "converged_reporting": CrsSchema.CONVERGED_REPORTING,
    "coal_related_financing": CrsSchema.COAL_FINANCING,
    "flow_type": CrsSchema.FLOW_TYPE,
    "type_of_flow": CrsSchema.FLOW_TYPE,
}


OECD_CLIMATE_INDICATORS: dict[str, str] = {
    CrsSchema.ADAPTATION: "Adaptation",
    CrsSchema.MITIGATION: "Mitigation",
    CrsSchema.CROSS_CUTTING: "Cross-cutting",
    CrsSchema.NOT_CLIMATE: "Not climate relevant",
    CrsSchema.CLIMATE_UNSPECIFIED: "Climate unspecified",
}

MULTISYSTEM_INDICATORS: dict = {
    "multisystem_multilateral_contributions_disbursement_gross": CrsSchema.USD_DISBURSEMENT,
    "multisystem_multilateral_contributions_commitments_gross": CrsSchema.USD_COMMITMENT,
}

VALUE_COLUMNS: list[str] = [
    CrsSchema.ADAPTATION_VALUE,
    CrsSchema.MITIGATION_VALUE,
    CrsSchema.CROSS_CUTTING_VALUE,
    CrsSchema.CLIMATE_FINANCE_VALUE,
    CrsSchema.COMMITMENT_CLIMATE_SHARE,
    CrsSchema.CLIMATE_UNSPECIFIED,
    CrsSchema.USD_COMMITMENT,
    CrsSchema.USD_DISBURSEMENT,
    CrsSchema.USD_RECEIVED,
    CrsSchema.USD_GRANT_EQUIV,
    CrsSchema.USD_NET_DISBURSEMENT,
    CrsSchema.VALUE,
    CrsSchema.TOTAL_VALUE,
    CrsSchema.SHARE,
]


CRS_TYPES = {
    CrsSchema.YEAR: "Int32",
    CrsSchema.PARTY_CODE: "Int32",
    CrsSchema.PARTY_NAME: "category",
    CrsSchema.RECIPIENT_NAME: "category",
    CrsSchema.RECIPIENT_CODE: "Int32",
    CrsSchema.AGENCY_NAME: "str",
    CrsSchema.AGENCY_CODE: "Int32",
    CrsSchema.FLOW_NAME: "str",
    CrsSchema.FLOW_CODE: "Int32",
    CrsSchema.MITIGATION: "str",
    CrsSchema.ADAPTATION: "str",
    CrsSchema.PURPOSE_CODE: "Int32",
    CrsSchema.SECTOR_CODE: "Int32",
    CrsSchema.FINANCE_TYPE: "Int32",
}

CLIMATE_VALUES = [
    CrsSchema.ADAPTATION_VALUE,
    CrsSchema.MITIGATION_VALUE,
    CrsSchema.CROSS_CUTTING_VALUE,
]

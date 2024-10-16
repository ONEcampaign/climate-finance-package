def flow_name_mapping() -> dict:
    return {
        11: "ODA Grants",
        13: "ODA Loans",
        19: "Equity Investment",
        14: "Other Official Flows (non Export Credits)",
        30: "Private Development Finance",
        0: "Unspecified",
    }


def mapping_flow_name_to_code() -> dict:
    return {
        11: "ODA Grants",
        13: "ODA Loans",
        14: "Other Official Flows (non Export Credit)",
        19: "Equity Investment",
        30: "Private Development Finance",
    }


def high_confidence_multilateral_crdf_providers() -> list:
    return [
        "990",
        "909",
        "915",
        "976",
        "901",
        "905",
        "1024",
        "1015",
        "1011",
        "1016",
        "1313",
        "988",
        "981",
        "910",
        "906",
    ]

import pandas as pd
import numpy as np
from climate_finance.common.schema import ClimateSchema


def build_crs_data(
    years: list[int] = None,
    providers: list[int] = None,
    recipients: list[int] = None,
    adaptation_markers: list[int] = None,
    mitigation_markers: list[int] = None,
    n_rows: int = 10,
    commitment_amount: float = 1000000,
    add_value_column: bool = True,
) -> pd.DataFrame:
    """
    Build synthetic CRS data for testing.

    Args:
        years: List of years. Defaults to [2020].
        providers: List of provider codes. Defaults to [4] (France).
        recipients: List of recipient codes. Defaults to [50] (Bangladesh).
        adaptation_markers: List of adaptation marker values (0, 1, 2).
                           If None, randomly assigned.
        mitigation_markers: List of mitigation marker values (0, 1, 2).
                           If None, randomly assigned.
        n_rows: Number of rows to generate.
        commitment_amount: Default commitment amount per row.
        add_value_column: If True, add a VALUE column based on FLOW_TYPE.
                         This simulates melted CRS data. Defaults to True.

    Returns:
        DataFrame with synthetic CRS data.
    """
    if years is None:
        years = [2020]
    if providers is None:
        providers = [4]
    if recipients is None:
        recipients = [50]

    provider_names = {4: "France", 12: "United Kingdom", 6: "Germany"}
    recipient_names = {50: "Bangladesh", 64: "Ethiopia", 106: "Vietnam"}

    np.random.seed(42)

    data = []
    for i in range(n_rows):
        provider_code = np.random.choice(providers)
        recipient_code = np.random.choice(recipients)

        if adaptation_markers is not None:
            adaptation = adaptation_markers[i % len(adaptation_markers)]
        else:
            adaptation = np.random.choice([0, 1, 2])

        if mitigation_markers is not None:
            mitigation = mitigation_markers[i % len(mitigation_markers)]
        else:
            mitigation = np.random.choice([0, 1, 2])

        # Use exact amount for single row tests, add variation for multi-row tests
        variation = np.random.uniform(-0.2, 0.2) if n_rows > 1 else 0
        usd_commitment = commitment_amount * (1 + variation)
        usd_disbursement = commitment_amount * 0.8 * (1 + variation)

        row = {
            ClimateSchema.YEAR: np.random.choice(years),
            ClimateSchema.PROVIDER_CODE: provider_code,
            ClimateSchema.PROVIDER_NAME: provider_names.get(provider_code, "Unknown"),
            ClimateSchema.RECIPIENT_CODE: recipient_code,
            ClimateSchema.RECIPIENT_NAME: recipient_names.get(recipient_code, "Unknown"),
            ClimateSchema.ADAPTATION: adaptation,
            ClimateSchema.MITIGATION: mitigation,
            ClimateSchema.USD_COMMITMENT: usd_commitment,
            ClimateSchema.USD_DISBURSEMENT: usd_disbursement,
            ClimateSchema.FLOW_TYPE: "gross_disbursements",
            ClimateSchema.CURRENCY: "USD",
            ClimateSchema.PRICES: "current",
            ClimateSchema.CRS_ID: f"2020{i:06d}",
        }

        # Add VALUE column based on flow type to simulate melted data
        if add_value_column:
            row[ClimateSchema.VALUE] = usd_disbursement

        data.append(row)

    return pd.DataFrame(data)


def build_contributions_data(
    years: list[int] = None,
    providers: list[int] = None,
    agencies: list[int] = None,
    contribution_amount: float = 10000000,
    n_rows: int = 10,
) -> pd.DataFrame:
    """
    Build synthetic multilateral contributions data.

    Args:
        years: List of years. Defaults to [2020].
        providers: List of bilateral provider codes. Defaults to [4, 12].
        agencies: List of multilateral agency codes. Defaults to [901, 905].
        contribution_amount: Default contribution amount.
        n_rows: Number of rows to generate.

    Returns:
        DataFrame with synthetic contributions data.
    """
    if years is None:
        years = [2020]
    if providers is None:
        providers = [4, 12]
    if agencies is None:
        agencies = [901, 905]  # World Bank, Asian Development Bank

    provider_names = {4: "France", 12: "United Kingdom", 6: "Germany"}
    agency_names = {
        901: "World Bank",
        905: "Asian Development Bank",
        909: "African Development Fund",
    }

    np.random.seed(43)

    data = []
    for i in range(n_rows):
        provider_code = np.random.choice(providers)
        agency_code = np.random.choice(agencies)

        row = {
            ClimateSchema.YEAR: np.random.choice(years),
            ClimateSchema.PROVIDER_CODE: provider_code,
            ClimateSchema.PROVIDER_NAME: provider_names.get(provider_code, "Unknown"),
            ClimateSchema.AGENCY_CODE: agency_code,
            ClimateSchema.AGENCY_NAME: agency_names.get(agency_code, "Unknown"),
            ClimateSchema.USD_COMMITMENT: contribution_amount * (1 + np.random.uniform(-0.1, 0.1)),
            ClimateSchema.FLOW_TYPE: "commitments",
            ClimateSchema.CURRENCY: "USD",
            ClimateSchema.PRICES: "current",
        }
        data.append(row)

    return pd.DataFrame(data)


def build_spending_shares(
    agencies: list[int] = None,
    years: list[int] = None,
    climate_share: float = 0.25,
    n_rows: int = 5,
) -> pd.DataFrame:
    """
    Build synthetic spending shares for multilateral agencies.

    Args:
        agencies: List of agency codes. Defaults to [901, 905].
        years: List of years. Defaults to [2020].
        climate_share: Climate finance share (0-1). Defaults to 0.25.
        n_rows: Number of rows to generate.

    Returns:
        DataFrame with synthetic spending shares.
    """
    if agencies is None:
        agencies = [901, 905]
    if years is None:
        years = [2020]

    agency_names = {
        901: "World Bank",
        905: "Asian Development Bank",
        909: "African Development Fund",
    }

    np.random.seed(44)

    data = []
    for i in range(n_rows):
        agency_code = np.random.choice(agencies)

        row = {
            ClimateSchema.YEAR: np.random.choice(years),
            ClimateSchema.AGENCY_CODE: agency_code,
            ClimateSchema.AGENCY_NAME: agency_names.get(agency_code, "Unknown"),
            ClimateSchema.INDICATOR: "climate_adaptation",
            ClimateSchema.SHARE: climate_share * (1 + np.random.uniform(-0.1, 0.1)),
            ClimateSchema.FLOW_TYPE: "gross_disbursements",
        }
        data.append(row)

    return pd.DataFrame(data)

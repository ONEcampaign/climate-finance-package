import pandas as pd

from climate_finance.unfccc.cleaning_tools.tools import (
    ADAPTATION,
    CROSS_CUTTING,
    MITIGATION,
    OTHER,
    clean_currency,
    clean_status,
    fill_financial_instrument_gaps,
    fill_type_of_support_gaps,
    harmonise_type_of_support,
    rename_columns,
    clean_recipient_names,
    clean_funding_source,
)


def test_clean_currency():
    # Let's create a DataFrame with different scenarios to test
    df = pd.DataFrame(
        {
            "currency": [
                "USD",
                "(EUR)",
                "100 (JPY)",
                "(GBP) pounds",
                "not a currency",
                "",
                None,
            ],
        }
    )

    # This is the DataFrame we expect to get after cleaning the currency column
    expected = pd.DataFrame(
        {
            "currency": ["USD", "EUR", "JPY", "GBP", None, None, None],
        }
    )

    # Clean the currency column of the DataFrame using the function
    result = clean_currency(df)

    # Check if the resulting DataFrame matches our expected DataFrame
    pd.testing.assert_frame_equal(result, expected)


def test_fill_type_of_support_gaps():
    # Create a DataFrame with missing values in the 'type_of_support' column
    df = pd.DataFrame(
        {
            "type_of_support": [
                "Adaptation",
                None,
                "Mitigation",
                None,
                "Cross-cutting",
                None,
            ],
        }
    )

    # This is the DataFrame we expect to get after filling the gaps in the 'type_of_support' column
    expected = pd.DataFrame(
        {
            "type_of_support": [
                "Adaptation",
                "Cross-cutting",
                "Mitigation",
                "Cross-cutting",
                "Cross-cutting",
                "Cross-cutting",
            ],
        }
    )

    # Fill the gaps in the 'type_of_support' column of the DataFrame using the function
    result = fill_type_of_support_gaps(df)

    # Check if the resulting DataFrame matches our expected DataFrame
    pd.testing.assert_frame_equal(result, expected)


def test_harmonise_type_of_support():
    # Create a DataFrame with different types of strings in the 'type_of_support' column
    df = pd.DataFrame(
        {
            "type_of_support": [
                "ADAPTATION",
                "cross-Cutting measures",
                "Other measures",
                "mitigation project",
                "unknown",
                None,
            ],
        }
    )

    # This is the DataFrame we expect to get after harmonising the 'type_of_support' column
    expected = pd.DataFrame(
        {
            "type_of_support": [
                ADAPTATION,
                CROSS_CUTTING,
                OTHER,
                MITIGATION,
                pd.NA,
                pd.NA,
            ],
        }
    )

    # Harmonise the 'type_of_support' column of the DataFrame using the function
    result = harmonise_type_of_support(df)

    # Check if the resulting DataFrame matches our expected DataFrame
    pd.testing.assert_frame_equal(result, expected)


def test_fill_financial_instrument():
    # Create a DataFrame with missing values in the 'financial_instrument' column
    df = pd.DataFrame(
        {
            "financial_instrument": ["Grant", None, "Equity", None, "Loan", None],
        }
    )

    # This is the DataFrame we expect to get after filling the gaps in
    # the 'financial_instrument' column
    expected = pd.DataFrame(
        {
            "financial_instrument": [
                "Grant",
                "other",
                "Equity",
                "other",
                "Loan",
                "other",
            ],
        }
    )

    # Fill the gaps in the 'financial_instrument' column of the DataFrame using the function
    result = fill_financial_instrument_gaps(df)

    # Check if the resulting DataFrame matches our expected DataFrame
    pd.testing.assert_frame_equal(result, expected)


def test_clean_status():
    # Create a DataFrame with various status terms in the 'status' column
    df = pd.DataFrame(
        {
            "status": [
                "provided",
                "disbursed",
                "pledged",
                "committed",
                "unknown",
                None,
            ],
        }
    )

    # This is the DataFrame we expect to get after cleaning the 'status' column
    expected = pd.DataFrame(
        {
            "status": [
                "disbursed",
                "disbursed",
                "committed",
                "committed",
                "unknown",
                "unknown",
            ],
        }
    )

    # Clean the 'status' column of the DataFrame using the function
    result = clean_status(df)

    # Check if the resulting DataFrame matches our expected DataFrame
    pd.testing.assert_frame_equal(result, expected)


def test_rename_columns():
    df = pd.DataFrame(
        {
            "Party": ["A", "B"],
            "Status": ["provided", "committed"],
            "Funding source": ["source1", "source2"],
            "Contribution Type": ["type1", "type2"],
            "Data Source": ["source1", "source2"],
            "Recipient country/region": ["region1", "region2"],
        }
    )
    renamed_df = rename_columns(df)
    expected_columns = [
        "party",
        "status",
        "funding_source",
        "indicator",
        "br",
        "recipient",
    ]
    assert list(renamed_df.columns) == expected_columns


def test_clean_recipient_names():
    recipients = pd.Series(
        [
            "Other république Démocratique Du Congo",
            "France",
            "United States of America",
            "México",
        ]
    )
    cleaned_recipients = clean_recipient_names(recipients)
    assert cleaned_recipients.tolist() == [
        "DR Congo",
        "France",
        "United States",
        "México",
    ]


def test_clean_funding_source():
    df = pd.DataFrame(
        {
            "funding_source": [
                "ODA",
                "OOF",
                "other",
                "ODA/OOF",
                "unknown",
                "ODA other",
                "OOF other",
                "ODA/OOF other",
            ]
        }
    )
    cleaned_df = clean_funding_source(df)
    expected_results = [
        "oda",
        "oof",
        "other",
        "oda/oof",
        "unknown",
        "oda",
        "oof",
        "oda/oof",
    ]
    assert cleaned_df["funding_source"].tolist() == expected_results

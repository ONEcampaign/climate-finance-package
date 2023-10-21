from unittest.mock import MagicMock, patch

import pandas as pd

from climate_finance.oecd.multisystem.get_data import (
    _clean_multi_contributions,
    get_multilateral_contributions,
)

TEST_DF = pd.DataFrame(
    {
        "year": [2020, 2021],
        "indicator": [
            "multisystem_multilateral_contributions_disbursement_gross",
            "multisystem_multilateral_contributions_commitments_gross",
        ],
        "donor_code": [1, 2],
        "donor_name": ["DN1", "DN2"],
        "channel_code": [47144, 47043],
        "value": [1000, 2000],
    }
)


# Test for get_multilateral_contributions function
@patch("climate_finance.oecd.multisystem.get_data.ODAData")
def test_get_multilateral_contributions(mock_ODAData):
    # Create a mock ODAData object
    mock_oda = MagicMock()
    mock_oda.get_data.return_value = TEST_DF
    mock_ODAData.return_value = mock_oda

    # Call the function
    get_multilateral_contributions(2020, 2021)

    mock_ODAData.assert_called_once()

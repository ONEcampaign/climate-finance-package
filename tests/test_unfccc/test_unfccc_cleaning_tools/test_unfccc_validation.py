from unittest.mock import patch

import pandas as pd

from climate_finance.unfccc.cleaning_tools.validation import (
    _check_brs,
    _check_years,
    _check_parties,
    check_unfccc_data,
)


def test_check_brs(caplog):
    df = pd.DataFrame({"Data source": ["BR_1", "BR_2", "BR_3"]})
    _check_brs(df, [1, 2, 3])
    assert "The available data did not include the following BRs" not in caplog.text

    _check_brs(df, [1, 2, 4])
    assert "The available data did not include the following BRs [4]" in caplog.text

    _check_brs(df, 4)
    assert "The available data did not include the following BRs [4]" in caplog.text

    df = pd.DataFrame({"Data Source": ["BR_1", "BR_2", "BR_3"]})
    _check_brs(df, [1, 2, 4])
    assert "The available data did not include the following BRs [4]" in caplog.text


def test_check_years(caplog):
    df = pd.DataFrame({"Year": [2000, 2001, 2002]})
    _check_years(df, 2000, 2002)
    assert "The available data did not include the following years" not in caplog.text

    _check_years(df, 2000, 2003)
    assert (
        "The available data did not include the following years [2003]" in caplog.text
    )

    df = pd.DataFrame({"year": [2000, 2001, 2002]})
    _check_years(df, 2000, 2003)
    assert (
        "The available data did not include the following years [2003]" in caplog.text
    )


def test_check_parties(caplog):
    df = pd.DataFrame({"Party": ["Party1", "Party2", "Party3"]})
    _check_parties(df, ["Party1", "Party2"])
    assert "The available data did not include the following parties" not in caplog.text

    _check_parties(df, ["Party1", "Party4"])
    assert (
        "The available data did not include the following parties ['Party4']"
        in caplog.text
    )

    df = pd.DataFrame({"party": ["Party1", "Party2", "Party3"]})

    _check_parties(df, ["Party1", "Party4"])
    assert (
        "The available data did not include the following parties ['Party4']"
        in caplog.text
    )


@patch("climate_finance.unfccc.cleaning_tools.validation._check_brs")
@patch("climate_finance.unfccc.cleaning_tools.validation._check_years")
@patch("climate_finance.unfccc.cleaning_tools.validation._check_parties")
def test_check_unfccc_data(mock_check_parties, mock_check_years, mock_check_brs):
    df = pd.DataFrame()
    check_unfccc_data(df, "Party1", 1, 2000, 2002)
    mock_check_brs.assert_called_once()
    mock_check_years.assert_called_once()
    mock_check_parties.assert_called_once()

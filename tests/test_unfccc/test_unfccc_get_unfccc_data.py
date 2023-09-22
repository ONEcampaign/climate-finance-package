from unittest.mock import patch

import pytest

from climate_finance.unfccc.get_unfccc_data import (
    bilateral_unfccc,
    multilateral_unfccc,
    summary_unfccc,
)


@patch("climate_finance.unfccc.get_unfccc_data.unfccc_bilateral_from_interface")
def test_bilateral_unfccc(mock_unfccc_bilateral):
    bilateral_unfccc(
        2020,
        2022,
        party="PartyA",
        br=3,
        update_data=True,
        data_source="data_interface",
    )

    mock_unfccc_bilateral.assert_called_once_with(
        start_year=2020,
        end_year=2022,
        party="PartyA",
        br=3,
        force_download=True,
    )

    with pytest.raises(NotImplementedError):
        bilateral_unfccc(
            2020,
            2022,
            party="PartyA",
            br=3,
            update_data=True,
            data_source="br_files",
        )

    with pytest.raises(ValueError):
        bilateral_unfccc(
            2020,
            2022,
            party="PartyA",
            br=3,
            update_data=True,
            data_source="fdsg",
        )


@patch("climate_finance.unfccc.get_unfccc_data.unfccc_multilateral_from_interface")
def test_multilateral_unfccc(mock_unfccc_multi):
    multilateral_unfccc(
        2020,
        2022,
        party="PartyA",
        br=3,
        update_data=True,
        data_source="data_interface",
    )

    mock_unfccc_multi.assert_called_once_with(
        start_year=2020,
        end_year=2022,
        party="PartyA",
        br=3,
        force_download=True,
    )

    with pytest.raises(NotImplementedError):
        multilateral_unfccc(
            2020,
            2022,
            party="PartyA",
            br=3,
            update_data=True,
            data_source="br_files",
        )

    with pytest.raises(ValueError):
        multilateral_unfccc(
            2020,
            2022,
            party="PartyA",
            br=3,
            update_data=True,
            data_source="fdsg",
        )


@patch("climate_finance.unfccc.get_unfccc_data.unfccc_summary_from_interface")
def test_summary_unfccc(mock_unfccc_summary):
    summary_unfccc(
        2020,
        2022,
        party="PartyA",
        br=3,
        update_data=True,
        data_source="data_interface",
    )

    mock_unfccc_summary.assert_called_once_with(
        start_year=2020,
        end_year=2022,
        party="PartyA",
        br=3,
        force_download=True,
    )

    with pytest.raises(NotImplementedError):
        summary_unfccc(
            2020,
            2022,
            party="PartyA",
            br=3,
            update_data=True,
            data_source="br_files",
        )

    with pytest.raises(ValueError):
        summary_unfccc(
            2020,
            2022,
            party="PartyA",
            br=3,
            update_data=True,
            data_source="fdsg",
        )

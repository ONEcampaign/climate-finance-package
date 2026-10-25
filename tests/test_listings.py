"""`cf-build listings`: the listings gain every code the OECD files carry."""

from pathlib import Path

import polars as pl
import pytest

from climate_finance.build.listings import (
    LISTING_FILES,
    ListingsError,
    refresh_listings,
)

CRS = {
    "year": [1999, 2020, 2021, 2021, 2021, 2021],
    "donor_code": [8, 8, 8, 1058, 1058, 8],
    "donor_name": ["Norway", "Norway", "Norway", "IMF RST", "IMF RST", "Norway"],
    "agency_code": [1, 1, 0, None, 1, 3],
    "agency_name": ["x", "Norad", "y", None, "IMF RST", "Norfund"],
    # Netherlands Antilles reports only before the first CRDF year.
    "recipient_code": [361, 434, 434, 248, 248, 248],
    "recipient_name": ["Antilles", "Chili", "Chile", "Kenya", "Kenya", "Kenya"],
}
CRDF = {
    "year": [2020, 2021],
    "provider_code": [1058, 8],
    "provider_name": [None, "Norway"],
    "agency_code": [None, 1],
    "agency_name": [None, "Norad"],
    "recipient_code": [434, 248],
    "recipient_name": ["Chile", "Kenya"],
}
# The provider perspective's recipient is a multilateral pool.
CRDF_PROVIDER = CRDF | {"recipient_code": [3000, 3000], "recipient_name": ["P1"] * 2}
MULTI = {
    "year": [2021, 2021],
    "donor_code": [8, 20001],
    "donor_name": ["Norway", "DAC countries"],
}
LISTED = {
    "providers": "code,name,name_fr,type\n8,Norway,Norvège,DAC member\n",
    "agencies": "code,agency,name\n8,1,Norad\n",
    "recipients": "code,name\n248,Kenya\n",
}


def _raw(tmp_path: Path, crs: dict[str, list]) -> Path:
    oecd = tmp_path / "raw" / "oecd"
    oecd.mkdir(parents=True)
    for name, frame in {
        "CRS": crs,
        "crdf_recipient": CRDF,
        "crdf_provider": CRDF_PROVIDER,
        "multisystem": MULTI,
    }.items():
        pl.DataFrame(frame).write_parquet(oecd / f"{name}.parquet")
    return tmp_path / "raw"


def _data(tmp_path: Path) -> Path:
    for listing, text in LISTED.items():
        (tmp_path / LISTING_FILES[listing]).write_text(text, encoding="utf-8")
    return tmp_path


def test_a_refresh_adds_each_code_the_files_carry_with_its_latest_name(
    tmp_path: Path,
) -> None:
    data = _data(tmp_path)

    added = refresh_listings(_raw(tmp_path, CRS), data_dir=data)

    assert {listing: frame.rows() for listing, frame in added.items()} == {
        "providers": [(1058, "IMF RST")],
        "agencies": [(8, 3, "Norfund"), (1058, 1, "IMF RST")],
        "recipients": [(434, "Chile")],
    }
    assert pl.read_csv(data / LISTING_FILES["providers"]).rows() == [
        (8, "Norway", "Norvège", "DAC member"),
        (1058, "IMF RST", None, None),
    ]
    again = refresh_listings(_raw(tmp_path / "again", CRS), data_dir=data)
    assert {listing: frame.height for listing, frame in again.items()} == dict.fromkeys(
        added, 0
    )


def test_a_refresh_refuses_a_code_no_file_names(tmp_path: Path) -> None:
    unnamed = CRS | {"agency_name": [*CRS["agency_name"][:-1], None]}

    with pytest.raises(ListingsError, match="agencies 8/3"):
        refresh_listings(_raw(tmp_path, unnamed), data_dir=_data(tmp_path))

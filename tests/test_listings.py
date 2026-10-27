"""`cf-build listings`: the listings gain every code the OECD files carry."""

from pathlib import Path

import polars as pl
import pytest

from climate_finance.build.listings import (
    LISTING_FILES,
    Listing,
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
    # Recent CRS rows can lack an income group, and the CRDF's gives Kenya its type.
    "incomegroup_name": [None, "UMICs", "UMICs", None, None, None],
    "region_name": [None, "South America", "South America", None, None, None],
}
UNALLOCATED = "Part I unallocated by income"
MDB = "Multilateral development bank"
CRDF = {
    "year": [2020, 2021, 2021, 2021],
    "provider_code": [1058, 8, 905, 905],
    "provider_name": [None, "Norway", "IDA", "IDA"],
    # A blank type is no bank, though polars' `all()` skips nulls.
    "provider_type": [None, "DAC member", MDB, MDB],
    "agency_code": [None, 1, None, None],
    "agency_name": [None, "Norad", None, None],
    "recipient_code": [434, 248, 298, 998],
    "recipient_name": ["Chile", "Kenya", "Africa, regional", "Developing, unspecified"],
    "recipient_income_group": ["UMICs", "LMICs", UNALLOCATED, UNALLOCATED],
    "recipient_region": [
        "South America",
        "South of Sahara",
        "Africa",
        "Developing countries unspecified",
    ],
}
# The provider perspective's recipient is a multilateral pool.
UNTYPED_CRS = CRS | {"incomegroup_name": [None] * 6}
UNTYPED_CRDF = CRDF | {
    "recipient_income_group": [None, *CRDF["recipient_income_group"][1:]]
}
CRDF_PROVIDER = CRDF | {"recipient_code": [3000] * 4, "recipient_name": ["P1"] * 4}
MULTI = {
    "year": [2021, 2021],
    "donor_code": [8, 20001],
    "donor_name": ["Norway", "DAC countries"],
}
LISTED: dict[Listing, str] = {
    "providers": "code,name,name_fr,type\n8,Norway,Norvège,DAC member\n",
    "agencies": "code,agency,name\n8,1,Norad\n",
    "recipients": "code,name\n248,Kenya\n",
}


def _raw(tmp_path: Path, crs: dict[str, list], *, crdf: dict[str, list] = CRDF) -> Path:
    oecd = tmp_path / "raw" / "oecd"
    oecd.mkdir(parents=True)
    for name, frame in {
        "CRS": crs,
        "crdf_recipient": crdf,
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
        "providers": [(905, "IDA"), (1058, "IMF RST")],
        "agencies": [(8, 3, "Norfund"), (1058, 1, "IMF RST")],
        "recipients": [
            (298, "Africa, regional", "regional"),
            (434, "Chile", "country"),
            (998, "Developing, unspecified", "unspecified"),
        ],
    }
    assert pl.read_csv(data / LISTING_FILES["providers"]).rows() == [
        (8, "Norway", "Norvège", "DAC member", False),
        (905, "IDA", None, None, True),
        (1058, "IMF RST", None, None, False),
    ]
    # A listed recipient takes its type from the files too.
    assert pl.read_csv(data / LISTING_FILES["recipients"]).filter(code=248).rows() == [
        (248, "Kenya", "country")
    ]
    again = refresh_listings(_raw(tmp_path / "again", CRS), data_dir=data)
    assert {listing: frame.height for listing, frame in again.items()} == dict.fromkeys(
        added, 0
    )


@pytest.mark.parametrize(
    ("crs", "crdf", "match"),
    [
        (CRS | {"agency_name": [*CRS["agency_name"][:-1], None]}, CRDF, "8/3"),
        (UNTYPED_CRS, UNTYPED_CRDF, r"no type: 434\. Add each to recipients\.csv"),
    ],
)
def test_a_refresh_refuses_a_code_no_file_names_or_types(
    tmp_path: Path, crs: dict[str, list], crdf: dict[str, list], match: str
) -> None:
    data = _data(tmp_path)
    with pytest.raises(ListingsError, match=match):
        refresh_listings(_raw(tmp_path, crs, crdf=crdf), data_dir=data)

    # Nothing is written before the refusal.
    assert {
        listing: (data / name).read_text(encoding="utf-8")
        for listing, name in LISTING_FILES.items()
    } == LISTED


def test_a_recipient_the_files_do_not_type_keeps_its_listed_type(
    tmp_path: Path,
) -> None:
    data = _data(tmp_path)
    (data / LISTING_FILES["recipients"]).write_text(
        "code,name,type\n248,Kenya,country\n434,Chile,country\n", encoding="utf-8"
    )

    refresh_listings(_raw(tmp_path, UNTYPED_CRS, crdf=UNTYPED_CRDF), data_dir=data)

    assert pl.read_csv(data / LISTING_FILES["recipients"]).filter(code=434).rows() == [
        (434, "Chile", "country")
    ]

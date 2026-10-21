"""Organisation names resolved to DAC codes on exact matches only."""

import polars as pl
import pytest

from climate_finance import schema
from climate_finance.build.normalise.orgs import org_names, resolve_orgs

_NAMES = {"role": pl.String, "source_name": pl.String, "scope_code": pl.Int32}
_CHANNELS = {
    "channel_code": pl.Int32,
    "channel_name": pl.String,
    "channel_reported_name": pl.String,
}


def _resolve(
    names: list[tuple[str, str, int | None]],
    channels: list[tuple[int | None, str | None, str | None]] = (),
) -> pl.DataFrame:
    orgs = resolve_orgs(
        pl.LazyFrame(names, schema=_NAMES, orient="row"),
        channels=pl.LazyFrame(list(channels), schema=_CHANNELS, orient="row"),
    )
    schema.validate(schema.ORGS, orgs)
    return orgs


def _codes(orgs: pl.DataFrame, column: str) -> dict[str, tuple[object, str]]:
    return {
        name: (code, method)
        for name, code, method in orgs.select("source_name", column, "method").rows()
    }


def test_providers_resolve_on_exact_names_only() -> None:
    orgs = _resolve(
        [
            ("provider", "Germany", None),
            # The org pack labels this exact name full-text.
            ("provider", "Gates Foundation", None),
            # The geo pack offers the United States, at its fuzzy tier.
            ("provider", "UNAIDS", None),
            ("provider", "Lilongwe Youth Organization", None),
        ]
    )

    assert _codes(orgs, "donor_code") == {
        "Germany": (5, "exact_name"),
        "Gates Foundation": (1601, "exact_name"),
        "UNAIDS": (971, "exact_name"),
    }


def test_agencies_resolve_within_their_donor_only() -> None:
    name = "Norwegian Agency for Development Co-operation"
    orgs = _resolve([("agency", name, 8), ("agency", name, 10)])

    assert orgs.select("donor_code", "agency_code").rows() == [(8, 1)]


@pytest.mark.parametrize(
    ("name", "scope", "expected"),
    [
        ("CDC Group", 12, (12, 2)),
        ("IFU", None, (3, 3)),
        ("Norfund", 10, None),
    ],
)
def test_the_dfi_pack_adds_agencies_the_org_pack_misses(
    name: str, scope: int | None, expected: tuple[int, int] | None
) -> None:
    orgs = _resolve([("agency", name, scope)])

    if expected is None:
        assert orgs.is_empty()
    else:
        assert orgs.select("donor_code", "agency_code", "method").rows() == [
            (*expected, "pack")
        ]


def test_channel_strings_take_their_coded_rows_code_else_a_codelist_name() -> None:
    undp = (41114, "United Nations Development Programme", None)
    channels = [
        # Nine of ten coded rows give "Red Cross Kenya" one code.
        *[(23000, "Recipient NGOs", "Red Cross Kenya")] * 9,
        (21000, "International NGOs", "Red Cross Kenya"),
        # Eight of ten is not enough.
        *[(23000, "Recipient NGOs", "Wildlife Trust")] * 8,
        *[(21000, "International NGOs", "Wildlife Trust")] * 2,
        # Codes 0 and 99999 name no channel.
        *[(99999, None, "Village Fund")] * 3,
        undp,
    ]
    orgs = _resolve(
        [
            ("channel", "Red Cross Kenya", None),
            ("channel", "Wildlife Trust", None),
            ("channel", "Village Fund", None),
            ("channel", "UNITED NATIONS DEVELOPMENT PROGRAMME", None),
            ("channel", "United Nations Development Programme Kenya", None),
        ],
        channels,
    )

    assert _codes(orgs, "channel_code") == {
        "Red Cross Kenya": (23000, "propagated"),
        "UNITED NATIONS DEVELOPMENT PROGRAMME": (41114, "exact_name"),
    }


def test_org_names_lists_each_name_once_per_role_and_donor() -> None:
    crs = pl.LazyFrame(
        {
            "donor_code": [8, 8, 10],
            "donor_name": ["Norway", "Norway", "Sweden"],
            "agency_name": ["Norfund", " Norfund ", "Norfund"],
            "channel_code": [None, 99999, 41114],
            "channel_reported_name": ["Village Fund", "X", "UNDP"],
        },
        schema_overrides={"channel_code": pl.Int32, "donor_code": pl.Int32},
    )
    crdf = pl.LazyFrame(
        {
            "provider_code": [8],
            "provider_name": ["Norway"],
            "agency_name": ["Norfund"],
            "channel_name": ["Norwegian Red Cross"],
        },
        schema_overrides={"provider_code": pl.Int32},
    )

    names = org_names(crs=crs, crdf=[crdf]).collect()

    assert set(names.rows()) == {
        ("provider", "Norway", None),
        ("provider", "Sweden", None),
        ("agency", "Norfund", 8),
        ("agency", "Norfund", 10),
        # A channel name is read where the CRS row has no channel code; one
        # character names nothing.
        ("channel", "Village Fund", None),
        ("channel", "Norwegian Red Cross", None),
    }

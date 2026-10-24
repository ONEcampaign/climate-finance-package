"""Listings, and provider and recipient names resolved to DAC codes."""

from typing import get_args

import pytest
from make_fixture import FIXTURE_DIR
from polars.testing import assert_frame_equal

import climate_finance as cf
from climate_finance.catalogue import Catalogue
from climate_finance.spec import Flow

SNAP = cf.load(path=FIXTURE_DIR)


@pytest.mark.parametrize(
    ("providers", "recipients", "codes"),
    [
        ("Germany", "Kenya", ([5], [248])),
        (["germany", "FRANCE"], ["kenya", "Bangladesh"], ([5, 4], [248, 666])),
    ],
)
def test_names_select_the_same_rows_as_their_codes(
    providers: str | list[str],
    recipients: str | list[str],
    codes: tuple[list[int], list[int]],
) -> None:
    by_name = SNAP.spending(providers=providers, recipients=recipients)
    by_code = SNAP.spending(providers=codes[0], recipients=codes[1])

    assert by_name.table.to_polars().height
    assert_frame_equal(by_name.table.to_polars(), by_code.table.to_polars())


def test_names_match_without_accents_or_typographic_apostrophes() -> None:
    assert cf.recipients.code("Cote d'Ivoire") == 247
    assert cf.recipients.code("Türkiye") == cf.recipients.code("turkiye")
    assert cf.recipients.code("China (People\u2019s Republic of)") == 730


@pytest.mark.parametrize(
    ("argument", "name", "error", "suggestion"),
    [
        ("providers", "Germny", cf.UnknownProviderError, "Germany"),
        ("providers", "Development Bank", cf.UnknownProviderError, None),
        ("recipients", "Kenia", cf.UnknownRecipientError, "Kenya"),
        ("recipients", "Cote", cf.UnknownRecipientError, "Côte d'Ivoire"),
    ],
)
def test_an_unknown_name_raises_with_suggestions_and_never_substitutes(
    argument: str, name: str, error: type[cf.UnknownNameError], suggestion: str | None
) -> None:
    with pytest.raises(error) as raised:
        SNAP.spending(**{argument: name})

    assert raised.value.name == name
    assert raised.value.suggestions
    if suggestion is not None:
        assert raised.value.suggestions[0] == suggestion


def test_a_name_with_nothing_close_raises_without_suggestions() -> None:
    with pytest.raises(cf.UnknownProviderError) as raised:
        cf.providers.code("zzzz")

    assert raised.value.suggestions == ()


def test_codes_pass_through_without_a_lookup() -> None:
    # 1635 is a CRS provider code missing from the provider listing.
    assert SNAP.spending(providers=1635).table.to_polars().is_empty()


def test_listings_hold_the_entries_the_api_accepts() -> None:
    assert set(cf.flows.list()["name"]) == set(get_args(Flow.__value__))
    assert cf.methods.list()["name"].to_list() == ["ONE", "OECD", "PROVIDER", "RANGE"]
    germany = cf.providers.list().filter(code=5).row(0, named=True)
    assert germany == {"code": 5, "name": "Germany", "type": "DAC member"}
    assert cf.recipients.list().filter(code=248)["name"].to_list() == ["Kenya"]


def test_listing_codes_are_unique() -> None:
    for listing in (cf.providers, cf.recipients):
        assert listing.list()["code"].is_unique().all()


def test_search_matches_part_of_a_name_ignoring_case() -> None:
    assert set(cf.recipients.search("congo")["name"]) == {
        "Congo",
        "Democratic Republic of the Congo",
    }
    assert cf.methods.search("one")["name"].to_list() == ["ONE"]


@pytest.mark.parametrize(
    ("listing", "name", "expected"),
    [
        (cf.providers, "Germany", "Germany (DAC code 5): DAC member."),
        (cf.providers, 5, "Germany (DAC code 5): DAC member."),
        (cf.recipients, 248, "Kenya (DAC code 248)."),
    ],
)
def test_info_describes_an_entry_by_name_or_code(
    listing: Catalogue, name: str | int, expected: str
) -> None:
    assert listing.info(name) == expected


def test_method_info_is_the_method_description() -> None:
    assert cf.methods.info("PROVIDER") == cf.methods.describe(cf.methods.PROVIDER)
    assert "own significant coefficient" in cf.methods.info("PROVIDER")


@pytest.mark.parametrize(
    ("listing", "name", "kind"),
    [(cf.flows, "net_disbursements", "flow"), (cf.providers, 99999, "provider")],
)
def test_info_on_an_unknown_entry_names_the_listing(
    listing: Catalogue, name: str | int, kind: str
) -> None:
    with pytest.raises(cf.UnknownNameError) as raised:
        listing.info(name)

    assert raised.value.kind == kind

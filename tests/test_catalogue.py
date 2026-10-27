"""Listings, and provider and recipient names resolved to DAC codes."""

from typing import Any, Literal, get_args

import numpy as np
import pytest
from make_fixture import FIXTURE_DIR
from polars.testing import assert_frame_equal

import climate_finance as cf
from climate_finance.catalogue import Agency, Catalogue, resolve
from climate_finance.spec import Flow, SpecInputs, read_provider_codes

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

    assert by_name.table.height
    assert_frame_equal(by_name.table, by_code.table)


def test_names_match_without_accents_or_typographic_apostrophes() -> None:
    assert cf.recipients.code("Cote d'Ivoire") == 247
    assert cf.recipients.code("Türkiye") == cf.recipients.code("turkiye")
    assert cf.recipients.code("China (People\u2019s Republic of)") == 730


@pytest.mark.parametrize(
    ("argument", "name", "error", "suggestion"),
    [
        ("providers", "Germny", cf.UnknownProviderError, "Germany"),
        ("providers", "Development Bank", cf.UnknownProviderError, None),
        ("recipients", "Kenyaa", cf.UnknownRecipientError, "Kenya"),
        ("recipients", "Cote", cf.UnknownRecipientError, "Côte d'Ivoire"),
    ],
)
def test_an_unknown_name_raises_with_suggestions_and_never_substitutes(
    argument: str, name: str, error: type[cf.UnknownNameError], suggestion: str | None
) -> None:
    keywords: dict[str, Any] = {argument: name}
    with pytest.raises(error) as raised:
        SNAP.spending(**keywords)

    assert raised.value.name == name
    assert raised.value.suggestions
    if suggestion is not None:
        assert raised.value.suggestions[0] == suggestion


EIB = Agency(provider=918, agency=3)


@pytest.mark.parametrize(
    ("values", "kind", "expected"),
    [
        (["DEU", "Allemagne", 5, " Germany ", "DE", np.int64(5)], "providers", (5,)),
        ("EIB", "providers", (EIB,)),
        ("EC", "providers", (Agency(provider=918, agency=1),)),
        (["EIB", 918], "providers", (918,)),
        ([EIB, "KfW", 4, "4"], "providers", (4, Agency(provider=5, agency=2), EIB)),
        # Agency 0 and 1 stand for the provider itself, listed or not.
        (Agency(provider=4, agency=0), "providers", (Agency(provider=4, agency=0),)),
        # A group names each of its providers.
        ("World Bank", "providers", (901, 905)),
        (["world bank", "IDA", EIB], "providers", (901, 905, EIB)),
        ("KEN", "recipients", (248,)),
        ("Kenia", "recipients", (248,)),
        (None, "providers", None),
    ],
)
def test_resolve_reads_any_common_form_as_codes_and_agencies(
    values: str | list[str | int | Agency] | None,
    kind: Literal["providers", "recipients"],
    expected: tuple[int | Agency, ...] | None,
) -> None:
    assert resolve(values, kind=kind) == expected


@pytest.mark.parametrize(
    ("name", "kind", "error", "count", "candidate"),
    [
        (
            "Miscellaneous",
            "providers",
            cf.UnknownProviderError,
            36,
            "Miscellaneous (Germany)",
        ),
        ("Kongo", "recipients", cf.UnknownRecipientError, 2, "Congo"),
    ],
)
def test_a_name_of_several_entries_raises_with_every_candidate(
    name: str,
    kind: Literal["providers", "recipients"],
    error: type[cf.UnknownNameError],
    count: int,
    candidate: str,
) -> None:
    with pytest.raises(error) as raised:
        resolve(name, kind=kind)

    assert raised.value.note is not None
    assert len(raised.value.suggestions) == count
    assert candidate in raised.value.suggestions


def test_a_group_reads_as_its_providers_and_code_refuses_it() -> None:
    assert read_provider_codes("World Bank", reader="reconcile()") == (901, 905)
    with pytest.raises(cf.UnknownProviderError) as raised:
        cf.providers.code("World Bank")

    assert raised.value.suggestions == (
        "International Bank for Reconstruction and Development",
        "International Development Association",
    )


def test_an_unknown_name_suggests_the_display_names_of_close_keys() -> None:
    with pytest.raises(cf.UnknownProviderError) as raised:
        resolve("Dutchland", kind="providers")

    assert raised.value.note is None
    assert "Netherlands" in raised.value.suggestions


@pytest.mark.parametrize(
    ("values", "kind"),
    [
        (True, "providers"),
        ([5, False], "providers"),
        (1.5, "providers"),
        (EIB, "recipients"),
    ],
)
def test_resolve_refuses_bools_other_types_and_recipient_agencies(
    values: object, kind: Literal["providers", "recipients"]
) -> None:
    with pytest.raises(TypeError):
        # The float row is outside the annotation on purpose.
        resolve(values, kind=kind)  # ty: ignore[invalid-argument-type]


@pytest.mark.parametrize(
    ("listing", "name", "error", "other"),
    [
        (cf.recipients, "Iceland", cf.UnknownRecipientError, "provider Iceland"),
        (cf.providers, "kenya", cf.UnknownProviderError, "recipient Kenya"),
    ],
)
def test_a_name_of_the_other_listing_raises_naming_it_without_suggestions(
    listing: Catalogue, name: str, error: type[cf.UnknownNameError], other: str
) -> None:
    with pytest.raises(error) as raised:
        listing.code(name)

    assert raised.value.suggestions == ()
    assert raised.value.note == (
        f"{name!r} names the {other}, which cf.{listing.kind} does not hold."
    )


def test_a_private_donors_info_says_no_view_counts_it() -> None:
    assert cf.providers.info("Bill & Melinda Gates Foundation").endswith(
        "Private donor. Neither view counts private flows, so spending() returns no "
        "rows for it."
    )
    assert cf.providers.info("Japan") == "Japan (DAC code 701): DAC member."


def test_an_agency_serialises_as_its_provider_and_agency_codes() -> None:
    assert EIB.model_dump() == {"provider": 918, "agency": 3}


def test_a_name_with_nothing_close_raises_without_suggestions() -> None:
    with pytest.raises(cf.UnknownProviderError) as raised:
        cf.providers.code("zzzz")

    assert raised.value.suggestions == ()


@pytest.mark.parametrize(
    ("keywords", "error", "suggestion"),
    [
        ({"providers": 7010}, cf.UnknownProviderError, "701 (Japan)"),
        ({"providers": ["Japan", "7010"]}, cf.UnknownProviderError, "701 (Japan)"),
        (
            {"providers": Agency(provider=918, agency=77)},
            cf.UnknownProviderError,
            "European Investment Bank (agency 3)",
        ),
        ({"recipients": 2480}, cf.UnknownRecipientError, "248 (Kenya)"),
    ],
)
def test_a_code_the_listings_lack_raises_with_close_codes(
    keywords: SpecInputs, error: type[cf.UnknownNameError], suggestion: str
) -> None:
    with pytest.raises(error) as raised:
        SNAP.spending(**keywords)

    assert suggestion in raised.value.suggestions


def test_listings_hold_the_entries_the_api_accepts() -> None:
    assert set(cf.flows.list()["name"]) == set(get_args(Flow.__value__))
    assert cf.methods.list()["name"].to_list() == [
        "ONE",
        "OECD",
        "PROVIDER",
        "OXFAM_LOW",
        "OXFAM_HIGH",
        "RANGE",
        "OXFAM",
    ]
    germany = cf.providers.list().filter(code=5).row(0, named=True)
    assert germany == {"code": 5, "name": "Germany", "type": "DAC member", "mdb": False}
    assert cf.recipients.list().filter(code=248)["name"].to_list() == ["Kenya"]


@pytest.mark.parametrize(
    ("code", "mdb"),
    [(905, True), (903, True), (1044, True), (918, False), (1016, False)],
)
def test_mdb_marks_the_providers_the_crdf_types_as_development_banks(
    code: int, mdb: bool
) -> None:
    assert cf.providers.list().filter(code=code)["mdb"].to_list() == [mdb]


@pytest.mark.parametrize(
    ("code", "kind"),
    [
        # Slovenia and Croatia left the DAC recipient list and are still countries.
        (61, "country"),
        (62, "country"),
        (248, "country"),
        (298, "regional"),
        # The CRDF puts the former Yugoslav states in Europe, so the code is regional.
        (88, "regional"),
        (998, "unspecified"),
        (9998, "unspecified"),
    ],
)
def test_recipient_type_follows_the_oecd_income_group_and_region(
    code: int, kind: str
) -> None:
    recipients = cf.recipients.list()
    assert recipients.filter(code=code)["type"].to_list() == [kind]
    assert set(recipients["type"]) == {"country", "regional", "unspecified"}


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
    ("text", "found"),
    [
        ("kfw", ("KfW Bankengruppe (KfW banking group)", "Agency 2 of Germany", None)),
        ("EIB", ("European Investment Bank", "Agency 3 of EU Institutions", "eib")),
        ("deu", ("Germany", "DAC member", "deu")),
        ("Allemagne", ("Germany", "DAC member", "allemagne")),
        (
            "ger",
            (
                "Ministry of Foreign Affairs",
                "Agency 2 of Japan",
                "ministere des affaires etrangeres",
            ),
        ),
    ],
)
def test_search_finds_every_name_the_resolver_reads(
    text: str, found: tuple[str, str, str | None]
) -> None:
    found_rows = cf.providers.search(text)
    rows = found_rows.select("name", "type", "alias").rows()

    assert found in rows
    assert found_rows.select("code", "type").is_unique().all()
    assert found_rows["alias"].drop_nulls().str.contains(text.casefold()).all()


@pytest.mark.parametrize(
    ("listing", "name", "expected"),
    [
        (cf.providers, "Germany", "Germany (DAC code 5): DAC member."),
        (cf.providers, 5, "Germany (DAC code 5): DAC member."),
        (cf.providers, "DEU", "Germany (DAC code 5): DAC member."),
        (
            cf.providers,
            "EIB",
            "European Investment Bank (agency 3 of EU Institutions, DAC code 918).",
        ),
        (cf.recipients, 248, "Kenya (DAC code 248)."),
    ],
)
def test_info_describes_an_entry_by_name_or_code(
    listing: Catalogue, name: str | int, expected: str
) -> None:
    assert listing.info(name) == expected


def test_code_reads_a_name_or_passes_a_code_through() -> None:
    assert cf.providers.code("Allemagne") == 5
    assert cf.providers.code(8) == 8
    assert cf.providers.code("EIB") == EIB


def test_method_info_is_the_method_description() -> None:
    assert cf.methods.info("PROVIDER") == cf.methods.describe(cf.methods.PROVIDER)
    assert "reporting to the UNFCCC" in cf.methods.info("PROVIDER")


@pytest.mark.parametrize(
    ("listing", "name", "kind"),
    [(cf.flows, "gross_disbursements", "flow"), (cf.providers, 99999, "provider")],
)
def test_info_on_an_unknown_entry_names_the_listing(
    listing: Catalogue, name: str | int, kind: str
) -> None:
    with pytest.raises(cf.UnknownNameError) as raised:
        listing.info(name)

    assert raised.value.kind == kind

"""`cf.compare`, and how results, comparisons and methods print."""

from collections.abc import Iterator
from pathlib import Path

import polars as pl
import pytest
from make_fixture import FIXTURE_DIR
from releases import copy_release

import climate_finance as cf
from climate_finance.errors import InvalidInputError
from climate_finance.methods import ONE, RANGE
from climate_finance.schema import ReleaseManifest
from climate_finance.spec import Measure

SNAP = cf.load(path=FIXTURE_DIR)
KEYS = ["year", "provider_code", "recipient_code", "bi_multi"]
# A comparison's key columns: names beside codes.
COLUMNS = [
    "year",
    "provider",
    "recipient",
    "provider_code",
    "recipient_code",
    "bi_multi",
]


@pytest.fixture(autouse=True)
def _fixed_table_width() -> Iterator[None]:
    with pl.Config(tbl_width_chars=200, tbl_cols=-1, tbl_rows=10):
        yield


def _named_release(path: Path, name: str) -> cf.Release:
    """A copy of the fixture release whose manifest names the release `name`."""
    copy_release(path)
    manifest_path = path / "release.json"
    manifest = ReleaseManifest.model_validate_json(
        manifest_path.read_text(encoding="utf-8")
    )
    manifest_path.write_text(
        manifest.model_copy(update={"release": name}).model_dump_json(),
        encoding="utf-8",
    )
    return cf.load(path=path)


def _values(result: cf.Result) -> pl.DataFrame:
    return result.table.select(*KEYS, "value")


def test_compare_sets_one_value_column_per_result_on_shared_keys() -> None:
    one = SNAP.spending(view="provider")
    oecd = SNAP.spending(view="provider", method=cf.methods.OECD)

    table = cf.compare(one, oecd).table

    assert table.columns == [*COLUMNS, "ONE", "OECD"]
    # Attribution rows have no recipient. Their null key must match across results.
    # Only OECD has a share for AfDB in 2020, so its attribution keys cover ONE's.
    assert table.filter(bi_multi=2).height == _values(oecd).filter(bi_multi=2).height
    for name, result in (("ONE", one), ("OECD", oecd)):
        assert (
            table.select(*KEYS, value=name)
            .drop_nulls("value")
            .sort(KEYS)
            .equals(_values(result).drop_nulls("value").sort(KEYS))
        )


def test_a_key_one_result_lacks_is_null_in_its_column() -> None:
    germany = SNAP.spending(providers="Germany")
    france = SNAP.spending(providers="France")

    table = cf.compare(germany, france).table

    assert table.columns == [*COLUMNS, "providers Germany", "providers France"]
    assert table.height == _values(germany).height + _values(france).height
    assert table.filter(provider_code=5)["providers France"].is_null().all()


@pytest.mark.parametrize("measure", ["amount", "share_of_total"])
def test_an_agency_sits_beside_its_provider_on_one_row(measure: Measure) -> None:
    giz = SNAP.spending(providers=[cf.Agency(provider=5, agency=52)], measure=measure)
    germany = SNAP.spending(providers="Germany", measure=measure)

    table = cf.compare(giz, germany).table
    giz_column, germany_column = table.columns[len(COLUMNS) :]

    # Every Germany row in the fixture is GIZ's, so the two columns match.
    assert table.columns[: len(COLUMNS)] == COLUMNS
    assert (
        table.select(*KEYS).sort(KEYS).equals(_values(germany).select(KEYS).sort(KEYS))
    )
    assert table[giz_column].equals(table[germany_column], check_names=False)
    assert table[giz_column].is_not_null().all()


def test_a_result_under_several_methods_gives_a_column_per_method() -> None:
    table = cf.compare(SNAP.spending(), SNAP.spending(method=RANGE)).table

    assert table.columns == [
        *COLUMNS,
        "ONE",
        "RANGE: ONE",
        "RANGE: OECD",
        "RANGE: PROVIDER",
    ]


def test_compare_names_amounts_and_shares_apart() -> None:
    amounts = SNAP.spending(providers=5)

    table = cf.compare(amounts, amounts.share_of_total()).table

    assert table.columns[len(COLUMNS) :] == ["amounts", "share of total"]


def test_results_nothing_sets_apart_are_numbered() -> None:
    table = cf.compare(SNAP.spending(), SNAP.spending()).table

    assert table.columns[len(COLUMNS) :] == ["result 1: ONE", "result 2: ONE"]


def test_results_of_two_releases_are_named_by_release(tmp_path: Path) -> None:
    older = _named_release(tmp_path / "2025a", "2025a")
    newer = _named_release(tmp_path / "2026a", "2026a")

    table = cf.compare(older.spending(), newer.spending()).table

    assert table.columns[len(COLUMNS) :] == ["2025a", "2026a"]


def test_results_of_two_releases_under_several_methods_keep_the_release(
    tmp_path: Path,
) -> None:
    older = _named_release(tmp_path / "2025a", "2025a")
    newer = _named_release(tmp_path / "2026a", "2026a")

    table = cf.compare(older.spending(method=RANGE), newer.spending(method=RANGE)).table

    assert table.columns[len(COLUMNS) :] == [
        "2025a: ONE",
        "2025a: OECD",
        "2025a: PROVIDER",
        "2026a: ONE",
        "2026a: OECD",
        "2026a: PROVIDER",
    ]


def test_results_of_two_releases_and_two_specs_are_named_by_both(
    tmp_path: Path,
) -> None:
    older = _named_release(tmp_path / "2025a", "2025a")

    table = cf.compare(older.spending(years=2021), SNAP.spending(years=2022)).table

    assert table.columns[len(COLUMNS) :] == [
        "2025a, years 2021",
        "fixture, years 2022",
    ]


def test_results_that_differ_only_in_years_are_named_by_years() -> None:
    table = cf.compare(
        SNAP.spending(years=2021),
        SNAP.spending(years=[2021, 2022]),
        SNAP.spending(),
    ).table

    assert table.columns[len(COLUMNS) :] == [
        "years 2021",
        "years 2021 to 2022",
        "all years",
    ]


def test_compare_needs_a_result() -> None:
    with pytest.raises(InvalidInputError, match="at least one result"):
        cf.compare()


def test_compare_refuses_a_list_naming_the_call_it_takes() -> None:
    result = SNAP.spending()

    with pytest.raises(InvalidInputError, match=r"as cf\.compare\(a, b\)"):
        # A list is outside the type compare() takes; the test checks the run time.
        cf.compare([result, result])  # ty: ignore[invalid-argument-type]


RESULT_REPR = """\
Result: ONE; disbursements; recipient view; providers Germany; years 2021; USD at current prices; release fixture
shape: (1, 23)
┌──────┬────────┬────────┬────────┬────────┬────────┬────────┬────────┬────────┬────────┬────────┬────────┬────────┬────────┬────────┬────────┬────────┬───────┬───────┬───────┬───────┬───────┬───────┐
│ year ┆ provid ┆ recipi ┆ method ┆ provid ┆ recipi ┆ bi_mul ┆ flow   ┆ view   ┆ curren ┆ prices ┆ measur ┆ amount ┆ adapta ┆ mitiga ┆ cross_ ┆ unspli ┆ untra ┆ no_gr ┆ no_ad ┆ value ┆ low   ┆ high  │
│ ---  ┆ er     ┆ ent    ┆ ---    ┆ er_cod ┆ ent_co ┆ ti     ┆ ---    ┆ ---    ┆ cy     ┆ ---    ┆ e      ┆ ---    ┆ tion   ┆ tion   ┆ cuttin ┆ t      ┆ ced   ┆ ant_e ┆ aptat ┆ ---   ┆ ---   ┆ ---   │
│ i16  ┆ ---    ┆ ---    ┆ str    ┆ e      ┆ de     ┆ ---    ┆ str    ┆ str    ┆ ---    ┆ str    ┆ ---    ┆ f64    ┆ ---    ┆ ---    ┆ g      ┆ ---    ┆ ---   ┆ quiva ┆ ion_m ┆ f64   ┆ f64   ┆ f64   │
│      ┆ str    ┆ str    ┆        ┆ ---    ┆ ---    ┆ i8     ┆        ┆        ┆ str    ┆        ┆ str    ┆        ┆ f64    ┆ f64    ┆ ---    ┆ f64    ┆ f64   ┆ lent  ┆ arker ┆       ┆       ┆       │
│      ┆        ┆        ┆        ┆ i32    ┆ i32    ┆        ┆        ┆        ┆        ┆        ┆        ┆        ┆        ┆        ┆ f64    ┆        ┆       ┆ ---   ┆ ---   ┆       ┆       ┆       │
│      ┆        ┆        ┆        ┆        ┆        ┆        ┆        ┆        ┆        ┆        ┆        ┆        ┆        ┆        ┆        ┆        ┆       ┆ f64   ┆ f64   ┆       ┆       ┆       │
╞══════╪════════╪════════╪════════╪════════╪════════╪════════╪════════╪════════╪════════╪════════╪════════╪════════╪════════╪════════╪════════╪════════╪═══════╪═══════╪═══════╪═══════╪═══════╪═══════╡
│ 2021 ┆ German ┆ Kenya  ┆ ONE    ┆ 5      ┆ 248    ┆ 1      ┆ disbur ┆ recipi ┆ USD    ┆ curren ┆ amount ┆ 1.2667 ┆ 1.2667 ┆ 0.0    ┆ 0.0    ┆ 0.0    ┆ 0.0   ┆ 0.0   ┆ 0.0   ┆ 1.266 ┆ 1.266 ┆ 1.33e │
│      ┆ y      ┆        ┆        ┆        ┆        ┆        ┆ sement ┆ ent    ┆        ┆ t      ┆        ┆ e7     ┆ e7     ┆        ┆        ┆        ┆       ┆       ┆       ┆ 7e7   ┆ 7e7   ┆ 7     │
│      ┆        ┆        ┆        ┆        ┆        ┆        ┆ s      ┆        ┆        ┆        ┆        ┆        ┆        ┆        ┆        ┆        ┆       ┆       ┆       ┆       ┆       ┆       │
└──────┴────────┴────────┴────────┴────────┴────────┴────────┴────────┴────────┴────────┴────────┴────────┴────────┴────────┴────────┴────────┴────────┴───────┴───────┴───────┴───────┴───────┴───────┘"""  # noqa: E501 (a table snapshot)

COMPARISON_REPR = """\
Comparison of 2 results: ONE; OECD
shape: (1, 8)
┌──────┬──────────┬───────────┬───────────────┬────────────────┬──────────┬──────────┬──────────┐
│ year ┆ provider ┆ recipient ┆ provider_code ┆ recipient_code ┆ bi_multi ┆ ONE      ┆ OECD     │
│ ---  ┆ ---      ┆ ---       ┆ ---           ┆ ---            ┆ ---      ┆ ---      ┆ ---      │
│ i16  ┆ str      ┆ str       ┆ i32           ┆ i32            ┆ i8       ┆ f64      ┆ f64      │
╞══════╪══════════╪═══════════╪═══════════════╪════════════════╪══════════╪══════════╪══════════╡
│ 2021 ┆ Germany  ┆ Kenya     ┆ 5             ┆ 248            ┆ 1        ┆ 1.2667e7 ┆ 1.2667e7 │
└──────┴──────────┴───────────┴───────────────┴────────────────┴──────────┴──────────┴──────────┘"""  # noqa: E501 (a table snapshot)


def test_a_result_prints_its_spec_release_and_table() -> None:
    result = SNAP.spending(providers="Germany", years=2021)

    assert repr(result) == RESULT_REPR
    title = RESULT_REPR.splitlines()[0]
    assert result._repr_html_().startswith(f"<p>{title}</p><div>")


def test_a_comparison_prints_its_columns_and_table() -> None:
    def germany(method: cf.Method) -> cf.Result:
        return SNAP.spending(providers="Germany", years=2021, method=method)

    comparison = cf.compare(germany(ONE), germany(cf.methods.OECD))

    assert repr(comparison) == COMPARISON_REPR
    assert "<table" in comparison._repr_html_()


def test_methods_print_as_models_and_render_their_description() -> None:
    assert repr(ONE) == (
        "Method(significant=0.4, principal=1.0, highest_marker=True, "
        "coefficients='fixed', relevance='markers', eu='channel')"
    )
    assert ONE._repr_markdown_() == (
        "Method ONE: significant markers count 40% of face value and principal "
        "markers 100%; each activity counts once, under its higher Rio marker, with "
        "equal markers as cross-cutting. Climate-component rows from development "
        "banks count their reported climate share and ignore the coefficients. In "
        "the provider view EU Institutions count as a channel: members' core "
        "contributions to the EU budget, the EDF and the EIB carry the EU's climate "
        "share, and each EU agency's own rows count times one minus the share of its "
        "channel's attributed contributions that come from the providers in the query, "
        "so EU spending counts once. The EIB's own lending reaches members' totals "
        "only through their paid-in capital, as every development bank's own lending "
        "does: under ONE, USD 0.035bn attributed against 2.72bn the EIB disbursed in "
        "2021 and 2022. It stays in the recipient view, and quality() reports the EU "
        "rows left out."
    )
    assert RANGE._repr_markdown_() == (
        "Methods ONE / OECD / PROVIDER, reported together; low is the lowest of "
        "their lows, which count only what certain links carried, and high the "
        "highest value across them."
    )

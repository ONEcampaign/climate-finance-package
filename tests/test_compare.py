"""`cf.compare`, and how results, comparisons and methods print."""

from collections.abc import Iterator

import polars as pl
import pytest
from make_fixture import FIXTURE_DIR

import climate_finance as cf
from climate_finance.methods import ONE, RANGE

SNAP = cf.load(path=FIXTURE_DIR)
KEYS = ["year", "provider_code", "recipient_code", "bi_multi"]


@pytest.fixture(autouse=True)
def _fixed_table_width() -> Iterator[None]:
    with pl.Config(tbl_width_chars=200, tbl_cols=-1, tbl_rows=10):
        yield


def _values(result: cf.Result) -> pl.DataFrame:
    return result.table.to_polars().select(*KEYS, "value")


def test_compare_sets_one_value_column_per_result_on_shared_keys() -> None:
    one = SNAP.spending(view="provider")
    oecd = SNAP.spending(view="provider", method=cf.methods.OECD)

    table = cf.compare(one, oecd).table.to_polars()

    assert table.columns == [*KEYS, "ONE", "OECD"]
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

    table = cf.compare(germany, france).table.to_polars()

    assert table.columns == [*KEYS, "providers Germany", "providers France"]
    assert table.height == _values(germany).height + _values(france).height
    assert table.filter(provider_code=5)["providers France"].is_null().all()


def test_a_result_under_several_methods_gives_a_column_per_method() -> None:
    table = cf.compare(SNAP.spending(), SNAP.spending(method=RANGE)).table

    assert table.to_polars().columns == [
        *KEYS,
        "ONE",
        "RANGE: ONE",
        "RANGE: OECD",
        "RANGE: PROVIDER",
    ]


def test_compare_names_amounts_and_shares_apart() -> None:
    amounts = SNAP.spending(providers=5)

    table = cf.compare(amounts, amounts.share_of_total()).table.to_polars()

    assert table.columns[len(KEYS) :] == ["amounts", "share of total"]


def test_results_nothing_sets_apart_are_numbered() -> None:
    table = cf.compare(SNAP.spending(), SNAP.spending()).table.to_polars()

    assert table.columns[len(KEYS) :] == ["result 1: ONE", "result 2: ONE"]


def test_compare_needs_a_result() -> None:
    with pytest.raises(ValueError, match="at least one result"):
        cf.compare()


RESULT_REPR = """\
Result: ONE; disbursements; recipient view; providers Germany; years 2021; USD at current prices; release fixture
shape: (1, 13)
┌──────┬───────────────┬────────────────┬──────────┬────────┬──────────┬────────────┬────────────┬───────────────┬─────────┬──────────┬──────────┬────────┐
│ year ┆ provider_code ┆ recipient_code ┆ bi_multi ┆ method ┆ amount   ┆ adaptation ┆ mitigation ┆ cross_cutting ┆ unsplit ┆ value    ┆ low      ┆ high   │
│ ---  ┆ ---           ┆ ---            ┆ ---      ┆ ---    ┆ ---      ┆ ---        ┆ ---        ┆ ---           ┆ ---     ┆ ---      ┆ ---      ┆ ---    │
│ i16  ┆ i32           ┆ i32            ┆ i8       ┆ str    ┆ f64      ┆ f64        ┆ f64        ┆ f64           ┆ f64     ┆ f64      ┆ f64      ┆ f64    │
╞══════╪═══════════════╪════════════════╪══════════╪════════╪══════════╪════════════╪════════════╪═══════════════╪═════════╪══════════╪══════════╪════════╡
│ 2021 ┆ 5             ┆ 248            ┆ 1        ┆ ONE    ┆ 1.2667e7 ┆ 1.2667e7   ┆ 0.0        ┆ 0.0           ┆ 0.0     ┆ 1.2667e7 ┆ 1.2667e7 ┆ 1.33e7 │
└──────┴───────────────┴────────────────┴──────────┴────────┴──────────┴────────────┴────────────┴───────────────┴─────────┴──────────┴──────────┴────────┘"""  # noqa: E501 (a table snapshot)

COMPARISON_REPR = """\
Comparison of 2 results: ONE; OECD
shape: (1, 6)
┌──────┬───────────────┬────────────────┬──────────┬──────────┬──────────┐
│ year ┆ provider_code ┆ recipient_code ┆ bi_multi ┆ ONE      ┆ OECD     │
│ ---  ┆ ---           ┆ ---            ┆ ---      ┆ ---      ┆ ---      │
│ i16  ┆ i32           ┆ i32            ┆ i8       ┆ f64      ┆ f64      │
╞══════╪═══════════════╪════════════════╪══════════╪══════════╪══════════╡
│ 2021 ┆ 5             ┆ 248            ┆ 1        ┆ 1.2667e7 ┆ 1.2667e7 │
└──────┴───────────────┴────────────────┴──────────┴──────────┴──────────┘"""


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
        "share, and EU Institutions' own rows leave a result in each year where it "
        "holds such an attribution, so EU spending counts once."
    )
    assert RANGE._repr_markdown_() == (
        "Methods ONE / OECD / PROVIDER, reported together; low and high are the "
        "lowest and highest value across them."
    )

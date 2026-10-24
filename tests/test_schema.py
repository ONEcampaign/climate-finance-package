from typing import get_args

import polars as pl
import pytest
from make_fixture import FIXTURE_DIR

from climate_finance import catalogue
from climate_finance.errors import SchemaError
from climate_finance.schema import (
    FLOWS,
    PERSPECTIVES,
    RELEASE_TABLES,
    Column,
    TableSchema,
    validate,
)
from climate_finance.spec import Flow, View

TABLE = TableSchema(
    name="example",
    columns={
        "row_id": Column(pl.String()),
        "year": Column(pl.Int16()),
        "status": Column(pl.String(), values=("disbursed", "not_traceable")),
        "marker": Column(pl.Int8(), nullable=True, values=(0, 1, 2)),
        "levels": Column(pl.Struct, nullable=True),
        "amount_usd": Column(pl.Float64(), nullable=True),
    },
    sort_key=("row_id",),
)


def _frame(**overrides: pl.Series) -> pl.DataFrame:
    columns = {
        "row_id": pl.Series(["a", "b"]),
        "year": pl.Series([2021, 2022], dtype=pl.Int16),
        "status": pl.Series(["disbursed", "not_traceable"]),
        "marker": pl.Series([2, None], dtype=pl.Int8),
        "levels": pl.Series([{"title": 1}, {"title": 0}]),
        "amount_usd": pl.Series([1.5, None]),
    }
    return pl.DataFrame({**columns, **overrides})


def test_valid_frame_passes() -> None:
    validate(TABLE, _frame())


def test_every_bad_column_is_named() -> None:
    frame = _frame(
        year=pl.Series([2021, 2022], dtype=pl.Int64),
        status=pl.Series(["disbursed", "lost"]),
        row_id=pl.Series(["a", None]),
        extra=pl.Series([1, 2]),
    ).drop("marker")

    with pytest.raises(SchemaError) as caught:
        validate(TABLE, frame)

    assert set(caught.value.problems) == {"year", "status", "row_id", "extra", "marker"}
    assert caught.value.table == "example"


def test_undeclared_sort_key_is_refused() -> None:
    with pytest.raises(ValueError, match="sort key"):
        TableSchema(name="t", columns={"a": Column(pl.String())}, sort_key=("b",))


@pytest.mark.parametrize(
    ("table", "column", "value"),
    [
        ("disbursements", "record_grant_equiv_usd", float("nan")),
        ("commitments", "adaptation_usd", float("inf")),
        ("commitments", "climate_share", float("-inf")),
    ],
)
def test_non_finite_amounts_are_refused(table: str, column: str, value: float) -> None:
    frame = pl.read_parquet(FIXTURE_DIR / f"{table}.parquet")
    broken = frame.with_columns(
        pl.when(pl.int_range(pl.len()) == 0)
        .then(value)
        .otherwise(pl.col(column))
        .alias(column)
    )
    validate(RELEASE_TABLES[table], frame)

    with pytest.raises(SchemaError) as caught:
        validate(RELEASE_TABLES[table], broken)

    assert set(caught.value.problems) == {column}


def test_the_query_vocabularies_match_the_schema() -> None:
    assert (
        set(get_args(Flow.__value__))
        == set(FLOWS)
        == set(catalogue.flows.list()["name"])
    )
    assert set(get_args(View.__value__)) == set(PERSPECTIVES)

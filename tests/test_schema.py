import polars as pl
import pytest

from climate_finance.errors import SchemaError
from climate_finance.schema import Column, TableSchema, validate

TABLE = TableSchema(
    name="example",
    columns={
        "row_id": Column(pl.String()),
        "year": Column(pl.Int16()),
        "status": Column(pl.String(), values=("disbursed", "not_traceable")),
        "marker": Column(pl.Int8(), nullable=True, values=(0, 1, 2)),
        "levels": Column(pl.Struct, nullable=True),
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

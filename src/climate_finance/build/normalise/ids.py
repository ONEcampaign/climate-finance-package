"""Content ids: the same row gets the same id in every build of the same source."""

import hashlib
from collections.abc import Sequence

import polars as pl

_SEPARATOR = "\x1f"
# Stands in for a null field, so that null and an empty string give different ids.
_NULL = "\x00"


def content_id(columns: Sequence[str], *, order_by: Sequence[str]) -> pl.Expr:
    """A 16-hex-digit id digested from `columns`.

    Rows equal on `columns` are numbered by `order_by`, and the number enters the
    digest, so the ids are unique and do not depend on the order rows arrive in.
    """
    key = _identity(columns)
    return _digest(key, pl.int_range(pl.len()).over(key, order_by=list(order_by)))


def with_content_id(
    frame: pl.LazyFrame, name: str, columns: Sequence[str], *, order_by: Sequence[str]
) -> pl.LazyFrame:
    """`frame` with `content_id(columns, order_by=order_by)` as column `name`.

    Only rows whose identity repeats are numbered, and the rest take 0, as a group of
    one numbers its row. On a table of millions of mostly distinct rows this spares
    the window ordering every row.
    """
    frame = frame.with_columns(_key=_identity(columns)).with_row_index("_row")
    repeated = frame.filter(pl.len().over("_key") > 1).select(
        "_row", _occurrence=pl.int_range(pl.len()).over("_key", order_by=order_by)
    )
    return (
        frame.join(repeated, on="_row", how="left", maintain_order="left")
        .with_columns(
            _digest(pl.col("_key"), pl.col("_occurrence").fill_null(0)).alias(name)
        )
        .drop("_key", "_row", "_occurrence")
    )


def _identity(columns: Sequence[str]) -> pl.Expr:
    return pl.concat_str(
        [pl.col(name).cast(pl.String).fill_null(_NULL) for name in columns],
        separator=_SEPARATOR,
    )


def _digest(key: pl.Expr, occurrence: pl.Expr) -> pl.Expr:
    return pl.concat_str(
        [key, occurrence.cast(pl.String)], separator=_SEPARATOR
    ).map_batches(_blake2b, return_dtype=pl.String, is_elementwise=True)


def text_digest(columns: Sequence[str]) -> pl.Expr:
    """The md5 of `columns` joined by newlines; null when every one is null."""
    text = pl.concat_str(
        [pl.col(name).fill_null("") for name in columns], separator="\n"
    )
    present = pl.any_horizontal(pl.col(name).is_not_null() for name in columns)
    return pl.when(present).then(
        text.map_batches(_md5, return_dtype=pl.String, is_elementwise=True)
    )


def _blake2b(values: pl.Series) -> pl.Series:
    return pl.Series(
        [hashlib.blake2b(v.encode(), digest_size=8).hexdigest() for v in values],
        dtype=pl.String,
    )


def _md5(values: pl.Series) -> pl.Series:
    return pl.Series(
        [hashlib.md5(v.encode(), usedforsecurity=False).hexdigest() for v in values],
        dtype=pl.String,
    )

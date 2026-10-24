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
    key = pl.concat_str(
        [pl.col(name).cast(pl.String).fill_null(_NULL) for name in columns],
        separator=_SEPARATOR,
    )
    occurrence = pl.int_range(pl.len()).over(key, order_by=list(order_by))
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

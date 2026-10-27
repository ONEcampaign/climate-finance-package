"""The IATI and GCF readers' rows in the `external` schema."""

from collections.abc import Iterable

import polars as pl

from climate_finance.schema import EXTERNAL

_ROW_SCHEMA = {
    "publisher": pl.String,
    "activity_id": pl.String,
    "title": pl.String,
    "recipient_iso": pl.String,
    "currency": pl.String,
    "cumulative_declared": pl.Boolean,
    "year": pl.Int16,
    "transaction_type": pl.String,
    "amount": pl.Float64,
}


def external_rows(rows: Iterable[dict[str, object]], *, source: str) -> pl.DataFrame:
    """`rows` in the `external` schema, amounts summed over rows equal in every
    other field, in the order each first appears.

    `amount_usd` is filled for USD rows only, and `recipient_code` is left for the
    normalise stage.
    """
    keys = [name for name in _ROW_SCHEMA if name != "amount"]
    return (
        pl.DataFrame(rows, schema=_ROW_SCHEMA)
        .group_by(keys, maintain_order=True)
        .agg(pl.col("amount").sum())
        .with_columns(
            source=pl.lit(source),
            amount_usd=pl.when(pl.col("currency") == "USD").then(pl.col("amount")),
            recipient_code=pl.lit(None, pl.Int32),
        )
        .select(list(EXTERNAL.columns))
    )

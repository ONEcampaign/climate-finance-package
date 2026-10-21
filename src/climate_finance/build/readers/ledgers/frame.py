"""The frame every ledger reader returns, so `link/ledger.py` can stack them."""

from collections.abc import Iterable, Mapping

import polars as pl

from climate_finance.schema import LEDGER

# The columns a reader builds itself; `complete_ledger` adds the rest of `LEDGER`.
LEDGER_SCHEMA: Mapping[str, pl.DataType | type[pl.DataType]] = {
    name: column.dtype
    for name, column in LEDGER.columns.items()
    if name not in ("year", "amount_usd")
}


def ledger_frame(rows: Iterable[Mapping[str, object]]) -> pl.DataFrame:
    """Build the ledger frame from row dicts holding the `LEDGER_SCHEMA` fields."""
    return complete_ledger(pl.DataFrame(list(rows), schema=dict(LEDGER_SCHEMA)))


def complete_ledger(frame: pl.DataFrame) -> pl.DataFrame:
    """Add `year` and `amount_usd` to a frame holding the `LEDGER_SCHEMA` columns.

    `year` is the calendar year of `date` (null when `date` is). `amount_usd` is
    `amount` for USD rows and null for every other currency; the stage that holds the
    DAC exchange rates converts those.
    """
    return frame.with_columns(
        year=pl.col("date").dt.year().cast(pl.Int16),
        amount_usd=pl.when(pl.col("currency") == "USD").then(pl.col("amount")),
    ).select(
        "publisher",
        "key",
        "activity_id",
        "transaction_type",
        "date",
        "year",
        "currency",
        "amount",
        "amount_usd",
        "title",
        "other_ids",
    )

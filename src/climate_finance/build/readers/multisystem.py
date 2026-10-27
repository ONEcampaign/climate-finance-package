"""The OECD multilateral system (DSD_MULTI@DF_MULTI), as saved by the fetch stage."""

from pathlib import Path

import polars as pl

# MULTI measure 10 is core contributions. Measure 20, "Contributions through", is
# earmarked bilateral money.
_CORE_MEASURE = 10
# The cell that holds a donor's total: all developing countries, all sectors. The
# other recipient and sector cells repeat the same figure for core contributions.
_TOTAL_RECIPIENT = 10100
_TOTAL_SECTOR = 1000
# Donor codes from 20000 are groups (20001 DAC countries, 20006 non-DAC countries).
AGGREGATE_DONOR_FLOOR = 20000
# "A" is current prices and "D" constant prices.
_CURRENT_PRICES = "A"
# MULTI publishes commitments and disbursements. It has no grant-equivalent flow.
_FLOW_BY_TYPE = {"C": "commitments", "D": "disbursements"}

_KEY = ["provider_code", "channel_code", "year", "flow"]


def read_multisystem(path: Path) -> pl.DataFrame:
    """Core contributions to multilateral agencies, by provider, channel and year.

    Args:
        path: The parquet that fetch writes from `oda_reader.download_multisystem`.

    Returns:
        One row per provider, channel, year and flow, in USD at current prices, for
        the flows MULTI publishes. Aggregate donors and suppressed values are dropped.

    Raises:
        ValueError: when a provider, channel, year and flow has core contributions
            but no total cell, or several rows in its total cell.
    """
    core = (
        pl.read_parquet(path)
        .filter(
            pl.col("aidtype_code") == _CORE_MEASURE,
            pl.col("donor_code") < AGGREGATE_DONOR_FLOOR,
            pl.col("data_type_code") == _CURRENT_PRICES,
        )
        .select(
            provider_code=pl.col("donor_code"),
            channel_code=pl.col("channel_code"),
            year=pl.col("year").cast(pl.Int16),
            flow=pl.col("flow_type_code").replace_strict(_FLOW_BY_TYPE),
            recipient_code=pl.col("recipient_code"),
            sector_code=pl.col("sector_code"),
            contribution_usd=pl.col("value") * 10 ** pl.col("unit_multiplier"),
        )
    )
    total = core.filter(
        pl.col("recipient_code") == _TOTAL_RECIPIENT,
        pl.col("sector_code") == _TOTAL_SECTOR,
    )
    if total.select(_KEY).is_duplicated().any():
        raise ValueError("MULTI core contributions repeat within the total cell")
    if total.select(_KEY).n_unique() != core.select(_KEY).n_unique():
        raise ValueError("MULTI core contributions lack a total cell for some keys")
    return total.drop_nulls("contribution_usd").select(*_KEY, "contribution_usd")

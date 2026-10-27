"""Conversion joins one factor per provider and year from the release's
`conversion_factors`, with the DAC total as the series for providers that have
none. Provenance identifies the factors used."""

from typing import Literal

import polars as pl
import pytest

from climate_finance.build.fetch import dac_factors
from climate_finance.convert import (
    DAC_TOTAL,
    convert,
    factors_hash,
    provenance,
    target_factors,
)
from climate_finance.errors import InvalidInputError, SourceUnavailableError

GERMANY, IDA = 5, 905
FACTORS = pl.DataFrame(
    {
        "entity_code": [GERMANY, GERMANY, DAC_TOTAL, DAC_TOTAL],
        "year": [2021, 2022, 2021, 2022],
        "factor": [0.8, 0.9, 0.5, 0.6],
    },
    schema={"entity_code": pl.Int32, "year": pl.Int16, "factor": pl.Float64},
)

# A release's table: FACTORS as EUR at current prices and in 2022 and 2024 prices.
TABLE = pl.concat(
    FACTORS.with_columns(currency=pl.lit("EUR"), base_year=pl.lit(base, pl.Int16))
    for base in (None, 2022, 2024)
).lazy()


def _amounts(*rows: tuple[int, int, float]) -> pl.LazyFrame:
    return pl.LazyFrame(
        rows,
        schema={"provider_code": pl.Int32, "year": pl.Int16, "value": pl.Float64},
        orient="row",
    )


def test_each_provider_takes_its_own_factor_or_the_dac_total() -> None:
    frame = _amounts((GERMANY, 2021, 100.0), (GERMANY, 2022, 100.0), (IDA, 2022, 10.0))

    result = convert(frame, columns=["value"], factors=FACTORS, release="r").collect()

    assert result.columns == ["provider_code", "year", "value"]
    assert result.sort("provider_code", "year")["value"].to_list() == pytest.approx(
        [80.0, 90.0, 6.0]
    )


def test_a_year_without_any_factor_raises_on_collection() -> None:
    frame = _amounts((GERMANY, 2023, 100.0))

    converted = convert(frame, columns=["value"], factors=FACTORS, release="r")

    with pytest.raises(SourceUnavailableError, match=r"release r .*\(5, 2023\)"):
        converted.collect()


def test_usd_at_current_prices_reads_no_factors_and_returns_the_plan() -> None:
    frame = _amounts((IDA, 2030, 1.0))

    assert target_factors(TABLE, release="r", currency="USD", prices="current") is None
    assert convert(frame, columns=["value"], factors=None, release="r") is frame


@pytest.mark.parametrize("prices", ["current", 2022])
def test_a_target_reads_its_rows_of_the_release_table(prices: int | str) -> None:
    factors = target_factors(TABLE, release="r", currency="EUR", prices=prices)

    assert factors is not None
    assert factors.sort("entity_code", "year").equals(
        FACTORS.sort("entity_code", "year")
    )


@pytest.mark.parametrize(
    ("currency", "prices", "shipped"),
    [
        ("EUR", 2021, "2022 to 2024"),
        ("EUR", 2025, "2022 to 2024"),
        ("GBP", 2022, "none"),
    ],
)
def test_a_base_year_the_release_does_not_ship_is_refused(
    currency: str, prices: int, shipped: str
) -> None:
    with pytest.raises(InvalidInputError, match=f"base years {shipped}"):
        target_factors(TABLE, release="r", currency=currency, prices=prices)


def _factors(germany: float) -> pl.DataFrame:
    return pl.DataFrame(
        {"entity_code": [GERMANY], "year": [2022], "factor": [germany]},
        schema=FACTORS.schema,
    )


def test_factor_hash_follows_the_factors_and_ignores_row_order() -> None:
    assert factors_hash(_factors(0.8)) == factors_hash(_factors(0.8))
    assert factors_hash(_factors(0.8)) != factors_hash(_factors(0.9))
    assert factors_hash(FACTORS) == factors_hash(FACTORS.reverse())


def test_provenance_carries_the_table_digest_and_follows_the_factors() -> None:
    conversions = [
        provenance(_factors(germany), table_sha256="t") for germany in (0.8, 0.8, 0.9)
    ]

    assert all(conversion is not None for conversion in conversions)
    hashes = [conversion.factors_sha256 for conversion in conversions if conversion]
    assert hashes[0] == hashes[1] != hashes[2]
    assert {conversion.table_sha256 for conversion in conversions if conversion} == {
        "t"
    }


def test_usd_at_current_prices_has_no_provenance() -> None:
    assert provenance(None, table_sha256="t") is None


@pytest.mark.integration
@pytest.mark.parametrize(("currency", "prices"), [("GBP", 2024), ("CAD", "current")])
def test_fetched_factors_match_pydeflate_on_its_own_frame(
    currency: str, prices: int | Literal["current"]
) -> None:
    import pydeflate

    rows = [(GERMANY, 2022, 100.0), (IDA, 2022, 100.0)]
    factors = dac_factors(currency=currency, prices=prices)
    ours = convert(
        _amounts(*rows), columns=["value"], factors=factors, release="r"
    ).collect()
    frame = pl.DataFrame(
        rows, schema=["provider_code", "year", "value"], orient="row"
    ).to_pandas()
    if prices == "current":
        theirs = pydeflate.oecd_dac_exchange(
            frame, source_currency="USA", target_currency=currency,
            id_column="provider_code", use_source_codes=True,
        )  # fmt: skip
    else:
        theirs = pydeflate.oecd_dac_deflate(
            frame, base_year=prices, target_currency=currency,
            id_column="provider_code", use_source_codes=True,
        )  # fmt: skip

    assert ours.sort("provider_code")["value"].to_list() == pytest.approx(
        theirs.sort_values("provider_code")["value"].tolist(), rel=1e-6
    )
    assert ours["value"][0] != pytest.approx(100.0)


@pytest.mark.integration
def test_euro_conversion_uses_the_dac_euro_rate() -> None:
    frame = _amounts((GERMANY, 2022, 100.0))
    factors = dac_factors(currency="EUR", prices="current")

    result = convert(frame, columns=["value"], factors=factors, release="r")

    assert result.collect()["value"].to_list() == pytest.approx([95.09], rel=1e-3)

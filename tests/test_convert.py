"""Conversion joins one DAC factor per provider and year, with the DAC total as
the series for providers that have none."""

import polars as pl
import pytest

from climate_finance import convert as convert_module
from climate_finance.convert import DAC_TOTAL, convert
from climate_finance.errors import SourceUnavailableError

GERMANY, IDA = 5, 905
FACTORS = pl.DataFrame(
    {
        "entity_code": [GERMANY, GERMANY, DAC_TOTAL, DAC_TOTAL],
        "year": [2021, 2022, 2021, 2022],
        "factor": [0.8, 0.9, 0.5, 0.6],
    },
    schema={"entity_code": pl.Int32, "year": pl.Int16, "factor": pl.Float64},
)


@pytest.fixture
def factors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(convert_module, "dac_factors", lambda **_: FACTORS)


def _amounts(*rows: tuple[int, int, float]) -> pl.LazyFrame:
    return pl.LazyFrame(
        rows,
        schema={"provider_code": pl.Int32, "year": pl.Int16, "value": pl.Float64},
        orient="row",
    )


@pytest.mark.usefixtures("factors")
def test_each_provider_takes_its_own_factor_or_the_dac_total() -> None:
    frame = _amounts((GERMANY, 2021, 100.0), (GERMANY, 2022, 100.0), (IDA, 2022, 10.0))

    result = convert(frame, columns=["value"], currency="EUR", prices=2024).collect()

    assert result.columns == ["provider_code", "year", "value"]
    assert result.sort("provider_code", "year")["value"].to_list() == pytest.approx(
        [80.0, 90.0, 6.0]
    )


@pytest.mark.usefixtures("factors")
def test_a_year_without_any_factor_raises_on_collection() -> None:
    frame = _amounts((GERMANY, 2023, 100.0))

    converted = convert(frame, columns=["value"], currency="GBP", prices="current")

    with pytest.raises(SourceUnavailableError, match=r"\(5, 2023\)"):
        converted.collect()


def test_usd_at_current_prices_returns_the_plan_unchanged() -> None:
    frame = _amounts((IDA, 2030, 1.0))

    assert convert(frame, columns=["value"], currency="USD", prices="current") is frame


@pytest.mark.integration
@pytest.mark.parametrize(("currency", "prices"), [("GBP", 2024), ("CAD", "current")])
def test_factors_match_pydeflate_on_its_own_frame(
    currency: str, prices: int | str
) -> None:
    import pydeflate

    rows = [(GERMANY, 2022, 100.0), (IDA, 2022, 100.0)]
    ours = convert(
        _amounts(*rows), columns=["value"], currency=currency, prices=prices
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
@pytest.mark.xfail(
    strict=True,
    reason="pydeflate 2.6.1 maps EUR to EU Institutions, reported in USD; passes "
    "once L1 pins the pydeflate release with the EUR fix",
)
def test_euro_conversion_uses_the_dac_euro_rate() -> None:
    frame = _amounts((GERMANY, 2022, 100.0))

    result = convert(frame, columns=["value"], currency="EUR", prices="current")

    assert result.collect()["value"].to_list() == pytest.approx([95.09], rel=1e-3)

"""The Eurostat reader gives `eurostat_climate_finance` from the SDMX-CSV file."""

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from climate_finance import schema
from climate_finance.build.errors import BuildError
from climate_finance.build.readers.eurostat import (
    LINES,
    MEMBER_PROVIDERS,
    read_eurostat,
)

FIXTURE = Path(__file__).parent / "fixtures" / "raw" / "eurostat" / "sdg_13_50.csv"
NAMES = Path(__file__).parents[1] / "src" / "climate_finance" / "data" / "names.csv"


def _value(frame: pl.DataFrame, geo: str, year: int) -> float:
    return frame.filter(geo=geo, year=year)["value_eur"].item()


def test_the_table_gives_euro_with_the_codes_of_each_line() -> None:
    frame = read_eurostat(FIXTURE)

    schema.validate(schema.EUROSTAT_CLIMATE_FINANCE, frame)
    assert frame.height == 60
    assert _value(frame, "DE", 2024) == 10_686_550_000
    codes = {
        geo: (provider, agency)
        for geo, provider, agency in frame.select("geo", "provider_code", "agency_code")
        .unique()
        .iter_rows()
    }
    assert codes["EL"] == (40, None)
    assert codes["COMM"] == (918, None)
    assert codes["EIB"] == (918, 3)
    assert codes["EU27_2020"] == (None, None)
    assert set(codes) == set(LINES)
    assert frame["last_update"].unique().to_list() == [date(2026, 4, 29)]
    # Eurostat flags a break in the series in 2020.
    assert frame.filter(pl.col("obs_flag").is_not_null())[
        "year"
    ].unique().to_list() == [2020]


@pytest.mark.parametrize("year", [2020, 2024])
def test_the_union_is_the_sum_of_its_members(year: int) -> None:
    frame = read_eurostat(FIXTURE).filter(year=year)

    members = frame.filter(pl.col("geo").is_in(list(MEMBER_PROVIDERS)))["value_eur"]
    assert members.len() == 27
    # Eurostat rounds each value to 10,000 euro.
    assert _value(frame, "EU27_2020", year) == pytest.approx(members.sum(), abs=5e4)


def test_each_member_code_is_the_provider_of_its_state() -> None:
    names = pl.read_csv(NAMES).filter(kind="provider", source="iso2")
    iso2_codes = dict(
        zip(
            names["key"].str.to_uppercase(),
            names["code"],
            strict=True,
        )
    )
    # Eurostat spells Greece EL, where ISO 3166 has GR.
    assert {
        geo: iso2_codes["GR" if geo == "EL" else geo] for geo in MEMBER_PROVIDERS
    } == MEMBER_PROVIDERS


@pytest.mark.parametrize(
    ("old", "new", "match"),
    [
        (",EIB,", ",XX,", "XX"),
        (",MIO_EUR,", ",PC_GDP,", "million euro"),
        (",A,MIO_EUR,", ",Q,MIO_EUR,", "million euro"),
    ],
)
def test_a_new_line_or_unit_stops_the_build(
    tmp_path: Path, old: str, new: str, match: str
) -> None:
    path = tmp_path / "sdg_13_50.csv"
    path.write_text(
        FIXTURE.read_text(encoding="utf-8").replace(old, new), encoding="utf-8"
    )

    with pytest.raises(BuildError, match=match):
        read_eurostat(path)


def test_an_empty_observation_leaves_no_row(tmp_path: Path) -> None:
    lines = FIXTURE.read_text(encoding="utf-8").splitlines()
    path = tmp_path / "sdg_13_50.csv"
    path.write_text(
        "\n".join(
            line.replace("10686.55,", ",") if ",DE,2024," in line else line
            for line in lines
        ),
        encoding="utf-8",
    )

    frame = read_eurostat(path)

    assert frame.height == 59
    assert frame.filter(geo="DE", year=2024).is_empty()

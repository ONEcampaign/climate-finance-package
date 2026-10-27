"""The flag stage's logic on the archived CRS, replayed from the scan's Jev cache.

The cuts on Jev's answers were measured by a scan of the CRS records of 2015 to 2024
with a Rio mitigation or adaptation marker of 1 or 2 and a positive commitment or
disbursement. This runs `flag_records` and `jev_flags` over the same file with an
offline client on the scan's own cache, `$CF_E2E_DATA/jev-cache/flags_jev-1.14.0.jsonl`,
so a request that differs from the scan's by a byte is a cache miss. The record
count, the asked-text count, the no-aim records and their amounts, and Jev's
answers match the scan's outputs, pinned below.
Needs CF_E2E_DATA pointing at the scoping `data/`.
"""

import os
from pathlib import Path

import polars as pl
import pytest

from climate_finance import schema
from climate_finance.build.jev_client import JevClient
from climate_finance.build.jev_flags import (
    FLAG_QUESTIONS,
    MIN_TEXT_CHARS,
    flag_records,
    jev_flags,
)
from climate_finance.build.normalise.crs import normalise_crs

pytestmark = pytest.mark.slow

MODEL = "jev-1.14.0"
FIRST_YEAR, LAST_YEAR = 2015, 2024

# The scan's own outputs, scan/data/{answers.parquet, agg_total.csv,
# agg_short_texts.csv}. USD amounts are the scan's millions in dollars.
SCAN_RECORDS = 332_771
SCAN_SHORT_RECORDS = 24_109
SCAN_ASKED_TEXTS = 113_781
SCAN_NO_AIM_RECORDS = 172_071
SCAN_COMMITMENT_USD = 498_505_322_784.0
SCAN_NO_AIM_COMMITMENT_USD = 224_925_715_933.0
SCAN_DISBURSEMENT_USD = 351_841_829_806.0
SCAN_NO_AIM_DISBURSEMENT_USD = 166_850_104_945.0
# The sums of each answer over the scan's distinct asked texts.
SCAN_ANSWER_SUMS = {
    "mitigation_aim": 33_680.76,
    "adaptation_aim": 35_355.16,
    "counterproductive": 14_792.86,
    "surprising": 52_452.22,
}
# Rows of the scan's answers.parquet: a text and its four answers, in
# `FLAG_QUESTIONS` order.
SCAN_ANSWERS = {
    "499 KW Ground Mounted Solar in Nauru 500 KW Ground Mounted Solar in Nauru": (
        0.97,
        0.17,
        0.04,
        0.07,
    ),
    "ABC Adaptation des communautés burkinabés aux changements climatiques (ABC)": (
        0.07,
        0.98,
        0.08,
        0.03,
    ),
    "2014/355 598   Digital printing media center Digital printing media center": (
        0.03,
        0.02,
        0.14,
        0.93,
    ),
    "Borg El Arab International Airport Extension Project Extend the airport": (
        0.03,
        0.04,
        0.96,
        0.87,
    ),
    "ACC_Sustainable_Palm_Oil Palm Oil Programme with WAFF/Solidaridad": (
        0.43,
        0.16,
        0.41,
        0.36,
    ),
    "AECID 2022 Euroclima LAC Contract AECID 2022 Euroclima LAC Contract": (
        0.36,
        0.48,
        0.12,
        0.16,
    ),
}


@pytest.fixture(scope="module")
def records() -> pl.DataFrame:
    """The records the flag stage asks about, with their `text`."""
    raw = pl.scan_parquet(
        Path(os.environ["CF_E2E_DATA"]) / "raw" / "oecd" / "CRS_v20261006.parquet"
    )
    crs = normalise_crs(raw, years=(FIRST_YEAR, LAST_YEAR))
    return flag_records(crs, first_year=FIRST_YEAR, last_year=LAST_YEAR).collect()


@pytest.fixture(scope="module")
def flags(
    records: pl.DataFrame, tmp_path_factory: pytest.TempPathFactory
) -> pl.DataFrame:
    """`jev_flags` over `records`, answered offline from the scan's cache."""
    seed = Path(os.environ["CF_E2E_DATA"]) / "jev-cache" / f"flags_{MODEL}.jsonl"
    assert seed.is_file(), f"the scan's Jev cache is missing: {seed}"
    cache_dir = tmp_path_factory.mktemp("jev-cache")
    # The scan's cache is in the client's own format. A link leaves the 485 MB file
    # in place, and an offline client only reads it.
    (cache_dir / f"{MODEL}.jsonl").symlink_to(seed)
    client = JevClient(model=MODEL, cost_cap_usd=0.0, cache_dir=cache_dir, offline=True)
    flags = jev_flags(records, client=client)
    assert client.live_calls == 0
    return flags


def test_flags_follow_the_schema_and_the_release_invariant(
    records: pl.DataFrame, flags: pl.DataFrame
) -> None:
    schema.validate(schema.JEV_FLAGS, flags)
    assert schema._jev_flag_problems(flags) == []
    assert flags["record_id"].sort().equals(records["record_id"].sort())


def test_records_and_amounts_equal_the_scans(flags: pl.DataFrame) -> None:
    no_aim = flags.filter("no_aim")

    assert flags.height == SCAN_RECORDS
    assert flags.filter("short_text").height == SCAN_SHORT_RECORDS
    assert no_aim.height == SCAN_NO_AIM_RECORDS
    assert flags["commitment_usd"].sum() == pytest.approx(SCAN_COMMITMENT_USD, rel=1e-9)
    assert flags["disbursement_usd"].sum() == pytest.approx(
        SCAN_DISBURSEMENT_USD, rel=1e-9
    )
    assert no_aim["commitment_usd"].sum() == pytest.approx(
        SCAN_NO_AIM_COMMITMENT_USD, rel=1e-9
    )
    assert no_aim["disbursement_usd"].sum() == pytest.approx(
        SCAN_NO_AIM_DISBURSEMENT_USD, rel=1e-9
    )


def test_answers_equal_the_scans(records: pl.DataFrame, flags: pl.DataFrame) -> None:
    """Each distinct asked text has one set of answers, which sum as the scan's do
    and match the scan's for a sample of texts."""
    asked = (
        records.select("record_id", "text")
        .join(flags, on="record_id", validate="1:1")
        .filter(pl.col("text").str.len_chars() >= MIN_TEXT_CHARS)
        .group_by("text")
        .agg(pl.col(f"{name}_p").first() for name in FLAG_QUESTIONS)
        .sort("text")
    )

    assert asked.height == SCAN_ASKED_TEXTS
    for name, total in SCAN_ANSWER_SUMS.items():
        assert asked[f"{name}_p"].sum() == pytest.approx(total, rel=1e-9)
    sample = asked.filter(pl.col("text").is_in(list(SCAN_ANSWERS)))
    assert sample.height == len(SCAN_ANSWERS)
    for text, *answers in sample.iter_rows():
        assert tuple(answers) == SCAN_ANSWERS[text]

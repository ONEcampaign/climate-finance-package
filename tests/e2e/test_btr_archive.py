"""The BTR reader over the archived BTR1 workbooks, and party weights from the
prototype's linked pairs. Needs CF_E2E_DATA pointing at the scoping `data/`."""

import os
from pathlib import Path

import polars as pl
import pytest

from climate_finance import schema
from climate_finance.build.readers.btr import estimate_party_weights, ingest_unfccc

pytestmark = pytest.mark.slow


def _data() -> Path:
    return Path(os.environ["CF_E2E_DATA"])


def test_archive_gives_every_party_table_iii1_rows(tmp_path: Path) -> None:
    out = ingest_unfccc(
        _data() / "raw" / "unfccc_btr1", out_path=tmp_path / "btr.parquet"
    )

    btr = pl.read_parquet(out)
    schema.validate(schema.BTR, btr)
    assert btr.height == 27_409
    assert btr["party"].n_unique() == 39
    assert btr["face_usd"].sum() == pytest.approx(75.41996e9, rel=1e-6)
    # Chile (2 rows for 2023 and 2024) and Indonesia (9 rows, grant equivalent only)
    # report South-South support. The other 37 parties give 27,398 rows.
    others = btr.filter(~pl.col("party").is_in(["CHL", "IDN"]))
    assert others.height == 27_398
    assert others["face_usd"].sum() == pytest.approx(75.41991e9, rel=1e-6)


def test_prototype_pairs_give_germany_half_and_norway_forty_percent() -> None:
    links = _data() / "prototypes" / "unfccc"
    pairs = pl.concat(
        pl.read_parquet(links / f"links_{party}.parquet")
        .filter(pl.col("match_probability") >= 0.95)
        .select(
            party=pl.lit(party),
            btr_usd="amt",
            crs_usd=pl.when(pl.col("status") == "disbursed")
            .then("amt_d_crs")
            .otherwise("amt_c_crs"),
            rio_marker=pl.max_horizontal("ad_crs", "mi_crs").cast(pl.Int8),
        )
        for party in ("DEU", "NOR")
    )

    weights = {row["party"]: row for row in estimate_party_weights(pairs).to_dicts()}

    assert weights["DEU"]["significant"] == 0.5
    assert weights["DEU"]["share_at"]["0.5"] == pytest.approx(0.43, abs=0.01)
    assert weights["DEU"]["share_at"]["1.0"] == pytest.approx(0.47, abs=0.01)
    assert weights["NOR"]["significant"] == 0.4
    assert weights["NOR"]["share_at"]["0.4"] == pytest.approx(0.37, abs=0.01)
    assert weights["NOR"]["share_at"]["1.0"] == pytest.approx(0.61, abs=0.01)
    assert weights["NOR"]["principal_at_100"] > 0.99

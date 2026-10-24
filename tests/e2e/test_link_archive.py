"""Units, id practice and the exact links on the archived OECD files, against the
prototype runs under `prototypes/` in CF_E2E_DATA."""

import json
import os
from pathlib import Path

import polars as pl
import pytest

from climate_finance import schema
from climate_finance.build.link.crdf_crs import link_within_year
from climate_finance.build.link.id_practice import classify_id_practice
from climate_finance.build.link.keys import build_units, link_by_keys
from climate_finance.build.normalise.crdf import normalise_crdf
from climate_finance.build.normalise.crs import normalise_crs, trace_population

pytestmark = pytest.mark.slow

_SHARES = ("id_share", "parent_share", "pn_share")
# The references take every crs id longer than ten characters as a contract under
# its first ten. `parent_id` takes only EU decision ids and `LETTER_SUFFIX_DONORS`
# letters, so these agencies continue less by parent id in 2019-2022, and those in
# `_NOW_BY_PN` continue by project number instead.
_FEWER_BY_PARENT = {
    (1, 8),
    (1, 9),
    (1, 13),
    (4, 2),
    (4, 44),
    (5, 2),
    (6, 4),
    (10, 6),
    (68, 1),
    (82, 1),
    (301, 2),
    (301, 3),
    (742, 2),
    (742, 99),
    (820, 1),
}
_NOW_BY_PN = {
    (1, 9),
    (4, 2),
    (6, 4),
    (10, 6),
    (68, 1),
    (82, 1),
    (301, 2),
    (301, 3),
    (742, 2),
}
# Disbursement the key tiers link beyond the reference's tiers, by rule. The
# reference's rules reproduce its total to the dollar.
# `parent_id` reaches no parent for other donors' 11-digit sequence numbers. A
# lettered record reaches its parent only in its recipient under its project
# number (0.92bn less), and only EU contracts pass their floor (0.01bn less).
PARENT_ID_USD = -3.008258137e9
# `crs_key` keeps an id that names several projects within its recipient, a
# missing project number counting as one more (0.03bn less).
CRS_KEY_USD = -3.016440996e9
# The project-number tiers hold for agencies whose ids persist.
PN_EVERY_AGENCY_USD = 19.540454511e9
# A floor naming no commitment under the disbursement's own crs id passes; 1.60bn
# of this is EU Institutions' disbursement under its decision ids.
FLOOR_UNSEEN_USD = 1.819184508e9
# A component award keys each component by purpose, so its key is unique and the
# project-number tiers reach it: 24.25bn more is linked, 13.03bn of it AsDB loans
# and 9.11bn US awards, and 3.28bn linked across components is not.
COMPONENT_AWARD_USD = 20.978664440e9


@pytest.fixture(scope="module")
def data() -> Path:
    return Path(os.environ["CF_E2E_DATA"])


@pytest.fixture(scope="module")
def crs(data: Path) -> pl.DataFrame:
    # Slow: digests 3.8 million records.
    raw = pl.scan_parquet(data / "raw" / "oecd" / "CRS_v20261006.parquet")
    return normalise_crs(raw, years=(2010, 2025)).collect()


@pytest.fixture(scope="module")
def units(crs: pl.DataFrame) -> pl.DataFrame:
    units = build_units(trace_population(crs.lazy())).collect()
    schema.validate(schema.UNITS, units)
    return units


@pytest.fixture(scope="module")
def id_practice(units: pl.DataFrame) -> pl.DataFrame:
    return classify_id_practice(units)


def _compare(
    reference: pl.DataFrame, built: pl.DataFrame, on: list[str]
) -> pl.DataFrame:
    return reference.join(built, on=on, how="full", suffix="_built", coalesce=True)


def test_one_period_id_practice_reproduces_the_deterministic_reference(
    data: Path, units: pl.DataFrame
) -> None:
    reference = pl.read_csv(
        data / "prototypes" / "deterministic-bulk" / "agency_classes.csv",
        schema_overrides={"donor_code": pl.Int32, "agency": pl.Int32},
    ).rename({"agency": "agency_code"})
    built = classify_id_practice(units, periods={"2019-2022": (2019, 2022)})
    rows = _compare(reference, built, ["donor_code", "agency_code"])

    assert rows.height == reference.height == built.height == 311
    assert (rows["n_open"] == rows["open_commitments"]).all()
    assert (rows["id_share"] == rows["id_share_built"]).all()
    fewer = rows.filter(pl.col("parent_share") != pl.col("parent_share_built"))
    assert set(fewer.select("donor_code", "agency_code").iter_rows()) == (
        _FEWER_BY_PARENT
    )
    assert (fewer["parent_share_built"] < fewer["parent_share"]).all()
    # That reference collapsed spaces inside project numbers, where the build drops
    # them, so seven more agencies continue a little more by project number. One of
    # them crosses the 0.2 continued-share line.
    moved = rows.filter(pl.col("pn_share") != pl.col("pn_share_built"))
    assert (
        set(moved.select("donor_code", "agency_code").iter_rows())
        == {
            (3, 1),
            (5, 9),
            (5, 63),
            (5, 66),
            (5, 94),
            (910, 1),
            (951, 1),
        }
        | _NOW_BY_PN
    )
    assert (moved["pn_share_built"] > moved["pn_share"]).all()
    reclassed = rows.filter(pl.col("id_class") != pl.col("id_class_built"))
    assert reclassed.select(
        "donor_code", "agency_code", "id_class", "id_class_built"
    ).rows() == [(951, 1, "neither", "pn_persistent")]


def test_id_practice_reproduces_the_three_period_reference(
    data: Path, id_practice: pl.DataFrame
) -> None:
    periods = {"p2010": "2010-2014", "p2015": "2015-2018", "p2019": "2019-2022"}
    reference = (
        pl.read_parquet(
            data / "prototypes" / "a1-reference" / "dated_pnuniq" / "classes.parquet"
        )
        .filter(pl.col("period").is_in(list(periods)))
        .select(
            pl.col("donor_code").cast(pl.Int32),
            agency_code=pl.col("agency").cast(pl.Int32),
            period=pl.col("period").replace_strict(periods),
            id_class="id_class",
            open_commitments=pl.col("n_open").cast(pl.Int32),
            **{share: share for share in _SHARES},
        )
    )
    schema.validate(schema.ID_PRACTICE, id_practice)
    key = ["donor_code", "agency_code", "period"]
    rows = _compare(reference, id_practice, key)
    assert rows.height == reference.height == id_practice.height == 798
    for column in ("open_commitments", "id_share"):
        assert (rows[column] == rows[f"{column}_built"]).all()
    # Only `parent_id` moves a row: less continues by parent id, and some of it by
    # project number.
    fewer = rows.filter(pl.col("parent_share") != pl.col("parent_share_built"))
    assert fewer.height == 45
    assert (fewer["parent_share_built"] < fewer["parent_share"]).all()
    assert (rows["pn_share_built"] >= rows["pn_share"]).all()
    reclassed = rows.filter(pl.col("id_class") != pl.col("id_class_built"))
    assert reclassed.sort(key).select(*key, "id_class", "id_class_built").rows() == [
        (4, 10, "2015-2018", "mixed", "id_persistent"),
        (301, 9, "2010-2014", "parent_id_persistent", "neither"),
    ]


def test_key_tiers_link_the_reference_disbursements(
    data: Path, units: pl.DataFrame
) -> None:
    metrics = json.loads(
        (
            data / "prototypes" / "a1-reference" / "dated_pnuniq" / "trace_metrics.json"
        ).read_text()
    )
    links = link_by_keys(units, first_year=2010)
    schema.validate(schema.LINKS, links)
    tiers = links.select("left_id", "link_method").unique()
    assert tiers["left_id"].is_unique().all()
    assert set(tiers["link_method"]) == set(schema.KEY_TIERS)

    linked = units.join(tiers, left_on="unit_id", right_on="left_id")
    assert linked["disbursement_usd"].sum() == pytest.approx(
        metrics["linked_disbursement_bn"] * 1e9
        + PARENT_ID_USD
        + CRS_KEY_USD
        + PN_EVERY_AGENCY_USD
        + FLOOR_UNSEEN_USD
        + COMPONENT_AWARD_USD,
        abs=1e3,
    )


def test_within_year_join_reproduces_the_reference_population(
    data: Path, crs: pl.DataFrame, units: pl.DataFrame
) -> None:
    summary = json.loads(
        (data / "prototypes" / "deterministic-bulk" / "summary.json").read_text()
    )["population"]
    crdf = (
        normalise_crdf(
            pl.scan_parquet(data / "raw" / "oecd" / "crdf_recipient.parquet"),
            perspective="recipient",
            crs=crs.lazy(),
        )
        .filter(pl.col("year").is_in([2021, 2022]))
        .collect()
    )
    links = link_within_year(crdf.lazy(), units=units.lazy()).collect()
    linked = crdf.filter(pl.col("row_id").is_in(links["left_id"].implode()))

    assert crdf.height == summary["rows"]
    assert linked.height == summary["traceable_rows"]
    assert links["group_id"].n_unique() == summary["keys"]

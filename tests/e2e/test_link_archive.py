"""Units, id practice and the exact links on the archived OECD files, against the
prototype runs under `prototypes/` in CF_E2E_DATA."""

import json
import os
from pathlib import Path

import polars as pl
import pytest

from climate_finance import schema
from climate_finance.build.link import id_practice as practice
from climate_finance.build.link.crdf_crs import link_within_year
from climate_finance.build.link.id_practice import (
    LETTER_SUFFIX_DONORS,
    classify_id_practice,
)
from climate_finance.build.link.keys import build_units, link_by_keys
from climate_finance.build.normalise.crdf import normalise_crdf
from climate_finance.build.normalise.crs import normalise_crs, trace_population

pytestmark = pytest.mark.slow

_SHARES = ("id_share", "parent_share", "pn_share")
# The references take every crs id longer than ten characters as a contract under
# its first ten. Under the build's own rule, `parent_id` takes only EU decision ids
# and `LETTER_SUFFIX_DONORS` letters, so these agencies continue less by parent id in
# 2019-2022, and those in `_NOW_BY_PN` continue by project number instead. Germany's
# agency 2 writes 189 of its 2019-2022 lettered ids with a trailing space, as
# `2018128770a `; normalise trims it, so `parent_id` reads them as the references do.
_FEWER_BY_PARENT = {
    (1, 8),
    (1, 9),
    (1, 13),
    (4, 2),
    (4, 44),
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
# Disbursement the `wb_loan` tier links beyond the reference's tiers: World Bank
# units whose loan reaches a commitment that no crs id or project number reaches.
LOAN_TIER_LINKED_USD = 16.840693e9
# Disbursement the build's other key rules link beyond the reference's, in USD, each
# measured with the rules above it in place.
KEY_RULES_LINKED_USD = {
    # Normalise trims crs ids and project numbers and nulls project numbers Excel
    # rounded (`normalise_pn`), which the reference kept as written.
    "normalised identifiers": -0.537792009e9,
    # The project-number tiers run for every agency, where the reference turned
    # them off for agencies whose ids persist.
    "project-number tiers for every agency": 18.503133299e9,
    # The crs-id tier keys an id that names several projects by recipient
    # (`crs_key`): one in several recipients under several project numbers, then
    # one in several countries.
    "crs_key on several projects": -2.410684899e9,
    "crs_key on several countries": -0.849078567e9,
    # A parent id is the EU's decision id or a `LETTER_SUFFIX_DONORS` base id
    # (`parent_id`), where the reference read the first ten characters of every
    # longer crs id.
    "parent_id": -1.391169180e9,
    # The crs-id tiers compare ids with their line suffix unpadded
    # (`canonical_crs_id`).
    "canonical_crs_id": 0.242025595e9,
    # The project-number tiers compare numbers without `-` and `_` (`pn_key`).
    "pn_key": 1.711175508e9,
    # Each project-number tier asks for one crs id over its own key
    # (`Tier.unique_by`), so the purpose tier links a number filed under several
    # crs ids across purposes in one recipient: AsDB loans, US awards, Canadian
    # lines.
    "unique_by": 25.000660969e9,
    # `pn_recipient_date` links a disbursement to the one commitment unit under its
    # number and recipient whose commitment date it repeats.
    "pn_recipient_date": 9.642897127e9,
    # A disbursement whose floor precedes the first CRS year links where its key
    # reaches a commitment placed no later than the floor (`floor_seen`).
    "floor_seen": 5.042315754e9,
    # A lettered record reaches its parent only in its recipient under its project
    # number (`_parent_key`): Germany's 1.06bn and the US's 0.24bn no longer reach
    # a parent elsewhere. Only an EU contract passes its floor in the parent tier
    # (`parent_floor_year`).
    "lettered parent": -1.336838177e9,
    "EU parent floor": -0.002612380e9,
    # A missing project number counts as one more project in `crs_key`.
    "crs_key on a missing project number": -0.029908498e9,
    # A floor naming no commitment under the disbursement's own crs id passes
    # (`same_crs_id`); 1.64bn of this is EU Institutions' disbursement under its
    # decision ids.
    "floor unseen": 1.862874714e9,
    # A component award keys each component by purpose (`split_components`).
    # `unique_by` already reaches each component in the purpose tier, so the split
    # only stops links across components, 0.86bn of them US awards.
    "component awards": -0.887649091e9,
    # A line filed under several purposes is one unit (`build_units`), and its
    # purposes join the purpose groups of its award (`purpose_group`), so no tier
    # finds one commitment for it. 10.81bn is the AsDB's: a technical-assistance
    # disbursement line spans the purposes its award commits under separate crs
    # ids. Key repair links 11.57bn of the AsDB's 12.19bn under new crs ids.
    "one unit per line": -11.087756573e9,
}
_PERIOD_KEY = ["donor_code", "agency_code", "period"]
# The CRS years the tests normalise; the first is the first year they load.
CRS_YEARS = (2010, 2025)


@pytest.fixture(scope="module")
def data() -> Path:
    return Path(os.environ["CF_E2E_DATA"])


@pytest.fixture(scope="module")
def crs(data: Path) -> pl.DataFrame:
    # Slow: digests 3.8 million records.
    raw = pl.scan_parquet(data / "raw" / "oecd" / "CRS_v20261006.parquet")
    return normalise_crs(raw, years=CRS_YEARS).collect()


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


def _prototype_parent_id(donor_code: pl.Expr, crs_id: pl.Expr) -> pl.Expr:
    """The references' parent id, the first ten characters of any donor's longer crs
    id. The build reads one for the EU and `LETTER_SUFFIX_DONORS` alone
    (`parent_id`), so the tests class agencies under this rule to compare."""
    return pl.when(crs_id.str.len_chars() > 10).then(crs_id.str.slice(0, 10))


def _moved(rows: pl.DataFrame, column: str, key: list[str]) -> pl.DataFrame:
    """The rows whose `column` differs from the reference's."""
    return rows.filter(pl.col(column) != pl.col(f"{column}_built")).select(
        *key, column, f"{column}_built"
    )


def test_one_period_id_practice_reproduces_the_deterministic_reference(
    data: Path, units: pl.DataFrame, monkeypatch: pytest.MonkeyPatch
) -> None:
    reference = pl.read_csv(
        data / "prototypes" / "deterministic-bulk" / "agency_classes.csv",
        schema_overrides={"donor_code": pl.Int32, "agency": pl.Int32},
    ).rename({"agency": "agency_code"})
    periods = {"2019-2022": (2019, 2022)}
    own = classify_id_practice(units, periods=periods)
    monkeypatch.setattr(practice, "parent_id", _prototype_parent_id)
    built = classify_id_practice(units, periods=periods)
    key = ["donor_code", "agency_code"]
    rows = _compare(reference, built, key)

    assert rows.height == reference.height == built.height == 311
    assert (rows["n_open"] == rows["open_commitments"]).all()
    assert (rows["parent_share"] == rows["parent_share_built"]).all()
    # The build trims crs ids, which that reference kept padded, so the UK's agency 8
    # and Germany's agencies 1 and 2 continue more by id. Germany's continue that
    # much less by project number.
    by_id = _moved(rows, "id_share", key)
    assert set(by_id.select(key).iter_rows()) == {(1, 8), (5, 1), (5, 2)}
    assert (by_id["id_share_built"] > by_id["id_share"]).all()
    # That reference collapsed spaces inside project numbers, where the build drops
    # them, so seven agencies continue a little more by project number. One of them
    # crosses the 0.2 continued-share line.
    by_pn = _moved(rows, "pn_share", key)
    collapsed = {(3, 1), (5, 9), (5, 63), (5, 66), (5, 94), (910, 1), (951, 1)}
    assert set(by_pn.select(key).iter_rows()) == collapsed | {(5, 1), (5, 2)}
    rises = by_pn.filter(pl.col("pn_share_built") > pl.col("pn_share"))
    assert set(rises.select(key).iter_rows()) == collapsed
    reclassed = rows.filter(pl.col("id_class") != pl.col("id_class_built"))
    assert reclassed.select(
        "donor_code", "agency_code", "id_class", "id_class_built"
    ).rows() == [(951, 1, "neither", "pn_persistent")]
    # The build's own parent rule moves only parent and project-number shares.
    moves = _compare(built, own, key)
    assert (moves["id_share"] == moves["id_share_built"]).all()
    fewer = _moved(moves, "parent_share", key)
    assert set(fewer.select(key).iter_rows()) == _FEWER_BY_PARENT
    assert (fewer["parent_share_built"] < fewer["parent_share"]).all()
    by_pn = _moved(moves, "pn_share", key)
    assert set(by_pn.select(key).iter_rows()) == _NOW_BY_PN
    assert (by_pn["pn_share_built"] > by_pn["pn_share"]).all()


def test_id_practice_reproduces_the_three_period_reference(
    data: Path,
    units: pl.DataFrame,
    id_practice: pl.DataFrame,
    monkeypatch: pytest.MonkeyPatch,
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
    # Under its own parent rule the build reads no parent id outside the EU and
    # `LETTER_SUFFIX_DONORS`.
    assert (
        id_practice.filter(
            ~pl.col("donor_code").is_in([schema.EU_INSTITUTIONS, *LETTER_SUFFIX_DONORS])
        )["parent_share"]
        == 0.0
    ).all()

    # Each agency's 2000-2009 row carries its 2010-2014 class, and the reference
    # holds the measured periods alone.
    early, first = (
        id_practice.filter(period=period)
        .drop("period", "keyed_disbursement_share")
        .sort("donor_code", "agency_code")
        for period in ("2000-2009", "2010-2014")
    )
    assert early.equals(first)
    measured = id_practice.filter(pl.col("period") != "2000-2009")

    monkeypatch.setattr(practice, "parent_id", _prototype_parent_id)
    built = classify_id_practice(units).filter(pl.col("period") != "2000-2009")
    rows = _compare(reference, built, _PERIOD_KEY)

    assert rows.height == reference.height == built.height == 798
    for column in ("id_class", "open_commitments", "parent_share"):
        assert (rows[column] == rows[f"{column}_built"]).all(), column
    # The build trims crs ids, which the reference kept padded, so the UK's agency 8
    # and Germany's agencies 1 and 2 continue more by id, and Germany's that much
    # less by project number.
    by_id = _moved(rows, "id_share", _PERIOD_KEY)
    assert set(by_id.select(_PERIOD_KEY).iter_rows()) == {
        (1, 8, "2019-2022"),
        (5, 1, "2015-2018"),
        (5, 1, "2019-2022"),
        (5, 2, "2015-2018"),
        (5, 2, "2019-2022"),
    }
    assert (by_id["id_share_built"] > by_id["id_share"]).all()
    # The build nulls the AfDB's and the AfDF's project numbers that Excel rounded,
    # so their agencies continue less by project number.
    by_pn = _moved(rows, "pn_share", _PERIOD_KEY)
    assert set(by_pn.select(_PERIOD_KEY).iter_rows()) == {
        (5, 1, "2015-2018"),
        (5, 1, "2019-2022"),
        (5, 2, "2015-2018"),
        (5, 2, "2019-2022"),
        (913, 1, "2010-2014"),
        (913, 1, "2015-2018"),
        (913, 11, "2015-2018"),
        (913, 14, "2010-2014"),
        (913, 15, "2010-2014"),
        (913, 16, "2010-2014"),
        (914, 1, "2010-2014"),
        (914, 1, "2015-2018"),
        (914, 13, "2015-2018"),
    }
    assert (by_pn["pn_share_built"] < by_pn["pn_share"]).all()
    # Under its own parent rule only `parent_id` moves a row: less continues by
    # parent id, and some of it by project number.
    moves = _compare(built, measured, _PERIOD_KEY)
    for column in ("open_commitments", "id_share"):
        assert (moves[column] == moves[f"{column}_built"]).all(), column
    # The 2019-2022 rows of `_FEWER_BY_PARENT` and 30 of earlier periods.
    fewer = _moved(moves, "parent_share", _PERIOD_KEY)
    assert fewer.height == 44
    assert (fewer["parent_share_built"] < fewer["parent_share"]).all()
    assert (moves["pn_share_built"] >= moves["pn_share"]).all()
    reclassed = moves.filter(pl.col("id_class") != pl.col("id_class_built"))
    assert reclassed.sort(_PERIOD_KEY).select(
        *_PERIOD_KEY, "id_class", "id_class_built"
    ).rows() == [
        (4, 10, "2015-2018", "mixed", "id_persistent"),
        (301, 9, "2010-2014", "parent_id_persistent", "neither"),
    ]


def test_key_tiers_link_the_reference_disbursements(
    data: Path, units: pl.DataFrame
) -> None:
    metrics = json.loads(
        (
            data / "prototypes" / "a1-reference" / "dated_pnuniq" / "trace_metrics.json"
        ).read_text(encoding="utf-8")
    )
    links = link_by_keys(units, first_year=CRS_YEARS[0])
    schema.validate(schema.LINKS, links)
    tiers = links.select("left_id", "link_method").unique()
    assert tiers["left_id"].is_unique().all()
    assert set(tiers["link_method"]) == set(schema.KEY_TIERS)

    linked = units.join(tiers, left_on="unit_id", right_on="left_id")
    assert linked["disbursement_usd"].sum() == pytest.approx(
        metrics["linked_disbursement_bn"] * 1e9
        + LOAN_TIER_LINKED_USD
        + sum(KEY_RULES_LINKED_USD.values()),
        abs=1e3,
    )


def test_within_year_join_reproduces_the_reference_population(
    data: Path, crs: pl.DataFrame, units: pl.DataFrame
) -> None:
    summary = json.loads(
        (data / "prototypes" / "deterministic-bulk" / "summary.json").read_text(
            encoding="utf-8"
        )
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

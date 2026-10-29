"""The BTR-to-CRS link over the archived BTR1 workbooks and the bulk CRS, and the
reconciliation of party totals on the archive release. Needs CF_E2E_DATA pointing at
the scoping `data/`."""

import os
import shutil
from pathlib import Path
from typing import NamedTuple

import polars as pl
import pytest

from climate_finance import schema
from climate_finance.build import release as assemble
from climate_finance.build.btr_coefficients import btr_coefficients
from climate_finance.build.fetch import DAC_EXCHANGE_FILE
from climate_finance.build.link.btr import (
    ID_PATTERN,
    LINK_PROBABILITY,
    MIN_ID_CHARS,
    MIN_MAP_OUTCOMES,
    _id_key,
    btr_rows,
    calibrate,
    crs_records,
    link_btr,
    link_by_ids,
    linked_pairs,
    score_party,
)
from climate_finance.build.link.score import is_heldout
from climate_finance.build.normalise import btr_at_dac_rates
from climate_finance.build.normalise.crs import normalise_crs
from climate_finance.build.readers.btr import estimate_party_weights, ingest_unfccc
from climate_finance.build.stages import raw_dir
from climate_finance.release import Release
from climate_finance.result import Reconciliation

pytestmark = pytest.mark.slow

# The prototype's party weights, by DAC provider code: 0.4 for the EU, Norway,
# Sweden, the Netherlands and Switzerland, 0.5 for Germany.
PROTOTYPE_WEIGHTS = {918: 0.4, 8: 0.4, 10: 0.4, 7: 0.4, 11: 0.4, 5: 0.5}

# Agreement of the BTR model with the ids on held-out activities, measured only where
# a row's ids resolve. The calibration map sees the training half's labels alone.
# Held-out rows agree at 0.946
# over 1,786 linked. Norway agrees at 0.981 held out against 0.983 in-sample, so the
# calibration does not overfit. The UK falls below 0.8. Halving France's labels
# leaves it short of `MIN_MAP_OUTCOMES`, so it reads the pooled map from
# `POOLED_FLOOR` up and links fewer than `MIN_PARTY_LINKED` held-out rows. Each
# party with `MIN_PARTY_LINKED` rows linked has (rows linked, agreement, whether the
# party holds its own map).
HELDOUT_AGREEMENT = {"linked": 1786, "pooled_parties": 25, "agreement": 0.946}
HELDOUT_BY_PARTY: dict[str, tuple[int, float, bool]] = {
    "DEU": (451, 0.96, True),
    "DNK": (383, 0.932, True),
    "GBR": (164, 0.774, True),
    "NLD": (176, 0.989, False),
    "NOR": (577, 0.981, True),
}
MIN_PARTY_LINKED = 50


@pytest.fixture(scope="module")
def data() -> Path:
    return Path(os.environ["CF_E2E_DATA"])


@pytest.fixture(scope="module")
def btr(data: Path, tmp_path_factory: pytest.TempPathFactory) -> pl.LazyFrame:
    out = tmp_path_factory.mktemp("btr") / "btr.parquet"
    return pl.scan_parquet(ingest_unfccc(data / "raw" / "unfccc_btr1", out_path=out))


@pytest.fixture(scope="module")
def crs(data: Path) -> pl.LazyFrame:
    raw = pl.scan_parquet(data / "raw" / "oecd" / "CRS_v20261006.parquet")
    return normalise_crs(raw, years=(2021, 2022)).collect().lazy()


@pytest.fixture(scope="module")
def links(btr: pl.LazyFrame, crs: pl.LazyFrame) -> pl.DataFrame:
    # Slow: fits a Splink model for each of the 28 parties with CRS records.
    return link_btr(btr, crs=crs).links


def test_uk_programme_numbers_find_their_programme_years(
    btr: pl.LazyFrame, crs: pl.LazyFrame
) -> None:
    rows = btr_rows(btr.filter(party="GBR"))
    by_id = link_by_ids(rows, records=crs_records(crs, rows=rows))
    programmes = rows.with_columns(
        programme=pl.col("title").str.extract(r"^\s*(\d{6})")
    ).drop_nulls("programme")
    found = programmes.join(by_id, on="row_id", how="semi")

    years = programmes.select("programme", "year").unique().height
    found_years = found.select("programme", "year").unique().height
    # Six of the seven not found have CRS records only in the other year. The
    # seventh, 300137 in 2022, has one record with no amount, which `normalise_crs`
    # drops. The prototype's join on the raw CRS finds it, giving 285.
    assert (years, found_years) == (291, 284)


class Scored(NamedTuple):
    """Every party's BTR rows, their id links, and their best-scoring candidates."""

    rows: pl.DataFrame
    truth: pl.DataFrame
    best: pl.DataFrame


def score_all(btr: pl.LazyFrame, crs: pl.LazyFrame) -> Scored:
    """Every party's BTR rows, id links and best candidates. Slow: fits a Splink
    model for each of the 28 parties with CRS records."""
    rows = btr_rows(btr)
    records = crs_records(crs, rows=rows)
    parties = rows.filter(pl.col("donor_code").is_not_null())["party"].unique().sort()
    return Scored(
        rows=rows,
        truth=link_by_ids(rows, records=records),
        best=pl.concat(
            [
                score_party(rows.filter(party=party), records=records)
                for party in parties
            ]
        ),
    )


@pytest.fixture(scope="module")
def scored(btr: pl.LazyFrame, crs: pl.LazyFrame) -> Scored:
    return score_all(btr, crs)


def label_counts(best: pl.DataFrame, *, truth: pl.DataFrame) -> pl.DataFrame:
    """Each party's `right` and `wrong` calibration labels from `truth`: its best
    candidates of id-linked rows that are, or are not, among the row's id records."""
    return (
        best.join(truth.select("row_id").unique(), on="row_id", how="semi")
        .join(
            truth.with_columns(right=pl.lit(value=True)),
            on=["row_id", "record_id"],
            how="left",
        )
        .group_by("party")
        .agg(
            right=pl.col("right").is_not_null().sum(),
            wrong=pl.col("right").is_null().sum(),
        )
    )


def _activities(rows: pl.DataFrame) -> pl.DataFrame:
    """`row_id` and `activity` for each row that writes an id: its party and the
    smallest id key among the ids of every row connected to it by a shared id, so
    an activity keeps its years together."""
    ids = (
        rows.select(
            "row_id",
            "party",
            key=pl.concat_str(
                pl.col("title").fill_null(""),
                pl.col("additional_information").fill_null(""),
                separator=" ",
            ).str.extract_all(ID_PATTERN),
        )
        .explode("key")
        .with_columns(key=_id_key(pl.col("key")))
        .filter(pl.col("key").str.len_chars() >= MIN_ID_CHARS)
        .group_by("row_id", "party")
        .agg(pl.col("key").unique().sort())
    )
    parent: dict[tuple[str, str], tuple[str, str]] = {}

    def root(node: tuple[str, str]) -> tuple[str, str]:
        parent.setdefault(node, node)
        while parent[node] != node:
            parent[node] = parent[parent[node]]
            node = parent[node]
        return node

    for party, keys in ids.select("party", "key").iter_rows():
        for key in keys:
            low, high = sorted((root((party, keys[0])), root((party, key))))
            parent[high] = low
    return pl.DataFrame(
        {
            "row_id": ids["row_id"],
            "activity": [
                "|".join(root((party, keys[0])))
                for party, keys in ids.select("party", "key").iter_rows()
            ],
        }
    )


def _agreement(linked: pl.DataFrame) -> float:
    """The share of `linked` rows whose candidate is in the truth."""
    return linked.select(pl.col("correct").mean()).item()


def test_model_agrees_with_ids_on_unseen_activities(scored: Scored) -> None:
    # Agreement is measured only where a row's ids resolve, because a row whose ids
    # find no CRS record carries no label and counts as wrong whatever it links to.
    activities = _activities(scored.rows)
    heldout = activities.filter(is_heldout(activities["activity"]))
    training = scored.truth.join(heldout, on="row_id", how="anti")
    calibrated = calibrate(scored.best, truth=training)
    linked = (
        calibrated.join(heldout.select("row_id"), on="row_id", how="semi")
        .join(scored.truth.select("row_id").unique(), on="row_id", how="semi")
        .filter(pl.col("probability") >= LINK_PROBABILITY)
        .join(
            scored.truth.with_columns(correct=pl.lit(value=True)),
            on=["row_id", "record_id"],
            how="left",
        )
        .with_columns(pl.col("correct").fill_null(value=False))
    )
    # A party whose training labels hold fewer than `MIN_MAP_OUTCOMES` of either
    # outcome takes the pooled map, or none when the pool is short too.
    own_map = label_counts(scored.best, truth=training).filter(
        pl.min_horizontal("right", "wrong") >= MIN_MAP_OUTCOMES
    )["party"]
    overall = {
        "linked": linked.height,
        "pooled_parties": scored.best["party"].n_unique() - len(own_map),
        "agreement": round(_agreement(linked), 3),
    }
    by_party = {
        party: (frame.height, round(_agreement(frame), 3), party in own_map)
        for (party,), frame in linked.group_by("party", maintain_order=True)
        if frame.height >= MIN_PARTY_LINKED
    }
    print(f"held-out agreement with the ids: {overall}")
    print(f"by party (linked, agreement, own map): {by_party}")

    assert overall == HELDOUT_AGREEMENT
    assert by_party == HELDOUT_BY_PARTY


def test_links_cover_the_reported_dollars(
    btr: pl.LazyFrame, links: pl.DataFrame
) -> None:
    rows = btr.select("row_id", "party", "face_usd").collect()
    linked = rows.join(links.select(row_id="left_id").unique(), on="row_id", how="semi")
    share = {
        party: linked.filter(party=party)["face_usd"].sum()
        / rows.filter(party=party)["face_usd"].sum()
        for party in ("NOR", "DEU", "GBR")
    }

    # "Global" rows hold 30% of Norway's dollars and 69% of the UK's, and key as
    # aggregates. Germany's map leaves 399 of its 1,411 id-less rows under the
    # threshold, most where it reads 0.21 to 0.33 on blocks of 14, 12 and 3 id
    # labels; `calibration_draw` draws rows there for people to label. Parties
    # without their own map link only from `POOLED_FLOOR` up.
    assert linked["face_usd"].sum() / rows["face_usd"].sum() == pytest.approx(
        0.615, abs=0.001
    )
    assert share == pytest.approx({"NOR": 1.0, "DEU": 0.727, "GBR": 0.910}, abs=0.001)


def test_parties_without_their_own_map_link_only_from_the_pooled_floor(
    btr: pl.LazyFrame, links: pl.DataFrame
) -> None:
    rows = btr.select("row_id", "party").collect()
    by_model = rows.join(
        links.filter(link_method="btr_splink").select(row_id="left_id").unique(),
        on="row_id",
        how="semi",
    )
    linked = dict(by_model.group_by("party").len().iter_rows())

    # None of the three writes an id the build resolves, so every model link reads
    # the pooled map. The United States' rows all score under the floor.
    # `docs/how-to/reconcile-with-btr.md` quotes the model's share of the links.
    assert links.filter(link_method="btr_splink").height / links.height == (
        pytest.approx(0.489, abs=0.001)
    )
    assert {party: linked.get(party, 0) for party in ("JPN", "USA", "SWE")} == {
        "JPN": 1438,
        "USA": 0,
        "SWE": 510,
    }


def test_linked_pairs_give_germany_half_and_norway_forty_percent(
    btr: pl.LazyFrame, crs: pl.LazyFrame, links: pl.DataFrame
) -> None:
    weights = {
        row["party"]: row
        for row in estimate_party_weights(
            linked_pairs(links, btr=btr, crs=crs)
        ).to_dicts()
    }

    assert weights["DEU"]["significant"] == 0.5
    assert weights["NOR"]["significant"] == 0.4
    assert weights["EUU"]["significant"] == 0.4
    assert weights["SWE"]["significant"] == 0.4
    assert weights["NLD"]["significant"] == 0.4
    assert weights["NOR"]["principal_at_100"] == pytest.approx(0.996, abs=0.001)


@pytest.fixture(scope="module")
def reconciliation(
    release: Path,
    btr: pl.LazyFrame,
    crs: pl.LazyFrame,
    links: pl.DataFrame,
    tmp_path_factory: pytest.TempPathFactory,
) -> Reconciliation:
    """The archive release with the BTR tables and the prototype's weights."""
    out = tmp_path_factory.mktemp("reconcile")
    _with_btr_tables(
        release,
        out,
        btr=btr,
        crs=crs,
        links=links,
        coefficients=_prototype_coefficients(),
    )
    return _reconcile(out)


@pytest.fixture(scope="module")
def own_release(
    release: Path,
    btr: pl.LazyFrame,
    crs: pl.LazyFrame,
    links: pl.DataFrame,
    tmp_path_factory: pytest.TempPathFactory,
) -> Path:
    """The archive release with the BTR tables, its own `provider_coefficients` and
    the `btr_coefficients` its links give."""
    out = tmp_path_factory.mktemp("reconcile_own")
    _with_btr_tables(
        release,
        out,
        btr=btr,
        crs=crs,
        links=links,
        coefficients=pl.read_parquet(release / "provider_coefficients.parquet"),
    )
    return out


@pytest.fixture(scope="module")
def reconciliation_at_own_coefficients(own_release: Path) -> Reconciliation:
    return _reconcile(own_release)


def _with_btr_tables(
    release: Path,
    out: Path,
    *,
    btr: pl.LazyFrame,
    crs: pl.LazyFrame,
    links: pl.DataFrame,
    coefficients: pl.DataFrame,
) -> None:
    """Copy `release` to `out` with `coefficients` as its `provider_coefficients`,
    and its BTR tables, `btr_coefficients` and `total_flows` built from `btr`, `crs`
    and `links`."""
    for name in (*schema.RELEASE_TABLES, "release"):
        suffix = ".json" if name == "release" else ".parquet"
        if (release / f"{name}{suffix}").exists():
            shutil.copyfile(release / f"{name}{suffix}", out / f"{name}{suffix}")
    rows = btr_at_dac_rates(
        btr.collect(),
        exchange=pl.read_parquet(raw_dir(release.parent) / DAC_EXCHANGE_FILE),
    )
    rows.write_parquet(out / "btr.parquet")
    btr_links = assemble.btr_links(links.lazy(), crs=crs).collect()
    btr_links.write_parquet(out / "btr_links.parquet")
    coefficients.write_parquet(out / "provider_coefficients.parquet")
    btr_coefficients(
        rows,
        links=btr_links.join(
            crs.select("record_id", "agency_code").collect(),
            on="record_id",
            how="left",
            validate="m:1",
        ),
        coefficients=coefficients,
    ).write_parquet(out / "btr_coefficients.parquet")
    assemble.btr_totals(rows.lazy(), links=btr_links.lazy()).collect().write_parquet(
        out / "btr_totals.parquet"
    )
    assemble.btr_crs_unlinked(
        crs, btr=rows.lazy(), links=links.lazy()
    ).collect().write_parquet(out / "btr_crs_unlinked.parquet")
    # The CRS of 2021 and 2022, so the years compared.
    assemble.total_flows(crs).collect().write_parquet(out / "total_flows.parquet")


def _prototype_coefficients() -> pl.DataFrame:
    """`provider_coefficients` at `PROTOTYPE_WEIGHTS`, with no both-significant
    coefficient."""
    return pl.DataFrame(
        {
            "provider_code": list(PROTOTYPE_WEIGHTS),
            "agency_code": None,
            "first_year": None,
            "last_year": None,
            "significant": list(PROTOTYPE_WEIGHTS.values()),
            "principal": 1.0,
            "both_significant": None,
            "case_by_case": False,
            "source": "prototype",
            "locator": "weights",
        },
        schema={
            name: column.dtype
            for name, column in schema.PROVIDER_COEFFICIENTS.columns.items()
        },
    )


def _reconcile(out: Path) -> Reconciliation:
    """`reconcile` over 2021 and 2022 on the release in `out`."""
    manifest = schema.ReleaseManifest.model_validate_json(
        (out / "release.json").read_text(encoding="utf-8")
    )
    return Release(path=out, manifest=manifest).reconcile(years=(2021, 2022))


@pytest.mark.parametrize(
    ("party", "year", "comparator", "ratio"),
    [
        # BTR amounts at the DAC rate; at the parties' own rates the EU reads 1.0032
        # and 1.0103, the Netherlands 1.0059, Switzerland 1.0118 and Germany 0.9385.
        ("EUU", 2021, "crdf_at_party_weight", 1.0025),
        ("EUU", 2022, "crdf_at_party_weight", 1.0093),
        # The CRS comparator leaves out records reported for memorandum. Norway's
        # BTR lists Norfund's investments, which the CRS reports for memorandum
        # (USD 197m at these weights, `crs_memorandum_disbursed_at_party_weight`),
        # so it reads 1.40. Sweden and Switzerland claim the capital paid into
        # Swedfund and SIFEM; with the memorandum records kept they read 1.0016 and
        # 1.0119.
        ("NOR", 2021, "crs_disbursed_at_party_weight", 1.4011),
        ("SWE", 2021, "crs_disbursed_at_party_weight", 1.0451),
        ("NLD", 2021, "crs_disbursed_at_party_weight", 1.0056),
        ("CHE", 2022, "crs_disbursed_at_party_weight", 1.0396),
        # 6.2% under, which the prototype's report rounds to 0.94. Its CRDF at 100%
        # is the same USD 12,411m.
        ("DEU", 2022, "crdf_at_party_weight", 0.9376),
    ],
)
def test_party_totals_agree_at_the_prototype_weights(
    reconciliation: Reconciliation,
    party: str,
    year: int,
    comparator: str,
    ratio: float,
) -> None:
    rows = reconciliation.table.filter(
        party=party, year=year, method="ONE", comparator=comparator
    )

    assert rows.height == 1
    assert rows["btr_over_comparator"].item() == pytest.approx(ratio, abs=0.0005)


def test_germany_counts_both_significant_disbursements_in_full(
    reconciliation_at_own_coefficients: Reconciliation,
) -> None:
    # Germany's coefficients count a record significant on both objectives at 1.0
    # and one significant on one at 0.5. Its 1,341 both-significant records of 2021
    # disburse USD 1.82bn, which at 0.5 would give USD 4.54bn.
    rows = reconciliation_at_own_coefficients.table.filter(
        party="DEU", year=2021, method="ONE"
    )

    assert rows.height == 1
    assert rows["crs_disbursed_at_party_weight"].item() == pytest.approx(
        5.45e9, rel=0.005
    )


METHOD_SHARES = (
    "crdf_method_share",
    "crs_method_share",
    "crs_grant_equivalent_method_share",
)


def _method_shares(reconciliation: Reconciliation, party: str) -> pl.Series:
    """`party`'s method shares of 2021 and 2022, every comparator's alike."""
    rows = reconciliation.table.filter(party=party, method="ONE")
    shares = rows.select(pl.concat_list(METHOD_SHARES).explode()).to_series()
    assert shares.null_count() == 0
    return shares


@pytest.mark.parametrize(
    ("party", "share"),
    [
        # AFD assesses each activity case by case and takes the coefficients
        # France's BTR implies, so none of France's comparators weigh at PROVIDER's
        # own; with PROVIDER's 0.4 and 1.0 its shares ran from 0.71 to 0.86.
        ("FRA", 0.0),
        ("CAN", 0.0),
        # The release links no US BTR row to a CRS record, so the US keeps
        # PROVIDER's 0.4 and 1.0 on every comparator.
        ("USA", 1.0),
    ],
)
def test_method_shares_follow_where_each_party_takes_its_coefficients(
    reconciliation_at_own_coefficients: Reconciliation, party: str, share: float
) -> None:
    shares = _method_shares(reconciliation_at_own_coefficients, party)

    assert len(shares) and (shares - share).abs().lt(0.01).all()


# For each provider and agency that assesses case by case, and each marker: the
# coefficient its BTR implies, which `PROVIDER` takes, why there is none, and the
# components behind it, after those above a share of 1.1 leave the evidence.
# Belgium, France's AFD and the UK take both markers from their BTR and Luxembourg
# its significant one. The rest, the US among them, have too little linked
# evidence or none, and Australia keeps its stated default.
type Scope = tuple[int, int | None, str]
CASE_BY_CASE: dict[Scope, tuple[float | None, str, str | None, int]] = {
    (2, None, "principal"): (0.902, "btr", None, 154),
    (2, None, "significant"): (0.158, "btr", None, 203),
    (4, 3, "principal"): (0.863, "btr", None, 77),
    (4, 3, "significant"): (0.618, "btr", None, 67),
    (12, None, "principal"): (0.89, "btr", None, 104),
    (12, None, "significant"): (0.398, "btr", None, 74),
    (18, None, "principal"): (None, "fallback", "no_links", 0),
    (18, None, "significant"): (None, "fallback", "no_links", 0),
    (22, None, "principal"): (None, "fallback", "thin", 83),
    (22, None, "significant"): (0.543, "btr", None, 86),
    (69, None, "principal"): (None, "fallback", "thin", 10),
    (69, None, "significant"): (None, "fallback", "thin", 30),
    (75, None, "principal"): (None, "fallback", "no_links", 0),
    (75, None, "significant"): (None, "fallback", "no_links", 0),
    (82, None, "principal"): (None, "fallback", "unaligned", 0),
    (82, None, "significant"): (None, "fallback", "no_links", 0),
    (84, None, "principal"): (None, "fallback", "no_links", 0),
    (84, None, "significant"): (None, "fallback", "no_links", 0),
    (301, 22, "principal"): (None, "fallback", "no_links", 0),
    (301, 22, "significant"): (None, "fallback", "no_links", 0),
    (301, 31, "principal"): (None, "fallback", "no_links", 0),
    (301, 31, "significant"): (None, "fallback", "no_links", 0),
    (302, None, "principal"): (None, "fallback", "no_links", 0),
    (302, None, "significant"): (None, "fallback", "no_links", 0),
    (801, None, "principal"): (None, "stated", "no_links", 0),
    (801, None, "significant"): (None, "stated", "no_links", 0),
}


def test_case_by_case_providers_take_the_coefficients_their_btr_implies(
    own_release: Path,
) -> None:
    coefficients = pl.read_parquet(own_release / "provider_coefficients.parquet")
    scopes = coefficients.filter("case_by_case").select("provider_code", "agency_code")
    table = pl.read_parquet(own_release / "btr_coefficients.parquet").join(
        scopes, on=["provider_code", "agency_code"], how="semi", nulls_equal=True
    )
    found = {
        (row["provider_code"], row["agency_code"], row["marker"]): (
            row["coefficient"] and round(row["coefficient"], 3),
            row["source"],
            row["reason"],
            row["components"],
        )
        for row in table.iter_rows(named=True)
    }

    assert found == CASE_BY_CASE

"""The grant fill and the OXFAM methods on the archive release: the normalised CRS
against the bulk file, the identities `flow="grant_equivalent"` holds for the
multilateral outflows of the recipient view, and OXFAM_LOW and OXFAM_HIGH for the
developed countries Oxfam counts. The `release` fixture in conftest.py builds the
archive.
"""

from pathlib import Path

import polars as pl
import pytest

import climate_finance as cf
from climate_finance.build.fetch import CRS_FILE
from climate_finance.build.stages import raw_dir, table_path
from climate_finance.schema import MULTILATERAL_OUTFLOW

pytestmark = pytest.mark.slow

YEARS = range(2015, 2025)
# The CRS finance types of a grant: a standard grant and an interest subsidy.
GRANT_TYPES = (110, 210)
# Oxfam's developed countries (oxfam/common.py DEVELOPED in the reproduction): the
# DAC members, EU Institutions, and the EU member states, Liechtenstein and Monaco
# the DAC does not list, by ISO 3 code. `cf.providers.code` reads each.
DEVELOPED = (
    "AUT", "BEL", "DNK", "FRA", "DEU", "ITA", "NLD", "NOR", "PRT", "SWE", "CHE", "GBR",
    "FIN", "ISL", "IRL", "LUX", "GRC", "ESP", "SVN", "CZE", "SVK", "HUN", "POL", "ROU",
    "EST", "LVA", "LTU", "CAN", "USA", "JPN", "AUS", "NZL", "EUU", "HRV", "LIE", "MCO",
    "BGR", "CYP", "MLT",
)  # fmt: skip
# The reproduction's CSNA for 2024 from the grant-element data the CRS holds, in USD
# billion, low and high: the snapshot the two methods are compared with by eye.
HELD_SNAPSHOT_2024 = (25.3, 34.0)
# Multilateral outflows, 2015 to 2024, under ONE in the recipient view, as shares of
# the climate-weighted disbursements: the part on records whose grant equivalent is
# their disbursement, and the part on records with no grant equivalent.
MULTILATERAL_SHARES = {"at_face": 0.0990, "no_grant_equivalent": 0.9010}
# OXFAM_LOW and OXFAM_HIGH `value` for 2024, in USD billion, under
# `flow="grant_equivalent"` in the provider view.
OXFAM_PINS = {"OXFAM_LOW": 25.77, "OXFAM_HIGH": 33.26}
PINNED_TOLERANCE = 0.01
TOLERANCE = 1e-9
ONE_DOLLAR = 1.0


@pytest.fixture(scope="module")
def loaded(release: Path) -> cf.Release:
    return cf.load(path=release)


@pytest.fixture(scope="module")
def crs(release: Path) -> pl.DataFrame:
    """The normalised CRS records' grant equivalents."""
    return pl.read_parquet(
        table_path(release.parent, "normalise", "crs"),
        columns=[
            "record_id",
            "year",
            "finance_t",
            "disbursement_usd",
            "grant_equiv_usd",
        ],
    )


def test_every_grant_has_a_grant_equivalent_and_no_other_record_changed(
    loaded: cf.Release, release: Path, crs: pl.DataFrame
) -> None:
    """Per year and finance type, the normalised grant equivalents are the bulk
    CRS's: a negative one on a positive disbursement null, and a grant without one
    at its disbursement. A record with a grant equivalent of its own keeps it."""
    raw = pl.scan_parquet(raw_dir(release.parent) / CRS_FILE).filter(
        pl.col("year").is_between(*loaded.manifest.years["crs"]),
        pl.any_horizontal(
            pl.col(name).fill_null(0) != 0
            for name in ("usd_commitment", "usd_disbursement", "usd_grant_equiv")
        ),
    )
    given, disbursed = pl.col("usd_grant_equiv"), pl.col("usd_disbursement")
    published = given.is_not_null() & ~((given < 0) & (disbursed > 0)).fill_null(False)
    kept = (
        pl.when(published)
        .then(given)
        .when(pl.col("finance_t").is_in(GRANT_TYPES))
        .then(disbursed)
    )
    expected = (
        raw.group_by(pl.col("year").cast(pl.Int16), pl.col("finance_t").cast(pl.Int32))
        .agg(
            records=pl.len(),
            without=kept.is_null().sum(),
            grant_equiv_usd=kept.sum() * 1e6,
        )
        .collect()
    )
    built = crs.group_by("year", "finance_t").agg(
        records=pl.len(),
        without=pl.col("grant_equiv_usd").is_null().sum(),
        grant_equiv_usd=pl.col("grant_equiv_usd").sum(),
    )
    keys = ["year", "finance_t"]
    both = expected.join(built, on=keys, how="full", nulls_equal=True, suffix="_built")
    print(
        both.group_by("finance_t")
        .agg(pl.col("records", "without", "without_built").sum())
        .sort("finance_t", nulls_last=True)
    )

    assert both.null_count().select(pl.sum_horizontal(pl.all())).item() == 0
    assert both["records"].to_list() == both["records_built"].to_list()
    assert both["without"].to_list() == both["without_built"].to_list()
    assert both["grant_equiv_usd_built"].to_list() == pytest.approx(
        both["grant_equiv_usd"].to_list(), rel=TOLERANCE, abs=ONE_DOLLAR
    )
    grants = crs.filter(
        pl.col("finance_t").is_in(GRANT_TYPES), pl.col("disbursement_usd") > 0
    )
    assert grants.height > 0
    assert grants["grant_equiv_usd"].null_count() == 0


@pytest.fixture(scope="module")
def multilateral(loaded: cf.Release, crs: pl.DataFrame) -> dict[str, pl.DataFrame]:
    """The multilateral outflows' rows of the recipient view under ONE, 2015 to
    2024, by flow, summed by the kind of record they come from: `at_face` when the
    record's grant equivalent is its disbursement, `none` when it has no grant
    equivalent, else `other`."""
    kinds = crs.select(
        "record_id",
        kind=pl.when(pl.col("grant_equiv_usd").is_null())
        .then(pl.lit("none"))
        .when(
            (pl.col("disbursement_usd") > 0)
            & (pl.col("grant_equiv_usd") == pl.col("disbursement_usd"))
        )
        .then(pl.lit("at_face"))
        .otherwise(pl.lit("other")),
    )
    sums: dict[str, pl.DataFrame] = {}
    for flow in ("disbursements", "grant_equivalent"):
        rows = loaded.spending(years=YEARS, flow=flow).rows(
            bi_multi=MULTILATERAL_OUTFLOW
        )
        joined = rows.join(kinds, on="record_id", how="left", validate="m:1")
        assert joined["kind"].null_count() == 0
        sums[flow] = (
            joined.group_by("kind")
            .agg(pl.col("value").sum(), pl.col("no_grant_equivalent").sum())
            .sort("kind")
        )
    return sums


def _kind(sums: pl.DataFrame, kind: str, column: str) -> float:
    return float(sums.filter(kind=kind)[column].sum())


def test_multilateral_grant_equivalents_add_up_to_the_disbursements(
    multilateral: dict[str, pl.DataFrame],
) -> None:
    """A record whose grant equivalent is its disbursement counts at face in
    `value`; one with none counts 0 in `value` and its face in
    `no_grant_equivalent`; every other record has no face value left out."""
    face, grant = multilateral["disbursements"], multilateral["grant_equivalent"]
    print(face, grant, sep="\n")

    assert _kind(grant, "at_face", "value") + _kind(
        grant, "at_face", "no_grant_equivalent"
    ) == pytest.approx(_kind(face, "at_face", "value"), rel=TOLERANCE)
    assert _kind(grant, "at_face", "no_grant_equivalent") == 0
    assert _kind(grant, "none", "value") == 0
    assert _kind(grant, "none", "no_grant_equivalent") == pytest.approx(
        _kind(face, "none", "value"), rel=TOLERANCE
    )
    assert _kind(grant, "other", "no_grant_equivalent") == 0


def test_multilateral_grants_at_face_hold_a_pinned_share(
    multilateral: dict[str, pl.DataFrame],
) -> None:
    face, grant = multilateral["disbursements"], multilateral["grant_equivalent"]
    total = float(face["value"].sum())
    shares = {
        "at_face": _kind(face, "at_face", "value") / total,
        "no_grant_equivalent": _kind(grant, "none", "no_grant_equivalent") / total,
    }
    print({name: round(share, 4) for name, share in shares.items()})

    assert shares == pytest.approx(MULTILATERAL_SHARES, rel=PINNED_TOLERANCE)


@pytest.fixture(scope="module")
def oxfam(loaded: cf.Release) -> dict[str, dict[str, float]]:
    """OXFAM_LOW and OXFAM_HIGH run apart for Oxfam's developed countries, 2024, in
    USD billion: `value` and `no_grant_equivalent`."""
    providers = [cf.providers.code(iso) for iso in DEVELOPED]
    assert len(set(providers)) == len(DEVELOPED)
    figures = {}
    methods = {"OXFAM_LOW": cf.methods.OXFAM_LOW, "OXFAM_HIGH": cf.methods.OXFAM_HIGH}
    for name, method in methods.items():
        table = loaded.spending(
            method=method,
            providers=providers,
            years=2024,
            flow="grant_equivalent",
            view="provider",
        ).table
        figures[name] = {
            "value": float(table["value"].sum()) / 1e9,
            "no_grant_equivalent": float(table["no_grant_equivalent"].sum()) / 1e9,
        }
    return figures


@pytest.mark.parametrize("method", ["OXFAM_LOW", "OXFAM_HIGH"])
def test_oxfam_bound_is_pinned(oxfam: dict[str, dict[str, float]], method: str) -> None:
    figures = oxfam[method]
    low, high = HELD_SNAPSHOT_2024
    print(
        f"{method} 2024: value {figures['value']:.2f}, "
        f"no_grant_equivalent {figures['no_grant_equivalent']:.2f} billion; "
        f"the held-data snapshot is {low} to {high}"
    )

    assert figures["value"] == pytest.approx(OXFAM_PINS[method], rel=PINNED_TOLERANCE)


def test_oxfam_low_does_not_exceed_oxfam_high(
    oxfam: dict[str, dict[str, float]],
) -> None:
    assert oxfam["OXFAM_LOW"]["value"] <= oxfam["OXFAM_HIGH"]["value"]

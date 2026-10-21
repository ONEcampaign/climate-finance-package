"""Programme stems and blocking recall on the archived OECD files, against the
end-to-end prototype under `prototypes/` in CF_E2E_DATA.

Id truth follows the prototype: a commitment a CRDF row reaches, of an agency classed
`id_persistent` in its record year's period, with a crs id, continues in every
disbursement unit of the same donor and crs id from a later record year. Blocking
recall is the share of those pairs blocking produces.
"""

import os
from pathlib import Path

import polars as pl
import pytest

from climate_finance import schema
from climate_finance.build.link.candidates import (
    generate_candidates,
    learn_programme_stems,
)
from climate_finance.build.link.crdf_crs import link_within_year
from climate_finance.build.link.id_practice import classify_id_practice, period_of
from climate_finance.build.link.key_repair import repair_keys
from climate_finance.build.link.keys import build_units, link_by_keys
from climate_finance.build.normalise.crdf import normalise_crdf
from climate_finance.build.normalise.crs import normalise_crs, trace_population

pytestmark = pytest.mark.slow

FIRST_YEAR = 2010
# The prototype's population years, and the CRDF years the build covers.
PROTOTYPE_YEARS = (2021, 2022)
CRDF_YEARS = (2015, 2024)
RECALL_TARGET = 0.985
_CATEGORY = {
    "DAC member": "bilateral",
    "Non-DAC member": "bilateral",
    "Multilateral development bank": "banks",
    "Other multilateral": "other_multilateral",
    "Private donor": "private",
}


@pytest.fixture(scope="module")
def data() -> Path:
    return Path(os.environ["CF_E2E_DATA"])


@pytest.fixture(scope="module")
def normalised(data: Path) -> pl.DataFrame:
    # Slow: digests 3.8 million records.
    raw = pl.scan_parquet(data / "raw" / "oecd" / "CRS_v20261006.parquet")
    return normalise_crs(raw, years=(2010, 2025)).collect()


@pytest.fixture(scope="module")
def crs(normalised: pl.DataFrame) -> pl.DataFrame:
    return trace_population(normalised.lazy()).collect()


@pytest.fixture(scope="module")
def units(crs: pl.DataFrame) -> pl.DataFrame:
    return build_units(crs.lazy()).collect()


@pytest.fixture(scope="module")
def id_practice(units: pl.DataFrame) -> pl.DataFrame:
    return classify_id_practice(units)


@pytest.fixture(scope="module")
def links(
    data: Path,
    normalised: pl.DataFrame,
    units: pl.DataFrame,
    id_practice: pl.DataFrame,
) -> pl.DataFrame:
    """The link stage's links with key repair on, as `run_link` builds them."""
    crdf = normalise_crdf(
        pl.scan_parquet(data / "raw" / "oecd" / "crdf_recipient.parquet"),
        perspective="recipient",
        crs=normalised.lazy(),
    )
    trace = link_by_keys(units, id_practice=id_practice, first_year=FIRST_YEAR)
    return pl.concat(
        [
            trace,
            repair_keys(
                units, crs=normalised.lazy(), links=trace, first_year=FIRST_YEAR
            ),
            link_within_year(crdf, units=units.lazy()).collect(),
        ]
    )


@pytest.fixture(scope="module")
def stems(crs: pl.DataFrame) -> pl.DataFrame:
    return learn_programme_stems(crs.lazy()).collect()


@pytest.fixture(scope="module")
def candidates(
    crs: pl.DataFrame,
    units: pl.DataFrame,
    links: pl.DataFrame,
    id_practice: pl.DataFrame,
    stems: pl.DataFrame,
) -> pl.DataFrame:
    return generate_candidates(
        units,
        crs=crs.lazy(),
        links=links,
        id_practice=id_practice,
        stems=stems,
        first_year=FIRST_YEAR,
    )


@pytest.fixture(scope="module")
def truth(
    data: Path,
    units: pl.DataFrame,
    links: pl.DataFrame,
    id_practice: pl.DataFrame,
    candidates: pl.DataFrame,
) -> pl.DataFrame:
    """Every id-truth pair, with whether blocking produced it and the commitment's
    provider category."""
    categories = (
        pl.scan_parquet(data / "raw" / "oecd" / "crdf_recipient.parquet")
        .select(
            pl.col("provider_code", "agency_code").cast(pl.Int32),
            category=pl.col("provider_type").replace_strict(_CATEGORY),
        )
        .unique()
        .collect()
    )
    persistent = id_practice.filter(id_class="id_persistent").select(
        "donor_code", "agency_code", "period"
    )
    commitments = (
        units.join(
            links.filter(task="crdf_crs").select(unit_id="right_id"),
            on="unit_id",
            how="semi",
        )
        .with_columns(period=period_of(pl.col("year")))
        .join(persistent, on=["donor_code", "agency_code", "period"], how="semi")
        .drop_nulls("crs_id")
    )
    disbursements = units.filter(
        pl.col("disbursement_usd") > 0,
        pl.col("floor_year").is_null() | (pl.col("floor_year") >= FIRST_YEAR),
    )
    return (
        commitments.select("unit_id", "donor_code", "agency_code", "crs_id", "year")
        .join(
            disbursements.select("unit_id", "donor_code", "crs_id", "year"),
            on=["donor_code", "crs_id"],
            suffix="_disbursement",
        )
        .filter(pl.col("year_disbursement") > pl.col("year"))
        .select(
            left_id="unit_id_disbursement",
            right_id="unit_id",
            year="year",
            provider_code="donor_code",
            agency_code="agency_code",
        )
        .join(
            candidates.select("left_id", "right_id", "same_crs_id"),
            on=["left_id", "right_id"],
            how="left",
        )
        .with_columns(blocked=pl.col("same_crs_id").is_not_null())
        .join(categories, on=["provider_code", "agency_code"], how="left")
    )


def test_programme_stems_overlap_the_prototype_rules(
    data: Path, stems: pl.DataFrame
) -> None:
    schema.validate(schema.PROGRAMME_STEMS, stems)
    # The prototype learned its rules on the training half; the build learns on
    # every record, so more candidate rules reach MIN_PAIRS.
    prototype = pl.read_csv(
        data / "prototypes" / "end-to-end" / "rules" / "rules.csv",
        schema_overrides={"donor_code": pl.Int32, "agency": pl.Int32},
    ).select("donor_code", agency_code="agency", pattern="shape", k="k")
    shared = prototype.join(stems, on=["donor_code", "agency_code", "pattern"])

    assert stems.height == 194
    assert stems.select("donor_code", "agency_code").n_unique() == 94
    assert prototype.height == 143
    assert shared.height == 111
    assert (shared["k"] == shared["segments"]).sum() == 102


def test_candidates_hold_eligible_and_training_pairs(
    candidates: pl.DataFrame,
) -> None:
    schema.validate(schema.CANDIDATES, candidates)
    counts = dict(candidates.group_by("eligible").len().iter_rows())
    assert counts == {True: 1_697_372, False: 6_067_017}
    assert (candidates["eligible"] != candidates["id_persistent"]).all()


def test_blocking_recall_on_id_truth(truth: pl.DataFrame) -> None:
    prototype_years = truth.filter(pl.col("year").is_between(*PROTOTYPE_YEARS))
    crdf_years = truth.filter(pl.col("year").is_between(*CRDF_YEARS))
    by_category = dict(
        crdf_years.group_by("category").agg(pl.col("blocked").mean()).iter_rows()
    )

    # Every blocked truth pair carries its label.
    assert truth.filter("blocked")["same_crs_id"].all()
    assert prototype_years.height == 14_605
    assert prototype_years["blocked"].mean() >= RECALL_TARGET
    # Over the build's CRDF years recall falls short of the target: five crs ids
    # spread over many recipients and purposes hold 441 of the 930 missed pairs.
    assert crdf_years.height == 57_795
    assert crdf_years["blocked"].mean() == pytest.approx(0.98391, abs=1e-5)
    assert by_category == pytest.approx(
        {
            "bilateral": 0.98403,
            "other_multilateral": 0.90948,
            "private": 1.0,
            "banks": 1.0,
        },
        abs=1e-5,
    )

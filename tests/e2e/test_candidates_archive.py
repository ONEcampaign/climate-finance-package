"""Programme stems and blocking recall on the archived OECD files, against the
end-to-end prototype under `prototypes/` in CF_E2E_DATA.

Id truth follows the prototype. A commitment a CRDF row reaches, of an agency classed
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

# The CRS years the tests normalise; the first is the first year they load.
CRS_YEARS = (2010, 2025)
FIRST_YEAR = CRS_YEARS[0]
# The prototype's population years, and the CRDF years the build covers.
PROTOTYPE_YEARS = (2021, 2022)
CRDF_YEARS = (2015, 2024)
RECALL_TARGET = 0.985
# Eligible and training pairs under the key tiers and repair rules before the rules
# in `KEY_RULES_ELIGIBLE`. A line filed under several purposes is one unit
# (`keys.build_units`), so a disbursement pairs with it once.
ELIGIBLE_PAIRS = 1_297_252
TRAINING_PAIRS = 1_228_227
# Eligible pairs each key rule adds by leaving disbursements unlinked, or takes by
# linking them, each measured with the rules above it in place. No rule moves a
# training pair.
KEY_RULES_ELIGIBLE = {
    # A lettered record reaches its parent only in its recipient under its project
    # number (`_parent_key`).
    "lettered parent": 2_945,
    # Only an EU contract passes its floor in the parent tier (`parent_floor_year`).
    "EU parent floor": 0,
    # A missing project number counts as one more project in `crs_key`.
    "crs_key on a missing project number": 2,
    # A floor naming no commitment under the disbursement's own crs id passes
    # (`same_crs_id`).
    "floor unseen": -19,
    # A component award keys each component by purpose (`split_components`), so
    # the project-number tiers link within one component, and the disbursements
    # they linked across components, mostly US, go to scoring.
    "component awards": 23_664,
    # Key repair compares awards too: it links 2,851 disbursement units within one
    # component, 2,528 of them US and 171 Canadian, and no longer links 5,529
    # across components. The units it now links held more pairs than those it
    # frees.
    "component awards in key repair": -12_127,
}
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
    return normalise_crs(raw, years=CRS_YEARS).collect()


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
    trace = link_by_keys(units, first_year=FIRST_YEAR)
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
    pairs = (
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
    )
    return (
        pairs.join(
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
    # The prototype learned its rules on the training half and the build learns on
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
    # `repair_stray_country` takes 6,784 eligible pairs, 6,777 of them Canada's. In
    # 6,724 it links the commitment, in 60 only the disbursement. `floor_seen` gives
    # pre-2010-floor commitments their self links, which closes them to key repair,
    # so 5 British and German disbursements lose repair links and add 9 pairs. The
    # `short_description` fallback titles untitled units, which raises training pairs
    # to 7,198,372; `template_lines` then takes 5,826,943 of them, almost all Japan's
    # "TC aggregated activities", and the two leave 4,774 fewer eligible pairs.
    assert counts == {
        True: ELIGIBLE_PAIRS + sum(KEY_RULES_ELIGIBLE.values()),
        False: TRAINING_PAIRS,
    }
    assert (candidates["eligible"] != candidates["id_persistent"]).all()


def test_blocking_recall_on_id_truth(truth: pl.DataFrame) -> None:
    prototype_years = truth.filter(pl.col("year").is_between(*PROTOTYPE_YEARS))
    crdf_years = truth.filter(pl.col("year").is_between(*CRDF_YEARS))
    by_category = dict(
        crdf_years.group_by("category").agg(pl.col("blocked").mean()).iter_rows()
    )

    # Every blocked truth pair carries its label.
    assert truth.filter("blocked")["same_crs_id"].all()
    # A line filed under several purposes is one unit, so truth holds one pair per
    # disbursement and line.
    assert prototype_years.height == 8_205
    assert prototype_years["blocked"].mean() >= RECALL_TARGET
    # Over the build's CRDF years two Italian crs ids hold 24 of the 321 missed
    # pairs. The `short_description` fallback blocks 21 more, New Zealand pairs
    # with an untitled side.
    assert crdf_years.height == 31_102
    assert crdf_years["blocked"].mean() == pytest.approx(0.98968, abs=1e-5)
    assert by_category == pytest.approx(
        {
            "bilateral": 0.98911,
            "other_multilateral": 1.0,
            "private": 1.0,
            "banks": 1.0,
        },
        abs=1e-5,
    )

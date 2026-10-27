"""`Release.jev_flags()` on the fixture release with a `jev_flags` table written in."""

from typing import Any

import polars as pl
import pytest
from make_fixture import DEU, FIXTURE_DIR
from releases import copy_release

import climate_finance as cf
from climate_finance.errors import (
    InvalidInputError,
    MissingLabelsError,
    UnknownProviderError,
)
from climate_finance.release import Release
from climate_finance.schema import JEV_FLAG_PROBABILITIES, ReleaseManifest

NORWAY = 8
# The CRS id and purpose of the fixture's DEU-1 commitment of 2021, and records of
# the fixture's `disbursements` and `untraced` tables.
DEU_COMMITMENT_CRS_ID = "202100DEU-1"
DEU_COMMITMENT_PURPOSE = 41010
IN_DISBURSEMENTS = "DEU-1-R0"
IN_UNTRACED = "DEU-UNLINKED"
OUT_OF_TOTALS = "DEU-NOT-IN-RELEASE"


def _flag(
    record_id: str,
    *,
    provider: int,
    year: int,
    crs_id: str | None = None,
    purpose: int | None = None,
    commitment: float | None = 1.0,
    no_aim: float = 0.1,
    surprising: float = 0.1,
    short_text: bool = False,
) -> dict[str, Any]:
    answers = dict.fromkeys(JEV_FLAG_PROBABILITIES)
    flags: dict[str, Any] = {"no_aim": None, "surprising": None}
    if not short_text:
        answers = {
            "no_aim_p": no_aim,
            "adaptation_aim_p": 1 - no_aim,
            "mitigation_aim_p": 0.2,
            "surprising_p": surprising,
            "counterproductive_p": 0.05,
        }
        flags = {"no_aim": no_aim >= 0.7, "surprising": surprising >= 0.8}
    return {
        "record_id": record_id,
        "year": year,
        "provider_code": provider,
        "agency_code": None,
        "recipient_code": 248,
        "purpose_code": purpose,
        "flow_code": 11,
        "project_number": None,
        "crs_id": crs_id,
        "title": record_id,
        "commitment_usd": commitment,
        "disbursement_usd": 1.0,
        "rio_adaptation": 1,
        "rio_mitigation": 0,
        "short_text": short_text,
        **answers,
        **flags,
        "model": "jev-1.14.0",
    }


FLAGS = [
    _flag(IN_DISBURSEMENTS, provider=DEU, year=2021, no_aim=0.9),
    _flag(IN_UNTRACED, provider=DEU, year=2022, surprising=0.9),
    # In neither table, but it commits under DEU-1's provider, CRS id, year and
    # purpose.
    _flag(
        "DEU-COMMITTED",
        provider=DEU,
        year=2021,
        crs_id=DEU_COMMITMENT_CRS_ID,
        purpose=DEU_COMMITMENT_PURPOSE,
    ),
    # DEU-1's key on a record that disburses only.
    _flag(
        "DEU-DISBURSES",
        provider=DEU,
        year=2021,
        crs_id=DEU_COMMITMENT_CRS_ID,
        purpose=DEU_COMMITMENT_PURPOSE,
        commitment=0.0,
    ),
    # DEU-1's key on a record with no commitment amount: false, never null.
    _flag(
        "DEU-NO-AMOUNT",
        provider=DEU,
        year=2021,
        crs_id=DEU_COMMITMENT_CRS_ID,
        purpose=DEU_COMMITMENT_PURPOSE,
        commitment=None,
    ),
    # DEU-1's CRS id and purpose in a year with no commitment row of it.
    _flag(
        "DEU-OTHER-YEAR",
        provider=DEU,
        year=2023,
        crs_id=DEU_COMMITMENT_CRS_ID,
        purpose=DEU_COMMITMENT_PURPOSE,
    ),
    # The same CRS id under another provider names no commitment.
    _flag(
        OUT_OF_TOTALS,
        provider=NORWAY,
        year=2022,
        crs_id=DEU_COMMITMENT_CRS_ID,
        no_aim=0.8,
    ),
    _flag("NOR-SHORT", provider=NORWAY, year=2023, short_text=True),
]
IN_RELEASE = {
    IN_DISBURSEMENTS: True,
    IN_UNTRACED: True,
    "DEU-COMMITTED": True,
    "DEU-DISBURSES": False,
    "DEU-NO-AMOUNT": False,
    "DEU-OTHER-YEAR": False,
    OUT_OF_TOTALS: False,
    "NOR-SHORT": False,
}


@pytest.fixture(scope="module")
def release(tmp_path_factory: pytest.TempPathFactory) -> Release:
    path = copy_release(
        tmp_path_factory.mktemp("flags") / "release",
        edits={"jev_flags": lambda held: pl.DataFrame(FLAGS, schema=held.schema)},
    )
    manifest = ReleaseManifest.model_validate_json(
        (path / "release.json").read_text(encoding="utf-8")
    )
    switched = manifest.model_copy(
        update={"settings": {**manifest.settings, "jev_flags": True}}
    )
    (path / "release.json").write_text(
        switched.model_dump_json(indent=2), encoding="utf-8"
    )
    return cf.load(path=path)


def test_in_release_follows_disbursements_untraced_and_commitments(
    release: Release,
) -> None:
    flags = release.jev_flags()

    assert flags["in_release"].null_count() == 0
    assert dict(zip(flags["record_id"], flags["in_release"], strict=True)) == IN_RELEASE


def test_the_rows_carry_names_and_the_record_facts(release: Release) -> None:
    flags = release.jev_flags()

    assert {"provider", "recipient", "title", "commitment_usd", "crs_id"} <= set(
        flags.columns
    )
    assert flags.height == len(FLAGS)


def test_mitigation_aim_comes_only_with_experimental(release: Release) -> None:
    assert "mitigation_aim_p" not in release.jev_flags().columns
    assert "mitigation_aim_p" in release.jev_flags(experimental=True).columns


@pytest.mark.parametrize(
    ("flagged", "records"),
    [
        ("no_aim", {IN_DISBURSEMENTS, OUT_OF_TOTALS}),
        ("surprising", {IN_UNTRACED}),
    ],
)
def test_flagged_keeps_the_records_where_the_flag_holds(
    release: Release, flagged: Any, records: set[str]
) -> None:
    assert set(release.jev_flags(flagged=flagged)["record_id"]) == records


@pytest.mark.parametrize(
    ("inputs", "records"),
    [
        (
            {"provider": "Germany"},
            {
                IN_DISBURSEMENTS,
                IN_UNTRACED,
                "DEU-COMMITTED",
                "DEU-DISBURSES",
                "DEU-NO-AMOUNT",
                "DEU-OTHER-YEAR",
            },
        ),
        ({"provider": [NORWAY], "year": 2022}, {OUT_OF_TOTALS}),
        (
            {"year": [2021, 2023]},
            {
                IN_DISBURSEMENTS,
                "DEU-COMMITTED",
                "DEU-DISBURSES",
                "DEU-NO-AMOUNT",
                "DEU-OTHER-YEAR",
                "NOR-SHORT",
            },
        ),
    ],
)
def test_provider_and_year_filter_the_rows(
    release: Release, inputs: dict[str, Any], records: set[str]
) -> None:
    assert set(release.jev_flags(**inputs)["record_id"]) == records


def test_a_release_built_without_jev_flags_refuses() -> None:
    with pytest.raises(MissingLabelsError, match="jev_flags"):
        cf.load(path=FIXTURE_DIR).jev_flags()


@pytest.mark.parametrize(
    ("inputs", "error"),
    [
        ({"flagged": "counterproductive"}, InvalidInputError),
        ({"experimental": "yes"}, InvalidInputError),
        ({"year": "2021"}, InvalidInputError),
        ({"provider": []}, InvalidInputError),
        ({"provider": "Atlantis"}, UnknownProviderError),
        ({"flaged": "no_aim"}, InvalidInputError),
    ],
)
def test_inputs_are_validated(
    release: Release, inputs: dict[str, Any], error: type[Exception]
) -> None:
    with pytest.raises(error):
        release.jev_flags(**inputs)

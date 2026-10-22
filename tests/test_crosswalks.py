"""The GEF crosswalk on hand-built CRS records, and the provider profile."""

import shutil
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from climate_finance import schema
from climate_finance.build import write_stage
from climate_finance.build.allocate import allocate
from climate_finance.build.config import load_config
from climate_finance.build.fetch import DAC_EXCHANGE_FILE, IATI_DIR
from climate_finance.build.link import run_link
from climate_finance.build.link.crosswalks import (
    GEF_DONOR,
    US_CROSSWALK_FILE,
    US_DONOR,
    crosswalk_profile,
    gef_crosswalk,
    link_by_crosswalk,
    read_us_crosswalk,
)
from climate_finance.build.link.keys import build_units, link_by_keys
from climate_finance.build.normalise.crdf import normalise_crdf
from climate_finance.build.normalise.crs import normalise_crs
from climate_finance.build.readers.iati import read_related_activities
from climate_finance.build.stages import REGISTRY, StageContext, raw_dir, table_path

FIXTURES = Path(__file__).parent / "fixtures" / "raw" / "iati"
OECD = FIXTURES.parent / "oecd"
FIRST_YEAR = 2010
GEF = "XI-BRIDGE-6385676864-"
_RECORD = {
    "donor_code": GEF_DONOR,
    "agency_code": 11,
    "crs_id": None,
    "project_number": None,
    "title": None,
    "recipient_code": 238,
    "purpose_code": 41010,
    "year": 2019,
    "commitment_date": None,
    "commitment_usd": None,
    "disbursement_usd": None,
    "expected_start_date": None,
    "completion_date": None,
}
_CRS_SCHEMA = {
    "record_id": pl.String,
    "donor_code": pl.Int32,
    "agency_code": pl.Int32,
    "crs_id": pl.String,
    "project_number": pl.String,
    "title": pl.String,
    "recipient_code": pl.Int32,
    "purpose_code": pl.Int32,
    "year": pl.Int16,
    "commitment_date": pl.Date,
    "commitment_usd": pl.Float64,
    "disbursement_usd": pl.Float64,
    "expected_start_date": pl.Date,
    "completion_date": pl.Date,
}


def _related(*pairs: tuple[str, str]) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {"activity_id": f"{GEF}{gef_id}", "related_id": ref, "relation": "5"}
            for gef_id, ref in pairs
        ],
        schema={
            "activity_id": pl.String,
            "related_id": pl.String,
            "relation": pl.String,
        },
    )


def _crs(records: dict[str, dict[str, object]]) -> pl.LazyFrame:
    return pl.LazyFrame(
        [{**_RECORD, "record_id": name, **fields} for name, fields in records.items()],
        schema=_CRS_SCHEMA,
    )


def _commitment(**fields: object) -> dict[str, object]:
    return {"project_number": "5078", "commitment_usd": 10.0, **fields}


def _disbursement(**fields: object) -> dict[str, object]:
    return {
        "agency_code": 1,
        "crs_id": "D",
        "project_number": "00090420",
        "year": 2021,
        "disbursement_usd": 4.0,
        **fields,
    }


def _links(
    records: dict[str, dict[str, object]], *related: tuple[str, str]
) -> tuple[pl.DataFrame, pl.DataFrame, pl.LazyFrame]:
    """Units, exact plus crosswalk links, and the CRS records."""
    crs = _crs(records)
    units = build_units(crs).collect()
    exact = link_by_keys(units, first_year=FIRST_YEAR)
    crosswalk = gef_crosswalk(
        _related(*(related or [("5078", "XM-DAC-41114-OUTPUT-90420")]))
    )
    linked = link_by_crosswalk(
        units, crosswalk=crosswalk, earlier=exact, first_year=FIRST_YEAR
    )
    schema.validate(schema.LINKS, linked)
    return units, pl.concat([exact, linked]), crs


def _crosswalked(
    records: dict[str, dict[str, object]], *related: tuple[str, str]
) -> dict[str, list[str]]:
    """Each disbursement record the crosswalk links, with the commitment records it
    reaches. Every test record is its own unit."""
    units, links, _ = _links(records, *related)
    names = dict(zip(units["unit_id"], units["record_ids"].list.first(), strict=True))
    reached: dict[str, list[str]] = {}
    for row in links.filter(link_method="crosswalk").iter_rows(named=True):
        reached.setdefault(names[row["left_id"]], []).append(names[row["right_id"]])
    return {name: sorted(commitments) for name, commitments in reached.items()}


@pytest.mark.parametrize(
    ("ref", "agency_id"),
    [
        ("XM-DAC-41114-OUTPUT-0013367", "UNDP:13367"),
        ("XM-DAC-41301-623884", "FAO:623884"),
        ("44000-P130492", "WB:P130492"),
        ("XI-IATI-IADB-AR-G1002", "IDB:AR-G1002"),
        # An organisation, not an activity.
        ("XM-DAC-41182", None),
        # Another GEF project.
        (f"{GEF}9071", None),
    ],
)
def test_gef_crosswalk_reads_agency_ids(ref: str, agency_id: str | None) -> None:
    crosswalk = gef_crosswalk(_related(("0912", ref)))
    expected = [] if agency_id is None else [("912", agency_id)]
    assert crosswalk.select("gef_id", "agency_id").rows() == expected


def test_gef_crosswalk_groups_the_projects_an_agency_id_connects() -> None:
    crosswalk = gef_crosswalk(
        _related(
            ("1", "XM-DAC-41114-OUTPUT-10"),
            ("2", "XM-DAC-41114-OUTPUT-10"),
            ("2", "XM-DAC-41114-OUTPUT-20"),
            ("3", "XM-DAC-41114-OUTPUT-20"),
            ("4", "XM-DAC-41114-OUTPUT-30"),
        )
    )
    assert dict(crosswalk.select("gef_id", "component").unique().rows()) == {
        "1": "1",
        "2": "1",
        "3": "1",
        "4": "4",
    }


def test_read_related_activities_reads_the_gef_file() -> None:
    related = read_related_activities(FIXTURES / "gef")
    assert related.rows() == [
        (f"{GEF}216", "XM-DAC-41114-OUTPUT-13367", "5"),
    ]


@pytest.mark.parametrize(
    ("commitment", "disbursement"),
    [
        ({}, {}),
        # The 2013 and 2014 form of a GEF project number.
        ({"project_number": "5078-1-123"}, {}),
        # UNDP numbers without their padding, FAO, World Bank and IDB ids.
        ({}, {"project_number": "90420"}),
        ({"project_number": "4000"}, {"project_number": "PRJ_623884"}),
        ({"project_number": "4000"}, {"project_number": "P130492"}),
        ({"project_number": "4000"}, {"project_number": "AR-G1002&GRT/FM-1-AR"}),
    ],
)
def test_a_disbursement_links_to_the_gef_project_its_agency_id_carries(
    commitment: dict[str, object], disbursement: dict[str, object]
) -> None:
    reached = _crosswalked(
        {"c": _commitment(**commitment), "d": _disbursement(**disbursement)},
        ("5078", "XM-DAC-41114-OUTPUT-90420"),
        ("4000", "XM-DAC-41301-623884"),
        ("4000", "44000-P130492"),
        ("4000", "XI-IATI-IADB-AR-G1002"),
    )
    assert reached == {"d": ["c"]}


@pytest.mark.parametrize(
    "disbursement",
    [
        {"recipient_code": 289},
        {"year": 2018},
        {"project_number": "00012345"},
        {"donor_code": 913},
        # The exact tiers link it to the commitment under its crs id.
        {"crs_id": "C"},
    ],
)
def test_a_disbursement_outside_the_crosswalk_links_to_nothing(
    disbursement: dict[str, object],
) -> None:
    records = {
        "c": _commitment(crs_id="C"),
        "d": _disbursement(**disbursement),
    }
    assert _crosswalked(records) == {}


def test_a_disbursement_before_its_floor_links_to_nothing() -> None:
    records = {
        "c": _commitment(),
        "d": _disbursement(commitment_date=date(2020, 1, 1)),
    }
    assert _crosswalked(records) == {}


def test_allocation_pools_a_group_over_the_projects_it_connects() -> None:
    # One UNDP output carries GEF projects 5078 and 5079, and another carries 5079,
    # so both disbursements reach both projects.
    records = {
        "c1": _commitment(project_number="5078"),
        "c2": _commitment(project_number="5079", year=2020),
        "d1": _disbursement(
            crs_id="D1", project_number="00090420", disbursement_usd=12.0
        ),
        "d2": _disbursement(
            crs_id="D2", project_number="00090421", disbursement_usd=3.0
        ),
    }
    related = (
        ("5078", "XM-DAC-41114-OUTPUT-90420"),
        ("5079", "XM-DAC-41114-OUTPUT-90420"),
        ("5079", "XM-DAC-41114-OUTPUT-90421"),
    )
    units, links, crs = _links(records, *related)
    crosswalk = links.filter(link_method="crosswalk")
    assert crosswalk["group_id"].unique().to_list() == ["crosswalk|5078|238"]
    assert crosswalk.group_by("left_id").len()["len"].to_list() == [2, 2]

    pairs = allocate(units.lazy(), crs=crs, links=links.lazy())
    filled = (
        pairs.filter(pl.col("commitment_id").is_not_null())
        .group_by("commitment_id")
        .agg(pl.col("allocated_usd").sum())
    )
    # Oldest commitment first, each capped at its face value.
    assert dict(filled.rows()) == {"c1": 10.0, "c2": 5.0}


def _crdf(rows: dict[str, dict[str, object]]) -> pl.LazyFrame:
    base = {
        "perspective": "recipient",
        "provider_code": GEF_DONOR,
        "climate_usd": 1.0,
        "repeat_of": None,
        "parent_row_id": None,
    }
    return pl.LazyFrame(
        [{"row_id": name, **base, **fields} for name, fields in rows.items()],
        schema={
            "row_id": pl.String,
            "perspective": pl.String,
            "provider_code": pl.Int32,
            "climate_usd": pl.Float64,
            "repeat_of": pl.String,
            "parent_row_id": pl.String,
        },
    )


def _link_rows(*rows: tuple[str, str, str, str]) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {"task": task, "left_id": left, "right_id": right, "link_method": method}
            for task, left, right, method in rows
        ]
    )


def test_crosswalk_profile_measures_the_climate_value_a_crosswalk_reaches() -> None:
    crdf = _crdf(
        {
            "reached": {"climate_usd": 3.0},
            "missed": {"climate_usd": 1.0},
            # A repeat and an aggregate parent with children do not count.
            "repeat": {"climate_usd": 5.0, "repeat_of": "reached"},
            "parent": {"climate_usd": 7.0},
            "child": {"climate_usd": 4.0, "parent_row_id": "parent"},
            "awarded": {"provider_code": US_DONOR, "climate_usd": 2.0},
            "unawarded": {"provider_code": US_DONOR, "climate_usd": 6.0},
            "other": {"provider_code": 918, "climate_usd": 2.0},
            "provider_view": {"perspective": "provider", "climate_usd": 9.0},
        }
    )
    links = _link_rows(
        ("crdf_crs", "reached", "c1", "within_year"),
        ("crdf_crs", "repeat", "c1", "within_year"),
        ("crdf_crs", "parent", "c1", "within_year"),
        ("crdf_crs", "missed", "c2", "within_year"),
        ("crdf_crs", "other", "c1", "within_year"),
        ("crdf_crs", "awarded", "u1", "within_year"),
        ("crdf_crs", "unawarded", "u2", "within_year"),
        ("trace", "d1", "c1", "crosswalk"),
        ("trace", "d2", "c2", "crs_id"),
    )
    units = pl.DataFrame(
        {
            "unit_id": ["u1", "u2"],
            "donor_code": [US_DONOR, US_DONOR],
            "project_number": ["76_1", "76_2"],
        },
        schema_overrides={"donor_code": pl.Int32},
    )
    awards = pl.DataFrame({"project_number": ["76_1"], "award": ["AID-1"]})

    profile = crosswalk_profile(
        crdf, links=links, units=units, us_awards=awards, crosswalks=True
    )

    assert profile.sort("provider_code").rows() == [
        (US_DONOR, 8.0, 2.0, 2.0 / 8.0),
        (GEF_DONOR, 8.0, 3.0, 3.0 / 8.0),
        (918, 2.0, None, None),
    ]
    # Without the US crosswalk the US is not measured.
    unmeasured = crosswalk_profile(
        crdf, links=links, units=units, us_awards=None, crosswalks=True
    )
    assert unmeasured.filter(provider_code=US_DONOR).rows() == [
        (US_DONOR, 8.0, None, None)
    ]
    # Without recoveries no provider is measured, though climate value is.
    off = crosswalk_profile(
        crdf, links=links, units=units, us_awards=awards, crosswalks=False
    )
    assert off.sort("provider_code").rows() == [
        (US_DONOR, 8.0, None, None),
        (GEF_DONOR, 8.0, None, None),
        (918, 2.0, None, None),
    ]


def _us_crosswalk(tmp_path: Path, *rows: tuple[str, str, str | None]) -> Path:
    pl.DataFrame(
        [
            {
                "submission_id": submission,
                "submission_activity_id": activity,
                "project_number": number,
                "managing_agency": "USAID",
            }
            for submission, activity, number in rows
        ],
        schema=dict.fromkeys(
            [
                "submission_id",
                "submission_activity_id",
                "project_number",
                "managing_agency",
            ],
            pl.String,
        ),
    ).write_parquet(tmp_path / US_CROSSWALK_FILE)
    return tmp_path


def test_the_us_crosswalk_keeps_the_awards_a_project_number_names(
    tmp_path: Path,
) -> None:
    folder = _us_crosswalk(
        tmp_path,
        ("76", "62884", "72DFFP19IO 00066"),
        # Two awards under one activity name neither.
        ("76", "1", "AID-306-A-00-08-00513"),
        ("76", "1", "AID-306-A-00-08-00514"),
        ("76", "2", "redacted ID"),
        ("76", "3", "Information Redacted"),
        ("76", "4", "N/A ADMIN"),
        ("76", "5", "NULL"),
        ("76", "6", None),
        ("76", "7", "OE"),
    )
    assert read_us_crosswalk(folder).rows() == [("76_62884", "72DFFP19IO00066")]


def test_run_link_writes_the_provider_profile_with_recoveries(tmp_path: Path) -> None:
    crs = normalise_crs(
        pl.scan_parquet(OECD / "CRS.parquet"), years=(2010, 2025)
    ).collect()
    crdf = normalise_crdf(
        pl.scan_parquet(OECD / "crdf_recipient.parquet"),
        perspective="recipient",
        crs=crs.lazy(),
    ).collect()
    external = pl.DataFrame(
        schema={name: c.dtype for name, c in schema.EXTERNAL.columns.items()}
    )
    for name, frame in (("crs", crs), ("crdf", crdf), ("external", external)):
        path = table_path(tmp_path, "normalise", name)
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.write_parquet(path)
    raw = raw_dir(tmp_path)
    shutil.copytree(FIXTURES / "gef", raw / IATI_DIR / "gef")
    pl.DataFrame(
        {"iso_code": ["EUI"] * 16, "year": range(2010, 2026), "exchange_rate": 0.8}
    ).write_parquet(raw / DAC_EXCHANGE_FILE)
    config = load_config(Path(__file__).parents[1] / "build.toml")
    context = StageContext(
        release="test",
        release_dir=tmp_path,
        config=config.model_copy(update={"key_repair": False, "recoveries": True}),
        offline=True,
    )
    link = next(stage for stage in REGISTRY if stage.name == "link")

    hashes = write_stage(link, run_link(context), tmp_path)

    assert set(hashes) == {
        "links",
        "units",
        "id_practice",
        "programme_stems",
        "candidates",
        "pair_scores",
        "provider_profile",
    }
    profile = pl.read_parquet(table_path(tmp_path, "link", "provider_profile"))
    assert set(profile["provider_code"]) == set(crdf["provider_code"])

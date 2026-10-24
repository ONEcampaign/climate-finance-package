"""Agency climate shares and the modelled line on hand-built CRDF and CRS rows."""

from collections.abc import Callable
from pathlib import Path

import numpy as np
import polars as pl
import polars.selectors as cs
import pytest
from normalised import crdf_rows, crs_records

from climate_finance import schema
from climate_finance.build import measure, write_stage
from climate_finance.build.config import load_config
from climate_finance.build.errors import ConfigError
from climate_finance.build.measure import (
    SHARE_FLOOR_USD,
    AgencyChannel,
    ModelledProvider,
    ProviderFacts,
    ReportRatio,
    agency_shares,
    load_provider_facts,
    modelled_ranges,
    unshared_contributions,
)
from climate_finance.build.stages import REGISTRY, StageContext, table_path

BUILD_TOML = Path(__file__).parents[1] / "build.toml"
ASDB, EU, ADAPTATION_FUND, CAF, IFC, NDB = 915, 918, 1012, 1015, 903, 1044
DEU = 5
PERU, MEXICO, AMERICA_REGIONAL, UNSPECIFIED = 454, 358, 498, 998
AGENCIES = (
    AgencyChannel(channel_code=46004, provider_code=ASDB),
    AgencyChannel(channel_code=42004, provider_code=EU, agency_codes=(3,)),
    AgencyChannel(channel_code=47111, provider_code=ADAPTATION_FUND),
)
NO_IMPUTED = pl.LazyFrame(
    schema={
        name: column.dtype for name, column in schema.IMPUTED_SHARES.columns.items()
    }
)


def _bank(row_id: str, year: int, **row: object) -> dict[str, object]:
    return {"row_id": row_id, "year": year, "bi_multi": 4, "agency_code": 1, **row}


def _component(
    row_id: str, year: int, *, face: float, a: float, m: float, o: float
) -> dict[str, object]:
    return _bank(
        row_id,
        year,
        provider_code=ASDB,
        face_usd=face,
        climate_usd=a + m - o,
        adaptation_usd=a,
        mitigation_usd=m,
        overlap_usd=o,
        climate_share=(a + m - o) / face,
        rio_adaptation=None,
        rio_mitigation=None,
    )


def _shares(frame: pl.LazyFrame) -> dict[tuple[int, int, str, str], float]:
    """`one` shares by channel, year, objective and share source."""
    collected = frame.collect()
    schema.validate(schema.AGENCY_SHARES, collected)
    return {
        (row["channel_code"], row["year"], row["objective"], row["share_source"]): (
            row["share"]
        )
        for row in collected.filter(pl.col("source") == "one").iter_rows(named=True)
    }


def _in_millions(frame: pl.DataFrame) -> pl.LazyFrame:
    """`frame` with its amounts read as millions, so that shares clear
    `SHARE_FLOOR_USD`."""
    return frame.lazy().with_columns(cs.ends_with("_usd") * 1e6)


def _imputed(*rows: tuple[int, int, float]) -> pl.LazyFrame:
    """OECD `climate` shares by channel and year."""
    return pl.LazyFrame(
        [(channel, year, "climate", share) for channel, year, share in rows],
        schema={n: c.dtype for n, c in schema.IMPUTED_SHARES.columns.items()},
        orient="row",
    )


def _committed(record_id: str, year: int, donor: int, usd: float) -> dict[str, object]:
    return {"record_id": record_id, "year": year, "donor_code": donor,
            "commitment_usd": usd}  # fmt: skip


def test_one_shares_are_inclusive_two_year_shares_of_crs_commitments() -> None:
    crdf = crdf_rows(
        _component("a20", 2020, face=100.0, a=30.0, m=20.0, o=10.0),
        _component("a21", 2021, face=50.0, a=0.0, m=50.0, o=0.0),
        # A significant adaptation marker counts at 0.4 under ONE.
        _bank("f21", 2021, provider_code=ADAPTATION_FUND, rio_adaptation=1,
              rio_mitigation=0, face_usd=10.0, climate_usd=10.0,
              adaptation_usd=10.0, mitigation_usd=0.0),
        _bank("e20", 2020, provider_code=EU, agency_code=3, rio_adaptation=0,
              rio_mitigation=2, face_usd=10.0, climate_usd=10.0),
        _bank("e21", 2021, provider_code=EU, agency_code=3, rio_adaptation=0,
              rio_mitigation=2, face_usd=20.0, climate_usd=20.0),
        # The EU's agency 99 reports for no listed channel.
        _bank("x21", 2021, provider_code=EU, agency_code=99, face_usd=1e3,
              climate_usd=1e3),
    )  # fmt: skip
    # A record's own channel of delivery says nothing about who reports it.
    crs = crs_records(
        {"record_id": "c20", "year": 2020, "donor_code": ASDB, "commitment_usd": 400.0,
         "channel_code": 12000},
        {"record_id": "c21", "year": 2021, "donor_code": ASDB, "commitment_usd": 600.0,
         "channel_code": 46002},
        {"record_id": "d21", "year": 2021, "donor_code": ASDB, "disbursement_usd": 9e3},
        {"record_id": "f21", "year": 2021, "donor_code": ADAPTATION_FUND,
         "commitment_usd": 20.0},
        {"record_id": "e20", "year": 2020, "donor_code": EU, "agency_code": 3,
         "commitment_usd": 30.0},
        {"record_id": "e21", "year": 2021, "donor_code": EU, "agency_code": 3,
         "commitment_usd": 50.0},
        {"record_id": "x21", "year": 2021, "donor_code": EU, "agency_code": 99,
         "commitment_usd": 9e3},
    )  # fmt: skip

    shares = _shares(
        agency_shares(
            _in_millions(crdf),
            crs=_in_millions(crs),
            imputed_shares=NO_IMPUTED,
            agencies=AGENCIES,
            years=(2021, 2021),
        )
    )

    # 2020's rows give 2021 its two-year span, as the CRDF's 2014 rows do for 2015.
    # The Adaptation Fund's first year of CRS commitments stands on one year.
    expected = {
        (46004, 2021, "adaptation", "computed"): 30.0 / 1000,
        (46004, 2021, "mitigation", "computed"): 70.0 / 1000,
        (46004, 2021, "cross_cutting", "computed"): 10.0 / 1000,
        (47111, 2021, "adaptation", "computed"): 4.0 / 20,
        (47111, 2021, "mitigation", "computed"): 0.0,
        (47111, 2021, "cross_cutting", "computed"): 0.0,
        (42004, 2021, "adaptation", "computed"): 0.0,
        (42004, 2021, "mitigation", "computed"): 30.0 / 80,
        (42004, 2021, "cross_cutting", "computed"): 0.0,
    }
    assert shares.keys() == expected.keys()
    for key, share in expected.items():
        assert shares[key] == pytest.approx(share), key


def test_a_marker_weight_stops_at_the_published_climate_share() -> None:
    # A significant mitigation marker counts 0.4 under ONE; the provider published
    # 30% of face value as climate.
    crdf = crdf_rows(
        _bank("i21", 2021, provider_code=ADAPTATION_FUND, rio_adaptation=0,
              rio_mitigation=1, face_usd=100.0, climate_usd=30.0),
    )  # fmt: skip
    crs = crs_records(_committed("c21", 2021, ADAPTATION_FUND, 100.0))

    shares = _shares(
        agency_shares(
            _in_millions(crdf),
            crs=_in_millions(crs),
            imputed_shares=NO_IMPUTED,
            agencies=AGENCIES[2:],
            years=(2021, 2021),
        )
    )

    assert shares[(47111, 2021, "mitigation", "computed")] == pytest.approx(0.3)


def test_oecd_shares_are_the_imputed_shares_unchanged() -> None:
    imputed = pl.DataFrame(
        {
            "channel_code": [44002, 44002, 44002, 41108],
            "year": [2021, 2021, 2021, 2019],
            "objective": ["adaptation", "mitigation", "cross_cutting", "climate"],
            "share": [0.2, 0.15, 0.05, 0.37],
        },
        schema={name: c.dtype for name, c in schema.IMPUTED_SHARES.columns.items()},
    )
    crdf, crs = crdf_rows(), crs_records()

    shares = agency_shares(
        crdf.lazy(),
        crs=crs.lazy(),
        imputed_shares=imputed.lazy(),
        agencies=(),
        years=(2015, 2024),
    ).collect()

    schema.validate(schema.AGENCY_SHARES, shares)
    oecd = shares.filter(pl.col("source") == "oecd")
    assert (
        oecd.drop("source", "share_source")
        .sort(pl.all())
        .equals(imputed.sort(pl.all()))
    )
    assert set(oecd["share_source"]) == {"oecd"}


def test_a_share_needs_crdf_rows_over_its_span_else_takes_the_oecd_share() -> None:
    crdf = crdf_rows(
        _bank("a20", 2020, provider_code=ASDB, face_usd=10.0, climate_usd=10.0),
        _bank("a21", 2021, provider_code=ASDB, face_usd=30.0, climate_usd=30.0),
        _bank("f21", 2021, provider_code=ADAPTATION_FUND, face_usd=5.0,
              climate_usd=5.0),
    )  # fmt: skip
    crs = crs_records(
        _committed("a19", 2019, ASDB, 100.0),
        _committed("a20", 2020, ASDB, 100.0),
        _committed("a21", 2021, ASDB, 100.0),
        # The Adaptation Fund commits from 2020 and has CRDF rows only from 2021, and
        # CAF commits with no CRDF rows at all.
        _committed("f20", 2020, ADAPTATION_FUND, 50.0),
        _committed("f21", 2021, ADAPTATION_FUND, 50.0),
        _committed("c21", 2021, CAF, 70.0),
    )
    agencies = (
        AgencyChannel(channel_code=46004, provider_code=ASDB),
        AgencyChannel(channel_code=47111, provider_code=ADAPTATION_FUND),
        AgencyChannel(channel_code=46008, provider_code=CAF),
    )
    imputed = _imputed(
        (46004, 2020, 0.3), (46004, 2021, 0.9), (47111, 2021, 0.8),
        # The Montreal Protocol fund has no reporter; 2019 lies outside the years.
        (47078, 2019, 1.0), (47078, 2020, 1.0),
    )  # fmt: skip

    shares = _shares(
        agency_shares(
            _in_millions(crdf),
            crs=_in_millions(crs),
            imputed_shares=imputed,
            agencies=agencies,
            years=(2020, 2021),
        )
    )

    # ASDB's 2020 span would hold 2019's commitments without their climate rows, and
    # the Adaptation Fund's 2021 span 2020's. CAF has no CRDF rows and no OECD share.
    assert {
        key: shares[key] for key in shares if key[2] in ("mitigation", "climate")
    } == {
        (46004, 2020, "climate", "oecd"): 0.3,
        (46004, 2021, "mitigation", "computed"): pytest.approx(40.0 / 200),
        (47111, 2021, "climate", "oecd"): 0.8,
        (47078, 2020, "climate", "oecd"): 1.0,
    }


def test_a_span_below_the_floor_holds_the_last_computed_share() -> None:
    floor = SHARE_FLOOR_USD
    crdf = crdf_rows(
        _bank("a19", 2019, provider_code=ASDB, face_usd=floor / 2,
              climate_usd=floor / 2),
        _bank("a21", 2021, provider_code=ASDB, face_usd=floor / 2,
              climate_usd=floor / 2),
    )  # fmt: skip
    # The 2021 span commits exactly the floor, the 2022 span just below it and the
    # 2023 span nothing, as the European Development Fund once it stops committing.
    crs = crs_records(
        _committed("a19", 2019, ASDB, 2 * floor),
        _committed("a20", 2020, ASDB, 1.0),
        _committed("a21", 2021, ASDB, floor - 1.0),
    )

    shares = _shares(
        agency_shares(
            crdf.lazy(),
            crs=crs.lazy(),
            imputed_shares=NO_IMPUTED,
            agencies=AGENCIES[:1],
            years=(2020, 2023),
        )
    )

    assert {key: shares[key] for key in shares if key[2] == "mitigation"} == {
        (46004, 2020, "mitigation", "computed"): pytest.approx(0.5 / (2 + 1 / floor)),
        (46004, 2021, "mitigation", "computed"): pytest.approx(0.5),
        (46004, 2022, "mitigation", "held"): pytest.approx(0.5),
        (46004, 2023, "mitigation", "held"): pytest.approx(0.5),
    }


@pytest.mark.parametrize(
    ("entry", "error"),
    [
        # A reporter with no CRS commitments, as the Asian Development Fund (916) had.
        (AgencyChannel(channel_code=46005, provider_code=916), "916 has no CRS"),
        # A pooled agency with commitments and no CRDF rows, as Macro-Financial
        # Assistance in the European Development Fund's pool.
        (AgencyChannel(channel_code=42003, provider_code=EU, agency_codes=(2, 5)),
         r"agencies \[5\] have no CRDF rows"),
        (AgencyChannel(channel_code=42003, provider_code=EU, agency_codes=(2, 5),
                       without_crdf={5: "a stated reason"}), None),
    ],
)  # fmt: skip
def test_the_build_refuses_an_entry_the_data_does_not_support(
    entry: AgencyChannel, error: str | None
) -> None:
    crdf = crdf_rows(
        _bank("e21", 2021, provider_code=EU, agency_code=2, face_usd=1.0,
              climate_usd=1.0),
    )  # fmt: skip
    crs = crs_records(
        {"record_id": "e2", "year": 2021, "donor_code": EU, "agency_code": 2,
         "commitment_usd": 10.0},
        {"record_id": "e5", "year": 2021, "donor_code": EU, "agency_code": 5,
         "commitment_usd": 90.0},
    )  # fmt: skip

    def shares() -> pl.DataFrame:
        return agency_shares(
            _in_millions(crdf),
            crs=_in_millions(crs),
            imputed_shares=NO_IMPUTED,
            agencies=(entry,),
            years=(2021, 2021),
        ).collect()

    if error is None:
        assert not shares().is_empty()
    else:
        with pytest.raises(ConfigError, match=error):
            shares()


def test_a_reason_names_a_listed_agency() -> None:
    with pytest.raises(ValueError, match="unlisted"):
        AgencyChannel(
            channel_code=42003,
            provider_code=EU,
            agency_codes=(2,),
            without_crdf={5: "a stated reason"},
        )


def test_contributions_without_a_one_share_are_listed() -> None:
    shares = pl.LazyFrame(
        [
            (46004, 2021, "climate", 0.2, "one", "computed"),
            (46005, 2021, "climate", 0.1, "oecd", "oecd"),
        ],
        schema={n: c.dtype for n, c in schema.AGENCY_SHARES.columns.items()},
        orient="row",
    )
    core = pl.LazyFrame(
        [
            (DEU, 46004, 2021, "disbursements", 5.0),
            (DEU, 46004, 2022, "disbursements", 6.0),
            (DEU, 46004, 2022, "commitments", 9.0),
            (DEU, 46005, 2021, "disbursements", 7.0),
            (DEU, 47015, 2021, "disbursements", 8.0),
            # A channel with no share in any year, such as UNDP's.
            (DEU, 41114, 2021, "disbursements", 3.0),
        ],
        schema={n: c.dtype for n, c in schema.CORE_CONTRIBUTIONS.columns.items()},
        orient="row",
    )

    gaps = unshared_contributions(
        shares,
        core_contributions=core,
        agencies=(AgencyChannel(channel_code=47015, provider_code=1055),),
    ).collect()

    assert gaps.rows() == [(46004, 2022, 6.0), (46005, 2021, 7.0), (47015, 2021, 8.0)]


def _report(ratio: float) -> ReportRatio:
    return ReportRatio(ratio=ratio, year=2022, source="annual report")


def _ratios(
    frame: pl.LazyFrame,
) -> dict[tuple[int, int, int | None], tuple[float, ...]]:
    collected = frame.collect()
    schema.validate(schema.MODELLED, collected)
    return {
        (provider, year, recipient): (low, high)
        for provider, year, recipient, low, high, _ in collected.iter_rows()
    }


def _caf_records() -> pl.DataFrame:
    """CAF's project commitments and aggregate disbursements. 2018 lies outside the
    unbroken run of disbursing years that ends in 2022."""
    rows = [
        (2018, PERU, 100.0, 50.0),
        (2020, PERU, 100.0, 30.0),
        (2021, PERU, 100.0, 30.0),
        (2022, PERU, 100.0, 40.0),
        (2021, MEXICO, 100.0, None),
        (2022, MEXICO, None, 150.0),
        (2021, 998, 100.0, 50.0),
    ]
    return crs_records(
        *(
            {"record_id": f"{year}-{recipient}-{kind}", "year": year,
             "donor_code": CAF, "recipient_code": recipient, kind: amount}
            for year, recipient, commitment, disbursement in rows
            for kind, amount in (("commitment_usd", commitment),
                                 ("disbursement_usd", disbursement))
            if amount is not None
        )
    )  # fmt: skip


def _caf_rows() -> pl.DataFrame:
    return crdf_rows(
        *(
            _bank(f"r{recipient}", 2021, provider_code=CAF, recipient_code=recipient,
                  face_usd=climate, climate_usd=climate)
            for recipient, climate in (
                (PERU, 10.0), (MEXICO, 20.0), (AMERICA_REGIONAL, 30.0),
                (UNSPECIFIED, 10.0),
            )
        )
    )  # fmt: skip


def _modelled(
    crdf: pl.DataFrame,
    crs: pl.DataFrame,
    *providers: ModelledProvider,
    status: pl.DataFrame | None = None,
    last_year: int = 2022,
) -> dict[tuple[int, int, int | None], tuple[float, ...]]:
    if status is None:
        status = pl.DataFrame(schema={"row_id": pl.String, "status": pl.String})
    return _ratios(
        modelled_ranges(
            crdf.lazy(),
            crs=crs.lazy(),
            status=status.lazy(),
            providers=providers,
            last_year=last_year,
        )
    )


def test_recipient_ratios_run_from_cumulative_to_window_capped_at_one() -> None:
    caf = ModelledProvider(provider_code=CAF, basis="crs", by_recipient=True)

    ranges = _modelled(_caf_rows(), _caf_records(), caf)

    # Over the 2020 to 2022 run: Peru cumulative 100/300, window from 2021 70/200;
    # Mexico 150/100 both ways, capped; unspecified (998) 50/100 both ways;
    # the regional row has no CRS commitments, so it takes the portfolio's
    # cumulative 300/500 and window 270/400.
    expected = {
        PERU: (10.0 / 3, 3.5),
        MEXICO: (20.0, 20.0),
        UNSPECIFIED: (5.0, 5.0),
        AMERICA_REGIONAL: (30.0 * 0.6, 30.0 * 0.675),
    }
    assert ranges.keys() == {(CAF, 2021, recipient) for recipient in expected}
    for recipient, bounds in expected.items():
        assert ranges[CAF, 2021, recipient] == pytest.approx(bounds), recipient


def test_crs_ratios_end_at_the_last_complete_year() -> None:
    caf = ModelledProvider(provider_code=CAF, basis="crs", by_recipient=True)
    # An early 2023 report of Peru's disbursement.
    early = crs_records(
        {"record_id": "2023-peru", "year": 2023, "donor_code": CAF,
         "recipient_code": PERU, "disbursement_usd": 500.0}
    )  # fmt: skip
    records = pl.concat([_caf_records(), early])

    complete = _modelled(_caf_rows(), records, caf, last_year=2022)
    with_early = _modelled(_caf_rows(), records, caf, last_year=2023)

    assert complete == _modelled(_caf_rows(), _caf_records(), caf)
    assert with_early[CAF, 2021, PERU] != complete[CAF, 2021, PERU]


def test_portfolio_ratios_ignore_recipients() -> None:
    caf = ModelledProvider(provider_code=CAF, basis="crs")

    ranges = _modelled(_caf_rows(), _caf_records(), caf)

    assert ranges[CAF, 2021, PERU] == pytest.approx((10.0 * 0.6, 10.0 * 0.675))


def test_report_ratios_bound_the_range_and_negative_rows_are_left_out() -> None:
    ifc = ModelledProvider(
        provider_code=IFC,
        basis="report",
        crs_ratios=False,
        report_ratios=(_report(0.92), _report(1.05)),
    )
    crdf = crdf_rows(
        _bank("i1", 2022, provider_code=IFC, recipient_code=UNSPECIFIED,
              face_usd=100.0, climate_usd=100.0),
        _bank("i2", 2022, provider_code=IFC, recipient_code=UNSPECIFIED,
              face_usd=-5.0, climate_usd=-5.0),
    )  # fmt: skip

    ranges = _modelled(crdf, crs_records(), ifc)

    assert ranges == {(IFC, 2022, UNSPECIFIED): pytest.approx((92.0, 100.0))}


def test_cancelled_projects_model_nothing() -> None:
    ndb = ModelledProvider(
        provider_code=NDB,
        basis="crs",
        report_ratios=(_report(0.47),),
        cancelled_crs_ids=("2022900004",),
    )
    crdf = crdf_rows(
        _bank("n1", 2022, provider_code=NDB, crs_id="2022900004", face_usd=10.0,
              climate_usd=10.0),
        _bank("n2", 2022, provider_code=NDB, crs_id="2022900005", face_usd=20.0,
              climate_usd=20.0),
    )  # fmt: skip
    crs = crs_records(
        {"record_id": "c", "year": 2022, "donor_code": NDB, "commitment_usd": 100.0},
        {"record_id": "d", "year": 2022, "donor_code": NDB, "disbursement_usd": 96.0},
    )

    ranges = _modelled(crdf, crs, ndb)

    assert ranges[NDB, 2022, 248] == pytest.approx((20.0 * 0.47, 20.0 * 0.96))


def test_vintage_ratios_model_only_the_statuses_named() -> None:
    eib = ModelledProvider(
        provider_code=EU,
        agency_code=3,
        basis="vintage",
        vintage=True,
        statuses=("not_traceable",),
    )
    crdf = crdf_rows(
        _bank("v21", 2021, provider_code=EU, agency_code=3, face_usd=10.0,
              climate_usd=10.0),
        _bank("t21", 2021, provider_code=EU, agency_code=3, face_usd=10.0,
              climate_usd=10.0),
        _bank("v20", 2020, provider_code=EU, agency_code=3, recipient_code=PERU,
              face_usd=15.0, climate_usd=15.0),
        _bank("b21", 2021, provider_code=EU, agency_code=1, face_usd=10.0,
              climate_usd=10.0),
    )  # fmt: skip
    # P1 is first committed in 2021 and pays 70 of 100; P2, first committed in 2020,
    # pays 100 of 150, its 2021 top-up included. Agency 1's records do not count.
    crs = crs_records(
        *(
            {"record_id": f"{project}-{year}-{kind}", "year": year, "donor_code": EU,
             "agency_code": agency, "project_number": project, kind: amount}
            for project, year, agency, kind, amount in (
                ("P1", 2021, 3, "commitment_usd", 100.0),
                ("P1", 2022, 3, "disbursement_usd", 30.0),
                ("P1", 2023, 3, "disbursement_usd", 40.0),
                ("P2", 2020, 3, "commitment_usd", 100.0),
                ("P2", 2021, 3, "commitment_usd", 50.0),
                ("P2", 2021, 3, "disbursement_usd", 100.0),
                ("P3", 2021, 1, "commitment_usd", 100.0),
                ("P3", 2021, 1, "disbursement_usd", 5.0),
            )
        )
    )  # fmt: skip
    status = pl.DataFrame(
        {
            "row_id": ["v21", "t21", "v20", "b21"],
            "status": ["not_traceable", "disbursed", "not_traceable", "not_traceable"],
        }
    )

    ranges = _modelled(crdf, crs, eib, status=status, last_year=2023)

    assert ranges == {
        (EU, 2021, 248): pytest.approx((7.0, 7.0)),
        (EU, 2020, PERU): pytest.approx((10.0, 10.0)),
    }


def test_a_provider_without_crs_disbursement_needs_report_ratios() -> None:
    crdf = crdf_rows(
        _bank("i1", 2022, provider_code=IFC, face_usd=1.0, climate_usd=1.0)
    )
    crs = crs_records(
        {"record_id": "c", "year": 2022, "donor_code": IFC, "commitment_usd": 9.0}
    )

    with pytest.raises(ConfigError, match="903"):
        _modelled(crdf, crs, ModelledProvider(provider_code=IFC, basis="crs"))


def test_the_shipped_provider_facts_load() -> None:
    facts = load_provider_facts()

    channels = {entry.channel_code for entry in facts.agencies}
    assert {44001, 44002, 44004, 46008, 46012, 46028, 42004} <= channels
    # The Asian Development Fund and the Montreal Protocol fund report nothing and
    # take the OECD's share; the IMF Resilience and Sustainability Trust and CGIAR
    # report their own spending.
    assert not {46005, 47078} & channels
    reporters = {entry.channel_code: entry.provider_code for entry in facts.agencies}
    assert (reporters[43007], reporters[47015]) == (1058, 1055)
    # The IDB's Fund for Special Operations pools its own two agencies, without the
    # Climate Investment Funds money the IDB reports as agencies 11 and 14.
    fso = next(entry for entry in facts.agencies if entry.channel_code == 46013)
    assert fso.agency_codes == (2, 3)
    # Macro-Financial Assistance is EU lending, in no channel's pool.
    assert not any(
        entry.provider_code == EU and 5 in (entry.agency_codes or (5,))
        for entry in facts.agencies
    )
    assert {entry.provider_code for entry in facts.modelled} == {
        IFC, CAF, 1019, NDB, EU,
    }  # fmt: skip


@pytest.mark.parametrize(
    "agencies",
    [
        [{"channel_code": 1, "provider_code": EU},
         {"channel_code": 2, "provider_code": EU, "agency_codes": [3]}],
        [{"channel_code": 1, "provider_code": EU, "agency_codes": [1, 3]},
         {"channel_code": 2, "provider_code": EU, "agency_codes": [3]}],
    ],
)  # fmt: skip
def test_an_agency_reports_for_one_channel(agencies: list[dict[str, object]]) -> None:
    with pytest.raises(ValueError, match="two channels"):
        ProviderFacts.model_validate({"agencies": agencies, "modelled": []})


def _write(release_dir: Path, stage: str, table: str, frame: pl.DataFrame) -> None:
    path = table_path(release_dir, stage, table)
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(path)


def _measure(
    tmp_path: Path,
    crdf: pl.DataFrame,
    crs: pl.DataFrame,
    *,
    span: pl.DataFrame | None = None,
) -> dict:
    """The measure stage's tables, with CAF's channel the only agency entry and
    `span` as normalise's `crdf_span`, empty where not given."""
    release_dir = tmp_path / "r"
    status = pl.DataFrame(
        {"row_id": crdf["row_id"], "status": "not_traceable"},
        schema={"row_id": pl.String, "status": pl.String},
    )
    _write(release_dir, "normalise", "crdf", crdf)
    if span is None:
        span = pl.DataFrame(
            schema={n: c.dtype for n, c in schema.CRDF_SPAN.columns.items()}
        )
    _write(release_dir, "normalise", "crdf_span", span)
    _write(release_dir, "normalise", "crs", crs)
    _write(release_dir, "normalise", "imputed_shares", NO_IMPUTED.collect())
    _write(
        release_dir,
        "normalise",
        "core_contributions",
        pl.DataFrame(
            schema={n: c.dtype for n, c in schema.CORE_CONTRIBUTIONS.columns.items()}
        ),
    )
    _write(release_dir, "review", "commitments", status)
    context = StageContext(
        release="r", release_dir=release_dir, config=load_config(BUILD_TOML),
        offline=True,
    )  # fmt: skip
    stage = next(entry for entry in REGISTRY if entry.name == "measure")
    facts = load_provider_facts()
    caf = ProviderFacts(
        agencies=(AgencyChannel(channel_code=46008, provider_code=CAF),),
        modelled=facts.modelled,
    )
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(measure, "load_provider_facts", lambda: caf)
        write_stage(stage, stage.run(context), release_dir)
    return {
        table: pl.read_parquet(table_path(release_dir, "measure", table))
        for table in schema.STAGE_TABLES["measure"]
    }


def test_core_contribution_rows_change_no_measure(tmp_path: Path) -> None:
    crdf, crs = (
        _in_millions(frame).collect() for frame in (_caf_rows(), _caf_records())
    )
    baseline = _measure(tmp_path / "baseline", crdf, crs)

    # Provider-perspective imputed contributions to CAF and a CRS core contribution
    # by CAF itself, both Bi_Multi 2, plus a repeat of a CAF row.
    with_core = _measure(
        tmp_path / "core",
        pl.concat([
            crdf,
            crdf_rows(
                _bank("imputed", 2021, provider_code=CAF, perspective="provider",
                      bi_multi=2, objective="climate", face_usd=1e3, climate_usd=1e3),
                _bank("repeat", 2022, provider_code=CAF, recipient_code=PERU,
                      face_usd=1e3, climate_usd=1e3, repeat_of="rPERU"),
            ),
        ]),
        pl.concat([
            crs,
            crs_records({"record_id": "core", "year": 2021, "donor_code": CAF,
                         "bi_multi": 2, "commitment_usd": 1e3,
                         "disbursement_usd": 1e3}),
        ]),
    )  # fmt: skip

    assert baseline["modelled"].height == 4
    assert not baseline["agency_shares"].is_empty()
    for table, frame in baseline.items():
        assert with_core[table].equals(frame), table


@pytest.mark.parametrize("spanned", [True, False])
def test_the_first_share_year_reads_its_span_from_crdf_span(
    tmp_path: Path, spanned: bool
) -> None:
    crdf = crdf_rows(
        _bank("r15", 2015, provider_code=CAF, recipient_code=PERU, face_usd=30.0,
              climate_usd=30.0),
    )  # fmt: skip
    span = crdf_rows(
        _bank("r14", 2014, provider_code=CAF, recipient_code=PERU, face_usd=10.0,
              climate_usd=10.0),
    )  # fmt: skip
    crs = crs_records(
        *(
            {"record_id": f"c{year}", "year": year, "donor_code": CAF,
             "recipient_code": PERU, "commitment_usd": 100.0,
             "disbursement_usd": 50.0}
            for year in (2014, 2015)
        )
    )  # fmt: skip

    shares = _measure(
        tmp_path,
        _in_millions(crdf).collect(),
        _in_millions(crs).collect(),
        span=_in_millions(span).collect() if spanned else None,
    )["agency_shares"].filter(
        pl.col("year") == 2015, pl.col("objective") == "mitigation"
    )

    # Without the 2014 rows, CAF's 2014 commitments would sit in 2015's span with no
    # climate rows, so 2015 has no share.
    assert shares.rows(named=True) == (
        [{"channel_code": 46008, "year": 2015, "objective": "mitigation",
          "share": pytest.approx(40.0 / 200), "source": "one",
          "share_source": "computed"}]
        if spanned
        else []
    )  # fmt: skip


# Enough rows per group that polars' threaded group sum, left in place, changes the
# last bit of a float total on most runs.
RERUN_ROWS = 10_000
RERUNS = 10


def _rerun_frames() -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """ASDB and IFC commitments, and ASDB and CAF records, over 2020 and 2021."""
    rng = np.random.default_rng(7)
    climate, committed, disbursed = rng.lognormal(15, 3, (3, RERUN_ROWS)).tolist()
    crdf = crdf_rows(
        *(
            _bank(f"r{i}", 2020 + i % 2, provider_code=(ASDB, IFC)[i % 2],
                  recipient_code=UNSPECIFIED, climate_usd=amount,
                  adaptation_usd=amount / 3, overlap_usd=amount / 3)
            for i, amount in enumerate(climate)
        )
    )  # fmt: skip
    crs = crs_records(
        *(
            {"record_id": f"x{i}", "year": 2020 + i % 2,
             "donor_code": (ASDB, CAF)[i % 2], "project_number": f"p{i % 7}",
             "recipient_code": UNSPECIFIED, "commitment_usd": com,
             "disbursement_usd": dis}
            for i, (com, dis) in enumerate(zip(committed, disbursed, strict=True))
        )
    )  # fmt: skip
    return crdf.lazy(), crs.lazy()


def _rerun_agency_shares() -> pl.LazyFrame:
    crdf, crs = _rerun_frames()
    return agency_shares(
        crdf,
        crs=crs,
        imputed_shares=NO_IMPUTED,
        agencies=AGENCIES[:1],
        years=(2020, 2021),
    )


def _rerun_modelled_ranges() -> pl.LazyFrame:
    crdf, crs = _rerun_frames()
    providers = (
        ModelledProvider(
            provider_code=IFC,
            basis="report",
            crs_ratios=False,
            report_ratios=(_report(0.92), _report(1.05)),
        ),
        ModelledProvider(provider_code=CAF, basis="crs"),
    )
    status = pl.LazyFrame(schema={"row_id": pl.String, "status": pl.String})
    return modelled_ranges(
        crdf, crs=crs, status=status, providers=providers, last_year=2021
    )


@pytest.mark.parametrize("table", [_rerun_agency_shares, _rerun_modelled_ranges])
def test_measures_give_the_same_bits_on_every_run(
    table: Callable[[], pl.LazyFrame],
) -> None:
    first = table().collect()

    for _ in range(RERUNS):
        assert table().collect().sort(first.columns).equals(first.sort(first.columns))

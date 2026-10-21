"""Agency climate shares and the modelled line on hand-built CRDF and CRS rows."""

from pathlib import Path

import polars as pl
import pytest
from normalised import crdf_rows, crs_records

from climate_finance import schema
from climate_finance.build import write_stage
from climate_finance.build.config import load_config
from climate_finance.build.errors import ConfigError
from climate_finance.build.measure import (
    AgencyChannel,
    ModelledProvider,
    ProviderFacts,
    ReportRatio,
    agency_shares,
    load_provider_facts,
    modelled_ranges,
)
from climate_finance.build.stages import REGISTRY, StageContext, table_path

BUILD_TOML = Path(__file__).parents[1] / "build.toml"
ASDB, EU, ADAPTATION_FUND, CAF, IFC, NDB = 915, 918, 1012, 1015, 903, 1044
PERU, MEXICO, AMERICA_REGIONAL, UNSPECIFIED = 454, 358, 498, 998
AGENCIES = (
    AgencyChannel(channel_code=46004, provider_code=ASDB),
    AgencyChannel(channel_code=42001, provider_code=EU, agency_codes=(1,)),
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
    return {
        (channel, year, objective, source): share
        for channel, year, objective, share, source in frame.collect().iter_rows()
    }


def test_one_shares_are_inclusive_two_year_shares_of_crs_commitments() -> None:
    crdf = crdf_rows(
        _component("a20", 2020, face=100.0, a=30.0, m=20.0, o=10.0),
        _component("a21", 2021, face=50.0, a=0.0, m=50.0, o=0.0),
        # A significant adaptation marker counts at 0.4 under ONE.
        _bank("f21", 2021, provider_code=ADAPTATION_FUND, rio_adaptation=1,
              rio_mitigation=0, face_usd=10.0, climate_usd=10.0,
              adaptation_usd=10.0, mitigation_usd=0.0),
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
            crdf.lazy(),
            crs=crs.lazy(),
            imputed_shares=NO_IMPUTED,
            agencies=AGENCIES,
            years=(2020, 2021),
        )
    )

    # Only 2021 has its whole two-year span inside the years; the EU budget channel
    # has no CRS commitments, so no share.
    expected = {
        (46004, 2021, "adaptation", "one"): 30.0 / 1000,
        (46004, 2021, "mitigation", "one"): 70.0 / 1000,
        (46004, 2021, "cross_cutting", "one"): 10.0 / 1000,
        (47111, 2021, "adaptation", "one"): 4.0 / 20,
        (47111, 2021, "mitigation", "one"): 0.0,
        (47111, 2021, "cross_cutting", "one"): 0.0,
        (42004, 2021, "adaptation", "one"): 0.0,
        (42004, 2021, "mitigation", "one"): 20.0 / 80,
        (42004, 2021, "cross_cutting", "one"): 0.0,
    }
    assert shares.keys() == expected.keys()
    for key, share in expected.items():
        assert shares[key] == pytest.approx(share), key


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
        agencies=AGENCIES,
        years=(2015, 2024),
    ).collect()

    schema.validate(schema.AGENCY_SHARES, shares)
    assert shares.drop("source").sort(pl.all()).equals(imputed.sort(pl.all()))
    assert set(shares["source"]) == {"oecd"}


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
        (2021, 9998, 100.0, 50.0),
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
    # Mexico 150/100 both ways, capped; 9998 is the CRDF's 998, 50/100 both ways;
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


def _measure(tmp_path: Path, crdf: pl.DataFrame, crs: pl.DataFrame) -> dict:
    release_dir = tmp_path / "r"
    status = pl.DataFrame(
        {"row_id": crdf["row_id"], "status": "not_traceable"},
        schema={"row_id": pl.String, "status": pl.String},
    )
    _write(release_dir, "normalise", "crdf", crdf)
    _write(release_dir, "normalise", "crs", crs)
    _write(release_dir, "normalise", "imputed_shares", NO_IMPUTED.collect())
    _write(release_dir, "review", "commitments", status)
    context = StageContext(
        release="r", release_dir=release_dir, config=load_config(BUILD_TOML),
        offline=True,
    )  # fmt: skip
    stage = next(entry for entry in REGISTRY if entry.name == "measure")
    write_stage(stage, stage.run(context), release_dir)
    return {
        table: pl.read_parquet(table_path(release_dir, "measure", table))
        for table in schema.STAGE_TABLES["measure"]
    }


def test_core_contribution_rows_change_no_measure(tmp_path: Path) -> None:
    crdf, crs = _caf_rows(), _caf_records()
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

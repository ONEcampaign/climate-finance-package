"""The normalise stage on hand-built OECD raw files."""

import json
import shutil
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from climate_finance import schema
from climate_finance.build import write_stage
from climate_finance.build.config import Ledgers, load_config
from climate_finance.build.fetch import (
    BTR_DIR,
    CRDF_FILES,
    CRDF_SPAN_FILE,
    CRS_FILE,
    DAC_EXCHANGE_FILE,
    GCF_FILE,
    IATI_DIR,
    IMPUTED_SHARES_FILE,
    MULTISYSTEM_FILE,
)
from climate_finance.build.normalise import run_normalise
from climate_finance.build.normalise.crdf import CRDFNormaliseError, normalise_crdf
from climate_finance.build.normalise.crs import normalise_crs, trace_population
from climate_finance.build.normalise.external import MULTI_COUNTRY
from climate_finance.build.stages import REGISTRY, StageContext, table_path

OECD = Path(__file__).parent / "fixtures" / "raw" / "oecd"
YEARS = (2010, 2025)


@pytest.fixture(scope="module")
def crs() -> pl.DataFrame:
    return normalise_crs(pl.scan_parquet(OECD / "CRS.parquet"), years=YEARS).collect()


def _crdf(
    crs: pl.DataFrame, perspective: str = "recipient", raw: pl.DataFrame | None = None
) -> pl.DataFrame:
    if raw is None:
        raw = pl.read_parquet(OECD / f"crdf_{perspective}.parquet")
    return normalise_crdf(raw.lazy(), perspective=perspective, crs=crs.lazy()).collect()


def _row(frame: pl.DataFrame, **where: object) -> dict[str, object]:
    match = frame.filter(*(pl.col(k) == v for k, v in where.items()))
    assert match.height == 1, match
    return match.row(0, named=True)


def test_crs_keeps_records_with_an_amount_in_usd_units(crs: pl.DataFrame) -> None:
    schema.validate(schema.CRS, crs)
    # 17 rows: one without an amount and one from 2009 drop out.
    assert crs.height == 15
    assert "2022000777" not in crs["crs_id"]
    assert "2009000001" not in crs["crs_id"]
    # A record with only a (negative) grant equivalent stays.
    correction = _row(crs, crs_id="2021000555")
    assert correction["grant_equiv_usd"] == pytest.approx(-0.3e6)
    solar = _row(crs, crs_id="2022000102")
    assert solar["commitment_usd"] == pytest.approx(4e6)
    assert solar["disbursement_usd"] is None
    # Bi_Multi and missing markers as published.
    assert _row(crs, crs_id="2022000601")["bi_multi"] == 6
    core = _row(crs, crs_id="2022000900")
    assert core["bi_multi"] == 2
    assert core["rio_adaptation"] is None


def test_crs_writes_unspecified_recipients_as_the_crdf_does() -> None:
    # The bulk CRS codes "Developing countries, unspecified" 9998.
    raw = pl.read_parquet(OECD / "CRS.parquet").with_columns(
        recipient_code=pl.when(pl.col("crs_id") == "2022000601")
        .then(pl.lit(9998, pl.Int32))
        .otherwise(pl.col("recipient_code"))
    )

    crs = normalise_crs(raw.lazy(), years=YEARS).collect()

    assert _row(crs, crs_id="2022000601")["recipient_code"] == 998


def test_crs_keeps_the_expected_start_and_completion_dates() -> None:
    raw = pl.read_parquet(OECD / "CRS.parquet").with_columns(
        expected_start_date=pl.when(pl.col("crs_id") == "2022000102").then(
            pl.date(2022, 4, 1)
        ),
        completion_date=pl.when(pl.col("crs_id") == "2022000102").then(
            pl.date(2025, 12, 31)
        ),
    )
    crs = normalise_crs(raw.lazy(), years=YEARS).collect()
    solar = _row(crs, crs_id="2022000102")
    assert (solar["expected_start_date"], solar["completion_date"]) == (
        date(2022, 4, 1),
        date(2025, 12, 31),
    )
    assert crs["completion_date"].null_count() == crs.height - 1


def test_crs_record_ids_are_unique_and_stable(crs: pl.DataFrame) -> None:
    raw = pl.read_parquet(OECD / "CRS.parquet")
    shuffled = raw.sample(fraction=1.0, shuffle=True, seed=7)
    revised = raw.with_columns(
        usd_commitment=pl.when(pl.col("crs_id") == "2022000102")
        .then(4.5)
        .otherwise(pl.col("usd_commitment"))
    )

    assert crs["record_id"].is_unique().all()
    for variant in (shuffled, revised):
        rebuilt = normalise_crs(variant.lazy(), years=YEARS).collect()
        assert rebuilt.sort("record_id")["record_id"].equals(
            crs.sort("record_id")["record_id"]
        )
    # Two tranches of one project get two ids.
    assert crs.filter(crs_id="2022000101")["record_id"].n_unique() == 2


def test_crs_record_ids_follow_long_descriptions_through_a_revision() -> None:
    # Two tranches agree on every identifying field; their long descriptions differ.
    tranche = pl.col("crs_id") == "2022000101"
    raw = pl.read_parquet(OECD / "CRS.parquet").with_columns(
        long_description=pl.when(tranche)
        .then(pl.concat_str("long_description", pl.col("usd_commitment").cast(str)))
        .otherwise("long_description")
    )
    # The revision swaps the tranches' amounts.
    revised = raw.with_columns(
        pl.when(tranche)
        .then(pl.col(name).reverse().over(tranche))
        .otherwise(pl.col(name))
        .alias(name)
        for name in ("usd_commitment", "usd_disbursement", "usd_grant_equiv")
    )

    ids = [
        normalise_crs(frame.lazy(), years=YEARS)
        .filter(crs_id="2022000101")
        .select("long_description", "record_id")
        .sort("long_description")
        .collect()
        for frame in (raw, revised)
    ]

    assert ids[0].equals(ids[1])


def test_crs_trims_ids(crs: pl.DataFrame) -> None:
    raw = pl.read_parquet(OECD / "CRS.parquet").with_columns(
        pl.when(pl.col("crs_id") == "2022000102")
        .then(pl.concat_str(pl.col(name), pl.lit(" ")))
        .otherwise(pl.col(name))
        .alias(name)
        for name in ("crs_id", "project_number")
    )

    padded = normalise_crs(raw.lazy(), years=YEARS).collect()

    assert padded.equals(crs)


def test_trace_population_leaves_out_core_contributions(crs: pl.DataFrame) -> None:
    traced = trace_population(crs.lazy()).collect()

    assert traced.height == crs.height - 1
    assert 2 not in traced["bi_multi"]


def test_crdf_amounts_face_values_and_climate_shares(crs: pl.DataFrame) -> None:
    crdf = _crdf(crs)
    schema.validate(schema.CRDF, crdf)

    # A Rio-marked bilateral row: face is the full commitment, no climate share.
    water = _row(crdf, crs_id="2022000101")
    assert water["face_usd"] == water["climate_usd"] == pytest.approx(2.5e6)
    assert water["adaptation_usd"] == water["overlap_usd"] == pytest.approx(2.5e6)
    assert water["climate_share"] is None
    # IFAD publishes 40% of significant projects as climate.
    ifad = _row(crdf, crs_id="2022000301")
    assert ifad["face_usd"] == pytest.approx(4e6 / 0.4)
    assert ifad["climate_share"] is None
    # A bank's climate components: face from the published share.
    ida = _row(crdf, crs_id="2022P170001")
    assert ida["climate_usd"] == pytest.approx(67.5e6)
    assert ida["face_usd"] == pytest.approx(67.5e6 / 0.45)
    assert ida["climate_share"] == pytest.approx(0.45)
    # IFC publishes no share: face is the climate value.
    ifc = _row(crdf, provider_code=903)
    assert ifc["face_usd"] == ifc["climate_usd"] == pytest.approx(10e6)
    assert ifc["climate_share"] == 1.0


@pytest.mark.parametrize(
    ("crs_id", "objective"),
    [
        ("2022000101", "cross_cutting"),
        ("2022000102", "adaptation"),
        ("2022000301", "mitigation"),
        # No amount: the markers decide.
        ("2022000103", "adaptation"),
        # Components with adaptation and mitigation parts.
        ("2022P170001", "cross_cutting"),
    ],
)
def test_crdf_objective(crs: pl.DataFrame, crs_id: str, objective: str) -> None:
    assert _row(_crdf(crs), crs_id=crs_id)["objective"] == objective


def test_crdf_provider_perspective_imputed_rows(crs: pl.DataFrame) -> None:
    crdf = _crdf(crs, "provider")
    schema.validate(schema.CRDF, crdf)

    imputed = _row(crdf, crs_id="2022000900")
    assert imputed["objective"] == "climate"
    assert imputed["bi_multi"] == 2
    assert imputed["face_usd"] == imputed["climate_usd"] == pytest.approx(9e6)
    assert set(crdf["perspective"]) == {"provider"}
    # The same project in the two perspectives keeps two row ids.
    recipient = _row(_crdf(crs), crs_id="2022000101")["row_id"]
    assert _row(crdf, crs_id="2022000101")["row_id"] != recipient


@pytest.mark.parametrize(
    ("where", "bi_multi"),
    [
        ({"crs_id": "2022000101"}, 1),
        ({"crs_id": "2022P170001"}, 4),
        # EU Institutions, a development bank in the CRDF, report bilaterally.
        ({"crs_id": "2024381234565"}, 1),
        ({"crs_id": "2022000601"}, 6),
        # Denmark's one private-class record does not outvote its bilateral ones.
        ({"crs_id": "2022000701"}, 1),
    ],
)
def test_crdf_bi_multi_is_the_providers_crs_code(
    crs: pl.DataFrame, where: dict[str, object], bi_multi: int
) -> None:
    assert _row(_crdf(crs), **where)["bi_multi"] == bi_multi


def test_crdf_refuses_a_provider_without_crs_records(crs: pl.DataFrame) -> None:
    raw = pl.read_parquet(OECD / "crdf_recipient.parquet")

    with pytest.raises(CRDFNormaliseError, match=r"\[5\]"):
        _crdf(crs.filter(pl.col("donor_code") != 5), raw=raw)


def test_crdf_repairs_excel_damage(crs: pl.DataFrame) -> None:
    crdf = _crdf(crs)

    assert _row(crdf, crs_id="2022000102")["title"] is None
    # "1/2015" became a date in Excel; the CRS record gives it back.
    assert _row(crdf, crs_id="2015001842")["project_number"] == "1/2015"
    assert _row(crdf, crs_id="2015009999")["project_number"] is None


def test_crdf_trims_ids(crs: pl.DataFrame) -> None:
    raw = pl.read_parquet(OECD / "crdf_recipient.parquet")
    padded = raw.with_columns(
        pl.concat_str(pl.lit(" "), pl.col(name), pl.lit(" ")).alias(name)
        for name in ("crs_id", "project_number")
    )

    assert _crdf(crs, raw=padded).equals(_crdf(crs, raw=raw))


@pytest.mark.parametrize(
    ("project_number", "scheme"),
    [
        ("20130370", "none"),
        ("86532", "eib_serial"),
        ("90218", "sequence"),
        ("91828", "sequence"),
        ("81234", "bulk"),
    ],
)
def test_eib_id_scheme(crs: pl.DataFrame, project_number: str, scheme: str) -> None:
    crdf = _crdf(crs)

    assert _row(crdf, project_number=project_number)["id_scheme"] == scheme
    assert crdf.filter(pl.col("provider_code") != 918)["id_scheme"].is_null().all()


def test_eib_id_scheme_refuses_an_unknown_form(crs: pl.DataFrame) -> None:
    raw = pl.read_parquet(OECD / "crdf_recipient.parquet").with_columns(
        crs_id=pl.when(pl.col("project_number") == "86532")
        .then(pl.lit("EIB-86532"))
        .otherwise(pl.col("crs_id"))
    )

    with pytest.raises(CRDFNormaliseError, match="EIB-86532"):
        _crdf(crs, raw=raw)


def test_crdf_vintage_is_the_workbooks_latest_year(crs: pl.DataFrame) -> None:
    # The 2014-2015 workbook also holding a 2014 row.
    raw = pl.read_parquet(OECD / "crdf_recipient.parquet").with_columns(
        year=pl.when(pl.col("crs_id") == "2015009999")
        .then(2014)
        .otherwise(pl.col("year"))
    )
    crdf = _crdf(crs, raw=raw)

    vintages = crdf.group_by("year").agg(pl.col("vintage").unique())
    assert dict(vintages.iter_rows()) == {
        2014: [2015],
        2015: [2015],
        2022: [2022],
        2024: [2024],
    }


@pytest.mark.parametrize("ledgers", [False, True])
@pytest.mark.parametrize("recoveries", [False, True])
def test_run_normalise_writes_every_table_from_raw(
    tmp_path: Path, recoveries: bool, ledgers: bool
) -> None:
    raw = tmp_path / "raw"
    fixtures = OECD.parent
    for source, target in [
        (OECD / "CRS.parquet", CRS_FILE),
        (OECD / "crdf_recipient.parquet", CRDF_FILES["recipient"]),
        (OECD / "crdf_provider.parquet", CRDF_FILES["provider"]),
        (fixtures / "multisystem" / "multisystem_2022_2023.parquet", MULTISYSTEM_FILE),
        (
            fixtures / "imputed_shares" / "Imputed_multilateral_shares.xlsx",
            IMPUTED_SHARES_FILE,
        ),
        (fixtures / "gcf" / "projects.json", GCF_FILE),
    ]:
        (raw / target).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(source, raw / target)
    # 2014 rows of the 2014-2015 workbook, whose 2015 rows are in crdf_recipient:
    # orchards under their 2015 crs id, and school gardens under another, which
    # the 2015 row repeats by title and amount.
    pl.read_parquet(OECD / "crdf_recipient.parquet").filter(
        pl.col("crs_id").is_in(["2015009999", "2015001842"])
    ).with_columns(
        year=pl.lit(2014, pl.Int32),
        crs_id=pl.col("crs_id").replace({"2015001842": "2014001842"}),
    ).write_parquet(raw / CRDF_SPAN_FILE)
    shutil.copytree(
        fixtures / "iati", raw / IATI_DIR, ignore=shutil.ignore_patterns("*.md")
    )
    shutil.copytree(
        fixtures / "ledgers", raw / "ledgers", ignore=shutil.ignore_patterns("*.md")
    )
    (raw / BTR_DIR).mkdir(parents=True)
    shutil.copy(fixtures / "btr" / "NLD-table-1.xlsx", raw / BTR_DIR / "NLD.xlsx")
    (raw / BTR_DIR / "manifest.json").write_text(
        json.dumps(
            [
                {
                    "party": "Netherlands",
                    "name": "NLD.xlsx",
                    "ok": True,
                    "bytes": (raw / BTR_DIR / "NLD.xlsx").stat().st_size,
                }
            ]
        )
    )
    pl.DataFrame(
        {"iso_code": ["EUI"] * 12, "year": range(2015, 2027), "exchange_rate": 0.8}
    ).write_parquet(raw / DAC_EXCHANGE_FILE)
    config = load_config(Path(__file__).parents[1] / "build.toml")
    # The optional sources, the ledgers and the BTR archive, are named together.
    sources = config.sources.model_copy(
        update={
            "ledgers": Ledgers(
                years=(2021, 2022), publishers=("ibrd", "ida", "ebrd", "afdb", "gcf")
            )
            if ledgers
            else None,
            "btr_archive": tmp_path / "archive" if ledgers else None,
        }
    )
    context = StageContext(
        release="test",
        release_dir=tmp_path,
        config=config.model_copy(update={"recoveries": recoveries, "sources": sources}),
        offline=True,
    )
    normalise = next(stage for stage in REGISTRY if stage.name == "normalise")

    hashes = write_stage(normalise, run_normalise(context), tmp_path)

    # `orgs` is written with recoveries, and `ledger`, `btr` and `btr_workbooks`
    # when the config names ledgers and a BTR archive.
    btr_tables = {"btr", "btr_workbooks"}
    expected = set(schema.STAGE_TABLES["normalise"]) - {"orgs", "ledger", *btr_tables}
    assert set(hashes) == (
        expected
        | ({"orgs"} if recoveries else set())
        | ({"ledger", *btr_tables} if ledgers else set())
    )
    if ledgers:
        btr = pl.read_parquet(table_path(tmp_path, "normalise", "btr"))
        assert set(btr["party"]) == {"NLD"}
        assert set(btr["provider_code"]) == {7}
        # The Netherlands converts at 1/1.183 euros a dollar; the fixture's DAC
        # rate for the euro is 0.8.
        assert btr["dac_rate"].to_list() == [0.8]
        assert btr["face_usd_at_dac"].to_list() == pytest.approx([1.183e6 / 0.8])
        assert btr["face_usd"].to_list() == pytest.approx([1.183e6 * 1.183])
        assert btr["grant_equiv_usd_at_dac"].to_list() == pytest.approx([1.183e6 / 0.8])
        workbooks = pl.read_parquet(table_path(tmp_path, "normalise", "btr_workbooks"))
        assert workbooks.select("file", "rows_kept").rows() == [("NLD.xlsx", 1)]
    if ledgers:
        ledger = pl.read_parquet(table_path(tmp_path, "normalise", "ledger"))
        assert set(ledger["publisher"]) == {"ibrd", "ida", "ebrd", "afdb", "gcf"}
        # EBRD's euros convert at the euro's DAC rate. The DAC publishes no XDR
        # rate, so the AfDB's SDR amounts convert at the IMF's, which the fixture
        # gives as 1.25 US dollars in 2020 and not at all for 2019.
        ebrd = ledger.filter(publisher="ebrd")
        assert (ebrd["amount_usd"] == ebrd["amount"] / 0.8).all()
        xdr = ledger.filter(currency="XDR")
        assert set(xdr["year"]) == {2019, 2020}
        assert (
            xdr.filter(year=2020)
            .select(pl.col("amount") * 1.25 - pl.col("amount_usd"))
            .to_series()
            == 0
        ).all()
        assert xdr.filter(year=2019)["amount_usd"].is_null().all()
    if recoveries:
        orgs = pl.read_parquet(table_path(tmp_path, "normalise", "orgs"))
        eib = orgs.filter(role="agency", source_name="European Investment Bank")
        assert eib.select("donor_code", "agency_code").rows() == [(918, 3)]
    # The year before the CRDF years goes to crdf_span alone, dated by its workbook
    # and without the row a 2015 row repeats.
    span = pl.read_parquet(table_path(tmp_path, "normalise", "crdf_span"))
    assert span.select("year", "vintage", "crs_id").rows() == [
        (2014, 2015, "2015009999")
    ]
    crdf = pl.read_parquet(table_path(tmp_path, "normalise", "crdf"))
    assert crdf["year"].min() == config.years.crdf[0]
    external = pl.read_parquet(table_path(tmp_path, "normalise", "external"))
    # GCF's euro project converts at the euro's DAC rate.
    euro = external.filter(currency="EUR", source="gcf")
    assert euro.height
    assert (euro["amount_usd"] == euro["amount"] / 0.8).all()
    # The two-country project is multi-country.
    joined = external.filter(pl.col("recipient_iso").str.contains(","))
    assert (joined["recipient_code"] == MULTI_COUNTRY).all()

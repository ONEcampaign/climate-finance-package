"""The ledger readers parse their pinned raw slices into one stackable frame."""

import json
import logging
from collections.abc import Callable
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from climate_finance import schema
from climate_finance.build.readers.ledgers.finances_one import read_finances_one
from climate_finance.build.readers.ledgers.gcf import (
    read_gcf_disbursements,
    read_gcf_readiness,
)
from climate_finance.build.readers.ledgers.iati import read_iati_ledger
from climate_finance.errors import SourceUnavailableError

LEDGERS = Path(__file__).parent / "fixtures" / "raw" / "ledgers"
IATI = LEDGERS / "iati"
PUBLISHERS = sorted(p.name for p in IATI.iterdir() if p.is_dir())


def _amount(frame: pl.DataFrame, **where: object) -> float:
    """The summed `amount` of the rows matching `where`, which must be some."""
    match = frame.filter(*(pl.col(k) == v for k, v in where.items()))
    assert match.height, where
    return match["amount"].sum()


def _iati(publisher: str) -> pl.DataFrame:
    return read_iati_ledger(IATI / publisher, publisher=publisher)


@pytest.mark.parametrize("publisher", PUBLISHERS)
def test_every_iati_publisher_gives_the_ledger_columns(publisher: str) -> None:
    frame = _iati(publisher)

    schema.validate(schema.LEDGER, frame)
    assert frame["publisher"].unique().to_list() == [publisher]
    assert not frame["date"].is_null().any()
    assert frame["year"].to_list() == [d.year for d in frame["date"]]


def test_finances_one_gives_december_to_december_flows() -> None:
    folder = LEDGERS / "finances_one"
    ibrd = read_finances_one(folder, dataset="DS00975", years=[2021, 2022])

    schema.validate(schema.LEDGER, ibrd)
    assert set(ibrd["publisher"]) == {"ibrd"}
    assert set(ibrd["date"]) == {date(2021, 12, 31), date(2022, 12, 31)}
    # IBRD90002 held 1,000,000 at the end of 2020 and 1,500,000 a year later.
    assert _amount(ibrd, key="IBRD90002", year=2021) == 500_000
    assert _amount(ibrd, key="IBRD90002", year=2022) == 300_000
    # A loan absent from the 2020 snapshot starts from zero.
    assert _amount(ibrd, key="IBRD90001", year=2021) == 200_000
    # A loan with nothing disbursed still has a row; a correction stays negative.
    assert _amount(ibrd, key="IBRD90003", year=2022) == 0
    assert _amount(ibrd, key="IBRD90004", year=2022) == -20_000
    assert ibrd["amount_usd"].to_list() == ibrd["amount"].to_list()


def test_finances_one_reads_ida_credits_by_credit_number() -> None:
    ida = read_finances_one(LEDGERS / "finances_one", dataset="DS00976", years=[2021])

    schema.validate(schema.LEDGER, ida)
    assert set(ida["publisher"]) == {"ida"}
    # IDA90002: 700,000 at the end of 2020 and 760,000 at the end of 2021.
    assert _amount(ida, key="IDA90002") == 60_000


def _finances_one_2021(tmp_path: Path, edit: Callable[[list], list]) -> Path:
    """A copy of the IBRD fixture snapshots with the 2021 rows passed through `edit`."""
    folder = LEDGERS / "finances_one"
    before, after = "DS00975_31-Dec-2020.json", "DS00975_31-Dec-2021.json"
    (tmp_path / before).write_text((folder / before).read_text())
    rows = json.loads((folder / after).read_text())
    (tmp_path / after).write_text(json.dumps(edit(rows)))
    return tmp_path


def _split(row: dict, *, first: float, **second: object) -> list[dict]:
    """`row` as two rows holding `first` and the rest of its disbursement."""
    rest = row["disbursed_amount"] - first
    return [
        {**row, "disbursed_amount": first, "loan_status": "Repaying"},
        {**row, "disbursed_amount": rest, **second},
    ]


def test_finances_one_adds_up_a_loan_listed_in_several_rows(tmp_path: Path) -> None:
    # IBRD90002 stands at 1,500,000 at the end of 2021, listed as 900,000 and 600,000.
    folder = _finances_one_2021(
        tmp_path, lambda rows: [rows[0], *_split(rows[1], first=900_000), *rows[2:]]
    )

    ibrd = read_finances_one(folder, dataset="DS00975", years=[2021])

    assert ibrd.filter(key="IBRD90002").height == 1
    assert _amount(ibrd, key="IBRD90002") == 500_000


@pytest.mark.parametrize(
    ("edit", "match"),
    [
        # A row repeated whole is a paging overlap, which summed reads as a phantom
        # disbursement.
        (lambda rows: [*rows, rows[0]], r"whole .* \['IBRD90001'\].* overlapped"),
        (
            lambda rows: [
                *rows[:1],
                *_split(rows[1], first=1, project_id="P0"),
                *rows[2:],
            ],
            r"disagree on IBRD90002 \(project_id\)",
        ),
        (
            lambda rows: [
                *rows[:1],
                *_split(rows[1], first=1, project_name_="X"),
                *rows[2:],
            ],
            r"disagree on IBRD90002 \(title\)",
        ),
    ],
    ids=["row-repeated-whole", "parts-of-two-projects", "parts-of-two-titles"],
)
def test_finances_one_refuses_rows_it_cannot_add_up(
    tmp_path: Path, edit: Callable[[list], list], match: str
) -> None:
    with pytest.raises(SourceUnavailableError, match=match):
        read_finances_one(
            _finances_one_2021(tmp_path, edit), dataset="DS00975", years=[2021]
        )


def test_finances_one_names_a_missing_snapshot() -> None:
    with pytest.raises(SourceUnavailableError, match="31-Dec-2019"):
        read_finances_one(LEDGERS / "finances_one", dataset="DS00975", years=[2020])


def test_asdb_is_keyed_by_loan_number_and_leaves_out_co_financing_legs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.WARNING):
        asdb = _iati("asdb")

    # LN9001 disbursed 1,000,000 and 2,500,000 in 2014; its type 5 row stays out.
    assert (
        _amount(asdb, key="LN9001", transaction_type="disbursement", year=2014)
        == 3_500_000
    )
    assert set(asdb["transaction_type"]) == {"commitment", "disbursement"}
    assert asdb["currency"].unique().to_list() == ["USD"]
    # `...-LN9002-KfW` ends in a lender code, not a loan number: no key, no rows.
    assert "LN9002" not in asdb["key"].to_list()
    assert "transaction without key" in caplog.text


def test_afdb_is_keyed_by_the_finance_type_id_of_each_transaction() -> None:
    afdb = _iati("afdb")

    assert sorted(afdb["key"].unique()) == ["1", "9000000001"]
    assert _amount(afdb, key="9000000001", transaction_type="disbursement") == 80_000
    # XDR rows carry no dollar figure for the ledger check's conversion to fill in.
    assert afdb["amount_usd"].is_null().all()
    assert afdb["activity_id"].unique().to_list() == ["46002-P-ZZ-AA0-001"]


def test_iadb_counts_a_transaction_repeated_within_an_activity_once() -> None:
    iadb = _iati("iadb")

    assert iadb["key"].unique().to_list() == ["ZZ-L9001"]
    # The file lists 2,000 and 300 twice each on 28 February 2014.
    assert (
        _amount(iadb, transaction_type="disbursement", date=date(2014, 2, 28)) == 2_300
    )


def test_isdb_activities_parse_when_the_root_declares_their_namespace() -> None:
    isdb = _iati("isdb")

    assert isdb["key"].unique().to_list() == ["ZZZ9001"]
    # 500,000 + 40,000 + 90,000; the type 6 row stays out.
    assert _amount(isdb, transaction_type="disbursement") == 630_000
    assert isdb["title"].unique().to_list() == ["Invented hydro project"]


@pytest.mark.parametrize(
    ("publisher", "key", "currency", "disbursed"),
    [
        ("ebrd", "90001", "EUR", 7_500_000),
        ("bcie", "900001", "USD", 37_000_000),
    ],
)
def test_other_banks_strip_their_identifier_prefix(
    publisher: str, key: str, currency: str, disbursed: float
) -> None:
    frame = _iati(publisher)

    assert frame["key"].unique().to_list() == [key]
    assert frame["currency"].unique().to_list() == [currency]
    assert _amount(frame, transaction_type="disbursement") == disbursed
    assert (frame["amount_usd"].is_not_null()).all() == (currency == "USD")


def test_gef_cumulative_disbursements_become_the_rise_of_the_running_peak() -> None:
    gef = _iati("gef")

    # 9001 reports 280,000, 750,000, 350,000 and 0 in 2020 to 2023: the 2022 and 2023
    # figures fall, so only 2020 (280,000) and 2021 (470,000) add to the peak.
    by_year = {
        year: _amount(gef, key="9001", transaction_type="disbursement", year=year)
        for year in (2020, 2021, 2022, 2023)
    }
    assert by_year == pytest.approx({2020: 280_000, 2021: 470_000, 2022: 0, 2023: 0})
    # 9002 reports 1,000,000, 6,000,000 and 5,000,000 in 2020: the peak rises by
    # 5,000,000 on the second and not at all on the third.
    assert (
        _amount(gef, key="9002", transaction_type="disbursement", year=2020)
        == 6_000_000
    )
    assert _amount(gef, key="9002", date=date(2020, 9, 5)) == 0
    # A commitment is not cumulative and stays as published.
    assert _amount(gef, key="9001", transaction_type="commitment") == 2_700_000


def test_bilateral_publishers_are_keyed_by_their_whole_identifier() -> None:
    bmz = _iati("bmz")

    assert bmz["key"].unique().to_list() == ["DE-1-900001"]
    assert bmz["other_ids"][0].to_list() == ["900001"]
    # Expenditure counts beside disbursement: 320,000 + 28,000 euros in 2011.
    assert _amount(bmz, transaction_type="expenditure", year=2011) == 348_000

    ec = _iati("ec-intpa")
    assert (
        _amount(
            ec, key="XI-IATI-EC_INTPA-2019-PCC-900001", transaction_type="disbursement"
        )
        == 7_150_000
    )
    assert "XI-IATI-EC_INTPA-2019/900-001" in ec["other_ids"].explode().to_list()


def test_iati_ledger_names_a_directory_without_xml(tmp_path: Path) -> None:
    with pytest.raises(SourceUnavailableError, match=tmp_path.name):
        read_iati_ledger(tmp_path, publisher="asdb")


def test_gcf_disbursements_are_keyed_by_the_approved_reference(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.WARNING):
        gcf = read_gcf_disbursements(LEDGERS / "gcf" / "projects.json")

    schema.validate(schema.LEDGER, gcf)
    assert set(gcf["key"]) == {"FP901", "FP941", "FP951"}
    assert set(gcf["transaction_type"]) == {"disbursement"}
    # FP901 paid 1,500,000 and 400,000 in 2022.
    assert _amount(gcf, key="FP901", year=2022) == 1_900_000
    # FP941's first line is EUR 4,000,000; the API's own dollar figure is not used.
    assert _amount(gcf, key="FP941", date=date(2020, 11, 2)) == 4_000_000
    assert gcf.filter(key="FP941", currency="EUR")["amount_usd"].is_null().all()
    assert "without an effective date" in caplog.text


def test_gcf_readiness_gives_the_approval_and_the_amount_disbursed_to_date() -> None:
    readiness = read_gcf_readiness(LEDGERS / "gcf" / "readiness.json")

    schema.validate(schema.LEDGER, readiness)
    assert set(readiness["publisher"]) == {"gcf_readiness"}
    # The grant without an agreement reference has no key, so no row.
    assert set(readiness["key"]) == {"ZZA-RS-001", "ZZB-RS-007", "ZZC-RS-001"}
    commitment = readiness.filter(key="ZZB-RS-007", transaction_type="commitment")
    assert commitment.select("date", "year", "amount").row(0) == (
        date(2021, 11, 10),
        2021,
        220_000.5,
    )
    # The list dates no disbursement, so the figure carries no year to check against.
    disbursed = readiness.filter(key="ZZB-RS-007", transaction_type="disbursed_to_date")
    assert disbursed.select("date", "year", "amount").row(0) == (None, None, 200_000.25)
    assert readiness.filter(key="ZZC-RS-001")["transaction_type"].to_list() == [
        "commitment"
    ]


def test_gcf_co_financing_commitments_are_left_out(tmp_path: Path) -> None:
    transaction = (
        '<transaction><transaction-type code="2" />'
        '<transaction-date iso-date="2019-05-01" /><value currency="USD">{amount}'
        "</value><description><narrative>{text}</narrative></description>"
        "</transaction>"
    )
    (tmp_path / "gcf.xml").write_text(
        '<iati-activities version="2.03"><iati-activity default-currency="USD">'
        "<iati-identifier>XM-DAC-41317-FP901</iati-identifier>"
        + transaction.format(amount=10, text="GCF financing commitment")
        + transaction.format(amount=25, text="Co-financing commitment")
        + "</iati-activity></iati-activities>"
    )

    gcf = read_iati_ledger(tmp_path, publisher="gcf")

    assert gcf.select("key", "transaction_type", "amount").rows() == [
        ("FP901", "commitment", 10.0)
    ]

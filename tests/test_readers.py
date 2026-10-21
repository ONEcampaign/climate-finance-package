"""The four build readers parse their pinned raw slices into the tables they feed."""

import shutil
from pathlib import Path

import polars as pl
import pytest

from climate_finance import schema
from climate_finance.build.readers.gcf import read_gcf_projects
from climate_finance.build.readers.iati import read_iati_activities
from climate_finance.build.readers.imputed_shares import read_imputed_shares
from climate_finance.build.readers.multisystem import read_multisystem

RAW = Path(__file__).parent / "fixtures" / "raw"


def _value(frame: pl.DataFrame, **where: object) -> float:
    """The one `amount_usd`, `contribution_usd` or `share` of the matching row."""
    column = next(c for c in ("amount_usd", "contribution_usd", "share") if c in frame)
    match = frame.filter(*(pl.col(k) == v for k, v in where.items()))
    assert match.height == 1, match
    return match[column].item()


def test_multisystem_gives_core_contributions_in_usd_units() -> None:
    frame = read_multisystem(RAW / "multisystem" / "multisystem_2022_2023.parquet")

    schema.validate(schema.CORE_CONTRIBUTIONS, frame)
    # Norway to IBRD in 2023, read from the raw rows (USD million, current prices).
    norway_ibrd = {"provider_code": 8, "channel_code": 44001, "year": 2023}
    assert _value(frame, **norway_ibrd, flow="disbursements") == pytest.approx(8.125e6)
    assert _value(frame, **norway_ibrd, flow="commitments") == pytest.approx(2.4e6)
    # Aggregate donors, constant prices and "Contributions through" stay out.
    assert set(frame["provider_code"]) == {4, 8, 302}
    assert set(frame["flow"]) == {"commitments", "disbursements"}


def test_multisystem_refuses_a_core_contribution_without_a_total_cell(
    tmp_path: Path,
) -> None:
    raw = pl.read_parquet(RAW / "multisystem" / "multisystem_2022_2023.parquet")
    without_total = raw.filter(
        ~((pl.col("aidtype_code") == 10) & (pl.col("recipient_code") == 10100))
    )
    path = tmp_path / "multisystem.parquet"
    without_total.write_parquet(path)

    with pytest.raises(ValueError, match="total cell"):
        read_multisystem(path)


def test_imputed_shares_split_each_agency_by_objective() -> None:
    frame = read_imputed_shares(
        RAW / "imputed_shares" / "Imputed_multilateral_shares.xlsx"
    )

    schema.validate(schema.IMPUTED_SHARES, frame)
    # African Development Fund (46003) in 2024, as typed in the workbook.
    african_fund = {"channel_code": 46003, "year": 2024}
    assert _value(frame, **african_fund, objective="mitigation") == pytest.approx(0.2)
    assert _value(frame, **african_fund, objective="cross_cutting") == 0
    # CGIAR (47015) in 2024: mitigation only 0.08, adaptation only 0.2 and both 0.65.
    # Adaptation and mitigation each include both.
    cgiar = {"channel_code": 47015, "year": 2024}
    assert _value(frame, **cgiar, objective="cross_cutting") == pytest.approx(0.65)
    assert _value(frame, **cgiar, objective="mitigation") == pytest.approx(0.73)
    assert _value(frame, **cgiar, objective="adaptation") == pytest.approx(0.85)
    # The draft sheet adds no year.
    assert set(frame["year"]) == {2020, 2023, 2024}


def test_imputed_shares_give_a_climate_row_where_the_sheet_has_no_split() -> None:
    frame = read_imputed_shares(
        RAW / "imputed_shares" / "Imputed_multilateral_shares.xlsx"
    )

    # The 2020 sheet gives one share per agency: the African Development Bank's is
    # 0.4125. The UNFCCC (41316) has an overall share of 1 and no split in 2024.
    assert _value(frame, channel_code=46002, year=2020) == pytest.approx(0.4125)
    assert _value(frame, channel_code=41316, year=2024) == 1
    climate = frame.filter(pl.col("objective") == "climate")
    assert set(climate["year"]) == {2020, 2023, 2024}
    # A key holds split rows or a climate row, never both.
    split = frame.filter(pl.col("objective") != "climate")
    assert climate.join(split, on=["channel_code", "year"]).is_empty()


def test_imputed_shares_average_the_agencies_that_share_a_channel_code() -> None:
    frame = read_imputed_shares(
        RAW / "imputed_shares" / "Imputed_multilateral_shares.xlsx"
    )

    # IDB (3000m; mitigation only 0.25, both 0.08) and IDB Invest (1000m; 0.5 and
    # 0.02) report under 46012.
    expected = (3000 * (0.25 + 0.08) + 1000 * (0.5 + 0.02)) / (3000 + 1000)
    mitigation = _value(frame, channel_code=46012, year=2024, objective="mitigation")
    assert mitigation == pytest.approx(expected)


def test_imputed_shares_refuse_an_agency_without_a_channel_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from climate_finance.build.readers import imputed_shares

    codes = {k: v for k, v in imputed_shares._CHANNEL_CODES.items() if k != "CGIAR"}
    monkeypatch.setattr(imputed_shares, "_CHANNEL_CODES", codes)

    with pytest.raises(ValueError, match="CGIAR"):
        read_imputed_shares(RAW / "imputed_shares" / "Imputed_multilateral_shares.xlsx")


def test_gcf_projects_give_commitments_and_disbursements_per_year() -> None:
    frame = read_gcf_projects(RAW / "gcf" / "projects.json")

    schema.validate(schema.EXTERNAL, frame)
    fp001 = {"source": "gcf", "activity_id": "FP001"}
    assert _value(frame, **fp001, year=2015, transaction_type="commitment") == 4_800_000
    # Two disbursements of 1,150,000 and 275,000 dated 2022 add up.
    disbursed = frame.filter(**fp001, year=2022, transaction_type="disbursement")
    assert disbursed.select("amount", "amount_usd", "currency", "recipient_iso").row(
        0
    ) == (1_425_000, 1_425_000, "USD", "PER")
    assert not frame["cumulative_declared"].any()


def test_gcf_projects_keep_other_currencies_and_join_countries() -> None:
    frame = read_gcf_projects(RAW / "gcf" / "projects.json")

    # FP021's GCF funding is EUR 12m, approved in 2016; the USD value is left to the
    # normalise stage.
    senegal = frame.filter(activity_id="FP021", transaction_type="commitment")
    assert senegal.select("year", "currency", "amount", "amount_usd").row(0) == (
        2016,
        "EUR",
        12_000_000,
        None,
    )
    # FP005 runs in Kenya and Rwanda; its two GCF funding lines are USD 14m and 3.5m.
    regional = frame.filter(activity_id="FP005", transaction_type="commitment")
    assert regional.select("recipient_iso", "amount").row(0) == ("KEN,RWA", 17_500_000)


def test_iati_activities_give_transactions_by_publisher_and_currency() -> None:
    frame = read_iati_activities(RAW / "iati")

    schema.validate(schema.EXTERNAL, frame)
    # GCF's FP050: five commitments of 12.5m, 1.1m, 4.2m, 3m and 41m.
    bhutan = frame.filter(
        publisher="gcf", activity_id="XM-DAC-41317-FP050", year=2017
    ).filter(transaction_type="commitment")
    assert bhutan.select("currency", "recipient_iso").row(0) == ("USD", "BT")
    assert bhutan["amount"].item() == pytest.approx(61_800_000)
    assert bhutan["amount_usd"].item() == bhutan["amount"].item()
    assert set(frame["publisher"]) == {"caf", "eib", "gcf", "gef"}


def test_iati_keeps_a_euro_transaction_and_leaves_its_usd_value_null() -> None:
    frame = read_iati_activities(RAW / "iati")

    # EIB's Bosnia activity: two EUR commitments of 4.2m on 2018-03-13. The
    # Mozambique activity defaults to EUR but states its transactions in USD.
    bosnia = frame.filter(publisher="eib", activity_id="XM-DAC-918-3-20040329-90151")
    assert bosnia.select("year", "currency", "recipient_iso", "amount").row(0) == (
        2018,
        "EUR",
        "BA",
        8_400_000,
    )
    assert bosnia["amount_usd"].null_count() == 1
    mozambique = frame.filter(
        publisher="eib", activity_id="XM-DAC-918-3-20010242-84835"
    )
    assert mozambique.select("currency", "amount", "amount_usd").row(0) == (
        "USD",
        10_300_000,
        10_300_000,
    )


def test_iati_keeps_cumulative_disbursements_as_published_and_flags_them() -> None:
    frame = read_iati_activities(RAW / "iati")

    # GEF Bahrain 9734 reports 0 and 142,500 in 2020, 0 in 2022 and 2023, and 171,000
    # in 2024; the reader leaves them unchanged.
    activity = frame.filter(
        activity_id="XI-BRIDGE-6385676864-9734", transaction_type="disbursement"
    ).sort("year")
    assert activity.select("year", "amount").rows() == [
        (2020, 142_500),
        (2022, 0),
        (2023, 0),
        (2024, 171_000),
    ]
    assert activity["cumulative_declared"].all()
    # Only GEF carries the note.
    flagged = frame.filter("cumulative_declared")
    assert set(flagged["publisher"]) == {"gef"}
    assert set(frame.filter(publisher="gef")["cumulative_declared"]) == {True}


def test_iati_takes_recipients_and_currency_from_the_transaction_then_the_activity(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "pub"
    directory.mkdir()
    (directory / "a.xml").write_text(
        """<iati-activities>
<iati-activity default-currency="USD">
  <iati-identifier>A1</iati-identifier>
  <recipient-country code="KE"/><recipient-country code="RW"/>
  <transaction><transaction-type code="3"/><transaction-date iso-date="2020-01-01"/>
    <value currency="EUR">10</value></transaction>
  <transaction><transaction-type code="3"/><transaction-date iso-date="2020-01-01"/>
    <value>5</value><recipient-country code="UG"/></transaction>
</iati-activity>
<iati-activity>
  <iati-identifier>A2</iati-identifier>
  <transaction><transaction-type code="3"/><transaction-date iso-date="2020-01-01"/>
    <value>7</value></transaction>
</iati-activity>
</iati-activities>""",
        encoding="utf-8",
    )

    frame = read_iati_activities(tmp_path)

    schema.validate(schema.EXTERNAL, frame)
    # A2 states no currency anywhere and is skipped.
    assert frame.sort("currency").select(
        "activity_id", "recipient_iso", "currency", "amount", "amount_usd"
    ).rows() == [
        ("A1", "KE,RW", "EUR", 10, None),
        ("A1", "UG", "USD", 5, 5),
    ]


def test_iati_skips_an_activity_that_does_not_parse_and_keeps_the_rest() -> None:
    frame = read_iati_activities(RAW / "iati")

    # Bahrain's three activities, plus the one well-formed activity each of Lebanon
    # and Mexico; the ill-formed one in each of those files is lost.
    assert frame.filter(publisher="gef")["activity_id"].n_unique() == 5


def test_iati_reads_an_activity_repeated_across_files_once(tmp_path: Path) -> None:
    source = RAW / "iati" / "gef" / "gef-bh.xml"
    for name, files in {"once": ["a.xml"], "twice": ["a.xml", "b.xml"]}.items():
        publisher = tmp_path / name / "gef"
        publisher.mkdir(parents=True)
        for file in files:
            shutil.copy(source, publisher / file)

    assert read_iati_activities(tmp_path / "twice").equals(
        read_iati_activities(tmp_path / "once")
    )


def test_iati_refuses_a_directory_without_publishers(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        read_iati_activities(tmp_path)

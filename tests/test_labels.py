"""Labelling packets, labels CSVs and the decisions they become, on hand-built CRS
records with invented values."""

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from climate_finance import schema
from climate_finance.build.errors import ReviewDecisionsError
from climate_finance.build.link.decisions import import_labels, override_links
from climate_finance.build.link.keys import as_exact_links, build_units
from climate_finance.build.link.packet import (
    MATCH_DEFINITION,
    packet_pairs_path,
    write_packet,
)
from climate_finance.build.link.splink_models import read_decision_labels

CRS = {
    "record_id": pl.String,
    "year": pl.Int16,
    "donor_code": pl.Int32,
    "agency_code": pl.Int32,
    "crs_id": pl.String,
    "project_number": pl.String,
    "title": pl.String,
    "short_description": pl.String,
    "long_description": pl.String,
    "recipient_code": pl.Int32,
    "purpose_code": pl.Int32,
    "channel_code": pl.Int32,
    "flow_code": pl.Int32,
    "commitment_usd": pl.Float64,
    "disbursement_usd": pl.Float64,
    "commitment_date": pl.Date,
}


def record(
    record_id: str,
    year: int,
    *,
    crs_id: str,
    title: str,
    commit: float | None = None,
    disburse: float | None = None,
) -> dict[str, object]:
    return {
        "record_id": record_id,
        "year": year,
        "donor_code": 1,
        "agency_code": 10,
        "crs_id": crs_id,
        "project_number": f"PN-{crs_id}",
        "title": title,
        "short_description": None,
        "long_description": f"Long text of {title}",
        "recipient_code": 289,
        "purpose_code": 23210,
        "channel_code": 12000,
        "flow_code": 11,
        "commitment_usd": commit,
        "disbursement_usd": disburse,
        "commitment_date": date(year, 3, 1),
    }


# Two disbursements and the commitments they may continue; d22's title names the
# id it reports under, and c19 has a sibling of a similar title.
RECORDS = [
    record(
        "c19", 2019, crs_id="2019000001", title="Solar grid phase one", commit=100.0
    ),
    record("s19", 2019, crs_id="2019000002", title="Solar grid phase two", commit=80.0),
    record("d21", 2021, crs_id="2021000007", title="Solar grid", disburse=40.0),
    record(
        "d22", 2022, crs_id="2019000001", title="Solar grid 2019000001", disburse=30.0
    ),
]


class Case:
    def __init__(self, tmp_path: Path) -> None:
        self.crs = pl.DataFrame(RECORDS, schema=CRS)
        self.units = build_units(self.crs.lazy()).collect()
        self.tmp_path = tmp_path

    def unit(self, record_id: str) -> str:
        found = self.units.filter(pl.col("record_ids").list.contains(record_id))
        return found["unit_id"].item()

    def pairs(self) -> pl.DataFrame:
        """d21 and d22 against c19, queued for review."""
        return pl.DataFrame(
            {
                "left_id": [self.unit("d21"), self.unit("d22")],
                "right_id": [self.unit("c19")] * 2,
                "review_reason": ["band", "rule_disagreement"],
            }
        )

    def packet(self, *, show_ids: bool, name: str = "packet.md") -> Path:
        return write_packet(
            self.pairs(),
            units=self.units.lazy(),
            crs=self.crs.lazy(),
            out=self.tmp_path / name,
            title="Pairs",
            show_ids=show_ids,
        )

    def labels(self, rows: list[tuple[int, str, float]], name: str = "a.csv") -> Path:
        path = self.tmp_path / name
        pl.DataFrame(
            rows, schema=["pair", "label", "confidence"], orient="row"
        ).with_columns(reason=pl.lit("checked")).write_csv(path)
        return path


@pytest.mark.parametrize("show_ids", [True, False])
def test_a_packet_shows_ids_only_when_asked(tmp_path: Path, show_ids: bool) -> None:
    text = Case(tmp_path).packet(show_ids=show_ids).read_text(encoding="utf-8")

    for value in ("2019000001", "2021000007", "PN-2019000001"):
        assert (value in text) is show_ids
    # d22's title carries its id: blanked when ids hide.
    assert ("Solar grid [id]" in text) is not show_ids
    assert text.count(MATCH_DEFINITION) == 1
    assert text.count("## Pair ") == 2
    # c19 has one sibling of a similar title for its recipient: s19.
    assert "| Similar commitments | 1 | 1 |" in text


def test_an_agent_csv_round_trips_to_agent_labels_and_overrides(tmp_path: Path) -> None:
    case = Case(tmp_path)
    packet = case.packet(show_ids=False)
    decisions = tmp_path / "review_decisions.parquet"

    import_labels(
        packet=packet,
        labels_csv=case.labels([(1, "no_match", 0.1), (2, "match", 0.95)]),
        decisions_path=decisions,
        label_source="agent",
        reviewer="pilot-run",
        model="claude-opus-5-5",
    )

    recorded = pl.read_parquet(decisions)
    schema.validate(schema.REVIEW_DECISIONS, recorded)
    assert recorded.select(
        "decision", "label_source", "model", "confidence", "round", "review_reason"
    ).rows() == [
        ("reject", "agent", "claude-opus-5-5", 0.1, "label", "band"),
        ("accept", "agent", "claude-opus-5-5", 0.95, "label", "rule_disagreement"),
    ]
    labels = read_decision_labels(decisions)
    schema.validate(schema.PAIR_LABELS, labels.drop("stratified"))
    assert set(labels["pair_label_source"]) == {"agent"}
    links = as_exact_links(
        case.pairs()
        .select("left_id", "right_id", group_id="left_id")
        .with_columns(link_method=pl.lit("classifier"))
        .lazy(),
        task="trace",
    ).collect()
    kept = override_links(links, labels=labels)
    assert kept["left_id"].to_list() == [case.unit("d22")]


@pytest.mark.parametrize(
    "rows",
    [
        [(1, "match", 0.9)],
        [(1, "match", 0.9), (2, "match", 0.9), (3, "match", 0.9)],
        [(1, "match", 0.9), (1, "no_match", 0.1), (2, "match", 0.9)],
        [(1, "match", 0.9), (2, "maybe", 0.5)],
        [(1, "match", 1.5), (2, "match", 0.9)],
    ],
    ids=["missing", "extra", "repeated", "bad-label", "bad-confidence"],
)
def test_a_csv_that_does_not_answer_its_packet_is_refused(
    tmp_path: Path, rows: list[tuple[int, str, float]]
) -> None:
    case = Case(tmp_path)
    packet = case.packet(show_ids=False)
    decisions = tmp_path / "review_decisions.parquet"

    with pytest.raises(ReviewDecisionsError):
        import_labels(
            packet=packet,
            labels_csv=case.labels(rows),
            decisions_path=decisions,
            label_source="agent",
            reviewer="run",
            model="m",
        )
    assert not decisions.exists()


def test_a_packet_changed_after_its_pairs_were_written_is_refused(
    tmp_path: Path,
) -> None:
    case = Case(tmp_path)
    packet = case.packet(show_ids=False)
    packet.write_text(packet.read_text(encoding="utf-8") + "edited", encoding="utf-8")

    with pytest.raises(ReviewDecisionsError, match="changed"):
        import_labels(
            packet=packet,
            labels_csv=case.labels([(1, "match", 0.9), (2, "match", 0.9)]),
            decisions_path=tmp_path / "d.parquet",
            label_source="agent",
            reviewer="run",
            model="m",
        )
    assert packet_pairs_path(packet).exists()


def test_a_persons_later_decision_and_an_agents_latest_round_stand(
    tmp_path: Path,
) -> None:
    case = Case(tmp_path)
    packet = case.packet(show_ids=True)
    decisions = tmp_path / "review_decisions.parquet"

    def agent(rows: list[tuple[int, str, float]], label_round: str) -> None:
        import_labels(
            packet=packet,
            labels_csv=case.labels(rows, name=f"{label_round}.csv"),
            decisions_path=decisions,
            label_source="agent",
            reviewer="run",
            model="m",
            label_round=label_round,
        )

    agent([(1, "no_match", 0.3), (2, "match", 0.6)], "label")
    agent([(1, "match", 0.7), (2, "no_match", 0.4)], "adjudicate")
    import_labels(
        packet=packet,
        labels_csv=case.labels([(1, "no_match", 0.0), (2, "no_match", 0.0)], "p.csv"),
        decisions_path=decisions,
        label_source="human",
        reviewer="analyst",
    )
    # A second label recorded after the adjudication does not displace it.
    agent([(1, "no_match", 0.2), (2, "match", 0.9)], "second")

    labels = read_decision_labels(decisions)
    got = dict(labels.select("left_id", "pair_label_source").iter_rows())
    assert got == {case.unit("d21"): "human", case.unit("d22"): "human"}
    assert not labels["is_match"].any()

    # An adjudication after the person's decision stands over it.
    agent([(1, "match", 0.9), (2, "match", 0.9)], "adjudicate")
    labels = read_decision_labels(decisions)
    assert set(labels["pair_label_source"]) == {"agent"}
    assert labels["is_match"].all()


def test_pairs_with_neither_a_reason_nor_a_sample_id_are_refused(
    tmp_path: Path,
) -> None:
    case = Case(tmp_path)
    packet = write_packet(
        case.pairs().drop("review_reason"),
        units=case.units.lazy(),
        crs=case.crs.lazy(),
        out=tmp_path / "bare.md",
        title="Pairs",
        show_ids=True,
    )

    with pytest.raises(ReviewDecisionsError, match="review_reason or sample_id"):
        import_labels(
            packet=packet,
            labels_csv=case.labels([(1, "match", 0.9), (2, "match", 0.9)]),
            decisions_path=tmp_path / "d.parquet",
            label_source="human",
            reviewer="analyst",
        )

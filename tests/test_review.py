"""The review queue, pending status and decisions on hand-built CRS records with
invented values, linked by the link stage's own `build_units`, `link_by_keys` and
`link_within_year`."""

from collections.abc import Sequence
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from climate_finance import schema
from climate_finance.build import write_stage
from climate_finance.build.allocate import (
    allocate,
    allocate_to_rows,
    compute_status,
    counted_rows,
    status_basis,
)
from climate_finance.build.config import Ledgers, load_config
from climate_finance.build.link.crdf_crs import link_within_year
from climate_finance.build.link.decisions import (
    PAIR,
    drop_pairs,
    import_labels,
    override_links,
)
from climate_finance.build.link.keys import as_exact_links, build_units, link_by_keys
from climate_finance.build.link.packet import packet_pairs_path, write_packet
from climate_finance.build.link.review import (
    flag_review,
    pending_rows,
    review_queue,
    write_queue_packet,
)
from climate_finance.build.link.splink_models import read_decision_labels
from climate_finance.build.stages import REGISTRY, StageContext, table_path

BUILD_TOML = Path(__file__).parents[1] / "build.toml"
FIRST_YEAR = 2010
BAND = (0.3, 0.9)
TOLERANCE = 1e-6

CRS_COLUMNS = {
    "record_id": pl.String,
    "year": pl.Int16,
    "donor_code": pl.Int32,
    "agency_code": pl.Int32,
    "crs_id": pl.String,
    "project_number": pl.String,
    "recipient_code": pl.Int32,
    "purpose_code": pl.Int32,
    "bi_multi": pl.Int8,
    "commitment_usd": pl.Float64,
    "disbursement_usd": pl.Float64,
    "commitment_date": pl.Date,
    "expected_start_date": pl.Date,
    "completion_date": pl.Date,
    "title": pl.String,
    "short_description": pl.String,
    "long_description": pl.String,
    "channel_code": pl.Int32,
    "flow_code": pl.Int32,
}
CRDF_COLUMNS = {
    "row_id": pl.String,
    "perspective": pl.String,
    "provider_code": pl.Int32,
    "crs_id": pl.String,
    "project_number": pl.String,
    "title": pl.String,
    "recipient_code": pl.Int32,
    "year": pl.Int16,
    "purpose_code": pl.Int32,
    "face_usd": pl.Float64,
    "climate_usd": pl.Float64,
    "parent_row_id": pl.String,
    "repeat_of": pl.String,
}


def record(
    record_id: str,
    year: int,
    crs_id: str,
    *,
    commit: float | None = None,
    disburse: float | None = None,
    dated: int | None = None,
) -> dict[str, object]:
    return {
        "record_id": record_id,
        "year": year,
        "donor_code": 1,
        "agency_code": 10,
        "crs_id": crs_id,
        "project_number": None,
        "recipient_code": 289,
        "purpose_code": 230,
        "bi_multi": 1,
        "commitment_usd": commit,
        "disbursement_usd": disburse,
        "commitment_date": None if dated is None else date(dated, 6, 30),
        "short_description": None,
        "long_description": None,
        "channel_code": None,
        "flow_code": 11,
        "expected_start_date": None,
        "completion_date": None,
        "title": f"Project {crs_id}",
    }


def crdf_row(row_id: str, year: int, crs_id: str, face: float) -> dict[str, object]:
    return {
        "row_id": row_id,
        "perspective": "recipient",
        "provider_code": 1,
        "crs_id": crs_id,
        "project_number": None,
        "title": f"Project {crs_id}",
        "recipient_code": 289,
        "year": year,
        "purpose_code": 230,
        "face_usd": face,
        "climate_usd": face / 4,
        "parent_row_id": None,
        "repeat_of": None,
    }


class Linked:
    """CRS records and CRDF rows linked as the link stage links them."""

    def __init__(
        self, records: Sequence[dict[str, object]], rows: Sequence[dict[str, object]]
    ) -> None:
        self.crs = pl.DataFrame(list(records), schema=CRS_COLUMNS)
        self.crdf = pl.DataFrame(list(rows), schema=CRDF_COLUMNS)
        self.units = build_units(self.crs.lazy()).collect()
        within = link_within_year(
            counted_rows(self.crdf.lazy()), units=self.units.lazy()
        ).collect()
        self.links = pl.concat(
            [link_by_keys(self.units, first_year=FIRST_YEAR), within]
        )

    def unit(self, record_id: str) -> str:
        found = self.units.filter(pl.col("record_ids").list.contains(record_id))
        return found["unit_id"].item()

    def add(self, *links: tuple[str, str, str, str, float]) -> None:
        """Add trace links (left record, right record, group, method, probability)."""
        rows = pl.DataFrame(
            [
                (self.unit(left), self.unit(right), group, method)
                for left, right, group, method, _ in links
            ],
            schema=["left_id", "right_id", "group_id", "link_method"],
            orient="row",
        )
        added = (
            as_exact_links(rows.lazy(), task="trace")
            .collect()
            .with_columns(probability=pl.Series([link[4] for link in links]))
        )
        self.links = pl.concat([self.links, added])

    def statuses(self, links: pl.DataFrame) -> pl.DataFrame:
        """The allocate stage's allocations and commitments under `links`."""
        crs, rows = self.crs.lazy(), counted_rows(self.crdf.lazy())
        units = self.units.lazy()
        pairs = allocate(units, crs=crs, links=links.lazy())
        allocations = allocate_to_rows(
            pairs, crdf=rows, units=units, crs=crs, links=links.lazy()
        )
        basis = status_basis(
            rows,
            units=units,
            crs=crs,
            links=links.lazy(),
            pairs=pairs,
            id_practice=_empty(schema.ID_PRACTICE).lazy(),
        )
        return compute_status(commitments=basis, allocations=allocations).collect()


def _empty(table: schema.TableSchema) -> pl.DataFrame:
    return pl.DataFrame(
        schema={name: column.dtype for name, column in table.columns.items()}
    )


def no_labels() -> pl.DataFrame:
    return _empty(schema.PAIR_LABELS)


def pairs_of(frame: pl.DataFrame) -> set[tuple[str, str]]:
    return set(frame.select("left_id", "right_id").iter_rows())


# One component with three queued commitments. d20 fills c19 by crs id and gives its
# remaining 30 to c18 through a repair link. k20 fills c16 through a classifier link
# in the band, so its Splink link to c18, which passes later, finds nothing left. In
# 2021 two more classifier links in the band follow: e21 into the full c19, and f21
# into c18, which it fills to face.
#
# Rejecting f21 leaves c18 at 30 of 40, partly disbursed, so c18 waits on review,
# though rejecting every queued link at once would send k20's 10 to c18 and keep it
# disbursed. Rejecting k20's link leaves c16 with nothing, so c16 waits too. c19 is
# disbursed either way. g21 links to h17 above the band and is not queued.
RECORDS = [
    record("c19", 2019, "A", commit=100.0),
    record("c18", 2018, "B", commit=40.0),
    record("d20", 2020, "A", disburse=130.0),
    record("e21", 2021, "E", disburse=50.0),
    record("f21", 2021, "F", disburse=50.0),
    record("h17", 2017, "H", commit=60.0),
    record("g21", 2021, "G", disburse=50.0),
    record("c16", 2016, "K", commit=10.0),
    record("k20", 2020, "M", disburse=10.0),
]
ROWS = [
    crdf_row("row_c19", 2019, "A", 100.0),
    crdf_row("row_c18", 2018, "B", 40.0),
    crdf_row("row_h17", 2017, "H", 60.0),
    crdf_row("row_c16", 2016, "K", 10.0),
]


def linked() -> Linked:
    case = Linked(RECORDS, ROWS)
    case.add(
        ("d20", "c18", "repair|B", "repair_new_crs_id", 1.0),
        ("e21", "c19", "classifier|c19", "classifier", 0.5),
        ("f21", "c18", "classifier|c18", "classifier", 0.6),
        ("g21", "h17", "classifier|h17", "classifier", 0.95),
        ("k20", "c16", "classifier|c16", "classifier", 0.4),
        ("k20", "c18", "splink_jev|c18", "splink_jev", 0.95),
    )
    return case


def test_the_queue_holds_each_reason_once_and_leaves_decided_pairs_out() -> None:
    case = linked()
    unit = case.unit
    links = case.links.with_columns(
        ledger_check=pl.when(pl.col("left_id") == unit("d20"))
        .then(pl.lit("contradicted"))
        .otherwise("ledger_check"),
        records_disagree=pl.col("left_id").is_in([unit("d20"), unit("g21")]),
    )
    decided = pl.DataFrame(
        {
            "task": ["trace"],
            "left_id": [unit("e21")],
            "right_id": [unit("c19")],
            "is_match": [True],
            "pair_label_source": ["human"],
        }
    )
    flagged = flag_review(links, labels=decided, band=BAND)
    reasons = {
        (left, right): reason
        for left, right, reason in flagged.filter("review")
        .select("left_id", "right_id", "review_reason")
        .iter_rows()
    }
    assert reasons == {
        (unit("d20"), unit("c19")): "ledger_contradicted",
        (unit("d20"), unit("c18")): "ledger_contradicted",
        (unit("f21"), unit("c18")): "band",
        (unit("k20"), unit("c16")): "band",
        (unit("g21"), unit("h17")): "rule_disagreement",
    }

    commitments = case.statuses(case.links).with_columns(
        disbursed_usd=pl.when(pl.col("row_id") == "row_h17")
        .then(70.0)
        .otherwise("disbursed_usd")
    )
    queue = review_queue(
        flagged,
        commitments=commitments,
        pair_scores=_empty(schema.PAIR_SCORES),
        labels=decided,
    )
    schema.validate(schema.REVIEW_QUEUE, queue)
    assert queue.filter(review_reason="over_face")["left_id"].to_list() == ["row_h17"]
    assert queue.filter(task="trace").height == len(reasons)
    assert (
        queue.select("decision", "reviewer", "note").null_count().row(0)
        == (queue.height,) * 3
    )


def test_a_row_is_pending_exactly_when_rejecting_its_queued_links_changes_it() -> None:
    case = linked()
    links = flag_review(case.links, labels=no_labels(), band=BAND)
    accepted = case.statuses(links)

    pending = pending_rows(
        accepted,
        allocations=_allocations(case, links),
        links=links,
        units=case.units.lazy(),
        crs=case.crs.lazy(),
        crdf=counted_rows(case.crdf.lazy()),
    )

    # Reject each commitment group's queued links on the whole graph and compare.
    groups = links.filter(task="crdf_crs").select(
        "right_id", row_id="left_id", row_group="group_id"
    )
    queued = links.filter("review").join(
        groups.select("right_id", "row_group"), on="right_id"
    )
    expected = set()
    for group in queued["row_group"].unique():
        rejected = case.statuses(
            drop_pairs(links, pairs=queued.filter(row_group=group))
        )
        rows = groups.filter(row_group=group)["row_id"]
        changed = (
            rejected.filter(pl.col("row_id").is_in(rows.implode()))
            .join(accepted.select("row_id", accepted="status"), on="row_id")
            .filter(pl.col("status") != pl.col("accepted"))
        )
        expected |= set(changed["row_id"])
    assert set(pending["row_id"]) == expected == {"row_c16", "row_c18"}
    status = compute_status(
        commitments=accepted, allocations=_allocations(case, links), pending=pending
    ).collect()
    assert dict(status.select("row_id", "status").iter_rows()) == {
        "row_c18": "pending_review",
        "row_c16": "pending_review",
        "row_c19": "disbursed",
        "row_h17": "partly_disbursed",
    }
    assert status["disbursed_usd"].sum() == pytest.approx(
        accepted["disbursed_usd"].sum(), abs=TOLERANCE
    )


def _allocations(case: Linked, links: pl.DataFrame) -> pl.DataFrame:
    crs, units = case.crs.lazy(), case.units.lazy()
    pairs = allocate(units, crs=crs, links=links.lazy())
    return allocate_to_rows(
        pairs,
        crdf=counted_rows(case.crdf.lazy()),
        units=units,
        crs=crs,
        links=links.lazy(),
    )


# d20 reaches c19, d22 and d22b reach c19 and c21, all through one crs-id group.
# d22b, dated its own year, is a unit of its own in d22's pass.
GROUP = [
    record("c19", 2019, "A", commit=100.0),
    record("d20", 2020, "A", disburse=60.0),
    record("c21", 2021, "A", commit=100.0),
    record("d22", 2022, "A", disburse=100.0),
    record("d22b", 2022, "A", disburse=10.0, dated=2022),
]


@pytest.mark.parametrize(
    ("dropped", "accepted", "left"),
    [
        # d20 reaches nothing else in the group, so only its link goes.
        (
            ("d20", "c19"),
            None,
            {("d22", "c19"), ("d22", "c21"), ("d22b", "c19"), ("d22b", "c21")},
        ),
        # d22 keeps c21, so c19 leaves the group for every unit.
        (("d22", "c19"), None, {("d22", "c21"), ("d22b", "c21")}),
        # d22b's accepted link to c19 stays, and d22b's part of the group, holding
        # it, runs before d22's. d20, undecided, still loses c19.
        (
            ("d22", "c19"),
            ("d22b", "c19"),
            {("d22", "c21"), ("d22b", "c19"), ("d22b", "c21")},
        ),
    ],
)
def test_a_decision_keeps_a_groups_units_reaching_the_same_commitments(
    dropped: tuple[str, str],
    accepted: tuple[str, str] | None,
    left: set[tuple[str, str]],
) -> None:
    case = Linked(GROUP, [])
    unit = case.unit

    decided = [(dropped, False)] + ([(accepted, True)] if accepted else [])
    labels = pl.DataFrame(
        [
            {"task": "trace", "left_id": unit(left), "right_id": unit(right)}
            | {"is_match": is_match, "pair_label_source": "human"}
            for (left, right), is_match in decided
        ],
        schema={**dict.fromkeys(PAIR, pl.String), "is_match": pl.Boolean}
        | {"pair_label_source": pl.String},
    )

    kept = override_links(case.links, labels=labels)

    names = {unit(name): name for name in ("c19", "c21", "d20", "d22", "d22b")}
    assert {(names[a], names[b]) for a, b in pairs_of(kept)} == left
    allocated = allocate(case.units.lazy(), crs=case.crs.lazy(), links=kept.lazy())
    filled = allocated.drop_nulls("commitment_id")
    by_commitment = filled.group_by("commitment_id").agg(pl.col("allocated_usd").sum())
    face = case.crs.select(commitment_id="record_id", face="commitment_usd")
    assert (
        by_commitment.join(face, on="commitment_id")
        .filter(pl.col("allocated_usd") > pl.col("face") + TOLERANCE)
        .is_empty()
    )
    # Each linked record gives what it disbursed, excess included.
    given = allocated.group_by("record_id").agg(pl.col("allocated_usd").sum())
    disbursed = case.crs.join(given, on="record_id").select(
        "record_id", "disbursement_usd"
    )
    assert dict(given.iter_rows()) == pytest.approx(dict(disbursed.iter_rows()))
    if accepted:
        # d22b claims c19 first, and d22 fills c21 with all it disbursed.
        assert set(filled.select("record_id", "commitment_id").rows()) == {
            ("d22b", "c19"),
            ("d22", "c21"),
        }
        assert filled.filter(record_id="d22")["allocated_usd"].sum() == pytest.approx(
            100.0
        )


def _release(
    tmp_path: Path, case: Linked, queue: pl.DataFrame, commitments: pl.DataFrame
) -> Path:
    release_dir = tmp_path / "r"
    for stage, table, frame in [
        ("normalise", "crs", case.crs),
        ("normalise", "crdf", case.crdf),
        ("link", "units", case.units),
        ("review", "review_queue", queue),
        ("review", "commitments", commitments),
    ]:
        path = table_path(release_dir, stage, table)
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.write_parquet(path)
    return release_dir


def _answer(packet: Path, labels: dict[tuple[str, str], str]) -> Path:
    """A labels CSV giving `labels` by (left_id, right_id) to the packet's pairs."""
    pairs = pl.read_parquet(packet_pairs_path(packet))
    path = packet.with_suffix(".csv")
    pairs.select(
        "pair",
        label=pl.struct("left_id", "right_id").map_elements(
            lambda pair: labels[(pair["left_id"], pair["right_id"])],
            return_dtype=pl.String,
        ),
        confidence=pl.lit(None, dtype=pl.Float64),
        reason=pl.lit("checked"),
    ).write_csv(path)
    return path


def test_decisions_round_trip_as_human_labels_and_exact_overrides(
    tmp_path: Path,
) -> None:
    case = linked()
    unit = case.unit
    links = flag_review(case.links, labels=no_labels(), band=BAND)
    commitments = case.statuses(links).with_columns(
        disbursed_usd=pl.when(pl.col("row_id") == "row_h17")
        .then(70.0)
        .otherwise("disbursed_usd")
    )
    queue = review_queue(
        links,
        commitments=commitments,
        pair_scores=_empty(schema.PAIR_SCORES),
        labels=no_labels(),
    )
    release_dir = _release(tmp_path, case, queue, commitments)
    packet = write_queue_packet(release_dir=release_dir, out=tmp_path / "queue.md")
    text = packet.read_text(encoding="utf-8")
    assert text.count("## Pair ") == queue.filter(task="trace").height == 3
    assert "| row_h17 | USD 60 | USD 70 |" in text

    decisions_path = tmp_path / "data" / "review_decisions.parquet"
    import_labels(
        packet=packet,
        labels_csv=_answer(
            packet,
            {
                (unit("e21"), unit("c19")): "match",
                (unit("k20"), unit("c16")): "match",
                (unit("f21"), unit("c18")): "no_match",
            },
        ),
        decisions_path=decisions_path,
        label_source="human",
        reviewer="analyst",
    )
    # A blind-sample decision, recorded later, on a pair the queue also decided.
    sample = write_packet(
        pl.DataFrame(
            {"left_id": [unit("f21")], "right_id": [unit("c18")], "sample_id": ["s1"]}
        ),
        units=case.units.lazy(),
        crs=case.crs.lazy(),
        out=tmp_path / "sample.md",
        title="Sample",
        show_ids=False,
    )
    import_labels(
        packet=sample,
        labels_csv=_answer(sample, {(unit("f21"), unit("c18")): "match"}),
        decisions_path=decisions_path,
        label_source="human",
        reviewer="analyst",
    )

    recorded = pl.read_parquet(decisions_path)
    schema.validate(schema.REVIEW_DECISIONS, recorded)
    assert recorded.height == 4
    assert set(recorded.drop_nulls("review_reason")["review_reason"]) == {"band"}
    labels = read_decision_labels(decisions_path)
    schema.validate(schema.PAIR_LABELS, labels.drop("stratified"))
    # Queue decisions were chosen for review, not drawn by the stratified sample.
    assert not labels["stratified"].any()
    assert set(labels.select("left_id", "right_id", "is_match").iter_rows()) == {
        (unit("e21"), unit("c19"), True),
        (unit("k20"), unit("c16"), True),
        (unit("f21"), unit("c18"), False),
    }

    overridden = override_links(links, labels=labels)
    assert (unit("f21"), unit("c18")) not in pairs_of(overridden)
    accepted = overridden.filter(left_id=unit("e21"))
    assert accepted["probability"].to_list() == [1.0]
    again = flag_review(overridden, labels=labels, band=BAND)
    assert not again["review"].any()
    # A commitment row stays queued while it stays over face.
    requeued = review_queue(
        again,
        commitments=commitments,
        pair_scores=_empty(schema.PAIR_SCORES),
        labels=labels,
    )
    assert requeued["review_reason"].to_list() == ["over_face"]

    # Every linked record's allocation, excess included, still sums to its
    # disbursement; f21 lost its only link and gives nothing.
    allocated = _allocations(case, overridden)
    totals = allocated.group_by("record_id").agg(pl.col("allocated_usd").sum())
    linked_units = overridden.filter(task="trace").select(unit_id="left_id")
    linked_records = case.units.join(linked_units, on="unit_id", how="semi").select(
        record_id=pl.col("record_ids").explode()
    )
    assert "f21" not in linked_records["record_id"]
    disbursed = case.crs.join(linked_records, on="record_id").select(
        "record_id", "disbursement_usd"
    )
    joined = disbursed.join(totals, on="record_id", how="left").fill_null(0.0)
    assert (
        joined["allocated_usd"] - joined["disbursement_usd"]
    ).abs().max() < TOLERANCE


def _ledger() -> pl.DataFrame:
    """One IDA ledger row on a key no unit carries."""
    columns = {name: column.dtype for name, column in schema.LEDGER.columns.items()}
    row = {
        "publisher": "ida",
        "key": "IDA99999",
        "activity_id": "IDA99999",
        "transaction_type": "disbursement",
        "date": date(2021, 12, 31),
        "year": 2021,
        "currency": "USD",
        "amount": 1.0,
        "amount_usd": 1.0,
        "title": None,
        "other_ids": [],
    }
    return pl.DataFrame([row], schema=columns)


# An empty ledger once made polars panic in the ledger check's joins.
@pytest.mark.parametrize("ledger_rows", [1, 0])
def test_the_review_stage_writes_tables_that_pass_their_schemas(
    tmp_path: Path, ledger_rows: int
) -> None:
    case = linked()
    links = case.links
    commitments = case.statuses(links)
    release_dir = tmp_path / "r"
    for stage, table, frame in [
        ("normalise", "crs", case.crs),
        ("normalise", "crdf", case.crdf),
        ("normalise", "ledger", _ledger().head(ledger_rows)),
        ("link", "units", case.units),
        ("link", "links", links),
        ("link", "pair_scores", _empty(schema.PAIR_SCORES)),
        ("fill", "allocations", _allocations(case, links)),
        ("fill", "commitments", commitments),
    ]:
        path = table_path(release_dir, stage, table)
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.write_parquet(path)
    config = load_config(BUILD_TOML)
    sources = config.sources.model_copy(
        update={
            "ledgers": Ledgers(years=(2021, 2022), publishers=("ida",)),
            "review_decisions": tmp_path / "review_decisions.parquet",
        }
    )
    context = StageContext(
        release="r",
        release_dir=release_dir,
        config=config.model_copy(update={"review": True, "sources": sources}),
        offline=True,
    )
    stage = next(entry for entry in REGISTRY if entry.name == "review")

    written = write_stage(stage, stage.run(context), release_dir)

    assert set(written) == set(schema.STAGE_TABLES["review"])
    status = pl.read_parquet(table_path(release_dir, "review", "commitments"))
    assert status.filter(status="pending_review")["row_id"].to_list() == [
        "row_c16",
        "row_c18",
    ]
    queue = pl.read_parquet(table_path(release_dir, "review", "review_queue"))
    assert set(queue["review_reason"]) == {"band"}

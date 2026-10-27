"""The labelling page's queue, server and saves, on hand-built CRS records with
invented values."""

import http.client
import json
import random
import socket
import threading
import time
from collections import Counter
from collections.abc import Callable, Iterator
from itertools import pairwise
from pathlib import Path

import polars as pl
import pytest
from test_labels import CRS, record

from climate_finance import schema
from climate_finance.build.link.decisions import import_labels
from climate_finance.build.link.keys import build_units
from climate_finance.build.link.labelling import (
    SPOT_CHECKS,
    QueuedPair,
    Session,
    make_server,
    queue_pairs,
)
from climate_finance.build.link.packet import write_packet
from climate_finance.build.stages import table_path

# Provider 1 has 20 pairs over two packets and 25 draws, the first five pairs backing
# two each; provider 2 has 10 pairs and 10 draws. Pairs of even number are linked by
# a certain rule.
PROVIDERS = {1: 20, 2: 10}
DOUBLED = 5
SENTINELS = ("stratum-sentinel", "method-sentinel")


def _sample(tmp_path: Path) -> tuple[Path, Path]:
    """A release directory with units and CRS rows, and a blind sample of packets and
    a design over it; return both."""
    records = []
    for donor, count in PROVIDERS.items():
        commit = {
            **record(f"c{donor}", 2019, crs_id=f"C{donor}", title="Solar grid"),
            "donor_code": donor,
            "commitment_usd": 100.0,
        }
        records.append(commit)
        records += [
            {
                **record(
                    f"d{donor}-{n:02d}",
                    2020,
                    crs_id=f"D{donor}{n:02d}",
                    title=f"Solar grid {n}",
                    disburse=1.0 + n,
                ),
                "donor_code": donor,
            }
            for n in range(count)
        ]
    crs = pl.DataFrame(records, schema=CRS)
    units = build_units(crs.lazy()).collect()
    release = tmp_path / "release"
    for stage, table, frame in (("link", "units", units), ("normalise", "crs", crs)):
        path = table_path(release, stage, table)
        path.parent.mkdir(parents=True)
        frame.write_parquet(path)

    def unit(record_id: str) -> str:
        found = units.filter(pl.col("record_ids").list.contains(record_id))
        return found["unit_id"].item()

    sample = tmp_path / "sample"
    design = []
    for donor, count in PROVIDERS.items():
        pairs = pl.DataFrame(
            {
                "task": ["trace"] * count,
                "left_id": [unit(f"d{donor}-{n:02d}") for n in range(count)],
                "right_id": [unit(f"c{donor}")] * count,
                "sample_id": [f"r/{donor}/pop/{n:03d}" for n in range(count)],
            }
        )
        for batch, start in enumerate(range(0, count, 10), start=1):
            write_packet(
                pairs.slice(start, 10),
                units=units.lazy(),
                crs=crs.lazy(),
                out=sample / str(donor) / f"batch-{batch:02d}.md",
                title="Blind sample",
                show_ids=True,
            )
        for row in pairs.iter_rows(named=True):
            doubled = donor == 1 and int(row["sample_id"][-3:]) < DOUBLED
            design += [
                {
                    **row,
                    "provider_code": donor,
                    "stratum": SENTINELS[0],
                    "link_method": "crs_id"
                    if int(row["sample_id"][-3:]) % 2 == 0
                    else SENTINELS[1],
                    "weight_usd": 1.0,
                }
            ] * (2 if doubled else 1)
    pl.DataFrame(design).write_parquet(sample / "design.parquet")
    return release, sample


def _session(
    tmp_path: Path,
    reviewer: str = "analyst",
    share: float = 0.10,
    clock: Callable[[], float] = time.monotonic,
) -> Session:
    release, sample = _sample(tmp_path)
    return Session(
        release_dir=release,
        sample_dir=sample,
        decisions=tmp_path / "decisions.parquet",
        reviewer=reviewer,
        share=share,
        clock=clock,
    )


def _synthetic(*, judgement: int, certain: int, providers: int = 3) -> list[QueuedPair]:
    """Pairs over `providers` providers, one to three draws each."""
    return [
        QueuedPair(
            provider=n % providers,
            packet=Path("batch-01.md"),
            batch=1,
            pair=n,
            key=(f"s/{n:04d}", "trace", f"l{n}", f"r{n}"),
            draws=1 + n % 3,
            certain=n >= judgement,
        )
        for n in range(judgement + certain)
    ]


def test_the_queue_takes_judgement_calls_to_a_tenth_of_all_draws() -> None:
    pairs = _synthetic(judgement=300, certain=120)
    total = sum(pair.draws for pair in pairs)

    queue = queue_pairs(pairs, share=0.10)

    calls = [pair for pair in queue if not pair.certain]
    drawn = sum(pair.draws for pair in calls)
    # At least a tenth of every provider's draws together, and not a pair more.
    assert drawn >= 0.10 * total
    assert drawn - calls[-1].draws < 0.10 * total
    # Judgement calls only until then: no certain pair beyond the spot checks.
    assert len(queue) - len(calls) == SPOT_CHECKS


def test_the_queue_holds_fifty_spot_checks_spread_over_providers() -> None:
    queue = queue_pairs(_synthetic(judgement=300, certain=120), share=0.10)

    spot = [pair for pair in queue if pair.certain]

    assert SPOT_CHECKS == 50
    assert len(spot) == len({pair.key for pair in spot}) == SPOT_CHECKS
    counts = Counter(pair.provider for pair in spot)
    assert max(counts.values()) - min(counts.values()) <= 1
    # Fewer certain pairs than the spot checks: all of them.
    few = queue_pairs(_synthetic(judgement=30, certain=7), share=0.10)
    assert sum(pair.certain for pair in few) == 7


def test_the_queue_is_the_same_whatever_the_order_of_the_pairs() -> None:
    pairs = _synthetic(judgement=300, certain=120)
    shuffled = random.Random(7).sample(pairs, len(pairs))

    assert queue_pairs(pairs, share=0.10) == queue_pairs(shuffled, share=0.10)
    assert queue_pairs(pairs, share=0.10) == queue_pairs(reversed(pairs), share=0.10)


def test_the_spot_checks_are_spread_through_the_queue() -> None:
    queue = queue_pairs(_synthetic(judgement=900, certain=120), share=0.10)
    at = [i for i, pair in enumerate(queue) if pair.certain]
    calls = sum(not pair.certain for pair in queue)

    gaps = [later - earlier for earlier, later in pairwise(at)]
    # About one in every `calls / (SPOT_CHECKS + 1)` pairs, none bunched at an end.
    assert max(gaps) <= calls / (SPOT_CHECKS + 1) + 2
    assert at[0] <= calls / (SPOT_CHECKS + 1) + 1
    assert len(queue) - at[-1] <= calls / (SPOT_CHECKS + 1) + 2


def test_the_session_queue_is_fixed_by_the_design_and_counts_decided_pairs(
    tmp_path: Path,
) -> None:
    session = _session(tmp_path)
    first = session.queue[0]
    assert {pair.certain for pair in session.queue} == {True, False}
    assert sum(not pair.certain for pair in session.queue) >= 1
    labels = tmp_path / "other.csv"
    pl.DataFrame(
        {"pair": [first.pair], "label": ["match"], "confidence": [0.9], "reason": [""]}
    ).write_csv(labels)
    import_labels(
        packet=first.packet,
        labels_csv=labels,
        decisions_path=tmp_path / "decisions.parquet",
        label_source="human",
        reviewer="colleague",
        partial=True,
    )

    resumed = Session(
        release_dir=tmp_path / "release",
        sample_dir=tmp_path / "sample",
        decisions=tmp_path / "decisions.parquet",
        reviewer="analyst",
        share=0.10,
    )
    state = resumed.state()

    # A decided pair stays in the queue, counted done and skipped.
    assert resumed.queue == session.queue
    assert state["done"] == 1
    assert state["next"] == 1
    assert state["total"] == len(session.queue)


@pytest.fixture
def served(tmp_path: Path) -> Iterator[tuple[Session, http.client.HTTPConnection]]:
    session = _session(tmp_path)
    server = make_server(session)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    connection = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=10)
    yield session, connection
    connection.close()
    server.shutdown()
    server.server_close()


def _call(
    connection: http.client.HTTPConnection, method: str, path: str, body: object = None
) -> tuple[int, dict[str, object]]:
    connection.request(
        method,
        path,
        body=None if body is None else json.dumps(body),
        headers={"Content-Type": "application/json"},
    )
    response = connection.getresponse()
    return response.status, json.loads(response.read())


def test_a_pair_shows_what_the_packet_shows_and_nothing_more(
    served: tuple[Session, http.client.HTTPConnection],
) -> None:
    session, connection = served

    status, pair = _call(connection, "GET", "/api/pair?i=0")

    assert status == 200
    assert set(pair["view"]) == {"pair", "fields", "similar", "rows"}
    text = json.dumps(pair)
    assert not any(sentinel in text for sentinel in SENTINELS)
    assert "weight" not in text
    assert pair["answer"] is None
    # The labels in A and B's field rows are the packet's.
    assert [row[0] for row in pair["view"]["fields"]][:3] == [
        "crs id",
        "Project number",
        "Donor",
    ]
    assert session.state()["next"] == 0
    # Providers come by name, from the catalogue.
    assert pair["provider_name"] == session.provider_name(session.queue[0].provider)
    assert pair["kind"] == (
        "spot check" if session.queue[0].certain else "judgement call"
    )
    assert session.state()["seconds_left"] is None
    assert [p["name"] for p in session.state()["providers"]] == [
        "Austria (1)",
        "Belgium (2)",
    ]


def test_an_answer_is_saved_at_once_and_a_changed_answer_replaces_it(
    served: tuple[Session, http.client.HTTPConnection], tmp_path: Path
) -> None:
    session, connection = served
    entry = session.queue[0]

    status, saved = _call(
        connection,
        "POST",
        "/api/answer",
        {"i": 0, "label": "match", "strength": "3", "reason": "same loan"},
    )
    assert status == 200
    assert saved["next"] == 1
    _call(
        connection,
        "POST",
        "/api/answer",
        {"i": 0, "label": "no_match", "strength": "u", "reason": ""},
    )
    _, pair = _call(connection, "GET", "/api/pair?i=0")

    assert pair["answer"] == {"label": "no_match", "strength": "u", "reason": ""}
    decisions = pl.read_parquet(tmp_path / "decisions.parquet")
    schema.validate(schema.REVIEW_DECISIONS, decisions)
    assert decisions.select(
        "decision", "label_source", "reviewer", "confidence"
    ).rows() == [("reject", "human", "analyst", 0.5)]
    # The labels CSV beside the packet is one `import-labels --partial` accepts.
    labels = entry.packet.with_suffix(".analyst.labels.csv")
    assert pl.read_csv(labels).rows() == [(entry.pair, "no_match", 0.5, "")]
    import_labels(
        packet=entry.packet,
        labels_csv=labels,
        decisions_path=tmp_path / "again.parquet",
        label_source="human",
        reviewer="analyst",
        partial=True,
    )
    assert pl.read_parquet(tmp_path / "again.parquet").height == 1


def test_the_server_refuses_a_request_for_another_host(
    served: tuple[Session, http.client.HTTPConnection],
) -> None:
    _, connection = served

    connection.request("GET", "/api/state", headers={"Host": "example.com"})

    assert connection.getresponse().status == 403


def test_a_label_the_page_does_not_offer_is_refused(
    served: tuple[Session, http.client.HTTPConnection], tmp_path: Path
) -> None:
    _, connection = served

    status, body = _call(connection, "POST", "/api/answer", {"i": 0, "label": "maybe"})

    assert status == 400
    assert "error" in body
    assert not (tmp_path / "decisions.parquet").exists()


def test_an_idle_connection_does_not_stall_the_page(
    served: tuple[Session, http.client.HTTPConnection],
) -> None:
    _, connection = served

    # A browser's speculative connection: open, and never sends a request.
    with socket.create_connection(("127.0.0.1", connection.port)):
        status, state = _call(connection, "GET", "/api/state")

    assert status == 200
    assert state["next"] == 0


def test_the_next_pair_wraps_to_an_earlier_one_left_undecided(tmp_path: Path) -> None:
    session = _session(tmp_path, share=1.0)
    last = len(session.queue) - 1

    saved = session.save(last, label="match", strength=None, reason="")

    assert saved["next"] == 0
    assert saved["state"]["done"] == 1
    for index in range(last):
        saved = session.save(index, label="match", strength=None, reason="")
    assert saved["next"] is None
    assert saved["state"]["done"] == saved["state"]["total"]


def test_a_save_leaves_the_time_of_the_other_answers_in_its_packet(
    tmp_path: Path,
) -> None:
    session = _session(tmp_path, share=1.0)
    packets = [pair.packet for pair in session.queue]
    first = next(i for i, packet in enumerate(packets) if packets.count(packet) > 1)
    second = packets.index(packets[first], first + 1)

    def decided() -> dict[str, object]:
        return dict(
            pl.read_parquet(tmp_path / "decisions.parquet")
            .select("left_id", "decided_at")
            .iter_rows()
        )

    session.save(first, label="match", strength=None, reason="")
    before = decided()
    session.save(second, label="no_match", strength=None, reason="")

    after = decided()
    left = session.queue[first].key[2]
    assert len(after) == 2
    assert after[left] == before[left]
    labels = pl.read_csv(packets[first].with_suffix(".analyst.labels.csv"))
    assert labels.height == 2


def test_a_pair_set_aside_is_not_recorded_and_comes_back_after_the_others(
    tmp_path: Path,
) -> None:
    session = _session(tmp_path, share=1.0)
    last = len(session.queue) - 1

    skipped = session.skip(0)
    assert skipped["next"] == 1
    assert skipped["state"]["skipped"] == 1
    assert not (tmp_path / "decisions.parquet").exists()
    for index in range(1, last + 1):
        saved = session.save(index, label="match", strength=None, reason="")
    # Only the pair set aside is left, so it comes back.
    assert saved["next"] == 0
    assert saved["state"]["skipped"] == 0
    assert saved["state"]["done"] == last
    assert session.save(0, label="no_match", strength=None, reason="")["next"] is None


def test_the_time_left_follows_the_pace_of_recent_actions(tmp_path: Path) -> None:
    now = [0.0]
    session = _session(tmp_path, share=1.0, clock=lambda: now[0])

    def act(seconds: float) -> dict[str, object]:
        now[0] += seconds
        return session.save(
            len(session.answers), label="match", strength=None, reason=""
        )["state"]

    states = [act(10.0) for _ in range(5)]
    # Five actions make four gaps: too few to tell.
    assert states[-1]["seconds_left"] is None
    state = act(10.0)
    assert state["seconds_left"] == 10 * (state["total"] - state["done"])
    # A long break counts as two minutes, not as the pace.
    state = act(3600.0)
    gaps = [10.0] * 5 + [120.0]
    assert state["seconds_left"] == round(
        (state["total"] - state["done"]) * sum(gaps) / len(gaps)
    )

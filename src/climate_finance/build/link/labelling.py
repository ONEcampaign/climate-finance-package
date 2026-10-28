"""A local page for labelling a person's share of the blind sample, or a review
queue packet.

`queue_pairs` picks the blind sample's share: the judgement calls, the pairs whose
link did not come from a certain rule (`schema.CERTAIN_METHODS`), in a stable hash
order of their `sample_id` until the draws they back reach `human_audit`'s
`min_share` of all the design's draws, which is how the gate reads the share; and
`SPOT_CHECKS` pairs a certain rule linked, spread over the queue, as a check on that
rule. `Session` holds the queue and a person's answers, builds each pair's fields
with `packet.pair_views` (so the page is as blind as the packet) and saves every
answer at once: it records the answer with `decisions.import_labels`, which replaces
the person's earlier answer on the pair and leaves their other answers as they are,
then rewrites the person's labels CSV beside the pair's packet, atomically.
`make_server` serves a session on 127.0.0.1, one request at a time through the
session. A pair the person cannot tell is set aside, recorded nowhere, and comes
back once the others are decided. Two `cf-build label` runs, or one beside `cf-build
import-labels`, on one decisions file can lose each other's updates.

`packet_pairs` reads a review queue packet (`review.write_queue_packet`) in place of
a sample: every pair in the packet's order, shown with its review reason. Its
answers carry no `sample_id`, so `decisions.read_decision_labels` reads them as
queue decisions. A `btr_crs` packet shows each pair through `packet.btr_pair_views`,
and the page takes its heading, guide, definition and side labels from the task's
`WORDINGS`.
"""

import hashlib
import json
import tempfile
import threading
import time
from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass, replace
from functools import cached_property
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from itertools import chain
from pathlib import Path
from typing import override
from urllib.parse import parse_qs, urlparse

import polars as pl

from climate_finance import catalogue, schema
from climate_finance.build.errors import BuildError, ReviewDecisionsError
from climate_finance.build.evaluate import design_path
from climate_finance.build.link.decisions import (
    LABEL_COLUMNS,
    LABELS,
    import_labels,
    standing_decisions,
)
from climate_finance.build.link.packet import (
    BTR_MATCH_DEFINITION,
    MATCH_DEFINITION,
    BtrPairView,
    PairView,
    btr_pair_views,
    code_names,
    packet_pairs_path,
    pair_views,
)
from climate_finance.schema import CERTAIN_METHODS

PAGE = Path(__file__).with_name("label_page.html")
# How far from 0.5 a confidence of each strength lies, on the label's side.
STRENGTHS = {"u": 0.0, "1": 0.1, "2": 0.3, "3": 0.45}
# The pairs a certain rule linked that the queue holds, as a spot check of the rule.
SPOT_CHECKS = 50
# Pairs whose views are built in one read of the units and CRS rows.
CHUNK = 50
# The pace the time left reads: the last `PACE_WINDOW` gaps between a person's
# actions, each capped at `PACE_PAUSE` seconds so a break does not slow the estimate,
# once there are `PACE_MIN` of them.
PACE_WINDOW = 20
PACE_PAUSE = 120.0
PACE_MIN = 5
KEY = ["sample_id", "task", "left_id", "right_id"]
# A pair's sample id, None for a queue pair, task, left id and right id.
type PairKey = tuple[str | None, str, str, str]
type View = PairView | BtrPairView
# What the page says for each task: its heading, the guide's two lists, the full
# rule, the cards' titles and the yes button.
WORDINGS: dict[str, dict[str, object]] = {
    "trace": {
        "heading": "Is A money spent under commitment B?",
        "yes": "Yes, same commitment",
        "yes_items": [
            "Same project and recipient, and B was committed before or with A.",
            "B may be split over several lines, renumbered, or drawn down later.",
            "A regional commitment covers a country inside that region.",
        ],
        "no_items": [
            "A different recipient country.",
            "A sibling project, or a later phase with its own new commitment, even "
            "under a reused number.",
            "A separate yearly grant spent in its own year, or an unrelated activity.",
        ],
        "definition": MATCH_DEFINITION,
        "sides": ["A: later spending record", "B: earlier commitment record"],
    },
    "btr_crs": {
        "heading": "Does CRS record B report the activity in BTR row A?",
        "yes": "Yes, same activity",
        "yes_items": [
            "Same project or programme in the same year, or one component of it.",
            "B's recipient is A's, or a region or global programme that holds it.",
            "Amounts may differ (a climate share, another exchange rate, or several "
            "records summed in one row).",
        ],
        "no_items": [
            "A different project of the same donor in the same country or sector.",
            "A sibling, or an earlier or later phase with its own record.",
            "An unrelated activity.",
        ],
        "definition": BTR_MATCH_DEFINITION,
        "sides": ["A: BTR row", "B: CRS record"],
    },
}


class LabellingError(BuildError):
    """A request to the labelling page that cannot be answered."""


@dataclass(frozen=True)
class Answer:
    """A person's decision on a pair. `label` is `match` or `no_match`, and `strength`
    is one of `STRENGTHS`, or None when the person gave no confidence."""

    label: str
    strength: str | None
    reason: str
    reviewer: str

    @property
    def confidence(self) -> float | None:
        if self.strength is None:
            return None
        step = STRENGTHS[self.strength]
        return 0.5 + step if self.label == "match" else 0.5 - step


@dataclass(frozen=True)
class QueuedPair:
    """A pair to label, with its packet, its number there, its key and the draws it
    backs. `reason` is a queue pair's `review_reason`, None for the blind sample."""

    provider: int
    packet: Path
    batch: int
    pair: int
    key: PairKey
    draws: int
    certain: bool
    reason: str | None = None


def sample_pairs(sample_dir: Path) -> list[QueuedPair]:
    """Every pair of the blind sample in `sample_dir`, with the draws it backs in
    `design.parquet`, in provider, batch and pair order.

    Raises:
        LabellingError: when `sample_dir` holds no design or no packets.
    """
    design = design_path(sample_dir)
    if not design.exists():
        raise LabellingError(
            f"{sample_dir} holds no design.parquet; run `cf-build sample`"
        )
    draws = (
        pl.read_parquet(design, columns=["sample_id", "provider_code", "link_method"])
        .group_by("sample_id")
        .agg(
            provider=pl.col("provider_code").first(),
            certain=pl.col("link_method").first().is_in(CERTAIN_METHODS),
            draws=pl.len(),
        )
    )
    parts = []
    for pairs_file in sorted(sample_dir.glob("*/batch-*.pairs.parquet")):
        packet = pairs_file.with_name(
            pairs_file.name.removesuffix(".pairs.parquet") + ".md"
        )
        parts.append(
            pl.read_parquet(pairs_file)
            .select("pair", "task", "left_id", "right_id", "sample_id")
            .with_columns(
                packet=pl.lit(str(packet)),
                batch=pl.lit(int(packet.stem.removeprefix("batch-"))),
            )
        )
    if not parts:
        raise LabellingError(f"{sample_dir} holds no batch packets")
    joined = (
        pl.concat(parts).join(draws, on="sample_id").sort("provider", "packet", "pair")
    )
    return [
        QueuedPair(
            provider=row["provider"],
            packet=Path(row["packet"]),
            batch=row["batch"],
            pair=row["pair"],
            key=(row["sample_id"], row["task"], row["left_id"], row["right_id"]),
            draws=row["draws"],
            certain=row["certain"],
        )
        for row in joined.iter_rows(named=True)
    ]


def packet_pairs(packet: Path, *, release_dir: Path) -> list[QueuedPair]:
    """Every pair of the review queue packet `packet`, in its order, each backing one
    draw and none certain. A pair's provider is its BTR row's party's, or its trace
    disbursement unit's donor, from the release in `release_dir`.

    Raises:
        LabellingError: when `packet` has no pairs file, holds blind-sample pairs,
            or names a row or unit the release lacks.
    """
    from climate_finance.build.stages import table_path

    pairs_file = packet_pairs_path(packet)
    if not pairs_file.exists():
        raise LabellingError(
            f"{packet} has no pairs file; write it with `cf-build queue`"
        )
    pairs = pl.read_parquet(pairs_file)
    if pairs["sample_id"].is_not_null().any():
        raise LabellingError(
            f"{packet} holds blind-sample pairs; label them through SAMPLE_DIR"
        )
    btr = pairs.filter(task="btr_crs").select("left_id")
    trace = pairs.filter(pl.col("task") != "btr_crs").select("left_id")
    providers = pl.concat(
        [
            schema.scan_table(table_path(release_dir, "normalise", "btr"))
            .join(btr.lazy(), left_on="row_id", right_on="left_id", how="semi")
            .select(left_id="row_id", provider="provider_code")
            .collect()
            if btr.height
            else pl.DataFrame(schema={"left_id": pl.String, "provider": pl.Int32}),
            schema.scan_table(table_path(release_dir, "link", "units"))
            .join(trace.lazy(), left_on="unit_id", right_on="left_id", how="semi")
            .select(left_id="unit_id", provider="donor_code")
            .collect()
            if trace.height
            else pl.DataFrame(schema={"left_id": pl.String, "provider": pl.Int32}),
        ],
        how="vertical_relaxed",
    )
    joined = pairs.join(providers, on="left_id", how="left").sort("pair")
    if joined["provider"].null_count():
        raise LabellingError(
            f"{joined['provider'].null_count()} pairs of {packet} name a row or unit "
            f"the release in {release_dir} lacks"
        )
    return [
        QueuedPair(
            provider=row["provider"],
            packet=packet,
            batch=1,
            pair=row["pair"],
            key=(None, row["task"], row["left_id"], row["right_id"]),
            draws=1,
            certain=False,
            reason=row["review_reason"],
        )
        for row in joined.iter_rows(named=True)
    ]


def queue_pairs(
    pairs: Iterable[QueuedPair], *, share: float, spot_checks: int = SPOT_CHECKS
) -> list[QueuedPair]:
    """The pairs to label, in the order to show them.

    The judgement calls (pairs not linked by a certain rule) come first in the
    order of a hash of each pair's `sample_id`, the same on every run, until the
    draws they back reach `share` of the draws of all `pairs`. Then `spot_checks`
    pairs a certain rule linked: each provider's pairs in hash order, taken one
    provider after another a round at a time, so the checks spread over providers.
    The spot checks go in at even spacing among the judgement calls.
    """
    pairs = list(pairs)
    target = share * sum(pair.draws for pair in pairs)
    judgement = []
    taken = 0
    for pair in _hash_order(pair for pair in pairs if not pair.certain):
        if taken >= target:
            break
        judgement.append(pair)
        taken += pair.draws
    seen: Counter[int] = Counter()
    ranked = []
    for pair in _hash_order(pair for pair in pairs if pair.certain):
        ranked.append((seen[pair.provider], pair))
        seen[pair.provider] += 1
    # A stable sort by round keeps each round in hash order.
    spot = [pair for _, pair in sorted(ranked, key=lambda entry: entry[0])]
    spot = spot[:spot_checks]
    queue = []
    placed = 0
    for count, pair in enumerate(judgement, start=1):
        queue.append(pair)
        while placed < len(spot) and count >= (placed + 1) * len(judgement) / (
            len(spot) + 1
        ):
            queue.append(spot[placed])
            placed += 1
    return [*queue, *spot[placed:]]


def _hash_order(pairs: Iterable[QueuedPair]) -> list[QueuedPair]:
    """`pairs` in the order of a hash of each `sample_id`, then of the rest of the
    key."""
    return sorted(
        pairs,
        key=lambda pair: (
            hashlib.sha256((pair.key[0] or "").encode()).hexdigest(),
            pair.key[1:],
        ),
    )


def read_answers(decisions: Path) -> dict[PairKey, Answer]:
    """The standing decision of a person on each pair at `decisions`, keyed with its
    `sample_id`, None for a queue pair, and an empty dict when the file is
    missing."""
    if not decisions.exists():
        return {}
    people = pl.read_parquet(decisions).filter(label_source="human")
    reverse = {decision: label for label, decision in LABELS.items()}
    return {
        (row["sample_id"], row["task"], row["left_id"], row["right_id"]): Answer(
            label=reverse[row["decision"]],
            strength=_strength(row["confidence"]),
            reason=row["note"] or "",
            reviewer=row["reviewer"],
        )
        for row in standing_decisions(people, key=KEY).iter_rows(named=True)
    }


def _strength(confidence: float | None) -> str | None:
    """The strength whose confidence is nearest `confidence`."""
    if confidence is None:
        return None
    step = abs(confidence - 0.5)
    return min(STRENGTHS, key=lambda name: abs(STRENGTHS[name] - step))


class Session:
    """One reviewer's labelling of a queue; see the module docstring.

    `pairs` are every pair of the packets the queue draws on, whose answers the
    labels CSVs beside the packets hold, and `queue` the pairs to show, in order,
    all of one task.

    Raises:
        LabellingError: when the queue mixes tasks or holds a task with no wording.
    """

    def __init__(
        self,
        *,
        release_dir: Path,
        pairs: list[QueuedPair],
        queue: list[QueuedPair],
        decisions: Path,
        reviewer: str,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        from climate_finance.build.fetch import CRS_FILE
        from climate_finance.build.stages import raw_dir, table_path

        tasks = {pair.key[1] for pair in pairs}
        if len(tasks) > 1 or not tasks <= set(WORDINGS):
            raise LabellingError(
                f"a session labels pairs of one task of {sorted(WORDINGS)}, not "
                f"{sorted(tasks)}"
            )
        self.task = tasks.pop() if tasks else "trace"
        self.decisions = decisions
        self.reviewer = reviewer
        self.clock = clock
        self.acted_at: list[float] = []
        self.skipped: set[PairKey] = set()
        self.provider_names = dict(
            catalogue.providers.list().select("code", "name").iter_rows()
        )
        crs = schema.scan_table(table_path(release_dir, "normalise", "crs"))
        raw_crs = raw_dir(release_dir) / CRS_FILE
        names = code_names(raw_crs) if raw_crs.exists() else None
        if self.task == "btr_crs":
            btr = schema.scan_table(table_path(release_dir, "normalise", "btr"))
            self._read_views: Callable[[pl.DataFrame], list[View]] = lambda chunk: list(
                btr_pair_views(chunk, btr=btr, crs=crs, names=names)
            )
        else:
            units = schema.scan_table(table_path(release_dir, "link", "units"))
            self._read_views = lambda chunk: list(
                pair_views(chunk, units=units, crs=crs, show_ids=True, names=names)
            )
        self.pairs = pairs
        self.queue = queue
        self.answers = read_answers(decisions)
        self._views: dict[int, list[View]] = {}
        self.mine = {
            key: answer
            for key, answer in self.answers.items()
            if answer.reviewer == reviewer
        }

    @cached_property
    def _by_packet(self) -> dict[Path, list[QueuedPair]]:
        grouped: dict[Path, list[QueuedPair]] = {}
        for pair in self.pairs:
            grouped.setdefault(pair.packet, []).append(pair)
        return grouped

    def provider_name(self, provider: int) -> str:
        """`Austria (1)`, or the bare code of a provider the catalogue lacks."""
        name = self.provider_names.get(provider)
        return f"{name} ({provider})" if name else str(provider)

    def state(self, *, after: int = -1) -> dict[str, object]:
        """The queue's progress, `next`, the index to show after `after`, and
        `seconds_left` at the person's pace.

        `next` is the first pair past `after`, wrapping to the start of the queue,
        that is neither decided nor skipped. When only skipped pairs remain they
        come back (the skips are cleared), and `next` is None only when every pair
        is decided.
        """
        done = [pair.key in self.answers for pair in self.queue]
        progress = Counter(
            (pair.provider, flag) for pair, flag in zip(self.queue, done, strict=True)
        )
        providers = [
            {
                "provider": provider,
                "name": self.provider_name(provider),
                "done": progress[provider, True],
                "total": progress[provider, True] + progress[provider, False],
            }
            for provider in sorted({pair.provider for pair in self.queue})
        ]
        order = list(chain(range(after + 1, len(done)), range(after + 1)))
        if all(done[i] or self.queue[i].key in self.skipped for i in order):
            self.skipped.clear()
        return {
            "page": WORDINGS[self.task],
            "definition": WORDINGS[self.task]["definition"],
            "done": sum(done),
            "skipped": len(self.skipped),
            "total": len(done),
            "providers": providers,
            "seconds_left": self._seconds_left(remaining=len(done) - sum(done)),
            "next": next(
                (
                    i
                    for i in order
                    if not done[i] and self.queue[i].key not in self.skipped
                ),
                None,
            ),
        }

    def _seconds_left(self, *, remaining: int) -> int | None:
        """The seconds `remaining` pairs take at the pace of the person's recent
        actions, None until there are enough to tell."""
        gaps = [
            min(later - earlier, PACE_PAUSE)
            for earlier, later in zip(self.acted_at, self.acted_at[1:], strict=False)
        ][-PACE_WINDOW:]
        if len(gaps) < PACE_MIN:
            return None
        return round(remaining * sum(gaps) / len(gaps))

    def _view(self, index: int) -> View:
        """The view of queue entry `index`. Views come `CHUNK` at a time, since
        each read of the units and CRS rows costs about as much for fifty pairs as
        for one. The last few chunks stay for stepping back."""
        chunk = index // CHUNK
        if chunk not in self._views:
            entries = self.queue[chunk * CHUNK : (chunk + 1) * CHUNK]
            views = self._read_views(
                pl.DataFrame(
                    {
                        "pair": range(chunk * CHUNK, chunk * CHUNK + len(entries)),
                        "left_id": [entry.key[2] for entry in entries],
                        "right_id": [entry.key[3] for entry in entries],
                    },
                    schema={
                        "pair": pl.Int32,
                        "left_id": pl.String,
                        "right_id": pl.String,
                    },
                )
            )
            self._views = {**dict(list(self._views.items())[-2:]), chunk: views}
        return self._views[chunk][index - chunk * CHUNK]

    def pair(self, index: int) -> dict[str, object]:
        """The page's content for queue entry `index`."""
        entry = self._entry(index)
        view = replace(self._view(index), pair=entry.pair)
        answer = self.answers.get(entry.key)
        return {
            "i": index,
            "provider": entry.provider,
            "provider_name": self.provider_name(entry.provider),
            "kind": entry.reason
            or ("spot check" if entry.certain else "judgement call"),
            "batch": entry.batch,
            "view": asdict(view),
            "answer": None
            if answer is None
            else {
                "label": answer.label,
                "strength": answer.strength,
                "reason": answer.reason,
            },
        }

    def save(
        self, index: int, *, label: str, strength: str | None, reason: str
    ) -> dict[str, object]:
        """Record the reviewer's answer on queue entry `index` and return the
        state with the next undecided index.

        Raises:
            LabellingError: when the label or strength is not one the page offers.
        """
        entry = self._entry(index)
        if label not in LABELS or (strength is not None and strength not in STRENGTHS):
            raise LabellingError(f"unknown label {label!r} or strength {strength!r}")
        answer = Answer(label, strength, reason.strip(), self.reviewer)
        self._import(entry, answer)
        self.mine[entry.key] = answer
        self.answers[entry.key] = answer
        self.skipped.discard(entry.key)
        self.acted_at.append(self.clock())
        self._write_labels(entry.packet)
        state = self.state(after=index)
        return {"state": state, "next": state["next"]}

    def skip(self, index: int) -> dict[str, object]:
        """Set queue entry `index` aside as one the person cannot tell, and return
        the state with the next pair. Nothing is recorded: no label is true to a
        pair nobody can tell, and the pair comes back after the others."""
        entry = self._entry(index)
        self.skipped.add(entry.key)
        self.acted_at.append(self.clock())
        state = self.state(after=index)
        return {"state": state, "next": state["next"]}

    def _entry(self, index: int) -> QueuedPair:
        if not 0 <= index < len(self.queue):
            raise LabellingError(f"no pair {index} in a queue of {len(self.queue)}")
        return self.queue[index]

    def _import(self, entry: QueuedPair, answer: Answer) -> None:
        """Record `answer` on `entry` alone, so the reviewer's other answers keep the
        time they were given."""
        with tempfile.TemporaryDirectory() as scratch:
            labels = Path(scratch) / "answer.csv"
            _write_csv(labels, [(entry.pair, answer)])
            import_labels(
                packet=entry.packet,
                labels_csv=labels,
                decisions_path=self.decisions,
                label_source="human",
                reviewer=self.reviewer,
                partial=True,
            )

    def _write_labels(self, packet: Path) -> None:
        """Rewrite the reviewer's labels CSV beside `packet` from `mine`,
        atomically."""
        slug = "".join(c if c.isalnum() or c in "-_" else "_" for c in self.reviewer)
        labels = packet.with_suffix(f".{slug}.labels.csv")
        staging = labels.with_name(f".{labels.name}.tmp")
        _write_csv(
            staging,
            [
                (pair.pair, answer)
                for pair in self._by_packet[packet]
                if (answer := self.mine.get(pair.key))
            ],
        )
        staging.replace(labels)


def _write_csv(path: Path, rows: list[tuple[int, Answer]]) -> None:
    """Write `rows`, each a pair number and its answer, as a labels CSV."""
    pl.DataFrame(
        [(pair, a.label, a.confidence, a.reason) for pair, a in rows],
        schema=list(LABEL_COLUMNS),
        orient="row",
    ).write_csv(path)


def make_server(session: Session, *, port: int = 0) -> ThreadingHTTPServer:
    """A server for `session` on 127.0.0.1, at `port` or any free one.

    Each connection has its own thread, so a browser's idle speculative connection
    blocks nothing, and a socket that sends nothing for `Handler.timeout` seconds is
    dropped. One lock serialises every call into `session`, whose caches and saves
    do not tolerate overlap.
    """

    hosts: set[str] = set()
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        timeout = 5

        @override
        def log_message(self, format: str, *args: object) -> None:
            """Keep the terminal free of request logs."""

        def _send(self, status: int, body: bytes, content_type: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _json(self, payload: object, status: int = 200) -> None:
            self._send(status, json.dumps(payload).encode(), "application/json")

        def _local(self) -> bool:
            """Whether the request names this server, which refuses other hosts so a
            web page cannot reach it through the user's browser."""
            return self.headers.get("Host", "") in hosts

        def _reply(self, handle: Callable[[], object]) -> None:
            try:
                with lock:
                    payload = handle()
                self._json(payload)
            except (
                LabellingError,
                ReviewDecisionsError,
                KeyError,
                ValueError,
            ) as error:
                self._json({"error": str(error)}, 400)

        def do_GET(self) -> None:
            if not self._local():
                self._json({"error": "unknown host"}, 403)
                return
            url = urlparse(self.path)
            query = parse_qs(url.query)
            match url.path:
                case "/":
                    self._send(200, PAGE.read_bytes(), "text/html; charset=utf-8")
                case "/api/state":
                    self._reply(lambda: session.state())
                case "/api/pair":
                    self._reply(lambda: session.pair(int(query["i"][0])))
                case _:
                    self._json({"error": "not found"}, 404)

        def do_POST(self) -> None:
            if (
                not self._local()
                or self.headers.get("Content-Type") != "application/json"
            ):
                self._json({"error": "refused"}, 403)
                return
            body = json.loads(
                self.rfile.read(int(self.headers.get("Content-Length", 0)))
            )
            match self.path:
                case "/api/answer":
                    self._reply(
                        lambda: session.save(
                            int(body["i"]),
                            label=body["label"],
                            strength=body.get("strength"),
                            reason=body.get("reason", ""),
                        )
                    )
                case "/api/skip":
                    self._reply(lambda: session.skip(int(body["i"])))
                case _:
                    self._json({"error": "not found"}, 404)

    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    hosts.update({f"127.0.0.1:{server.server_port}", f"localhost:{server.server_port}"})
    return server

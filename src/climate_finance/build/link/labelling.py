"""A local page for labelling a person's share of the blind sample.

`queue_pairs` picks the share: per provider, the pairs in a stable hash order of
their `sample_id` until the draws they back reach `human_audit`'s `min_share` of the
provider's draws. `Session` holds the queue and a person's answers, builds each
pair's fields with `packet.pair_views` (so the page is as blind as the packet) and
saves every answer at once: it records the answer with `decisions.import_labels`,
which replaces the person's earlier answer on the pair and leaves their other
answers as they are, then rewrites the person's labels CSV beside the pair's packet,
atomically. `make_server` serves a session on 127.0.0.1, one request at a time
through the session. Two `cf-build label` runs, or one beside `cf-build
import-labels`, on one decisions file can lose each other's updates.
"""

import hashlib
import json
import tempfile
import threading
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

from climate_finance import schema
from climate_finance.build.errors import BuildError, ReviewDecisionsError
from climate_finance.build.evaluate import design_path
from climate_finance.build.link.decisions import (
    LABEL_COLUMNS,
    LABELS,
    import_labels,
    standing_decisions,
)
from climate_finance.build.link.packet import (
    MATCH_DEFINITION,
    PairView,
    code_names,
    pair_views,
)

PAGE = Path(__file__).with_name("label_page.html")
# How far from 0.5 a confidence of each strength lies, on the label's side.
STRENGTHS = {"u": 0.0, "1": 0.1, "2": 0.3, "3": 0.45}
# Pairs whose views are built in one read of the units and CRS rows.
CHUNK = 50
KEY = ["sample_id", "task", "left_id", "right_id"]
type PairKey = tuple[str, str, str, str]


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
    """A pair of the blind sample, with its packet, its number there, its key and
    the draws it backs."""

    provider: int
    packet: Path
    batch: int
    pair: int
    key: PairKey
    draws: int


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
        pl.read_parquet(design, columns=["sample_id", "provider_code"])
        .group_by("sample_id")
        .agg(provider=pl.col("provider_code").first(), draws=pl.len())
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
        )
        for row in joined.iter_rows(named=True)
    ]


def queue_pairs(pairs: Iterable[QueuedPair], *, share: float) -> list[QueuedPair]:
    """The share of `pairs` to label: per provider, in the order of a hash of each
    pair's `sample_id` (the same on every run), the first pairs whose draws reach
    `share` of the provider's draws. Providers follow in code order."""
    by_provider: dict[int, list[QueuedPair]] = {}
    for pair in pairs:
        by_provider.setdefault(pair.provider, []).append(pair)
    queue = []
    for provider in sorted(by_provider):
        ordered = sorted(
            by_provider[provider],
            key=lambda pair: (
                hashlib.sha256(pair.key[0].encode()).hexdigest(),
                pair.key,
            ),
        )
        target = share * sum(pair.draws for pair in ordered)
        taken = 0
        for pair in ordered:
            if taken >= target:
                break
            queue.append(pair)
            taken += pair.draws
    return queue


def read_answers(decisions: Path) -> dict[PairKey, Answer]:
    """The standing decision of a person on each blind-sample pair at `decisions`,
    an empty dict when the file is missing."""
    if not decisions.exists():
        return {}
    people = pl.read_parquet(decisions).filter(
        pl.col("sample_id").is_not_null(), label_source="human"
    )
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
    """One reviewer's labelling of a queue; see the module docstring."""

    def __init__(
        self,
        *,
        release_dir: Path,
        sample_dir: Path,
        decisions: Path,
        reviewer: str,
        share: float,
    ) -> None:
        from climate_finance.build.fetch import CRS_FILE
        from climate_finance.build.stages import raw_dir, table_path

        self.decisions = decisions
        self.reviewer = reviewer
        self.units = schema.scan_table(table_path(release_dir, "link", "units"))
        self.crs = schema.scan_table(table_path(release_dir, "normalise", "crs"))
        raw_crs = raw_dir(release_dir) / CRS_FILE
        self.names = code_names(raw_crs) if raw_crs.exists() else None
        self.pairs = sample_pairs(sample_dir)
        self.queue = queue_pairs(self.pairs, share=share)
        self.answers = read_answers(decisions)
        self._views: dict[int, list[PairView]] = {}
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

    def state(self, *, after: int = -1) -> dict[str, object]:
        """The queue's progress, and `next`, the first undecided index past `after`,
        wrapping to the start of the queue (None when every pair is decided)."""
        done = [pair.key in self.answers for pair in self.queue]
        progress = Counter(
            (pair.provider, flag) for pair, flag in zip(self.queue, done, strict=True)
        )
        providers = [
            {
                "provider": provider,
                "done": progress[provider, True],
                "total": progress[provider, True] + progress[provider, False],
            }
            for provider in sorted({pair.provider for pair in self.queue})
        ]
        order = chain(range(after + 1, len(done)), range(after + 1))
        return {
            "definition": MATCH_DEFINITION,
            "done": sum(done),
            "total": len(done),
            "providers": providers,
            "next": next((i for i in order if not done[i]), None),
        }

    def _view(self, index: int) -> PairView:
        """The view of queue entry `index`. Views come `CHUNK` at a time, since
        each read of the units and CRS rows costs about as much for fifty pairs as
        for one. The last few chunks stay for stepping back."""
        chunk = index // CHUNK
        if chunk not in self._views:
            entries = self.queue[chunk * CHUNK : (chunk + 1) * CHUNK]
            views = pair_views(
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
                ),
                units=self.units,
                crs=self.crs,
                show_ids=True,
                names=self.names,
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
        self._write_labels(entry.packet)
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
            if self.path != "/api/answer":
                self._json({"error": "not found"}, 404)
                return
            body = json.loads(
                self.rfile.read(int(self.headers.get("Content-Length", 0)))
            )
            self._reply(
                lambda: session.save(
                    int(body["i"]),
                    label=body["label"],
                    strength=body.get("strength"),
                    reason=body.get("reason", ""),
                )
            )

    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    hosts.update({f"127.0.0.1:{server.server_port}", f"localhost:{server.server_port}"})
    return server

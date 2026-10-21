import json
from collections.abc import Callable
from pathlib import Path

import httpx
import pytest

from climate_finance.build.errors import CostCapExceededError
from climate_finance.build.jev_client import (
    MAX_RETRIES,
    JevCacheMissError,
    JevClient,
    JevError,
    PrototypeCache,
    canonical_hash,
    canonical_request,
    import_prototype_cache,
)

FIXTURES = Path(__file__).parent / "fixtures" / "jev"
MODEL = "jev-1.14.0"
PAIR_QUESTIONS = {
    "relation": {
        "type": "score",
        "instructions": "How do they relate?",
        "criteria": ["0", "1", "2"],
    },
    "disbursement": {"type": "noul", "instructions": "Is the later a disbursement?"},
    "sibling": {"type": "noul", "instructions": "Is the later a sibling?"},
}
PROJECT_QUESTIONS = {
    "strict": {"type": "noul", "instructions": "Strictly adaptation?"},
    "lenient": {"type": "noul", "instructions": "Any adaptation?"},
    "level": {
        "type": "choice",
        "instructions": "How strongly?",
        "criteria": {"principal": "P", "significant": "S", "not_targeted": "N"},
    },
}
PROJECTS = {3: {"project": "Drought-resilient irrigation."}, 13: {"project": "Roads."}}


def _answer(model: str = MODEL) -> dict[str, object]:
    return {
        "model": model,
        "answers": {"sibling": {"type": "noul", "noul": 0.5}},
        "usage": {"input_tokens": 1000, "output_tokens": 10},
    }


def _transport(
    reply: Callable[[httpx.Request], httpx.Response],
) -> tuple[httpx.MockTransport, list[httpx.Request]]:
    sent: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        return reply(request)

    return httpx.MockTransport(handle), sent


def _client(cache_dir: Path, transport: httpx.MockTransport, **kwargs) -> JevClient:
    options = {"cost_cap_usd": 10.0, "offline": False, "max_workers": 1} | kwargs
    return JevClient(model=MODEL, cache_dir=cache_dir, transport=transport, **options)


def _request(text: str) -> dict[str, object]:
    return {"state": {"text": text}, "questions": PAIR_QUESTIONS}


@pytest.fixture(autouse=True)
def _api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TYPESAFE_API_KEY", "test-key")


@pytest.mark.parametrize(
    ("left", "right", "same"),
    [
        ({"a": 1, "b": {"c": 2, "d": 3}}, {"b": {"d": 3, "c": 2}, "a": 1}, True),
        ({"share": 1.0}, {"share": 1}, True),
        ({"share": 0.1 + 0.2}, {"share": 0.30000000000000004}, True),
        ({"levels": ["low", "high"]}, {"levels": ["low", "high"]}, True),
        ({"levels": ("low", "high")}, {"levels": ["low", "high"]}, True),
        ({"levels": ["low", "high"]}, {"levels": ["high", "low"]}, False),
        ({"model": "jev-1.14.0"}, {"model": "jev-1.13.0"}, False),
        ({"share": 0.5}, {"share": "0.5"}, False),
    ],
)
def test_canonical_hash_ignores_dict_order_and_float_form(
    left: dict[str, object], right: dict[str, object], same: bool
) -> None:
    assert (canonical_hash(left) == canonical_hash(right)) is same


@pytest.mark.parametrize("value", [float("nan"), float("inf"), object()])
def test_canonical_request_refuses_what_the_api_cannot_read(value: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        canonical_request({"state": value})


def _import(cache_dir: Path):
    states = json.loads((FIXTURES / "pair_states.json").read_text())
    return import_prototype_cache(
        [
            PrototypeCache(
                responses=FIXTURES / "pairs.jsonl",
                key="state_hash",
                states=states,
                questions=PAIR_QUESTIONS,
            ),
            PrototypeCache(
                responses=FIXTURES / "projects.jsonl",
                key="rid",
                states=PROJECTS,
                questions=PROJECT_QUESTIONS,
            ),
        ],
        cache_dir=cache_dir,
    )


def test_import_rekeys_and_collapses_reordered_options(tmp_path: Path) -> None:
    counts = _import(tmp_path)

    # Project 3 was asked twice with its options in two orders: one entry.
    assert (counts.read, counts.written, counts.collapsed) == (7, 6, 1)
    assert counts.unmatched == 0
    assert _import(tmp_path).written == 0


def test_import_counts_records_without_a_state(tmp_path: Path) -> None:
    counts = import_prototype_cache(
        [
            PrototypeCache(
                responses=FIXTURES / "projects.jsonl",
                key="rid",
                states={13: PROJECTS[13]},
                questions=PROJECT_QUESTIONS,
            )
        ],
        cache_dir=tmp_path,
    )

    assert (counts.written, counts.unmatched) == (1, 2)


@pytest.mark.parametrize("offline", [False, True])
def test_imported_batch_replays_without_a_live_call(
    tmp_path: Path, offline: bool
) -> None:
    _import(tmp_path)
    states = json.loads((FIXTURES / "pair_states.json").read_text())
    records = [
        json.loads(line) for line in (FIXTURES / "pairs.jsonl").read_text().splitlines()
    ]
    transport, sent = _transport(lambda _: httpx.Response(500))
    client = _client(tmp_path, transport, offline=offline)

    # Keys reordered, as a caller building the same state would.
    batch = [
        {
            "questions": PAIR_QUESTIONS,
            "state": dict(reversed(states[r["state_hash"]].items())),
        }
        for r in records
    ]
    responses = client.score(batch)

    assert responses == [r["response"] for r in records]
    assert sent == []
    assert client.live_calls == 0


def test_offline_refuses_a_miss_without_calling(tmp_path: Path) -> None:
    transport, sent = _transport(lambda _: httpx.Response(200, json=_answer()))
    client = _client(tmp_path, transport, offline=True)

    with pytest.raises(JevCacheMissError) as raised:
        client.score([_request("new")])

    assert raised.value.misses == 1
    assert sent == []


def test_cap_refuses_an_over_budget_batch_before_any_call(tmp_path: Path) -> None:
    transport, sent = _transport(lambda _: httpx.Response(200, json=_answer()))
    client = _client(tmp_path, transport, cost_cap_usd=1e-4)

    # Each request estimates about USD 4e-5: one fits, three more do not.
    client.score([_request("x" * 1500)])
    with pytest.raises(CostCapExceededError):
        client.score([_request(f"{i}" * 1500) for i in range(1, 4)])

    assert len(sent) == 1
    assert client.spent_usd == pytest.approx(1000 * 0.042e-6)


def test_live_answer_is_cached_and_replayed_by_the_next_build(tmp_path: Path) -> None:
    transport, sent = _transport(lambda _: httpx.Response(200, json=_answer()))
    request = _request("new")

    first = _client(tmp_path, transport).score([request, request])
    second = _client(tmp_path, transport, offline=True).score([request])

    assert first == second * 2 == [_answer()] * 2
    assert len(sent) == 1
    assert sent[0].content.decode() == canonical_request({**request, "model": MODEL})
    assert sent[0].headers["authorization"] == "Bearer test-key"


def test_rate_limit_backs_off_then_succeeds(tmp_path: Path) -> None:
    replies = iter(
        [
            httpx.Response(429, headers={"retry-after": "7"}),
            httpx.Response(529),
            httpx.Response(200, json=_answer()),
        ]
    )
    transport, sent = _transport(lambda _: next(replies))
    sleeps: list[float] = []

    responses = _client(tmp_path, transport, sleep=sleeps.append).score([_request("a")])

    assert responses == [_answer()]
    assert sleeps == [7.0, 2.0]
    assert len(sent) == 3


def test_rate_limit_gives_up_after_the_retries(tmp_path: Path) -> None:
    transport, sent = _transport(lambda _: httpx.Response(429))

    with pytest.raises(httpx.HTTPStatusError):
        _client(tmp_path, transport, sleep=lambda _: None).score([_request("a")])

    assert len(sent) == MAX_RETRIES + 1


def test_failed_batch_resumes_from_the_cache(tmp_path: Path) -> None:
    def refuse_c(request: httpx.Request) -> httpx.Response:
        if json.loads(request.content)["state"]["text"] == "c":
            return httpx.Response(429)
        return httpx.Response(200, json=_answer())

    batch = [_request(text) for text in "abc"]
    transport, _ = _transport(refuse_c)
    with pytest.raises(httpx.HTTPStatusError):
        _client(tmp_path, transport, sleep=lambda _: None).score(batch)

    transport, sent = _transport(lambda _: httpx.Response(200, json=_answer()))
    client = _client(tmp_path, transport)

    assert client.score(batch) == [_answer()] * 3
    assert [json.loads(r.content)["state"]["text"] for r in sent] == ["c"]


def test_answer_from_another_model_is_refused(tmp_path: Path) -> None:
    transport, _ = _transport(lambda _: httpx.Response(200, json=_answer("jev-1.13.0")))
    client = _client(tmp_path, transport)

    with pytest.raises(JevError):
        client.score([_request("a")])
    assert client.live_calls == 0


def test_request_cannot_name_its_own_model(tmp_path: Path) -> None:
    transport, sent = _transport(lambda _: httpx.Response(200, json=_answer()))

    with pytest.raises(ValueError, match="pinned model"):
        _client(tmp_path, transport).score([{**_request("a"), "model": "jev-1.13.0"}])
    assert sent == []

"""The flag stage and the `jev_flags` table on hand-built CRS records.

Jev's answers come from a stub that records each request, or replay offline from a
cache the tests write.
"""

import json
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from normalised import crs_records
from stage_runs import run_stage, stage_context, write_inputs, year_rows

from climate_finance import schema
from climate_finance.build.errors import ConfigError
from climate_finance.build.jev_client import canonical_hash, canonical_request
from climate_finance.build.jev_flags import (
    FLAG_QUESTIONS,
    MIN_TEXT_CHARS,
    flag_records,
    flag_request,
    jev_flags,
)
from climate_finance.build.label import ask_jev
from climate_finance.build.provenance import Provenance
from climate_finance.build.stages import REGISTRY, Stage, StageContext, table_path

MODEL = "jev-1.14.0"
# A text no CRS record holds, and the canonical hash of the request the scan that
# measured the cuts sends for it, computed with the scan's own request builder. A
# match means the scan's cached responses answer the flag stage's requests.
SYNTHETIC_TEXT = (
    "Solar water pumps for smallholder irrigation in the drylands of a fictional region"
)
SCAN_HASH = "374a9ab9d175069de0c8747d81fa2c9da4f45db0c81de4a34139c6e081171fa8"
# The hash of `ask_jev`'s request for "Flood defences", which the label stage's
# cached adaptation answers are keyed on.
ASK_JEV_HASH = "626694b0bfaca31be922829793ef69322ee06b09d14732452d7aabeb9afa9204"

LONG = "Rehabilitation of irrigation canals to cope with recurrent drought in the north"
OTHER = "Construction of a four-star hotel and conference centre in the capital city"
SHORT = "Hotel"


def _response(answers: Mapping[str, float]) -> dict[str, object]:
    return {
        "model": MODEL,
        "answers": {
            name: {"type": "noul", "noul": answers[name]} for name in FLAG_QUESTIONS
        },
        "usage": {"input_tokens": 900, "output_tokens": 40},
    }


def _answers(
    mitigation: float = 0.1,
    adaptation: float = 0.9,
    counterproductive: float = 0.05,
    surprising: float = 0.1,
) -> dict[str, float]:
    return {
        "mitigation_aim": mitigation,
        "adaptation_aim": adaptation,
        "counterproductive": counterproductive,
        "surprising": surprising,
    }


class RecordingClient:
    """A Jev client that answers each request with `respond` and keeps them all."""

    model = MODEL

    def __init__(self, respond: Callable[[Mapping[str, Any]], dict[str, object]]):
        self.respond = respond
        self.requests: list[Mapping[str, Any]] = []

    def score(self, requests: Sequence[Mapping[str, Any]]) -> list[dict[str, object]]:
        self.requests.extend(requests)
        return [self.respond(request) for request in requests]


def _flag_client(answers: Mapping[str, Mapping[str, float]]) -> Any:
    """A `RecordingClient` that answers `answers` by text."""
    return RecordingClient(
        lambda request: _response(answers[request["state"]["aid_activity_description"]])
    )


def _records(*records: dict[str, object]) -> pl.DataFrame:
    defaults = {"year": 2022, "commitment_usd": 1.0, "rio_mitigation": 1}
    crs = crs_records(*(defaults | record for record in records))
    return flag_records(crs.lazy(), first_year=2015, last_year=2024).collect()


def test_a_flag_request_is_byte_identical_to_the_scans() -> None:
    request = {**flag_request(SYNTHETIC_TEXT), "model": MODEL}

    assert canonical_hash(request) == SCAN_HASH


def test_the_adaptation_label_request_is_unchanged() -> None:
    answers = {
        "lenient": {"noul": 0.9},
        "level": {"probabilities": {"significant": 0.3, "principal": 0.6}},
    }
    client: Any = RecordingClient(lambda _: {"answers": answers})

    ask_jev(pl.DataFrame({"id": [1], "text": ["Flood defences"]}), client=client)

    (request,) = client.requests
    assert canonical_hash({**request, "model": MODEL}) == ASK_JEV_HASH


def test_each_long_text_is_asked_once_and_short_texts_not_at_all() -> None:
    assert len(SHORT) < MIN_TEXT_CHARS <= min(len(LONG), len(OTHER))
    records = _records(
        {"record_id": "a", "title": LONG},
        # The same text from the title and a description.
        {
            "record_id": "b",
            "title": "Rehabilitation",
            "long_description": LONG.removeprefix("Rehabilitation "),
        },
        {"record_id": "c", "title": OTHER},
        {"record_id": "d", "title": SHORT},
        {"record_id": "e"},
    )
    client = _flag_client({LONG: _answers(), OTHER: _answers(surprising=0.9)})

    flags = jev_flags(records, client=client)

    schema.validate(schema.JEV_FLAGS, flags)
    assert [request["state"] for request in client.requests] == [
        {"aid_activity_description": text} for text in sorted([LONG, OTHER])
    ]
    assert flags.sort("record_id").select(
        "record_id", "short_text", "surprising_p", "surprising"
    ).rows() == [
        ("a", False, 0.1, False),
        ("b", False, 0.1, False),
        ("c", False, 0.9, True),
        ("d", True, None, None),
        ("e", True, None, None),
    ]
    assert schema._jev_flag_problems(flags) == []


@pytest.mark.parametrize(
    ("mitigation", "adaptation", "surprising", "no_aim", "surprising_flag"),
    [
        (0.3, 0.1, 0.8, True, True),
        (0.1, 0.31, 0.79, False, False),
        (0.95, 0.0, 0.5, False, False),
    ],
)
def test_the_flags_hold_at_their_cuts(
    mitigation: float,
    adaptation: float,
    surprising: float,
    no_aim: bool,
    surprising_flag: bool,
) -> None:
    client = _flag_client(
        {LONG: _answers(mitigation, adaptation, surprising=surprising)}
    )

    flags = jev_flags(_records({"record_id": "a", "title": LONG}), client=client)

    row = flags.row(0, named=True)
    assert row["no_aim_p"] == pytest.approx(1 - max(mitigation, adaptation))
    assert (row["no_aim"], row["surprising"]) == (no_aim, surprising_flag)
    assert row["mitigation_aim_p"] == mitigation
    assert schema._jev_flag_problems(flags) == []


@pytest.mark.parametrize(
    ("edit", "problem"),
    [
        ({"no_aim": False}, "no_aim differs"),
        ({"counterproductive_p": 1.2}, "an answer above 1"),
        ({"short_text": True}, "null other than where short_text holds"),
    ],
)
def test_the_invariant_names_a_flag_that_breaks_its_rule(
    edit: dict[str, object], problem: str
) -> None:
    client = _flag_client({LONG: _answers(0.1, 0.2, surprising=0.5)})
    flags = jev_flags(_records({"record_id": "a", "title": LONG}), client=client)

    broken = flags.with_columns(
        pl.lit(value, flags.schema[name]).alias(name) for name, value in edit.items()
    )

    (found,) = schema._jev_flag_problems(broken)
    assert found.startswith("jev_flags a: ") and problem in found


def test_the_population_is_marked_records_with_money_in_the_flagged_years() -> None:
    crs = crs_records(
        {"record_id": "in", "year": 2015, "rio_mitigation": 0, "rio_adaptation": 2,
         "disbursement_usd": 1.0},
        {"record_id": "early", "year": 2014, "rio_mitigation": 2,
         "commitment_usd": 1.0},
        {"record_id": "late", "year": 2024, "rio_mitigation": 2,
         "commitment_usd": 1.0},
        {"record_id": "unmarked", "year": 2020, "rio_mitigation": 0,
         "commitment_usd": 1.0},
        {"record_id": "no_money", "year": 2020, "rio_mitigation": 1,
         "commitment_usd": 0.0, "disbursement_usd": None},
    )  # fmt: skip

    records = flag_records(crs.lazy(), first_year=2015, last_year=2023).collect()

    assert records["record_id"].to_list() == ["in"]


def _write_cache(cache_dir: Path, answers: Mapping[str, Mapping[str, float]]) -> None:
    """Cache Jev's `answers` under the requests the flag stage sends."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    with (cache_dir / f"{MODEL}.jsonl").open("w", encoding="utf-8") as lines:
        for text, answer in answers.items():
            request = json.loads(
                canonical_request({**flag_request(text), "model": MODEL})
            )
            lines.write(json.dumps({"request": request, "response": _response(answer)}))
            lines.write("\n")


def _flag_context(
    tmp_path: Path, *, crs_years: tuple[int, int], **switches: object
) -> StageContext:
    """A flag stage context whose CRS holds a record of the last complete year,
    2023, and an early report of 2024, with the Jev cache under `tmp_path`."""
    release_dir = tmp_path / "r"
    crs = crs_records(
        {"record_id": "complete", "year": 2023, "title": LONG, "commitment_usd": 1.0},
        {"record_id": "early_report", "year": 2024, "title": OTHER,
         "commitment_usd": 1.0},
    )  # fmt: skip
    write_inputs(release_dir, "fetch", {"raw_manifest": year_rows(crs=crs_years)})
    write_inputs(release_dir, "normalise", {"crs": crs})
    config = stage_context(release_dir).config
    config = config.model_copy(
        update={
            "jev": config.jev.model_copy(update={"cache_dir": tmp_path / "jev"}),
            "years": config.years.model_copy(update={"crs_last_complete": 2023}),
            **switches,
        }
    )
    return stage_context(release_dir, config=config)


def test_the_stage_flags_up_to_the_last_complete_year_from_the_cache(
    tmp_path: Path,
) -> None:
    _write_cache(tmp_path / "jev", {LONG: _answers()})
    context = _flag_context(tmp_path, crs_years=(2010, 2025), jev_flags=True)

    run_stage(Stage.FLAG, context)

    flags = pl.read_parquet(table_path(context.release_dir, "flag", "jev_flags"))
    assert flags.select("record_id", "model").rows() == [("complete", MODEL)]
    assert context.jev.live_calls == 0


def test_the_stage_refuses_a_crs_that_starts_after_the_first_flagged_year(
    tmp_path: Path,
) -> None:
    context = _flag_context(tmp_path, crs_years=(2016, 2025), jev_flags=True)

    with pytest.raises(ConfigError, match="jev_flags_first_year 2015"):
        run_stage(Stage.FLAG, context)


def test_with_the_switch_off_the_stage_writes_no_table(tmp_path: Path) -> None:
    context = _flag_context(tmp_path, crs_years=(2016, 2025))

    assert run_stage(Stage.FLAG, context) == {}
    assert not table_path(context.release_dir, "flag", "jev_flags").exists()


def test_the_switch_leaves_every_other_stage_identity_unchanged(
    tmp_path: Path,
) -> None:
    config = stage_context(tmp_path).config

    def record(stage: str, **switches: object) -> object:
        entry = next(entry for entry in REGISTRY if entry.name == stage)
        return Provenance(
            stage=stage,
            release_dir=tmp_path,
            config=config.model_copy(update=switches),
            settings=entry.settings,
        ).record({})

    changed = {
        entry.name
        for entry in REGISTRY
        if record(entry.name) != record(entry.name, jev_flags=True)
    }
    assert changed == {Stage.FLAG, Stage.ASSEMBLE}

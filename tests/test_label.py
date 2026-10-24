"""Marker inheritance, reported objective labels and Jev labels on hand-built rows.

Jev's answers replay from a cache written by the tests, offline.
"""

import json
from pathlib import Path

import polars as pl
import pytest
from normalised import crdf_rows, crs_records
from stage_runs import BUILD_TOML, run_stage, stage_context, write_inputs

from climate_finance import schema
from climate_finance.build.config import BuildConfig, load_config
from climate_finance.build.fetch import ADAPTATION_LABELS_FILE
from climate_finance.build.jev_client import JevClient, canonical_request
from climate_finance.build.label import (
    ADAPTATION_QUESTIONS,
    NO_TEXT,
    WEIKMANS_LEVEL,
    ask_jev,
    fit_adaptation_threshold,
    inherit_markers,
    jev_objectives,
    jev_records,
    read_adaptation_labels,
    reported_objectives,
)
from climate_finance.build.stages import StageContext, table_path

MODEL = "jev-1.14.0"


def _commitments() -> pl.DataFrame:
    return crdf_rows(
        # A bank's climate components: a climate share and no markers.
        {"row_id": "bank", "year": 2021, "provider_code": 905, "bi_multi": 4,
         "rio_adaptation": None, "rio_mitigation": None, "objective": "adaptation",
         "face_usd": 10.0, "climate_usd": 6.0, "adaptation_usd": 6.0,
         "mitigation_usd": 0.0, "climate_share": 0.6},
        {"row_id": "bank2", "year": 2022, "provider_code": 905, "bi_multi": 4,
         "rio_adaptation": None, "rio_mitigation": None, "objective": "adaptation",
         "face_usd": 10.0, "climate_usd": 6.0, "adaptation_usd": 6.0,
         "mitigation_usd": 0.0, "climate_share": 0.6},
        {"row_id": "marked", "year": 2021, "rio_adaptation": 1, "rio_mitigation": 2},
    )  # fmt: skip


def _records() -> pl.DataFrame:
    return crs_records(
        {"record_id": "r_bank", "year": 2022, "rio_adaptation": None,
         "rio_mitigation": None},
        {"record_id": "r_same", "year": 2022, "rio_adaptation": 1, "rio_mitigation": 2,
         "title": "Flood", "long_description": "defences "},
        {"record_id": "r_other", "year": 2022, "rio_adaptation": 0,
         "rio_mitigation": 2, "title": "Solar mini-grids"},
        {"record_id": "r_blank", "year": 2022, "rio_adaptation": None,
         "rio_mitigation": None},
        # Its one allocation is excess, so it is untraced.
        {"record_id": "r_excess", "year": 2022, "rio_adaptation": 2,
         "rio_mitigation": 2, "disbursement_usd": 1.0,
         "title": "Drought-tolerant seed"},
        # Linked to nothing, so untraced. The GEF's is untraced too unless the build
        # routes the GEF through its IATI files, which then count it.
        {"record_id": "r_unlinked", "year": 2022, "rio_adaptation": 1,
         "disbursement_usd": 1.0, "title": "Seawalls against sea level rise"},
        {"record_id": "r_fund", "year": 2022, "donor_code": 811, "rio_adaptation": 1,
         "disbursement_usd": 1.0, "title": "School meals"},
    )  # fmt: skip


def _allocations() -> pl.DataFrame:
    rows = [
        ("bank", "r_bank"),
        ("bank2", "r_bank"),
        ("marked", "r_same"),
        ("marked", "r_other"),
        ("marked", "r_blank"),
        (None, "r_excess"),
    ]
    return pl.DataFrame(
        [
            {
                "row_id": row_id,
                "record_id": record_id,
                "year": 2022,
                "allocated_usd": 1.0,
                "allocated_low_usd": 1.0,
                "allocated_high_usd": 1.0,
                "rule": "fifo" if row_id else "excess",
                "disbursement_source": "crs",
            }
            for row_id, record_id in rows
        ],
        schema=schema.ALLOCATIONS.dtypes,
    )


def test_disbursements_inherit_the_commitments_markers_and_keep_their_own() -> None:
    inherited = inherit_markers(
        _allocations().lazy(), commitments=_commitments().lazy(), crs=_records().lazy()
    ).collect()

    rows = {
        record: (source, inherited_a, inherited_m, own_a, own_m, share)
        for record, source, inherited_a, inherited_m, own_a, own_m, share in (
            inherited.filter(pl.col("row_id") != "bank2")
            .select(
                "record_id",
                "marker_source",
                "inherited_rio_adaptation",
                "inherited_rio_mitigation",
                "record_rio_adaptation",
                "record_rio_mitigation",
                "climate_share",
            )
            .iter_rows()
        )
    }
    # The excess row reaches no CRDF commitment, so it inherits nothing.
    assert rows == {
        "r_bank": ("components", None, None, None, None, 0.6),
        "r_same": ("commitment", 1, 2, 1, 2, None),
        "r_other": ("record_differs", 1, 2, 0, 2, None),
        "r_blank": ("record_differs", 1, 2, None, None, None),
    }
    assert inherited.height == 5


def test_reported_objectives_label_counted_records_once_per_source() -> None:
    labels = reported_objectives(
        _allocations().lazy(),
        commitments=_commitments().lazy(),
        crs=_records().lazy(),
        untraced=pl.LazyFrame({"record_id": ["r_excess"]}),
    ).collect()

    schema.validate(schema.OBJECTIVE_LABELS, labels)
    # The bank record fills two adaptation components and gets one crdf label; the
    # unmarked records get no marker labels; the untraced excess record gets its own
    # markers, and the records neither allocated nor untraced get nothing.
    assert sorted(
        labels.select("record_id", "objective", "level", "label_source").iter_rows()
    ) == [
        ("r_bank", "adaptation", 2, "crdf"),
        ("r_excess", "adaptation", 2, "marker"),
        ("r_excess", "mitigation", 2, "marker"),
        ("r_other", "adaptation", 0, "marker"),
        ("r_other", "mitigation", 2, "marker"),
        ("r_same", "adaptation", 1, "marker"),
        ("r_same", "mitigation", 2, "marker"),
    ]


def test_reported_objectives_leave_out_adaptation_before_the_crs_measures_it() -> None:
    records = crs_records(
        {"record_id": "r_2009", "year": 2009, "rio_adaptation": 2, "rio_mitigation": 1},
        {"record_id": "r_2010", "year": 2010, "rio_adaptation": 2, "rio_mitigation": 1},
    )  # fmt: skip

    labels = reported_objectives(
        pl.LazyFrame(schema=schema.ALLOCATIONS.dtypes),
        commitments=_commitments().lazy(),
        crs=records.lazy(),
        untraced=records.select("record_id").lazy(),
    ).collect()

    assert sorted(labels.select("record_id", "objective", "level").iter_rows()) == [
        ("r_2009", "mitigation", 1),
        ("r_2010", "adaptation", 2),
        ("r_2010", "mitigation", 1),
    ]


def _context(tmp_path: Path, config: BuildConfig) -> StageContext:
    release_dir = tmp_path / "r"
    write_inputs(release_dir, "fill", {"allocations": _allocations()})
    write_inputs(release_dir, "normalise", {"crdf": _commitments(), "crs": _records()})
    return stage_context(release_dir, config=config)


@pytest.mark.parametrize(("gap_fill", "untraced"), [(False, 3), (True, 2)])
def test_run_label_writes_tables_that_pass_their_schemas(
    tmp_path: Path, gap_fill: bool, untraced: int
) -> None:
    config = load_config(BUILD_TOML).model_copy(update={"gap_fill": gap_fill})
    context = _context(tmp_path, config)

    written = run_stage("label", context)

    assert set(written) == set(schema.STAGE_TABLES["label"])
    objectives = pl.read_parquet(table_path(context.release_dir, "label", "objectives"))
    # The five labels of the allocated records, and two markers per untraced one.
    assert objectives.height == 5 + 2 * untraced


# Jev's lenient answer, p(significant) and p(principal) by text.
ANSWERS = {
    "Seawalls against sea level rise": (0.95, 0.2, 0.7),
    "Drought-tolerant seed": (0.8, 0.6, 0.1),
    "School meals": (0.7, 0.1, 0.0),
    "Office rent": (0.2, 0.0, 0.0),
    "Flood defences": (0.9, 0.3, 0.6),
    "Solar mini-grids": (0.3, 0.1, 0.0),
}
# Human-coded projects: text and Weikmans et al.'s level.
HUMAN = (
    ("Seawalls against sea level rise", "2"),
    ("Drought-tolerant seed", "1"),
    ("School meals", "0"),
    ("Office rent", "99"),
)


def _response(lenient: float, significant: float, principal: float) -> dict:
    rest = 1.0 - significant - principal
    return {
        "model": MODEL,
        "answers": {
            "strict": {"type": "noul", "noul": lenient},
            "lenient": {"type": "noul", "noul": lenient},
            "level": {
                "type": "choice",
                "choice": "principal",
                "confidence": 0.5,
                "probabilities": {
                    "principal": principal,
                    "significant": significant,
                    "not_targeted": rest,
                    "insufficient_info": 0.0,
                },
            },
        },
        "usage": {"input_tokens": 700, "output_tokens": 90},
    }


def _write_cache(
    cache_dir: Path, answers: dict[str, tuple[float, float, float]] = ANSWERS
) -> None:
    """Cache Jev's `answers` under the requests `ask_jev` sends."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    with (cache_dir / f"{MODEL}.jsonl").open("w") as lines:
        for text, answer in answers.items():
            request = {
                "model": MODEL,
                "state": {"project": text},
                "questions": ADAPTATION_QUESTIONS,
            }
            entry = {
                "request": json.loads(canonical_request(request)),
                "response": _response(*answer),
            }
            lines.write(json.dumps(entry) + "\n")


def _client(tmp_path: Path) -> JevClient:
    _write_cache(tmp_path / "jev")
    return JevClient(
        model=MODEL, cost_cap_usd=10.0, cache_dir=tmp_path / "jev", offline=True
    )


def _write_human(path: Path) -> Path:
    rows = [
        {
            "text": text,
            "projecttitle": None,
            "longdescription": None,
            WEIKMANS_LEVEL: level,
        }
        for text, level in HUMAN
    ]
    rows += [
        # No `text`: the title and description stand in for it.
        {"text": None, "projecttitle": "Mangrove", "longdescription": "restoration",
         WEIKMANS_LEVEL: "2"},
        {"text": None, "projecttitle": None, "longdescription": None,
         WEIKMANS_LEVEL: "0"},
        # Not re-evaluated.
        {"text": "Bridges", "projecttitle": None, "longdescription": None,
         WEIKMANS_LEVEL: None},
    ]  # fmt: skip
    pl.DataFrame(rows, schema=dict.fromkeys(rows[0], pl.String)).write_csv(path)
    return path


def test_human_labels_fold_insufficient_information_into_zero(tmp_path: Path) -> None:
    labels = read_adaptation_labels(_write_human(tmp_path / "wk.csv"))

    assert labels.rows() == [
        (0, "Seawalls against sea level rise", 2),
        (1, "Drought-tolerant seed", 1),
        (2, "School meals", 0),
        (3, "Office rent", 0),
        (4, "Mangrove restoration", 2),
        (5, NO_TEXT, 0),
    ]


def test_jev_answers_each_distinct_text_once_and_joins_back(tmp_path: Path) -> None:
    texts = pl.DataFrame(
        {"id": [1, 2, 3], "text": ["Flood defences", "Office rent", "Flood defences"]}
    )

    answers = ask_jev(texts, client=_client(tmp_path))

    assert answers.select("id", "lenient", "p_principal").rows() == [
        (1, 0.9, 0.6),
        (2, 0.2, 0.0),
        (3, 0.9, 0.6),
    ]


def test_the_threshold_maximises_f1_and_takes_the_higher_cut_on_a_tie() -> None:
    # Any cut above 0.7 and up to 0.8 labels the two human-positive projects only.
    fitted = pl.DataFrame({"level": [2, 1, 0, 0], "lenient": [0.95, 0.8, 0.7, 0.2]})

    assert fit_adaptation_threshold(fitted) == 0.8


def test_jev_labels_only_records_the_release_weighs_as_marked_adaptation() -> None:
    commitments = crdf_rows(
        {"row_id": "adaptation", "year": 2021, "rio_adaptation": 1},
        # Mitigation only, and unmarked: outside the human-coded projects.
        {"row_id": "mitigation", "year": 2021, "rio_adaptation": 0},
        {"row_id": "unmarked", "year": 2021, "rio_adaptation": None},
    )
    records = crs_records(
        *(
            {"record_id": f"r_{row_id}", "year": 2022, "title": f"Project {row_id}"}
            for row_id in ("adaptation", "mitigation", "unmarked")
        )
    )
    allocations = pl.DataFrame(
        {"row_id": ["adaptation", "mitigation", "unmarked"]}
    ).with_columns(record_id="r_" + pl.col("row_id"))
    # Untraced records weigh by their own markers: adaptation 1, mitigation only,
    # and adaptation 2 in 2009, before the CRS measures adaptation.
    untraced = crs_records(
        {"record_id": "u_adaptation", "year": 2022, "rio_adaptation": 1,
         "title": "Project u_adaptation"},
        {"record_id": "u_mitigation", "year": 2022, "rio_adaptation": 0,
         "title": "Project u_mitigation"},
        {"record_id": "u_2009", "year": 2009, "rio_adaptation": 2,
         "title": "Project u_2009"},
    )  # fmt: skip

    texts = jev_records(
        allocations.lazy(),
        commitments=commitments.lazy(),
        crs=pl.concat([records, untraced]).lazy(),
        untraced=untraced.select("record_id").lazy(),
    ).collect()

    assert sorted(texts.rows()) == [
        ("r_adaptation", "Project adaptation"),
        ("u_adaptation", "Project u_adaptation"),
    ]


@pytest.mark.parametrize(
    ("lenient", "significant", "principal", "level"),
    [(0.9, 0.3, 0.6, 2), (0.9, 0.6, 0.3, 1), (0.84, 0.0, 0.9, 0)],
)
def test_a_jev_label_reaches_adaptation_at_the_threshold(
    lenient: float, significant: float, principal: float, level: int
) -> None:
    answers = pl.DataFrame(
        {
            "record_id": ["r"],
            "lenient": [lenient],
            "p_significant": [significant],
            "p_principal": [principal],
        }
    )

    labels = jev_objectives(answers, threshold=0.85)

    schema.validate(schema.OBJECTIVE_LABELS, labels)
    assert labels.select("objective", "level", "label_source").row(0) == (
        "adaptation",
        level,
        "jev",
    )


@pytest.mark.parametrize(("gap_fill", "fund"), [(False, [("r_fund", 0)]), (True, [])])
def test_run_label_labels_described_marker_records_with_one_shared_client(
    tmp_path: Path, gap_fill: bool, fund: list[tuple[str, int]]
) -> None:
    config = load_config(BUILD_TOML)
    config = config.model_copy(
        update={
            "gap_fill": gap_fill,
            "jev_labels": True,
            "jev": config.jev.model_copy(update={"cache_dir": tmp_path / "jev"}),
        }
    )
    # The human set's two texts outside ANSWERS.
    unlabelled = dict.fromkeys(("Mangrove restoration", NO_TEXT), (0.5, 0.2, 0.2))
    _write_cache(tmp_path / "jev", {**ANSWERS, **unlabelled})
    context = _context(tmp_path, config)
    # Fetch copies the human-coded labels into raw/.
    labels = context.raw_dir / ADAPTATION_LABELS_FILE
    labels.parent.mkdir(parents=True)
    _write_human(labels)

    run_stage("label", context)

    objectives = pl.read_parquet(table_path(context.release_dir, "label", "objectives"))
    # The bank record has climate components and r_blank no text, so Jev labels
    # r_same (its title and description) and r_other, which inherit adaptation, and
    # the untraced r_excess and r_unlinked, marked adaptation themselves. The GEF's
    # r_fund is untraced only without the IATI route.
    assert sorted(
        objectives.filter(pl.col("label_source") == "jev")
        .select("record_id", "level")
        .iter_rows()
    ) == sorted(
        [("r_excess", 1), ("r_other", 0), ("r_same", 2), ("r_unlinked", 2), *fund]
    )
    assert context.jev is context.jev
    assert context.jev.live_calls == 0


def test_jev_labels_need_the_human_labels_path() -> None:
    config = load_config(BUILD_TOML)

    with pytest.raises(ValueError, match="adaptation_labels"):
        BuildConfig.model_validate(
            {
                **config.model_dump(),
                "jev_labels": True,
                "sources": {
                    **config.sources.model_dump(),
                    "adaptation_labels": None,
                },
            }
        )

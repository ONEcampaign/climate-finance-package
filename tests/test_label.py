"""Marker inheritance and reported objective labels on hand-built rows."""

from pathlib import Path

import polars as pl
from normalised import crdf_rows, crs_records
from stage_runs import run_stage, stage_context, write_inputs

from climate_finance import schema
from climate_finance.build.label import inherit_markers, reported_objectives
from climate_finance.build.stages import table_path


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
        {"record_id": "r_same", "year": 2022, "rio_adaptation": 1, "rio_mitigation": 2},
        {"record_id": "r_other", "year": 2022, "rio_adaptation": 0,
         "rio_mitigation": 2},
        {"record_id": "r_blank", "year": 2022, "rio_adaptation": None,
         "rio_mitigation": None},
        {"record_id": "r_excess", "year": 2022, "rio_adaptation": 2,
         "rio_mitigation": 2},
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


def test_reported_objectives_label_linked_records_once_per_source() -> None:
    labels = reported_objectives(
        _allocations().lazy(), commitments=_commitments().lazy(), crs=_records().lazy()
    ).collect()

    schema.validate(schema.OBJECTIVE_LABELS, labels)
    # The bank record fills two adaptation components and gets one crdf label; the
    # unmarked records get no marker labels; the excess record gets none at all.
    assert sorted(
        labels.select("record_id", "objective", "level", "label_source").iter_rows()
    ) == [
        ("r_bank", "adaptation", 2, "crdf"),
        ("r_other", "adaptation", 0, "marker"),
        ("r_other", "mitigation", 2, "marker"),
        ("r_same", "adaptation", 1, "marker"),
        ("r_same", "mitigation", 2, "marker"),
    ]


def test_run_label_writes_tables_that_pass_their_schemas(tmp_path: Path) -> None:
    release_dir = tmp_path / "r"
    write_inputs(release_dir, "fill", {"allocations": _allocations()})
    write_inputs(release_dir, "normalise", {"crdf": _commitments(), "crs": _records()})

    written = run_stage("label", stage_context(release_dir))

    assert set(written) == set(schema.STAGE_TABLES["label"])
    objectives = pl.read_parquet(table_path(release_dir, "label", "objectives"))
    assert objectives.height == 5

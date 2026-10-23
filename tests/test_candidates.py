"""Programme stems and the candidate pairs blocking gives the scoring stage."""

from collections.abc import Mapping
from datetime import date

import numpy as np
import polars as pl
import pytest

from climate_finance import schema
from climate_finance.build.link.candidates import (
    MAX_FANOUT,
    MAX_PN_UNITS,
    MAX_TITLE_UNITS,
    MIN_PAIRS,
    Embedder,
    generate_candidates,
    learn_programme_stems,
    project_shape,
    strip_segments,
    with_programme,
)
from climate_finance.build.link.embed import DIMENSIONS, NEIGHBOURS
from climate_finance.build.link.id_practice import RENUMBERED_DONORS
from climate_finance.build.link.keys import build_units

FIRST_YEAR = 2010
_RECORD = {
    "donor_code": 5,
    "agency_code": 1,
    "crs_id": None,
    "project_number": None,
    "title": None,
    "short_description": None,
    "long_description": None,
    "recipient_code": 238,
    "purpose_code": 23210,
    "year": 2020,
    "commitment_date": None,
    "commitment_usd": None,
    "disbursement_usd": None,
}
_CRS_SCHEMA = {
    "record_id": pl.String,
    "donor_code": pl.Int32,
    "agency_code": pl.Int32,
    "crs_id": pl.String,
    "project_number": pl.String,
    "title": pl.String,
    "short_description": pl.String,
    "long_description": pl.String,
    "recipient_code": pl.Int32,
    "purpose_code": pl.Int32,
    "year": pl.Int16,
    "commitment_date": pl.Date,
    "commitment_usd": pl.Float64,
    "disbursement_usd": pl.Float64,
}
_STEMS_SCHEMA = {
    name: column.dtype for name, column in schema.PROGRAMME_STEMS.columns.items()
}
_CLASSES_SCHEMA = {
    name: column.dtype for name, column in schema.ID_PRACTICE.columns.items()
}


def _crs(records: dict[str, dict[str, object]]) -> pl.LazyFrame:
    """CRS records named by their record id, with `_RECORD`'s defaults."""
    return pl.LazyFrame(
        [{**_RECORD, "record_id": name, **fields} for name, fields in records.items()],
        schema=_CRS_SCHEMA,
    )


def _stems(*rules: tuple[str, int]) -> pl.DataFrame:
    """Rules for donor 5, agency 1, as (pattern, segments)."""
    return pl.DataFrame(
        [
            {
                "donor_code": 5,
                "agency_code": 1,
                "pattern": pattern,
                "segments": segments,
                "pairs": MIN_PAIRS,
                "consistent_pairs": MIN_PAIRS,
            }
            for pattern, segments in rules
        ],
        schema=_STEMS_SCHEMA,
    )


def _classes(*rows: tuple[int, str]) -> pl.DataFrame:
    """Id-practice classes for donor 5 in 2019 to 2022, as (agency, class)."""
    return pl.DataFrame(
        [
            {
                "donor_code": 5,
                "agency_code": agency,
                "period": "2019-2022",
                "id_class": id_class,
                "open_commitments": 10,
            }
            for agency, id_class in rows
        ],
        schema=_CLASSES_SCHEMA,
    )


def _candidates(
    records: dict[str, dict[str, object]],
    *,
    commitments: tuple[str, ...] = ("c",),
    trace: tuple[tuple[str, str], ...] = (),
    classes: pl.DataFrame | None = None,
    stems: pl.DataFrame | None = None,
    embedder: Embedder | None = None,
) -> dict[tuple[str, str], dict[str, object]]:
    """Candidate rows by (disbursement, commitment) record name.

    Every test record is its own unit. `commitments` names the units a CRDF row
    reaches, and `trace` the (disbursement, commitment) pairs a key linked.
    """
    crs = _crs(records)
    units = build_units(crs).collect()
    names = dict(zip(units["unit_id"], units["record_ids"].list.first(), strict=True))
    ids = {name: unit_id for unit_id, name in names.items()}
    links = pl.DataFrame(
        [
            {"task": "crdf_crs", "left_id": f"row {name}", "right_id": ids[name]}
            for name in commitments
        ]
        + [
            {"task": "trace", "left_id": ids[left], "right_id": ids[right]}
            for left, right in trace
        ],
        schema={"task": pl.String, "left_id": pl.String, "right_id": pl.String},
    )
    candidates = generate_candidates(
        units,
        crs=crs,
        links=links,
        id_practice=_classes() if classes is None else classes,
        stems=_stems() if stems is None else stems,
        first_year=FIRST_YEAR,
        embedder=embedder,
    )
    schema.validate(schema.CANDIDATES, candidates)
    return {
        (names[row["left_id"]], names[row["right_id"]]): row
        for row in candidates.iter_rows(named=True)
    }


def _commitment(**fields: object) -> dict[str, object]:
    return {"year": 2020, "commitment_usd": 10.0, **fields}


def _disbursement(**fields: object) -> dict[str, object]:
    return {
        "year": 2022,
        "disbursement_usd": 4.0,
        "recipient_code": 289,
        "purpose_code": 41010,
        **fields,
    }


@pytest.mark.parametrize(
    ("pn", "shape"),
    [
        ("7F-10454.01.03", "9A-9.9.9"),
        ("SE-0-SE-6-14311A0101-SYD-15160", "A-9-A-9-9A9-A-9"),
        ("QZA-21/0162", "A-9/9"),
    ],
)
def test_project_shape(pn: str, shape: str) -> None:
    assert pl.select(project_shape(pl.lit(pn))).item() == shape


@pytest.mark.parametrize(
    ("pn", "segments", "stem"),
    [
        ("7F-10454.01.03", 1, "7F-10454.01"),
        ("QZA-21/0162-28", 1, "QZA-21/0162"),
        ("63020-PAN104-OUTPUT6.2.", 1, "63020-PAN104-OUTPUT6"),
        ("202167591", 1, None),
        ("7F-10454.01.03", 2, "7F-10454"),
        ("AB-1", 2, None),
    ],
)
def test_strip_segments(pn: str, segments: int, stem: str | None) -> None:
    assert pl.select(strip_segments(pl.lit(pn), segments)).item() == stem


def test_a_rule_keys_only_its_own_shape_and_agency() -> None:
    units = pl.LazyFrame(
        {
            "donor_code": [5, 5, 5, 6],
            "agency_code": [1, 1, 1, 1],
            "project_number": [
                "7F-10454.01.03",
                "QZA-21/0162",
                "7F-10454",
                "7F-1.01.03",
            ],
        },
        schema={
            "donor_code": pl.Int32,
            "agency_code": pl.Int32,
            "project_number": pl.String,
        },
    )
    keyed = with_programme(units, stems=_stems(("9A-9.9.9", 1)).lazy()).collect()
    assert keyed["programme"].to_list() == [
        "7F-10454.01",
        "QZA-21/0162",
        "7F-10454",
        "7F-1.01.03",
    ]


def _components(
    n: int, *, stem: str = "7F-1.01", recipients: tuple[int, ...] = (238,)
) -> dict[str, dict[str, object]]:
    """`n` component records under `stem`, spread over `recipients`, one title."""
    return {
        f"{stem}.{i:02}": {
            "project_number": f"{stem}.{i:02}",
            "recipient_code": recipients[i % len(recipients)],
            "title": "rural water supply",
            "commitment_usd": 1.0,
        }
        for i in range(n)
    }


# Seven components merge into 21 pairs, at least MIN_PAIRS.
_SEVEN = 7
# Spreads MAX_FANOUT components over enough recipients to merge into at least
# MIN_PAIRS pairs.
_RECIPIENTS = tuple(range(1, 30))
_DISTINCT_TITLES = (
    "water",
    "roads",
    "schools",
    "clinics",
    "energy",
    "forests",
    "ports",
)


@pytest.mark.parametrize(
    ("records", "rules"),
    [
        # Siblings merge under one segment; two segments merge them as well, and
        # the smaller count wins the tie.
        (_components(_SEVEN), [("9A-9.9.9", 1, 21, 21)]),
        # The frame agreement's own number joins its components' group.
        (
            _components(_SEVEN - 1)
            | {"frame": {"project_number": "7F-1.01", "title": "rural water supply"}},
            [("9A-9.9.9", 1, 21, 21)],
        ),
        # Too few merged pairs.
        (_components(_SEVEN - 1), []),
        # Components in two recipients do not merge across them.
        (_components(_SEVEN, recipients=(238, 289)), []),
        # Titles that disagree.
        (
            {
                name: fields | {"title": title}
                for (name, fields), title in zip(
                    _components(_SEVEN).items(), _DISTINCT_TITLES, strict=True
                )
            },
            [],
        ),
        # A stem wider than MAX_FANOUT is a budget line: every pair it merges counts
        # as inconsistent, and here outweighs a consistent stem of the same shape.
        (
            _components(_SEVEN)
            | _components(MAX_FANOUT + 1, stem="7F-2.01", recipients=_RECIPIENTS),
            [],
        ),
    ],
)
def test_learn_programme_stems(
    records: dict[str, dict[str, object]],
    rules: list[tuple[str, int, int, int]],
) -> None:
    stems = learn_programme_stems(_crs(records)).collect()
    schema.validate(schema.PROGRAMME_STEMS, stems)
    assert stems.select("pattern", "segments", "pairs", "consistent_pairs").rows() == (
        rules
    )


def test_a_stem_at_the_fanout_limit_is_learned() -> None:
    records = _components(MAX_FANOUT, recipients=_RECIPIENTS)
    assert learn_programme_stems(_crs(records)).collect().height == 1


@pytest.mark.parametrize(
    "disbursement",
    [
        {"recipient_code": 238, "purpose_code": 23210},
        {"project_number": "p.123"},
        {"title": "Solar mini-grids, Kenya"},
        {"recipient_code": 238, "title": "Solar mini-grids Uganda"},
        {"short_description": "Solar mini-grids, Kenya"},
    ],
    ids=[
        "recipient_purpose",
        "project_number",
        "title",
        "recipient_title_head",
        "title_from_short_description",
    ],
)
def test_each_blocking_rule_pairs_a_later_disbursement(
    disbursement: dict[str, object],
) -> None:
    records = {
        "c": _commitment(project_number="P-123", title="solar mini grids kenya"),
        "d": _disbursement(**disbursement),
    }
    assert set(_candidates(records)) == {("d", "c")}


@pytest.mark.parametrize(
    ("donor", "paired"), [(RENUMBERED_DONORS[0], True), (5, False)]
)
def test_a_renumbered_project_number_reaches_its_old_crs_ids_commitment(
    donor: int, *, paired: bool
) -> None:
    # A later record under the commitment's crs id carries the project's new
    # number, which a disbursement under a new crs id repeats.
    records = {
        "c": _commitment(
            donor_code=donor, crs_id="2017000002", project_number="AFR/WATER/2015/1"
        ),
        "bridge": _disbursement(
            donor_code=donor, crs_id="2017000002", project_number="AF00000081"
        ),
        "d": _disbursement(
            donor_code=donor, crs_id="2022000015a", project_number="AF00000081"
        ),
    }
    assert (("d", "c") in _candidates(records)) is paired


@pytest.mark.parametrize(
    "short_description",
    # A title is never replaced by the short description.
    [None, "solar mini grids kenya"],
)
def test_no_rule_no_pair(short_description: str | None) -> None:
    records = {
        "c": _commitment(project_number="P-123", title="solar mini grids kenya"),
        "d": _disbursement(
            project_number="P-124",
            title="wind farm",
            short_description=short_description,
        ),
    }
    assert _candidates(records) == {}


def test_only_later_record_years_pair() -> None:
    records = {
        "c": _commitment(),
        "same_year": _disbursement(year=2020, recipient_code=238, purpose_code=23210),
        "earlier": _disbursement(year=2019, recipient_code=238, purpose_code=23210),
    }
    assert _candidates(records) == {}


def test_only_commitments_a_crdf_row_reaches_pair() -> None:
    records = {
        "c": _commitment(),
        "d": _disbursement(recipient_code=238, purpose_code=23210),
    }
    assert _candidates(records, commitments=()) == {}


@pytest.mark.parametrize(
    ("field", "value", "cap"),
    [
        ("project_number", "P-123", MAX_PN_UNITS),
        ("title", "solar mini grids kenya", MAX_TITLE_UNITS),
    ],
)
@pytest.mark.parametrize(
    ("extra_year", "paired"),
    # One unit past the cap stops the pairs; in an earlier year it does not count.
    [(None, True), (2022, False), (2019, True)],
)
def test_a_common_key_pairs_only_up_to_its_cap(
    field: str, value: str, cap: int, extra_year: int | None, paired: bool
) -> None:
    # Disbursements carry their own project numbers, so a shared title is not a
    # template line (`template_lines`).
    records = {"c": _commitment(**{field: value})} | {
        f"d{i}": _disbursement(
            recipient_code=1000 + i, **({"project_number": f"D{i}"} | {field: value})
        )
        for i in range(cap)
    }
    if extra_year is not None:
        records["extra"] = _disbursement(
            year=extra_year, **({"project_number": "E"} | {field: value})
        )
    assert bool(_candidates(records)) is paired


@pytest.mark.parametrize(
    ("change", "kept"),
    [
        ({}, True),
        # A key linked the disbursement.
        ({"trace": (("d", "other"),)}, False),
        # A key reaches the commitment from a later record year.
        ({"trace": (("d_keyed", "c"),)}, False),
        # The commitment's own-year disbursement does not count.
        ({"trace": (("c_own", "c"),)}, True),
    ],
)
def test_a_pair_is_eligible_only_where_the_keys_left_both_units(
    change: dict[str, tuple[tuple[str, str], ...]], kept: bool
) -> None:
    records = {
        "c": _commitment(),
        "c_own": {"year": 2020, "disbursement_usd": 1.0, "recipient_code": 1},
        "other": _commitment(recipient_code=2),
        "d": _disbursement(recipient_code=238, purpose_code=23210),
        "d_keyed": _disbursement(recipient_code=3),
    }
    pairs = _candidates(records, **change)
    assert (("d", "c") in pairs) is kept
    if kept:
        assert pairs["d", "c"]["eligible"]


_PERSISTENT = _classes((1, "id_persistent"))


@pytest.mark.parametrize(
    ("others", "project_number", "crs_ids", "classes", "kept"),
    [
        # The title, with no project number, reaches three recipients.
        ((2, 3), None, (None, None), None, True),
        # Four recipients in three years: a template line on both sides.
        ((2, 3, 4), None, (None, None), None, False),
        # The other lines carry project numbers, so the title names projects.
        ((2, 3, 4), "P", (None, None), None, True),
        # A template line pairs with nothing to train on either.
        ((2, 3, 4), None, (None, None), _PERSISTENT, False),
        # Two of its five lines continue crs id A under persistent ids, so the
        # title names an activity per country.
        ((2, 3, 4), None, ("A", "A"), _PERSISTENT, True),
        # No line continues a crs id.
        ((2, 3, 4), None, ("A", "B"), _PERSISTENT, False),
    ],
)
def test_a_template_line_pairs_with_nothing(
    others: tuple[int, ...],
    project_number: str | None,
    crs_ids: tuple[str | None, str | None],
    classes: pl.DataFrame | None,
    kept: bool,
) -> None:
    commitment_crs_id, disbursement_crs_id = crs_ids
    records = {
        "c": _commitment(title="Aggregate", crs_id=commitment_crs_id),
        "d": _disbursement(
            title="Aggregate",
            crs_id=disbursement_crs_id,
            recipient_code=238,
            purpose_code=23210,
        ),
        **{
            f"other_{recipient}": _disbursement(
                year=2021,
                title="Aggregate",
                recipient_code=recipient,
                project_number=project_number and f"{project_number}{recipient}",
            )
            for recipient in others
        },
    }
    assert (("d", "c") in _candidates(records, classes=classes)) is kept


@pytest.mark.parametrize(("commitment_year", "kept"), [(2019, False), (2020, True)])
def test_a_disbursement_pairs_only_with_commitments_from_its_floor_on(
    commitment_year: int, kept: bool
) -> None:
    records = {
        "c": _commitment(year=commitment_year),
        "d": _disbursement(
            recipient_code=238, purpose_code=23210, commitment_date=date(2020, 3, 1)
        ),
    }
    assert bool(_candidates(records)) is kept


@pytest.mark.parametrize(
    ("classes", "row"),
    [
        # The commitment's ids persist: a training pair, labelled by its crs id.
        (
            ((1, "id_persistent"),),
            {"eligible": False, "id_persistent": True, "same_crs_id": True},
        ),
        # Parent ids persist: neither scored nor a training pair.
        (((1, "parent_id_persistent"),), None),
        # The disbursement's agency's ids persist.
        (((2, "id_persistent"),), None),
        (
            ((1, "pn_persistent"), (2, "neither")),
            {"eligible": True, "id_persistent": False, "same_crs_id": True},
        ),
    ],
)
def test_where_ids_persist_the_id_is_the_only_evidence(
    classes: tuple[tuple[int, str], ...], row: dict[str, object] | None
) -> None:
    records = {
        "c": _commitment(crs_id="A"),
        "d": _disbursement(
            crs_id="A", agency_code=2, recipient_code=238, purpose_code=23210
        ),
    }
    pairs = _candidates(records, classes=_classes(*classes))
    if row is None:
        assert pairs == {}
    else:
        assert {name: pairs["d", "c"][name] for name in row} == row


def test_programme_level() -> None:
    records = {
        "c": _commitment(project_number="7F-1.01.01"),
        "same": _disbursement(
            recipient_code=238, purpose_code=23210, project_number="7F-1.01.01"
        ),
        "sibling": _disbursement(
            recipient_code=238, purpose_code=23210, project_number="7F-1.01.02"
        ),
        "other": _disbursement(
            recipient_code=238, purpose_code=23210, project_number="9X-2"
        ),
    }
    levels = {
        left: row["programme_level"]
        for (left, _), row in _candidates(
            records, stems=_stems(("9A-9.9.9", 1))
        ).items()
    }
    assert levels == {"same": 2, "sibling": 1, "other": 0}
    unruled = _candidates(records)
    assert unruled["sibling", "c"]["programme_level"] == 0


def test_a_unit_takes_its_largest_records_title() -> None:
    records = {
        "c": _commitment(title="solar mini grids kenya"),
        "d_small": _disbursement(title="Solar mini-grids, Kenya", disbursement_usd=1.0),
        "d_large": _disbursement(title="wind farm", disbursement_usd=9.0),
    }
    # One unit, named "d_large": both records share every key field.
    assert _candidates(records) == {}
    records["d_large"]["disbursement_usd"] = 0.5
    assert set(_candidates(records)) == {("d_large", "c")}


def _embedder(vectors: Mapping[str, list[float]]) -> Embedder:
    """An embedder giving each text its vector in `vectors`, padded with zeros."""

    def embed(texts: list[str]) -> np.ndarray:
        out = np.zeros((len(texts), DIMENSIONS), np.float32)
        for i, text in enumerate(texts):
            out[i, : len(vectors[text])] = vectors[text]
        return out

    return embed


# Two unit vectors whose cosine is `cosine`.
def _at(cosine: float) -> list[float]:
    return [cosine, (1 - cosine**2) ** 0.5]


@pytest.mark.parametrize(
    ("disbursement", "cosine", "paired"),
    [
        ({}, 0.95, True),
        ({}, 0.85, False),
        ({"year": 2020}, 1.0, False),
        ({"donor_code": 6}, 1.0, False),
    ],
    ids=["close", "far", "same_year", "other_donor"],
)
def test_embedding_neighbours_pair_close_texts(
    disbursement: dict[str, object], cosine: float, *, paired: bool
) -> None:
    # No other rule pairs the two: they share no recipient, number or title.
    records = {
        "c": _commitment(title="solar mini grids kenya"),
        "d": _disbursement(title="wind farm", **disbursement),
        # Keeps donor 6's disbursements in the pool.
        "other": _commitment(donor_code=6, recipient_code=1, title="tree nursery"),
    }
    embedder = _embedder(
        {
            "solar mini grids kenya": [1.0],
            "wind farm": _at(cosine),
            "tree nursery": [0.0, 0.0, 1.0],
        }
    )
    commitments = ("c", "other")
    pairs = _candidates(records, commitments=commitments, embedder=embedder)
    assert set(pairs) == ({("d", "c")} if paired else set())
    assert _candidates(records, commitments=commitments) == {}


def test_embedding_neighbours_rank_either_side_ties_to_the_lower_unit() -> None:
    # Every text has the same vector. Each commitment ranks the three disbursement
    # units of smallest unit id first, and each disbursement the three such
    # commitment units, so only the largest of each misses the other.
    count = NEIGHBOURS + 1
    commitments = tuple(f"c{i}" for i in range(count))
    disbursements = tuple(f"d{i}" for i in range(count))
    records = {
        name: _commitment(recipient_code=100 + i, title=name)
        for i, name in enumerate(commitments)
    } | {
        name: _disbursement(recipient_code=200 + i, title=name)
        for i, name in enumerate(disbursements)
    }
    embedder = _embedder({name: [1.0] for name in records})
    pairs = _candidates(records, commitments=commitments, embedder=embedder)
    assert len(pairs) == count * count - 1
    assert pairs == _candidates(records, commitments=commitments, embedder=embedder)

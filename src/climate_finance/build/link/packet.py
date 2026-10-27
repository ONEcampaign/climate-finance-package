"""Labelling packets: the pairs a person or an agent decides, as Markdown.

A packet opens with `MATCH_DEFINITION` and gives each pair a numbered section: the
later disbursement unit (A) and the earlier commitment unit (B) side by side, every
CRS row each unit sums (up to `MAX_ROWS` a side), and how many of the donor's other
commitments for the side's recipient share at least `SIBLING_SHARE` of the side's
title words. With `show_ids` the sections carry each unit's crs id and project
number. Without it they carry neither, and any id either unit holds is blanked in
both units' texts, as a pilot that measures labellers against id truth needs.

`write_packet` writes the packet and, beside it, the pairs its sections number
(`packet_pairs_path`), which `decisions.import_labels` keys a returned CSV back to.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import polars as pl

from climate_finance import catalogue
from climate_finance.build.errors import LabelPacketError
from climate_finance.build.link.candidates import word_share
from climate_finance.build.link.levels import title_words

MATCH_DEFINITION = (
    "A matches B when A's money is spending under the commitment B records. That "
    "includes a commitment the donor reported on several lines for the same recipient "
    "(same loan or grant, same commitment date), renumbered, or left partly "
    "undisbursed and drawn down later; a regional commitment line counts for a "
    "country inside that region. It excludes a line for a different recipient "
    "country, a sibling project, a later phase or module with its own new commitment "
    "even under a reused number, a separate yearly grant or contribution committed "
    "and spent in its own year, and an unrelated activity."
)
MAX_ROWS = 12
SIBLING_SHARE = 0.5
TEXT_LIMIT = 600
# Columns of the pairs frame a packet carries through to its pairs file.
CARRIED = ("task", "review_reason", "sample_id")
# The code columns whose names `code_names` reads from the bulk CRS.
NAMED_CODES = {
    "agency": ("donor_code", "agency_code"),
    "purpose": ("purpose_code",),
    "channel": ("channel_code",),
    "flow": ("flow_code",),
}
_TEXTS = ("title", "short_description", "long_description")
_IDS = ("crs_id", "project_number")


def code_names(raw_crs: Path) -> pl.DataFrame:
    """The most frequent name of each agency, purpose, channel and flow code in the
    bulk CRS file at `raw_crs`, as (kind, donor_code, code, name)."""
    crs = pl.scan_parquet(raw_crs)
    parts = []
    for kind, keys in NAMED_CODES.items():
        named = (
            crs.select(*keys, name=f"{kind}_name")
            .drop_nulls()
            .group_by(*keys, "name")
            .len()
            .collect(engine="streaming")
            .sort(
                [*keys, "len", "name"], descending=[False] * len(keys) + [True, False]
            )
            .unique(keys, keep="first", maintain_order=True)
        )
        parts.append(
            named.select(
                kind=pl.lit(kind),
                donor_code=pl.col("donor_code").cast(pl.Int32)
                if kind == "agency"
                else pl.lit(None, dtype=pl.Int32),
                code=pl.col(keys[-1]).cast(pl.Int32),
                name="name",
            )
        )
    return pl.concat(parts)


def packet_pairs_path(packet: Path) -> Path:
    """Where `write_packet` puts the pairs of the packet at `packet`."""
    return packet.with_suffix(".pairs.parquet")


def write_packet(
    pairs: pl.DataFrame,
    *,
    units: pl.LazyFrame,
    crs: pl.LazyFrame,
    out: Path,
    title: str,
    show_ids: bool,
    names: pl.DataFrame | None = None,
    appendix: str | None = None,
) -> Path:
    """Write the packet for `pairs` to `out` and its pairs beside it.

    Args:
        pairs: `left_id` (a disbursement unit) and `right_id` (a commitment unit),
            numbered from 1 in the order given, with any of `CARRIED`; `task`
            defaults to `trace`.
        units: the link stage's `units`. The siblings count reads all of them.
        crs: the CRS records the units sum, as the normalise stage writes them.
        out: the packet's path.
        title: the packet's heading.
        show_ids: whether the sections show crs ids and project numbers.
        names: `code_names` output; without it, codes show without names.
        appendix: Markdown to put after the pairs.

    Returns:
        `out`.

    Raises:
        LabelPacketError: when a pair names a unit `units` lacks, or a packet
            meant to hide ids still names one.
    """
    numbered = (
        pairs.with_columns(
            pl.lit(None, dtype=pl.String).alias(name)
            for name in CARRIED
            if name not in pairs.columns
        )
        .with_columns(task=pl.col("task").fill_null("trace"))
        .with_row_index("pair", offset=1)
        .with_columns(pl.col("pair").cast(pl.Int32))
    )
    text = render_packet(
        numbered, units=units, crs=crs, title=title, show_ids=show_ids, names=names
    )
    if appendix is not None:
        text = f"{text}\n{appendix}\n"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text, encoding="utf-8")
    numbered.select(
        "pair",
        "task",
        "left_id",
        "right_id",
        *CARRIED[1:],
        packet_sha256=pl.lit(packet_digest(out)),
    ).write_parquet(packet_pairs_path(out))
    return out


def packet_digest(packet: Path) -> str:
    """The sha256 of the packet file at `packet`."""
    return hashlib.sha256(packet.read_bytes()).hexdigest()


def render_packet(
    pairs: pl.DataFrame,
    *,
    units: pl.LazyFrame,
    crs: pl.LazyFrame,
    title: str,
    show_ids: bool,
    names: pl.DataFrame | None = None,
) -> str:
    """The packet text for `pairs` (`pair`, `left_id`, `right_id`); see
    `write_packet`."""
    view = _sides(pairs, units=units, crs=crs, names=names)
    missing = (
        pl.concat([pairs["left_id"], pairs["right_id"]])
        .unique()
        .to_frame("unit_id")
        .join(view, on="unit_id", how="anti")
    )
    if missing.height:
        raise LabelPacketError(
            f"{missing.height} units of the pairs are not in units, such as "
            f"{missing['unit_id'].sort().head(3).to_list()}"
        )
    by_unit = {row["unit_id"]: row for row in view.iter_rows(named=True)}
    rows = _unit_rows(view, crs=crs)
    siblings = _sibling_counts(pairs, view=view, units=units, crs=crs)
    lines = [
        f"# {title}",
        "",
        MATCH_DEFINITION,
        "",
        "Each section shows two aid records from the OECD CRS of one donor. Record A "
        "is a later disbursement; record B is an earlier commitment. Amounts are in US "
        "dollars at current prices, summed over the CRS rows that share the record's "
        "keys, and every such row is listed below the table (up to "
        f"{MAX_ROWS} a side); the texts in the table are those of the largest row. "
        "Similar commitments counts the donor's other commitment records for the same "
        "recipient, B aside, whose titles share at least half their words with the "
        "side's title: siblings, phases and yearly grants to tell apart from B.",
        "",
    ]
    fields = _FIELDS if not show_ids else [*_ID_FIELDS, *_FIELDS]
    for row in pairs.iter_rows(named=True):
        a, b = dict(by_unit[row["left_id"]]), dict(by_unit[row["right_id"]])
        hide = None if show_ids else _id_pattern([u[k] for u in (a, b) for k in _IDS])
        if hide is not None:
            for unit in (a, b):
                unit.update(
                    {
                        k: hide.sub("[id]", unit[k])
                        for k in _TEXTS
                        if unit[k] is not None
                    }
                )
        lines += [
            f"## Pair {row['pair']}",
            "",
            "| Field | A: later disbursement | B: earlier commitment |",
            "| --- | --- | --- |",
            *(f"| {label} | {show(a)} | {show(b)} |" for label, show in fields),
            "| Similar commitments | "
            f"{siblings.get((row['pair'], 'A'), 0)} | "
            f"{siblings.get((row['pair'], 'B'), 0)} |",
            "",
        ]
        for side, unit in (("A", a), ("B", b)):
            frame = rows.get(unit["unit_id"], pl.DataFrame())
            more = f", first {MAX_ROWS}" if frame.height > MAX_ROWS else ""
            lines += [
                f"{side}'s CRS rows ({frame.height}{more}):",
                "",
                "| Year | Commitment date | Commitment | Disbursement | Title |",
                "| --- | --- | --- | --- | --- |",
            ]
            for year, date, commit, disburse, row_title in frame.head(
                MAX_ROWS
            ).iter_rows():
                if hide is not None and row_title is not None:
                    row_title = hide.sub("[id]", row_title)
                lines.append(
                    f"| {year} | {date or '-'} | {_usd(commit)} | {_usd(disburse)} "
                    f"| {_text(row_title)} |"
                )
            lines.append("")
    text = "\n".join(lines)
    if not show_ids:
        _refuse_ids(text, view)
    return text


def _sides(
    pairs: pl.DataFrame,
    *,
    units: pl.LazyFrame,
    crs: pl.LazyFrame,
    names: pl.DataFrame | None,
) -> pl.DataFrame:
    """Each unit of `pairs` with its totals, its largest record's fields and the
    names of its codes."""
    ids = pl.concat([pairs["left_id"], pairs["right_id"]]).unique().to_frame("unit_id")
    wanted = units.join(ids.lazy(), on="unit_id", how="semi").collect()
    records = wanted.select("unit_id", record_id="record_ids").explode("record_id")
    size = pl.max_horizontal(
        pl.col("commitment_usd").abs(), pl.col("disbursement_usd").abs()
    ).fill_null(0.0)
    largest = (
        crs.join(records.lazy(), on="record_id")
        .select(
            "unit_id",
            "record_id",
            "commitment_usd",
            "disbursement_usd",
            "channel_code",
            "flow_code",
            *_TEXTS,
        )
        .collect()
        .sort("unit_id", size, "record_id", descending=[False, True, False])
        .group_by("unit_id", maintain_order=True)
        .agg(pl.col("channel_code", "flow_code", *_TEXTS).first(), records=pl.len())
    )
    view = wanted.join(largest, on="unit_id", how="left")
    view = view.join(
        catalogue.providers.list().select(donor_code="code", donor_name="name"),
        on="donor_code",
        how="left",
    ).join(
        catalogue.recipients.list().select(
            recipient_code="code", recipient_name="name"
        ),
        on="recipient_code",
        how="left",
    )
    named = (
        names
        if names is not None
        else pl.DataFrame(
            schema={
                "kind": pl.String,
                "donor_code": pl.Int32,
                "code": pl.Int32,
                "name": pl.String,
            }
        )
    )
    for kind, keys in NAMED_CODES.items():
        lookup = named.filter(kind=kind).select(
            *(["donor_code"] if len(keys) > 1 else []),
            pl.col("code").alias(keys[-1]),
            pl.col("name").alias(f"{kind}_name"),
        )
        view = view.join(
            lookup.cast({key: view.schema[key] for key in keys}),
            on=list(keys),
            how="left",
        )
    return view


def _unit_rows(view: pl.DataFrame, *, crs: pl.LazyFrame) -> dict[str, pl.DataFrame]:
    """Every CRS row of each unit of `view`: year, commitment date, amounts, title."""
    records = view.select("unit_id", record_id="record_ids").explode("record_id")
    columns = ("year", "commitment_date", "commitment_usd", "disbursement_usd", "title")
    rows = (
        crs.join(records.lazy(), on="record_id")
        .select("unit_id", *columns)
        .collect()
        .sort("unit_id", *columns, nulls_last=True)
    )
    return {
        unit_id: frame.drop("unit_id")
        for (unit_id,), frame in rows.partition_by("unit_id", as_dict=True).items()
    }


def _sibling_counts(
    pairs: pl.DataFrame,
    *,
    view: pl.DataFrame,
    units: pl.LazyFrame,
    crs: pl.LazyFrame,
) -> dict[tuple[int, str], int]:
    """For each pair and side, how many of the donor's commitment units for that
    side's recipient, other than B and the side's own unit, share at least
    `SIBLING_SHARE` of their title words with the side's title."""
    place = ["donor_code", "recipient_code"]
    sides = pl.concat(
        [
            pairs.select("pair", "right_id", side=pl.lit("A"), unit_id="left_id"),
            pairs.select("pair", "right_id", side=pl.lit("B"), unit_id="right_id"),
        ]
    ).join(view.select("unit_id", *place), on="unit_id")
    commitments = (
        units.filter(pl.col("commitment_usd") > 0)
        .join(sides.select(place).unique().lazy(), on=place, how="semi")
        .collect()
    )
    toks = title_words(
        pl.concat(
            [commitments, view.join(commitments, on="unit_id", how="anti")],
            how="diagonal_relaxed",
        ).select(commitments.columns),
        crs=crs,
    )
    others = commitments.select(*place, other="unit_id").join(
        toks.rename({"unit_id": "other", "title_toks": "other_toks"}), on="other"
    )
    counted = (
        sides.join(toks, on="unit_id", how="left")
        .join(others, on=place)
        .filter(
            pl.col("other") != pl.col("right_id"),
            pl.col("other") != pl.col("unit_id"),
            word_share(pl.col("title_toks"), pl.col("other_toks")) >= SIBLING_SHARE,
        )
        .group_by("pair", "side")
        .len()
    )
    return {(pair, side): n for pair, side, n in counted.iter_rows()}


def _id_pattern(values: list[object]) -> re.Pattern[str] | None:
    """A word-bounded pattern for the values that can name a record: four or more
    characters with a digit, since a bare word such as "SIDA" names an agency."""
    kept = sorted(
        {
            str(value).strip()
            for value in values
            if value is not None
            and len(str(value).strip()) >= 4
            and re.search(r"\d", str(value))
        },
        key=len,
        reverse=True,
    )
    if not kept:
        return None
    return re.compile(r"(?<!\w)(?:" + "|".join(map(re.escape, kept)) + r")(?!\w)")


def _refuse_ids(text: str, view: pl.DataFrame) -> None:
    """Refuse a packet that names a unit id, record id, crs id or project number of
    its units."""
    found: set[str] = set()
    for unit in view.iter_rows(named=True):
        pattern = _id_pattern(
            [unit["unit_id"], *(unit[k] for k in _IDS), *unit["record_ids"]]
        )
        if pattern is not None:
            found |= set(pattern.findall(text))
    if found:
        raise LabelPacketError(
            f"a packet meant to hide ids names {len(found)} of them, such as "
            f"{sorted(found)[:3]}"
        )


def _text(value: object) -> str:
    if value is None:
        return "-"
    text = " ".join(str(value).split()).replace("|", "/")
    return text if len(text) <= TEXT_LIMIT else text[:TEXT_LIMIT] + " [...]"


def _usd(value: float | None) -> str:
    return "-" if value is None or value == 0 else f"USD {value:,.0f}"


def _code(unit: Mapping[str, Any], kind: str, column: str) -> str:
    code, name = unit[column], unit[f"{kind}_name"]
    if code is None:
        return "-"
    return f"{_text(name)} ({code})" if name is not None else str(code)


type _Field = tuple[str, Callable[[Mapping[str, Any]], str]]

_ID_FIELDS: list[_Field] = [
    ("crs id", lambda u: _text(u["crs_id"])),
    ("Project number", lambda u: _text(u["project_number"])),
]
_FIELDS: list[_Field] = [
    ("Donor", lambda u: _code(u, "donor", "donor_code")),
    ("Agency", lambda u: _code(u, "agency", "agency_code")),
    ("Recipient", lambda u: _code(u, "recipient", "recipient_code")),
    ("Record year", lambda u: str(u["year"])),
    ("Commitment date", lambda u: str(u["commitment_date"] or "-")),
    ("Flow", lambda u: _code(u, "flow", "flow_code")),
    ("Purpose", lambda u: _code(u, "purpose", "purpose_code")),
    ("Channel", lambda u: _code(u, "channel", "channel_code")),
    ("Commitment", lambda u: _usd(u["commitment_usd"])),
    ("Disbursement", lambda u: _usd(u["disbursement_usd"])),
    ("CRS rows summed", lambda u: str(u["records"])),
    ("Title", lambda u: _text(u["title"])),
    ("Short description", lambda u: _text(u["short_description"])),
    ("Long description", lambda u: _text(u["long_description"])),
]

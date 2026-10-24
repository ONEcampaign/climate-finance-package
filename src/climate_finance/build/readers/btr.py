"""BTR1 Table III.1 workbooks, read by header role.

Parties add, drop and reorder columns, so each column's role comes from its header
label: a text column's from the header row's label prefix, an amount column's from
its currency label and the "Face value" or "Grant equivalent" label spanning it.
"""

import json
import re
import warnings
import zipfile
from collections import Counter
from collections.abc import Iterator, Sequence
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from statistics import median
from typing import IO, NamedTuple

import openpyxl
import polars as pl

from climate_finance import schema
from climate_finance.build.geo import provider_code
from climate_finance.catalogue import recipients
from climate_finance.errors import SourceUnavailableError, UnknownRecipientError

# Manifest party name -> (ISO 3 code, domestic currency in 2021 and 2022).
_PARTIES: dict[str, tuple[str, str]] = {
    "Algeria": ("DZA", "DZD"),
    "Armenia": ("ARM", "AMD"),
    "Australia": ("AUS", "AUD"),
    "Austria": ("AUT", "EUR"),
    "Belarus": ("BLR", "BYN"),
    "Belgium": ("BEL", "EUR"),
    "Bulgaria": ("BGR", "BGN"),
    "Burkina Faso": ("BFA", "XOF"),
    "Canada": ("CAN", "CAD"),
    "Chile": ("CHL", "CLP"),
    "China": ("CHN", "CNY"),
    "Colombia": ("COL", "COP"),
    "Croatia": ("HRV", "HRK"),
    "Cuba": ("CUB", "CUP"),
    "Cyprus": ("CYP", "EUR"),
    "Czechia": ("CZE", "CZK"),
    "Côte d'Ivoire": ("CIV", "XOF"),
    "Denmark": ("DNK", "DKK"),
    "Dominican Republic": ("DOM", "DOP"),
    "Ecuador": ("ECU", "USD"),
    "Egypt": ("EGY", "EGP"),
    "Estonia": ("EST", "EUR"),
    "European Union": ("EUU", "EUR"),
    "Finland": ("FIN", "EUR"),
    "France": ("FRA", "EUR"),
    "Georgia": ("GEO", "GEL"),
    "Germany": ("DEU", "EUR"),
    "Ghana": ("GHA", "GHS"),
    "Greece": ("GRC", "EUR"),
    "Guinea-Bissau": ("GNB", "XOF"),
    "Guyana": ("GUY", "GYD"),
    "Honduras": ("HND", "HNL"),
    "Hungary": ("HUN", "HUF"),
    "Iceland": ("ISL", "ISK"),
    "India": ("IND", "INR"),
    "Indonesia": ("IDN", "IDR"),
    "Ireland": ("IRL", "EUR"),
    "Israel": ("ISR", "ILS"),
    "Italy": ("ITA", "EUR"),
    "Japan": ("JPN", "JPY"),
    "Jordan": ("JOR", "JOD"),
    "Kazakhstan": ("KAZ", "KZT"),
    "Kenya": ("KEN", "KES"),
    "Lao People's Democratic Republic": ("LAO", "LAK"),
    "Latvia": ("LVA", "EUR"),
    "Lebanon": ("LBN", "LBP"),
    "Liechtenstein": ("LIE", "CHF"),
    "Lithuania": ("LTU", "EUR"),
    "Luxembourg": ("LUX", "EUR"),
    "Madagascar": ("MDG", "MGA"),
    "Malawi": ("MWI", "MWK"),
    "Maldives": ("MDV", "MVR"),
    "Malta": ("MLT", "EUR"),
    "Mauritius": ("MUS", "MUR"),
    "Mexico": ("MEX", "MXN"),
    "Monaco": ("MCO", "EUR"),
    "Mongolia": ("MNG", "MNT"),
    "Morocco": ("MAR", "MAD"),
    "Namibia": ("NAM", "NAD"),
    "Netherlands": ("NLD", "EUR"),
    "New Zealand": ("NZL", "NZD"),
    "Niger": ("NER", "XOF"),
    "Nigeria": ("NGA", "NGN"),
    "North Macedonia": ("MKD", "MKD"),
    "Norway": ("NOR", "NOK"),
    "Pakistan": ("PAK", "PKR"),
    "Panama": ("PAN", "PAB"),
    "Papua New Guinea": ("PNG", "PGK"),
    "Philippines": ("PHL", "PHP"),
    "Poland": ("POL", "PLN"),
    "Portugal": ("PRT", "EUR"),
    "Republic of Moldova": ("MDA", "MDL"),
    "Romania": ("ROU", "RON"),
    "Rwanda": ("RWA", "RWF"),
    "Serbia": ("SRB", "RSD"),
    "Singapore": ("SGP", "SGD"),
    "Slovakia": ("SVK", "EUR"),
    "Slovenia": ("SVN", "EUR"),
    "Solomon Islands": ("SLB", "SBD"),
    "South Africa": ("ZAF", "ZAR"),
    "Spain": ("ESP", "EUR"),
    "Sri Lanka": ("LKA", "LKR"),
    "Sweden": ("SWE", "SEK"),
    "Switzerland": ("CHE", "CHF"),
    "Türkiye": ("TUR", "TRY"),
    "Ukraine": ("UKR", "UAH"),
    "United Arab Emirates": ("ARE", "AED"),
    "United Kingdom": ("GBR", "GBP"),
    "United States of America": ("USA", "USD"),
    "Uruguay": ("URY", "UYU"),
    "Uzbekistan": ("UZB", "UZS"),
    "Venezuela": ("VEN", "VES"),
    "Zimbabwe": ("ZWE", "ZWL"),
}
_CURRENCIES = dict(_PARTIES.values())
# The EU reports as "EUU". The geo pack gives it no provider code.
_PARTY_PROVIDERS = {"EUU": 918}

# Parties whose Table III.1 amounts are in millions. Their workbooks do not say so.
_MILLIONS = frozenset({"GBR", "USA"})
# A sheet whose median amount is below this reads as millions. It checks `_MILLIONS`
# against the data: those two parties' sheets have medians of 0.5 to 1.4, every
# other party's are above 1,000.
_MILLIONS_BELOW = 10.0

# Indonesia's sheet names no year. Each row's year sits in its "Additional
# information" cell.
_YEAR_ROLES = {"IDN": "additional_information"}

# Rows that each cover a whole channel or programme of a party.
_AGGREGATE_TITLES = frozenset(
    {
        ("FIN", "Other bilateral projects"),
        ("FIN", "Finnfund climate-related investments"),
        ("FIN", "Finnfund climate-related OOF-loans"),
    }
)

# "Table1_2021", "Table 1_2022", "Table1-2021" and Canada's "Table III.1".
_SHEET = re.compile(r"Table\s*(?:III\.)?\s*1(?:\s*[_\- ]\s*(\d{4}))?", re.IGNORECASE)
_NOTATION = frozenset({"NA", "N/A", "UA", "NR", "NE", "NO", "IE", "C", "-"})
_THOUSANDS = re.compile(r"-?\d{1,3}(?:,\d{3})+(?:\.\d+)?")
_TOTAL = re.compile(r"(sub-?\s?)?total\b", re.IGNORECASE)
_CURRENCY_LABEL = re.compile(r"usd|domestic\s*currency")
# Text roles by header label prefix. Footnote letters stay glued to labels
# ("statusc"), so prefixes match them.
_TEXT_ROLES = (
    ("recipient", "recipient"),
    ("title", "title"),
    ("status", "status"),
    ("channel", "channel"),
    ("funding_source", "funding source"),
    ("instrument", "financial instrument"),
    ("support_type", "type of support"),
    ("sector", "sector"),
    ("additional_information", "additional information"),
    ("year", "year"),
)
_AMOUNTS = ("face_domestic", "face_usd", "grant_equiv_domestic", "grant_equiv_usd")
# Belgium reports its disbursements as "provided". Chile's "terminado" (finished)
# and "en curso" (ongoing) leave open whether the amount is committed or disbursed.
_STATUSES = {
    "committed": "committed",
    "disbursed": "disbursed",
    "provided": "disbursed",
    "terminado": None,
    "en curso": None,
}
_WORKBOOK_SUFFIXES = (".xlsx", ".xlsm")
# How far a sheet's stated rate may sit from the median rate its rows imply.
_RATE_TOLERANCE = 0.05

# Ratio levels a party's BTR amount takes of the linked CRS amount, and how close to
# a level a pair's ratio must be to count at it.
WEIGHT_LEVELS = (0.4, 0.5, 1.0)
LEVEL_TOLERANCE = 0.02
_SIGNIFICANT = 1
_PRINCIPAL = 2
type _Amounts = dict[str, float | None]

LINKED_PAIRS_COLUMNS = ("party", "btr_usd", "crs_usd", "rio_marker")
# The archive's list of workbooks.
MANIFEST = "manifest.json"
# Why a sheet row gives no `btr` row: a title with no amount, a notation key in an
# amount cell, or a total.
DROP_REASONS = ("blank", "notation", "total")
# The UNFCCC reporting tool writes a workbook's state into the exported file name.
# These two mark a workbook the party had not submitted when it was exported.
_DRAFT = re.compile(r"(?<![a-z])(started|awaiting_submission)(?![a-z])", re.IGNORECASE)


class BtrArchive(NamedTuple):
    """The archive's rows in the `btr` schema, and one `btr_workbooks` row per
    workbook saying what was read from it."""

    rows: pl.DataFrame
    workbooks: pl.DataFrame


def read_btr_workbook(path: Path, *, party: str) -> pl.DataFrame:
    """One party's Table III.1 rows in the `btr` schema.

    `path` is a workbook, or a zip of workbooks as Canada and the EU ship them.
    Every Table III.1 sheet gives a row per activity with an amount. Rows whose four
    amount cells are empty or a notation key ("NA") are dropped. Germany, Norway and
    Spain list every activity on both year sheets and leave the other year empty.
    Rows labelled as totals are dropped. Amounts of the parties in `_MILLIONS` are
    multiplied by a million. A USD amount the party leaves empty is filled from the
    domestic amount at the sheet's stated rate (domestic currency per USD). Status is
    `committed` or `disbursed`; a notation key gives null. `dac_rate` and the amounts
    restated at it are null; the normalise stage fills them.

    Args:
        path: The workbook or zip as the archive holds it.
        party: The party's ISO 3 code.

    Raises:
        ValueError: when a sheet's layout cannot be read, its amounts contradict the
            party's unit, a USD amount must be filled without a stated rate or at a
            rate the sheet's own rows contradict, or a status is outside the
            vocabulary.
    """
    return _frame(_read_workbook(path, party=party)[0])


def ingest_unfccc(archive_dir: Path, *, out_path: Path) -> Path:
    """Parse every workbook listed in the archive manifest into one parquet.

    Beside `out_path`, `<stem>.manifest.parquet` records each workbook in the
    `raw_manifest` schema: its path, sha256 and modification time, with method
    `manual` since a person downloads the files in a browser. A later build compares
    the hashes to detect a changed workbook.

    Raises:
        SourceUnavailableError: as `read_btr_archive`.
        ValueError: as `read_btr_archive`.
    """
    btr = read_btr_archive(archive_dir).rows
    out_path.parent.mkdir(parents=True, exist_ok=True)
    btr.sort(schema.BTR.sort_key).write_parquet(out_path)
    manifest = _raw_manifest([path for path, _ in archive_files(archive_dir)])
    schema.validate(schema.RAW_MANIFEST, manifest)
    manifest.sort(schema.RAW_MANIFEST.sort_key).write_parquet(
        out_path.with_suffix(".manifest.parquet")
    )
    return out_path


def read_btr_archive(archive_dir: Path) -> BtrArchive:
    """Every workbook the archive manifest lists, as one `btr` table, with what was
    read from each workbook.

    Each party's year must come from one sheet. A draft and a final copy of a
    workbook both giving rows stop the read. A workbook row of `btr_workbooks` names
    its party, the Table III.1 sheets read (as `btr.source_file` names them, none for
    a workbook without one), the rows kept, the sheet rows dropped for each of
    `DROP_REASONS`, and `draft_marker`, the tool state in the file name that marks
    an unsubmitted workbook.

    Raises:
        SourceUnavailableError: as `archive_files`.
        ValueError: for a party's year given by more than one sheet, or a sheet
            `read_btr_workbook` refuses.
    """
    read = [
        _read_workbook(path, party=party) for path, party in archive_files(archive_dir)
    ]
    btr = pl.concat(_frame(rows) for rows, _ in read)
    sheets = btr.group_by("party", "year").agg(pl.col("source_file").unique().sort())
    repeated = sheets.filter(pl.col("source_file").list.len() > 1)
    if not repeated.is_empty():
        raise ValueError(f"Several sheets give one party's year: {repeated.rows()}")
    schema.validate(schema.BTR, btr)
    workbooks = pl.DataFrame(
        [entry for _, entry in read],
        schema={
            name: column.dtype for name, column in schema.BTR_WORKBOOKS.columns.items()
        },
    )
    schema.validate(schema.BTR_WORKBOOKS, workbooks)
    return BtrArchive(
        rows=btr.sort(schema.BTR.sort_key),
        workbooks=workbooks.sort(schema.BTR_WORKBOOKS.sort_key),
    )


def archive_files(archive_dir: Path) -> list[tuple[Path, str]]:
    """Each workbook the archive manifest (`manifest.json`) lists, with its party's
    ISO 3 code. The manifest gives each file's party name, file name, size in bytes
    and whether its download succeeded.

    Raises:
        SourceUnavailableError: when the manifest or a listed file is missing, a
            file's size differs from the manifest, or a download is marked failed.
        ValueError: for an unknown party name.
    """
    manifest_path = archive_dir / MANIFEST
    if not manifest_path.exists():
        raise SourceUnavailableError(f"No BTR manifest at {manifest_path}")
    files = []
    for entry in json.loads(manifest_path.read_text(encoding="utf-8")):
        path = archive_dir / entry["name"]
        if not entry["ok"]:
            raise SourceUnavailableError(f"{entry['name']}: download marked failed")
        if not path.exists():
            raise SourceUnavailableError(f"{entry['name']}: listed but missing")
        if path.stat().st_size != entry["bytes"]:
            raise SourceUnavailableError(
                f"{entry['name']}: {path.stat().st_size} bytes, manifest says "
                f"{entry['bytes']}"
            )
        if entry["party"] not in _PARTIES:
            raise ValueError(f"No ISO 3 code for the party {entry['party']!r}")
        files.append((path, _PARTIES[entry["party"]][0]))
    return files


def _raw_manifest(files: list[Path]) -> pl.DataFrame:
    """One `raw_manifest` row per archived workbook, with no years."""
    return pl.DataFrame(
        [
            {
                "source": "unfccc_btr1",
                "path": str(path),
                "sha256": schema.sha256_file(path),
                "fetched_at": datetime.fromtimestamp(path.stat().st_mtime, tz=UTC),
                "method": "manual",
            }
            for path in files
        ],
        schema=schema.RAW_MANIFEST.dtypes,
    )


def estimate_party_weights(pairs: pl.DataFrame) -> pl.DataFrame:
    """Each party's weight on Significant activities, with the evidence for it.

    A pair's ratio is its BTR amount over the CRS amount it is set against. A ratio
    within `LEVEL_TOLERANCE` of 0.4, 0.5 or 1.0 counts at that level. The party's
    significant coefficient is the level holding the most of its pairs whose CRS
    record is marked Significant, the lower level on a tie, and null when none of
    those pairs sits at a level.

    Args:
        pairs: Linked BTR and CRS pairs with the columns in `LINKED_PAIRS_COLUMNS`:
            `party` (ISO 3), `btr_usd`, `crs_usd` (the CRS commitment or
            disbursement, following the BTR row's status) and `rio_marker` (the CRS
            record's highest climate Rio marker, 0, 1 or 2).

    Returns:
        One row per party: `pairs`, `share_at` and `significant_share_at` (structs
        of the share of all pairs and of Significant pairs at each level, null with
        no such pairs),
        `principal_at_100` (the share of pairs at 1.0 marked Principal, null with
        none at 1.0) and
        `significant`.

    Raises:
        ValueError: when `pairs` lacks a column.
    """
    missing = sorted(set(LINKED_PAIRS_COLUMNS) - set(pairs.columns))
    if missing:
        raise ValueError(f"Linked pairs lack the columns {missing}")
    ratio = pl.col("btr_usd") / pl.col("crs_usd")
    level = pl.lit(None, dtype=pl.Float64)
    for value in WEIGHT_LEVELS:
        level = (
            pl.when((ratio / value - 1).abs() <= LEVEL_TOLERANCE)
            .then(pl.lit(value))
            .otherwise(level)
        )
    significant = pl.col("rio_marker") == _SIGNIFICANT
    at_100 = pl.col("level") == 1.0

    def shares(within: pl.Expr) -> pl.Expr:
        return pl.struct(
            pl.when(within.sum() > 0)
            .then(((pl.col("level") == value) & within).sum() / within.sum())
            .alias(f"{value:.1f}")
            for value in WEIGHT_LEVELS
        )

    return (
        pairs.filter(pl.col("crs_usd") > 0)
        .with_columns(level=level)
        .group_by("party")
        .agg(
            pairs=pl.len().cast(pl.Int32),
            share_at=shares(pl.col("party").is_not_null()),
            significant_share_at=shares(significant),
            principal_at_100=pl.when(at_100.sum() > 0).then(
                (at_100 & (pl.col("rio_marker") == _PRINCIPAL)).sum() / at_100.sum()
            ),
            significant=pl.col("level")
            .filter(significant & pl.col("level").is_not_null())
            .mode()
            .min(),
        )
        .sort("party")
    )


def check_party_weights(
    pairs: pl.DataFrame, *, coefficients: pl.DataFrame
) -> pl.DataFrame:
    """Parties whose fitted significant coefficient differs from the one their
    provider states.

    The fitted coefficient is `estimate_party_weights`'s. The stated one is the
    `significant` of each row of `coefficients` (a `provider_coefficients` table)
    that covers every agency of the party's provider and a year of its pairs, so a
    provider whose coefficient changes within those years gives a row per span. The
    fit and `share_at_stated` read the pairs of the span's years. A row with no
    stated coefficient, as
    for a provider that assesses each activity case by case, and a party with no
    fitted coefficient are left out. The fit only takes the levels in
    `WEIGHT_LEVELS`, so a stated coefficient off them, such as 0.3, always differs;
    `share_at_stated` is the share of the party's Significant pairs whose ratio sits
    within `LEVEL_TOLERANCE` of the stated coefficient, which says whether the pairs
    bear it out.

    Args:
        pairs: Linked BTR and CRS pairs, as `estimate_party_weights` takes them,
            with the BTR row's `year`.
        coefficients: The release's `provider_coefficients` table.

    Returns:
        One row per party and span: `party`, `provider_code`, `first_year`,
        `last_year`, `stated`, `fitted`, `share_at_stated`, `pairs`, and `source`
        (the stated coefficient's source and locator).

    Raises:
        ValueError: when `pairs` has no `year`.
    """
    if "year" not in pairs.columns:
        raise ValueError("Linked pairs lack the column year")
    ratio = pl.col("btr_usd") / pl.col("crs_usd")
    stated = pl.col("stated")
    significant = pl.col("rio_marker") == _SIGNIFICANT
    at_stated = significant & ((ratio - stated).abs() <= LEVEL_TOLERANCE * stated)
    first, last, year = pl.col("first_year"), pl.col("last_year"), pl.col("year")
    providers = {party: party_provider(party) for party in pairs["party"].unique()}
    spans = (
        pairs.with_columns(
            provider_code=pl.col("party").replace_strict(
                providers, return_dtype=pl.Int32
            )
        )
        .join(
            coefficients.filter(
                pl.col("agency_code").is_null(), pl.col("significant").is_not_null()
            ).select(
                "provider_code",
                "first_year",
                "last_year",
                stated="significant",
                source=pl.format("{}, {}", "source", "locator"),
            ),
            on="provider_code",
        )
        .filter(first.is_null() | (first <= year), last.is_null() | (year <= last))
    )
    span = ["party", "provider_code", "first_year", "last_year", "stated", "source"]
    rows = []
    for key, members in spans.partition_by(span, as_dict=True).items():
        estimate = estimate_party_weights(members)
        if estimate.is_empty():
            continue
        fitted = estimate.row(0, named=True)
        share = (
            members.filter(pl.col("crs_usd") > 0)
            .select(
                pl.when(significant.sum() > 0).then(at_stated.sum() / significant.sum())
            )
            .item()
        )
        rows.append(
            dict(zip(span, key, strict=True))
            | {
                "fitted": fitted["significant"],
                "share_at_stated": share,
                "pairs": fitted["pairs"],
            }
        )
    columns = {
        "party": pl.String,
        "provider_code": pl.Int32,
        "first_year": pl.Int16,
        "last_year": pl.Int16,
        "stated": pl.Float64,
        "fitted": pl.Float64,
        "share_at_stated": pl.Float64,
        "pairs": pl.Int32,
        "source": pl.String,
    }
    return (
        pl.DataFrame(rows, schema=columns)
        .filter(pl.col("fitted").is_not_null(), pl.col("fitted") != stated)
        .sort("party", "first_year", nulls_last=False)
    )


def _workbooks(path: Path) -> Iterator[tuple[str, IO[bytes]]]:
    """Each workbook in `path` with its name: the file, or each workbook in a zip."""
    if path.suffix.lower() != ".zip":
        with path.open("rb") as handle:
            yield path.name, handle
        return
    with zipfile.ZipFile(path) as archive:
        for member in sorted(archive.namelist()):
            if member.lower().endswith(_WORKBOOK_SUFFIXES):
                yield f"{path.name}/{member}", BytesIO(archive.read(member))


def _read_workbook(
    path: Path, *, party: str
) -> tuple[list[dict[str, object]], dict[str, object]]:
    """A workbook's rows, and its `btr_workbooks` row."""
    rows: list[dict[str, object]] = []
    sheets: list[str] = []
    dropped: Counter[str] = Counter()
    for member, handle in _workbooks(path):
        for title, year, sheet in _table_sheets(handle):
            source_file = f"{member}#{title}"
            kept, lost = _sheet_rows(
                sheet, party=party, year=year, source_file=source_file
            )
            rows.extend(kept)
            sheets.append(source_file)
            dropped.update(lost)
    draft = _DRAFT.search(path.name)
    return rows, {
        "file": path.name,
        "party": party,
        "sheets": sheets,
        "rows_kept": len(rows),
        **{f"dropped_{reason}": dropped[reason] for reason in DROP_REASONS},
        "draft_marker": draft[1].lower() if draft else None,
    }


def _table_sheets(
    handle: IO[bytes],
) -> list[tuple[str, int | None, list[tuple[object, ...]]]]:
    """Each Table III.1 sheet in a workbook: its title, the year its title names and
    its cell values."""
    with warnings.catch_warnings():
        # openpyxl drops the cells' input rules, which leaves their values as they are.
        warnings.filterwarnings("ignore", "Data Validation extension", UserWarning)
        workbook = openpyxl.load_workbook(handle, read_only=True, data_only=True)
        try:
            return [
                (
                    sheet.title,
                    int(match[1]) if match[1] else None,
                    list(sheet.iter_rows(values_only=True)),
                )
                for sheet in workbook.worksheets
                if (match := _SHEET.fullmatch(sheet.title.strip()))
            ]
        finally:
            workbook.close()


def _sheet_rows(
    sheet: Sequence[Sequence[object]],
    *,
    party: str,
    year: int | None,
    source_file: str,
) -> tuple[list[dict[str, object]], Counter[str]]:
    """The activity rows of one Table III.1 sheet, amounts in USD units, and the
    count of rows dropped for each of `DROP_REASONS`. A row with neither a title
    nor an amount, such as a footnote, is not counted."""
    header, first_data = _header_block(sheet, source_file=source_file)
    columns = _column_roles(sheet[header:first_data], source_file=source_file)
    rows: list[dict[str, object]] = []
    amount_rows: list[_Amounts] = []
    dropped: Counter[str] = Counter()
    for number, cells in enumerate(sheet[first_data:], start=first_data + 1):
        record = {role: _cell(cells, column) for role, column in columns.items()}
        cells_read = {role: record.pop(role) for role in _AMOUNTS}
        amounts = {role: _number(value) for role, value in cells_read.items()}
        labels = {role: _text(value) for role, value in record.items()}
        if all(amount is None for amount in amounts.values()):
            if any(_is_notation(value) for value in cells_read.values()):
                dropped["notation"] += 1
            elif labels.get("title"):
                dropped["blank"] += 1
            continue
        if any(_TOTAL.match(labels.get(role) or "") for role in ("recipient", "title")):
            dropped["total"] += 1
            continue
        row_year = _number(labels.pop(_YEAR_ROLES.get(party, "year"), None)) or year
        labels.pop("year", None)
        if row_year is None:
            raise ValueError(f"{source_file} row {number}: no year in sheet or row")
        rows.append(
            {
                "party": party,
                "year": int(row_year),
                "row": number,
                "source_file": source_file,
                **labels,
            }
        )
        amount_rows.append(amounts)
    if amount_rows:
        _fill_usd(
            amount_rows, rate=_exchange_rate(sheet), party=party, sheet=source_file
        )
    kept = [{**row, **amounts} for row, amounts in zip(rows, amount_rows, strict=True)]
    return kept, dropped


def _header_block(
    sheet: Sequence[Sequence[object]], *, source_file: str
) -> tuple[int, int]:
    """The header row's index and the first data row's index.

    The header row holds the "Recipient" label. The block ends at the row holding the
    currency labels, or at the "Face value" row under it where a party puts it there
    (the USA).
    """
    header = next(
        (
            i
            for i, row in enumerate(sheet)
            if any(_label(cell).startswith("recipient") for cell in row)
        ),
        None,
    )
    if header is None:
        raise ValueError(f"{source_file}: no 'Recipient' header row")
    end = next(
        (
            i
            for i in range(header, min(header + 4, len(sheet)))
            if any(_CURRENCY_LABEL.match(_label(cell)) for cell in sheet[i])
        ),
        None,
    )
    if end is None:
        raise ValueError(f"{source_file}: no currency labels under the header row")
    while end + 1 < len(sheet) and _is_value_label_row(sheet[end + 1]):
        end += 1
    return header, end + 1


def _is_value_label_row(row: Sequence[object]) -> bool:
    return not any(isinstance(cell, int | float) for cell in row) and any(
        _value_kind(_label(cell)) for cell in row
    )


def _value_kind(label: str) -> str | None:
    if label.startswith("face value"):
        return "face"
    if label.startswith("grant equivalent"):
        return "grant_equiv"
    return None


def _column_roles(
    block: Sequence[Sequence[object]], *, source_file: str
) -> dict[str, int]:
    """Each role's column. A text column takes the role of its topmost label that
    names one (Canada repeats the labels in a lower row). A "Face value" or "Grant
    equivalent" label spans right to the next label in its row. A currency column
    takes the one above or below it."""
    width = max(len(row) for row in block)
    labels = [
        [_label(row[j]) if j < len(row) else "" for j in range(width)] for row in block
    ]
    roles: dict[str, int] = {}
    for j in range(width):
        role = next(
            (
                role
                for row in labels
                for role, prefix in _TEXT_ROLES
                if row[j].startswith(prefix)
            ),
            None,
        )
        if role is not None and role not in roles:
            roles[role] = j
    kinds: list[str | None] = [None] * width
    for row in labels:
        current = None
        for j, label in enumerate(row):
            if label:
                current = _value_kind(label)
            kinds[j] = kinds[j] or current
    for j in range(width):
        currency = next(
            (row[j] for row in labels if _CURRENCY_LABEL.match(row[j])), None
        )
        if currency is None:
            continue
        if kinds[j] is None:
            raise ValueError(f"{source_file}: column {j + 1} has no face value label")
        role = f"{kinds[j]}_{'usd' if currency.startswith('usd') else 'domestic'}"
        if role in roles:
            raise ValueError(f"{source_file}: two {role} columns")
        roles[role] = j
    missing = [role for role in ("recipient", "title", *_AMOUNTS) if role not in roles]
    if missing:
        raise ValueError(f"{source_file}: no columns for {missing}")
    return roles


def _fill_usd(
    amount_rows: list[_Amounts], *, rate: float | None, party: str, sheet: str
) -> None:
    """Scale a sheet's amounts to units and fill each empty USD amount at the sheet's
    rate, in place."""
    positive = [
        v for row in amount_rows for v in row.values() if v is not None and v > 0
    ]
    in_millions = bool(positive) and median(positive) < _MILLIONS_BELOW
    if in_millions != (party in _MILLIONS):
        unit = "millions" if in_millions else "units"
        raise ValueError(f"{sheet}: amounts read as {unit}, unlike the party's unit")
    scale = 1e6 if in_millions else 1.0
    for row in amount_rows:
        for role, value in row.items():
            if value is not None:
                row[role] = value * scale
    for kind in ("face", "grant_equiv"):
        domestic, usd = f"{kind}_domestic", f"{kind}_usd"
        to_fill = sum(
            row[usd] is None and row[domestic] is not None for row in amount_rows
        )
        if not to_fill:
            continue
        if rate is None:
            raise ValueError(f"{sheet}: {to_fill} {usd} to fill, no rate stated")
        implied = [
            d / u
            for row in amount_rows
            if (d := row[domestic]) is not None
            and (u := row[usd]) is not None
            and d > 0
            and u > 0
        ]
        if implied and abs(median(implied) / rate - 1) > _RATE_TOLERANCE:
            raise ValueError(
                f"{sheet}: stated rate {rate} differs from the rate its rows imply, "
                f"{median(implied):.4g}"
            )
        for row in amount_rows:
            if row[usd] is None and (d := row[domestic]) is not None:
                row[usd] = d / rate


def _exchange_rate(sheet: Sequence[Sequence[object]]) -> float | None:
    """The sheet's stated rate in domestic currency per USD.

    It sits in a cell after the "Exchange rate used" label, or inside the label
    (Belgium). "1 EUR = 1.183 USD" and "$1=0.727 GBP" give the domestic amount of
    one dollar.
    """
    for row in sheet[:6]:
        cells = [cell for cell in row if cell not in (None, "")]
        label = next(
            (
                k
                for k, cell in enumerate(cells)
                if isinstance(cell, str) and _label(cell).startswith("exchange rate")
            ),
            None,
        )
        if label is None:
            continue
        inside = str(cells[label]).split(":", 1)[-1]
        for candidate in (inside, *cells[label + 1 :]):
            if isinstance(candidate, int | float):
                return float(candidate)
            if (rate := _rate_text(str(candidate))) is not None:
                return rate
    return None


_RATE_PAIR = re.compile(
    r"1\s*([a-z$]+)\s*=\s*(\d+(?:[.,]\d+)?)\s*([a-z]*)", re.IGNORECASE
)


def _rate_text(text: str) -> float | None:
    text = re.sub(r"\$\s*1\b", "1 USD", text)
    if pair := _RATE_PAIR.search(text):
        left, value, right = pair[1].upper(), _decimal(pair[2]), pair[3].upper()
        if left == "USD":
            return value
        if right == "USD":
            return 1 / value
        raise ValueError(f"Exchange rate {text!r} does not state a USD rate")
    number = re.search(r"\d+(?:[.,]\d+)?", text)
    return _decimal(number[0]) if number else None


def _decimal(text: str) -> float:
    """A rate written with a decimal comma ("303,29") or point."""
    return float(text.replace(",", "."))


def _frame(rows: list[dict[str, object]]) -> pl.DataFrame:
    """Rows of `_sheet_rows` as the `btr` table."""
    frame = pl.DataFrame(
        rows,
        schema={
            "party": pl.String,
            "year": pl.Int16,
            "row": pl.Int32,
            "source_file": pl.String,
            **{role: pl.String for role, _ in _TEXT_ROLES if role != "year"},
            **dict.fromkeys(_AMOUNTS, pl.Float64),
        },
    )
    unknown = [
        status
        for status in frame["status"].drop_nulls().str.to_lowercase().unique().sort()
        if status not in _STATUSES and status.upper() not in _NOTATION
    ]
    if unknown:
        raise ValueError(f"Statuses outside the vocabulary: {unknown}")
    codes = {
        name: code
        for name in frame["recipient"].drop_nulls().unique()
        if (code := _recipient_code(name)) is not None
    }
    aggregates = [f"{party}|{title}" for party, title in _AGGREGATE_TITLES]
    return frame.select(
        row_id=pl.format(
            "BTR-{}-{}-{}",
            "party",
            "year",
            pl.col("row").cast(pl.String).str.pad_start(5, "0"),
        ),
        party="party",
        provider_code=pl.col("party").replace_strict(
            {party: party_provider(party) for party in frame["party"].unique()},
            default=None,
            return_dtype=pl.Int32,
        ),
        year="year",
        recipient="recipient",
        recipient_code=pl.col("recipient").replace_strict(
            codes, default=None, return_dtype=pl.Int32
        ),
        title="title",
        amount_domestic="face_domestic",
        grant_equiv_domestic="grant_equiv_domestic",
        currency=pl.col("party").replace_strict(_CURRENCIES, return_dtype=pl.String),
        face_usd="face_usd",
        grant_equiv_usd="grant_equiv_usd",
        dac_rate=pl.lit(None, dtype=pl.Float64),
        face_usd_at_dac=pl.lit(None, dtype=pl.Float64),
        grant_equiv_usd_at_dac=pl.lit(None, dtype=pl.Float64),
        status=pl.col("status")
        .str.to_lowercase()
        .replace_strict(_STATUSES, default=None, return_dtype=pl.String),
        channel="channel",
        funding_source="funding_source",
        instrument="instrument",
        support_type="support_type",
        sector="sector",
        additional_information="additional_information",
        is_aggregate=pl.concat_str("party", "title", separator="|")
        .is_in(aggregates)
        .fill_null(value=False),
        source_file="source_file",
    )


def party_provider(party: str) -> int | None:
    """The DAC provider code of the party with ISO 3 code `party`, or None."""
    return _PARTY_PROVIDERS.get(party) or provider_code(party)


def _recipient_code(name: str) -> int | None:
    try:
        return recipients.code(name)
    except UnknownRecipientError:
        return None


def _cell(cells: Sequence[object], column: int) -> object:
    return cells[column] if column < len(cells) else None


def _label(cell: object) -> str:
    """A header cell lower-cased, its whitespace collapsed."""
    return "" if cell is None else re.sub(r"\s+", " ", str(cell)).strip().lower()


def _text(value: object) -> str | None:
    """A text cell without notation keys. Finland stacks "NA" lines around the
    recipient ("NA\\nAfrica\\nNA"). Those lines go."""
    if value is None:
        return None
    lines = [line.strip() for line in str(value).splitlines()]
    kept = [line for line in lines if line and line.upper() not in _NOTATION]
    return re.sub(r"\s+", " ", " ".join(kept)).strip() or None


def _is_notation(value: object) -> bool:
    return isinstance(value, str) and value.strip().upper() in _NOTATION


def _number(value: object) -> float | None:
    """An amount cell as a float; a blank or a notation key gives None."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    # Monaco glues footnote numbers to amounts: "100000(2)".
    text = re.sub(r"\s|\(\d+\)$", "", str(value))
    if not text or text.upper() in _NOTATION:
        return None
    # "1,691" groups thousands. Chile's "44050,34" has a decimal comma.
    if _THOUSANDS.fullmatch(text):
        return float(text.replace(",", ""))
    return float(text.replace(",", "."))

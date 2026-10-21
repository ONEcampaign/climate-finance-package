"""`Imputed_multilateral_shares.xlsx`: the OECD's imputed climate shares."""

import re
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

import openpyxl
import polars as pl

_HEADER = "MDB or International Organisation"
_NAME_COLUMN = 3
_CLIMATE_FINANCE_COLUMN = 5
# Header text, in the header row or the row above it, of the column holding an
# agency's whole climate share.
_SHARE_HEADERS = ("Imputed multilateral contribution", "Overall climate share")
# Header text naming the years a sheet's shares average, such as "2021-2022 average".
_SPAN = re.compile(r"(\d{4})-(\d{4}) average")
# Header text of the three columns that split an agency's climate share. The columns
# are disjoint; the reader adds "both" to the other two.
_OBJECTIVES = {
    "Mitigation only": "mitigation",
    "Adaptation only": "adaptation",
    "Both mitigation and adaption": "cross_cutting",
}

# CRS channel code (codelist "Channel of delivery") of each agency name in column D.
# A name outside this table stops the read, so a new agency gets a code on purpose.
# `None` marks an agency with no channel code; its rows are left out.
_CHANNEL_CODES: dict[str, int | None] = {
    "Adaptation Fund": 47111,
    "African Development Bank": 46002,
    "African Development Fund": 46003,
    "AsDB Concessional": 46005,
    "AsDB regular OCR": 46004,
    "Asian Infrastructure Investment Bank": 46026,
    "Black Sea Trade & Development Bank": 46006,
    "Caribbean Development Bank": 46009,
    "Central American Bank for Economic Integration": 46007,
    "CGIAR": 47015,
    "Clean Technology Fund": 47134,
    "Council of Europe Development Bank": 46024,
    "Development Bank of Latin America": 46008,
    "European Bank for Reconstruction and Development": 46015,
    "European Investment Bank": 42004,
    "Food and Agriculture Organisation": 41301,
    "Fund for Special Operations (FSO)": 46013,
    "GEF General Trust Fund": 47044,
    # The Global Biodiversity Framework Fund has no code in the channel codelist.
    "GEF Global Biodiversity Framework Fund (GBFF)": None,
    "GEF Least Developed Countries Trust Fund (LDCF)": 47129,
    "GEF Special Climate Change Trust Fund (SCCF)": 47130,
    "Global Green Growth Institute": 47136,
    "Green Climate Fund": 41317,
    "IDB Invest": 46012,
    "IMF Resilience and Sustainability Trust": 43007,
    "Inter-American Development Bank": 46012,
    "Inter-American Investment Corporation and Multilateral Investment Fund": 46012,
    "Intergovernmental Panel on Climate Change": 47067,
    "International Bank for Reconstruction and Development": 44001,
    "International Development Association": 44002,
    "International Finance Corporation": 44004,
    "International Fund for Agricultural Development": 41108,
    "Islamic Development Bank": 46025,
    "Multilateral Fund for the Implementation of the Montreal Protocol": 47078,
    "New Development Bank": 46028,
    "Nordic Development Fund": 47128,
    "Strategic Climate Fund": 47135,
    "United Nations Framework Convention on Climate Change": 41316,
}


@dataclass(frozen=True)
class _SheetRow:
    """One share of one agency on a year sheet."""

    name: str
    year: int
    objective: str
    share: float
    # The agency's climate finance in the sheet's year (column F), its whole climate
    # share, and the years the sheet's shares average.
    climate: float | None
    whole: float | None
    span: range


def read_imputed_shares(path: Path) -> pl.DataFrame:
    """Climate shares by channel, year and objective, skipping the draft sheet.

    The workbook has one sheet per year. An agency the sheet splits by objective gives
    a row per objective with a figure. The sheet's columns are disjoint ("adaptation
    only", "mitigation only", "both"); the rows are inclusive, as in
    `agency_shares`: adaptation and mitigation each add the "both" share, which is
    also the cross_cutting row. An agency
    with only a whole climate share, as every agency has before 2021 and the IPCC,
    the Montreal Protocol and the UNFCCC have after, gives one `climate` row. An
    agency with no share gives no rows. The Inter-American Development Bank, IDB
    Invest and the IIC and MIF share one channel code; their shares are averaged,
    weighted by each agency's total development finance over the years the shares
    average (two years, such as 2021-2022 on the 2022 sheet). That total is the
    agency's climate finance in those years, column F of each year's sheet, over its
    whole climate share; a year without a sheet or without the agency adds nothing.
    The pooled share is then the group's climate finance over its total finance.

    Args:
        path: The OECD workbook.

    Raises:
        ValueError: when a year sheet lacks its header row or its climate share
            column, or names an agency that has no entry in the table of channel
            codes.
    """
    workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
    rows = [
        row
        for sheet in workbook.worksheets
        if re.fullmatch(r"\d{4}", sheet.title)
        for row in _sheet_rows(list(sheet.iter_rows(values_only=True)), sheet.title)
    ]
    unknown = sorted({row.name for row in rows} - _CHANNEL_CODES.keys())
    if unknown:
        raise ValueError(f"No channel code for the agencies {unknown}")
    climate = {(row.name, row.year): row.climate for row in rows}
    return (
        pl.DataFrame(
            [
                (
                    row.name,
                    row.year,
                    row.objective,
                    row.share,
                    _span_finance(row, climate=climate),
                )
                for row in rows
            ],
            orient="row",
            schema={
                "name": pl.String,
                "year": pl.Int16,
                "objective": pl.String,
                "share": pl.Float64,
                "finance": pl.Float64,
            },
        )
        .with_columns(
            channel_code=pl.col("name").replace_strict(
                _CHANNEL_CODES, return_dtype=pl.Int32
            )
        )
        .drop_nulls("channel_code")
        .group_by("channel_code", "year", "objective", maintain_order=True)
        .agg(
            pl.when(pl.len() == 1)
            .then(pl.col("share").first())
            .otherwise(
                (pl.col("share") * pl.col("finance")).sum() / pl.col("finance").sum()
            )
            .alias("share")
        )
        .select("channel_code", "year", "objective", "share")
    )


def _span_finance(
    row: _SheetRow, *, climate: Mapping[tuple[str, int], float | None]
) -> float | None:
    """The agency's total finance over the years its share averages."""
    if row.climate is None or row.whole is None or row.whole <= 0:
        return None
    amounts = (climate.get((row.name, year)) for year in row.span)
    return sum(amount for amount in amounts if amount is not None) / row.whole


def _sheet_rows(sheet: Sequence[Sequence[object]], title: str) -> Iterator[_SheetRow]:
    """Per agency of a year sheet, its split rows, else its `climate` row.

    Each row carries the agency's climate finance (column F), its whole climate
    share and the years the sheet's shares average.
    """
    header = next((i for i, row in enumerate(sheet) if _HEADER in row), None)
    if header is None:
        raise ValueError(f"Sheet {title} has no {_HEADER!r} header")
    climate_column = next(
        (
            column
            for row in (sheet[header - 1], sheet[header])
            for column, cell in enumerate(row)
            if isinstance(cell, str) and cell.startswith(_SHARE_HEADERS)
        ),
        None,
    )
    if climate_column is None:
        raise ValueError(f"Sheet {title} has no climate share column")
    averaged = next(
        (
            found
            for row in sheet[: header + 1]
            for cell in row
            if isinstance(cell, str) and (found := _SPAN.search(cell))
        ),
        None,
    )
    span = (
        range(int(averaged[1]), int(averaged[2]) + 1)
        if averaged
        else range(int(title), int(title) + 1)
    )
    columns = {
        column: _OBJECTIVES[cell]
        for column, cell in enumerate(sheet[header])
        if isinstance(cell, str) and cell in _OBJECTIVES
    }
    for row in sheet[header + 1 :]:
        if not row[_NAME_COLUMN]:
            continue
        shares = {
            objective: share
            for column, objective in columns.items()
            if isinstance(share := row[column], int | float)
        }
        if both := shares.get("cross_cutting"):
            for objective in ("adaptation", "mitigation"):
                if objective in shares:
                    shares[objective] += both
        if not shares and isinstance(share := row[climate_column], int | float):
            shares = {"climate": share}
        for objective, share in shares.items():
            yield _SheetRow(
                name=str(row[_NAME_COLUMN]).strip(),
                year=int(title),
                objective=objective,
                share=share,
                climate=_number(row[_CLIMATE_FINANCE_COLUMN]),
                whole=_number(row[climate_column]),
                span=span,
            )


def _number(cell: object) -> float | None:
    return float(cell) if isinstance(cell, int | float) else None

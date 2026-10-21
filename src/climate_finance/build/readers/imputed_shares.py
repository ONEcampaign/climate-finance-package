"""`Imputed_multilateral_shares.xlsx`: the OECD's imputed climate shares."""

import re
from collections.abc import Iterator, Sequence
from pathlib import Path

import openpyxl
import polars as pl

_HEADER = "MDB or International Organisation"
_NAME_COLUMN = 3
_CLIMATE_FINANCE_COLUMN = 5
# Header text, in the header row or the row above it, of the column holding an
# agency's whole climate share.
_SHARE_HEADERS = ("Imputed multilateral contribution", "Overall climate share")
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
    weighted by each agency's total development finance, which is its climate
    finance (column F) over its whole climate share. The pooled share is then the
    group's climate finance over its total finance.

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
    unknown = sorted({str(row["name"]) for row in rows} - _CHANNEL_CODES.keys())
    if unknown:
        raise ValueError(f"No channel code for the agencies {unknown}")
    return (
        pl.DataFrame(
            rows,
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


def _sheet_rows(
    sheet: Sequence[Sequence[object]], title: str
) -> Iterator[dict[str, object]]:
    """Per agency of a year sheet, its split rows, else its `climate` row."""
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
        climate, whole = row[_CLIMATE_FINANCE_COLUMN], row[climate_column]
        finance = (
            climate / whole
            if isinstance(climate, int | float)
            and isinstance(whole, int | float)
            and whole > 0
            else None
        )
        for objective, share in shares.items():
            yield {
                "name": str(row[_NAME_COLUMN]).strip(),
                "year": int(title),
                "objective": objective,
                "share": share,
                "finance": finance,
            }

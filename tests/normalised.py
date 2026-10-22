"""Hand-built rows in the normalised `crdf` and `crs` schemas.

Codes are real DAC codes; ids, titles and amounts are invented.
"""

import polars as pl

from climate_finance import schema

_CRDF_DEFAULTS: dict[str, object] = {
    "perspective": "recipient",
    "provider_code": 8,
    "agency_code": None,
    "crs_id": None,
    "project_number": None,
    "title": None,
    "recipient_code": 248,
    "channel_code": None,
    "bi_multi": 1,
    "purpose_code": 23210,
    "flow_category": 10,
    "objective": "mitigation",
    "rio_adaptation": 0,
    "rio_mitigation": 2,
    "climate_usd": 1e6,
    "adaptation_usd": 0.0,
    "overlap_usd": 0.0,
    "climate_share": None,
    "id_scheme": None,
    "repeat_of": None,
    "parent_row_id": None,
}
_CRS_DEFAULTS: dict[str, object] = {
    "donor_code": 8,
    "agency_code": 1,
    "crs_id": None,
    "project_number": None,
    "title": None,
    "recipient_code": 248,
    "purpose_code": 23210,
    "channel_code": None,
    "bi_multi": 1,
    "flow_code": 11,
    "commitment_usd": None,
    "disbursement_usd": None,
    "grant_equiv_usd": None,
    "commitment_date": None,
    "rio_adaptation": 0,
    "rio_mitigation": 2,
    "text_hash": None,
}


def crdf_rows(*rows: dict[str, object]) -> pl.DataFrame:
    """CRDF rows; each needs `row_id` and `year`, and `face_usd` defaults to climate."""
    filled = []
    for row in rows:
        full = _CRDF_DEFAULTS | {"vintage": row["year"]} | row
        full.setdefault("face_usd", full["climate_usd"])
        full.setdefault("mitigation_usd", full["climate_usd"])
        filled.append(full)
    return _frame(schema.CRDF, filled)


def crs_records(*records: dict[str, object]) -> pl.DataFrame:
    """CRS records; each needs `record_id` and `year`."""
    return _frame(schema.CRS, [_CRS_DEFAULTS | record for record in records])


def _frame(table: schema.TableSchema, rows: list[dict[str, object]]) -> pl.DataFrame:
    frame = pl.DataFrame(
        rows, schema={name: column.dtype for name, column in table.columns.items()}
    )
    schema.validate(table, frame)
    return frame

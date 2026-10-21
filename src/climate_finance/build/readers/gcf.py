"""The GCF projects API (`api.gcfund.org/v1/projects`), as saved by the fetch stage."""

import json
from pathlib import Path

import polars as pl

from climate_finance.schema import EXTERNAL


def read_gcf_projects(path: Path) -> pl.DataFrame:
    """GCF commitments and disbursements per project, year and currency.

    A commitment is the project's GCF funding (co-financing excluded) in the year of
    its approval. A disbursement is dated by its effective date; the API's undated
    rows (all zero in the 2026 file) are dropped. Amounts are in the currency the
    API reports them in, and `amount_usd` is filled for USD rows only.
    `recipient_iso` holds the project's ISO 3 country codes, sorted and joined by
    commas, and is null for a project that names none.

    Args:
        path: The API's `projects.json`.
    """
    rows = []
    for project in json.loads(path.read_text(encoding="utf-8")):
        base = {
            "activity_id": project["ApprovedRef"],
            "title": project["ProjectName"],
            "recipient_iso": ",".join(
                sorted({c["ISO3"] for c in project.get("Countries") or [] if c["ISO3"]})
            )
            or None,
        }
        rows.extend(
            {
                **base,
                "year": int(project["ApprovalDate"][:4]),
                "transaction_type": "commitment",
                "currency": line["Currency"],
                "amount": line["Budget"],
            }
            for line in project.get("Funding") or []
            if line["Source"] == "GCF" and line["Budget"] is not None
        )
        rows.extend(
            {
                **base,
                "year": int(paid["DateEffective"][:4]),
                "transaction_type": "disbursement",
                "currency": paid["Currency"],
                "amount": paid["AmountDisbursed"],
            }
            for paid in project.get("Disbursements") or []
            if paid.get("DateEffective") and paid["AmountDisbursed"] is not None
        )
    return (
        pl.DataFrame(
            rows,
            schema={
                "activity_id": pl.String,
                "title": pl.String,
                "recipient_iso": pl.String,
                "year": pl.Int16,
                "transaction_type": pl.String,
                "currency": pl.String,
                "amount": pl.Float64,
            },
        )
        .group_by(
            "activity_id",
            "title",
            "recipient_iso",
            "year",
            "transaction_type",
            "currency",
            maintain_order=True,
        )
        .agg(pl.col("amount").sum())
        .with_columns(
            source=pl.lit("gcf"),
            publisher=pl.lit("gcf"),
            amount_usd=pl.when(pl.col("currency") == "USD").then(pl.col("amount")),
            recipient_code=pl.lit(None, dtype=pl.Int32),
            cumulative_declared=pl.lit(False),
        )
        .select(list(EXTERNAL.columns))
    )

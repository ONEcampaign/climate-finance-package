"""GCF disbursements from the API and the readiness list."""

import json
import logging
from datetime import date
from pathlib import Path

import polars as pl

from climate_finance.build.readers.ledgers.frame import ledger_frame

logger = logging.getLogger(__name__)

_PUBLISHER = "gcf"
_READINESS_PUBLISHER = "gcf_readiness"


def read_gcf_disbursements(path: Path) -> pl.DataFrame:
    """GCF project disbursements by project id and date.

    Rows are keyed by the project's approved reference (`FP001`, `SAP001`), which the
    CRS project number carries for 96% of GCF disbursement value. Amounts are in the
    currency the API reports them in; the API's own dollar figure is at the exchange
    rate of the day it was served, so the stage that holds the DAC annual rates
    converts the euro lines instead. Disbursements without an effective date are
    skipped and counted in the log.

    Args:
        path: The API's `projects.json`.
    """
    rows = []
    undated = 0
    for project in json.loads(path.read_text(encoding="utf-8")):
        for paid in project.get("Disbursements") or []:
            if paid["AmountDisbursed"] is None:
                continue
            if not paid.get("DateEffective"):
                undated += 1
                continue
            rows.append(
                {
                    "publisher": _PUBLISHER,
                    "key": project["ApprovedRef"],
                    "activity_id": project["ApprovedRef"],
                    "transaction_type": "disbursement",
                    "date": date.fromisoformat(paid["DateEffective"][:10]),
                    "currency": paid["Currency"],
                    "amount": paid["AmountDisbursed"],
                    "title": project["ProjectName"],
                    "other_ids": [],
                }
            )
    if undated:
        logger.warning("GCF disbursements without an effective date: %d", undated)
    return ledger_frame(rows).sort("key", "date", "amount")


def read_gcf_readiness(path: Path) -> pl.DataFrame:
    """GCF readiness grants by id and year.

    Readiness grants are absent from the projects API. Each grant gives its approved
    amount as a `commitment` dated by approval, and its disbursed amount as
    `disbursed_to_date`: the list carries no disbursement dates, so that row has no
    date or year and no calendar-year check can use it. Rows are keyed by the grant's
    agreement reference (`MLI-RS-001`), which the CRS project number carries. Grants
    without an agreement reference are skipped and counted in the log.

    Args:
        path: The readiness list as saved by the download, a JSON array of grants.
    """
    rows = []
    unreferenced = 0
    for grant in json.loads(path.read_text(encoding="utf-8")):
        key = (grant.get("AgreementReference") or "").strip().upper()
        if not key:
            unreferenced += 1
            continue
        base = {
            "publisher": _READINESS_PUBLISHER,
            "key": key,
            "activity_id": grant["RequestID"],
            "currency": "USD",
            "title": grant["ProjectTitle"],
            "other_ids": [],
        }
        if grant["AmountApprovedInUSD"] is not None and grant["ApprovedDate"]:
            rows.append(
                {
                    **base,
                    "transaction_type": "commitment",
                    "date": date.fromisoformat(grant["ApprovedDate"][:10]),
                    "amount": grant["AmountApprovedInUSD"],
                }
            )
        if grant["AmountDisbursedInUSD"] is not None:
            rows.append(
                {
                    **base,
                    "transaction_type": "disbursed_to_date",
                    "date": None,
                    "amount": grant["AmountDisbursedInUSD"],
                }
            )
    if unreferenced:
        logger.warning("GCF readiness grants without a reference: %d", unreferenced)
    return ledger_frame(rows).sort("key", "transaction_type", "date")

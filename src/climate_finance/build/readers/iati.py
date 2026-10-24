"""IATI activity XML for GEF and other publishers, as saved by the fetch stage."""

import logging
import re
import xml.etree.ElementTree as ET
from collections.abc import Iterator
from pathlib import Path

import polars as pl

from climate_finance.schema import EXTERNAL

logger = logging.getLogger(__name__)

# Transaction types 2 and 3 of the IATI TransactionType codelist.
_TRANSACTION_TYPES = {"2": "commitment", "3": "disbursement"}
# GEF states in each activity's description that its disbursements are cumulative.
_CUMULATIVE_NOTE = re.compile(
    r"disbursement\s+data\b[^.]*\bis\s+cumulative", re.IGNORECASE
)
# Some publishers leave stray `<` in free text, which makes the whole file ill-formed,
# so each activity is parsed alone and a bad one costs only itself. The prolog never
# reaches the parser, which leaves no place to declare entities.
_ACTIVITY = re.compile(r"<iati-activity\b.*?</iati-activity>", re.DOTALL)


def read_iati_activities(path: Path) -> pl.DataFrame:
    """Commitment (type 2) and disbursement (type 3) transactions by activity.

    Every transaction is kept in its own currency, and `amount_usd` is filled for USD
    rows only. `recipient_iso` holds the ISO 3166 codes the transaction or, failing
    that, its activity names, sorted and joined by commas (null when it names no
    country). Amounts are summed by year as published. `cumulative_declared` is true
    for every row of an activity whose description declares its disbursement data
    cumulative (GEF). The figures are left as published, since the reconciling stage
    owns their interpretation. Activities that do not parse or repeat an identifier
    already read, and transactions without a date, value or currency, are skipped
    and counted in the log.

    Args:
        path: A directory with one subdirectory of activity XML files per publisher.
            The subdirectory name becomes `publisher`.

    Raises:
        FileNotFoundError: when `path` holds no publisher subdirectory with XML files.
    """
    publishers = sorted(
        d for d in path.iterdir() if d.is_dir() and any(d.glob("*.xml"))
    )
    if not publishers:
        raise FileNotFoundError(f"No publisher directory of IATI XML under {path}")
    rows: list[dict[str, object]] = []
    skipped = {"unparsed": 0, "repeated": 0, "incomplete": 0}
    for directory in publishers:
        seen: set[str] = set()
        for file in sorted(directory.glob("*.xml")):
            for activity in _activities(file, skipped):
                activity_id = _text(activity.find("iati-identifier"))
                if activity_id is None:
                    skipped["unparsed"] += 1
                    continue
                if activity_id in seen:
                    skipped["repeated"] += 1
                    continue
                seen.add(activity_id)
                rows.extend(
                    _transactions(activity, directory.name, activity_id, skipped)
                )
    if any(skipped.values()):
        logger.warning("IATI rows skipped: %s", skipped)
    return (
        pl.DataFrame(
            rows,
            schema={
                "publisher": pl.String,
                "activity_id": pl.String,
                "title": pl.String,
                "recipient_iso": pl.String,
                "currency": pl.String,
                "cumulative_declared": pl.Boolean,
                "year": pl.Int16,
                "transaction_type": pl.String,
                "amount": pl.Float64,
            },
        )
        .group_by(
            "publisher",
            "activity_id",
            "title",
            "recipient_iso",
            "currency",
            "cumulative_declared",
            "year",
            "transaction_type",
            maintain_order=True,
        )
        .agg(pl.col("amount").sum())
        .with_columns(
            source=pl.lit("iati"),
            amount_usd=pl.when(pl.col("currency") == "USD").then(pl.col("amount")),
            recipient_code=pl.lit(None, pl.Int32),
        )
        .select(list(EXTERNAL.columns))
    )


def read_related_activities(path: Path) -> pl.DataFrame:
    """Every activity's `related-activity` references, as published.

    `relation` is the IATI RelatedActivityType code: 1 parent, 2 child, 3 sibling,
    4 co-funded, 5 third party. `related_id` is upper-cased with its spaces removed.
    References without a `ref`, and activities that do not parse, are skipped.

    Args:
        path: One publisher's directory of activity XML files.

    Raises:
        FileNotFoundError: when `path` holds no XML file.
    """
    files = sorted(path.glob("*.xml"))
    if not files:
        raise FileNotFoundError(f"No IATI XML under {path}")
    skipped = {"unparsed": 0}
    rows = [
        {
            "activity_id": activity_id,
            "related_id": re.sub(r"\s+", "", ref).upper(),
            "relation": related.get("type"),
        }
        for file in files
        for activity in _activities(file, skipped)
        if (activity_id := _text(activity.find("iati-identifier")))
        for related in activity.iterfind("related-activity")
        if (ref := related.get("ref", "").strip())
    ]
    if skipped["unparsed"]:
        logger.warning("IATI activities skipped: %s", skipped)
    return pl.DataFrame(
        rows,
        schema={
            "activity_id": pl.String,
            "related_id": pl.String,
            "relation": pl.String,
        },
    ).unique(maintain_order=True)


def _activities(file: Path, skipped: dict[str, int]) -> Iterator[ET.Element]:
    for fragment in _ACTIVITY.findall(file.read_text(encoding="utf-8-sig")):
        try:
            yield ET.fromstring(fragment)
        except ET.ParseError:
            skipped["unparsed"] += 1


def _transactions(
    activity: ET.Element, publisher: str, activity_id: str, skipped: dict[str, int]
) -> Iterator[dict[str, object]]:
    title = _text(activity.find("title/narrative")) or _text(activity.find("title"))
    default_currency = activity.get("default-currency")
    activity_recipients = _recipients(activity)
    cumulative_declared = any(
        _CUMULATIVE_NOTE.search("".join(narrative.itertext()))
        for narrative in activity.iterfind("description/narrative")
    )
    for transaction in activity.findall("transaction"):
        type_code = transaction.find("transaction-type")
        value = transaction.find("value")
        date = transaction.find("transaction-date")
        kind = _TRANSACTION_TYPES.get(
            type_code.get("code", "") if type_code is not None else ""
        )
        if kind is None:
            continue
        amount = _number(_text(value))
        year = _year(date.get("iso-date") if date is not None else None)
        currency = (value.get("currency") if value is not None else None) or (
            default_currency
        )
        if amount is None or year is None or not currency:
            skipped["incomplete"] += 1
            continue
        yield {
            "publisher": publisher,
            "activity_id": activity_id,
            "title": title,
            "recipient_iso": _recipients(transaction) or activity_recipients,
            "currency": currency.strip().upper(),
            "year": year,
            "transaction_type": kind,
            "cumulative_declared": cumulative_declared,
            "amount": amount,
        }


def _recipients(element: ET.Element) -> str | None:
    codes = {
        code.strip()
        for country in element.findall("recipient-country")
        if (code := country.get("code"))
    }
    return ",".join(sorted(codes - {""})) or None


def _text(element: ET.Element | None) -> str | None:
    if element is None:
        return None
    return (element.text or "").strip() or None


def _number(text: str | None) -> float | None:
    try:
        return float(text) if text else None
    except ValueError:
        return None


def _year(iso_date: str | None) -> int | None:
    match = re.match(r"\s*(\d{4})-", iso_date or "")
    return int(match[1]) if match else None

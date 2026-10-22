"""IATI disbursement and expenditure transactions as ledgers."""

import logging
import re
import xml.etree.ElementTree as ET
from collections import Counter
from collections.abc import Iterator
from datetime import date
from pathlib import Path

import polars as pl

from climate_finance.build.readers.iati import _CUMULATIVE_NOTE
from climate_finance.build.readers.ledgers.frame import LEDGER_SCHEMA, complete_ledger
from climate_finance.errors import SourceUnavailableError

logger = logging.getLogger(__name__)

_TRANSACTION_TYPES = {"2": "commitment", "3": "disbursement", "4": "expenditure"}
# The part of an IATI identifier that comes before the id the CRS project number
# carries, by publisher short name on the IATI Bulk Data Service. Other publishers,
# the bilateral donors among them, are keyed by their whole identifier.
_ID_PREFIXES = {
    "bcie": "XM-DAC-46007-",
    "ebrd": "XI-IATI-EBRD-",
    "gcf": "XM-DAC-41317-",
    "gef": "XI-BRIDGE-6385676864-",
    "iadb": "XI-IATI-IADB-",
    "isdb": "XM-DAC-46025-",
}
# The GCF lists its co-financiers' commitments as its own, by this description.
_CO_FINANCING = re.compile(r"co-financ", re.IGNORECASE)
# ADB's CRS project numbers carry the loan or grant number at the end of the identifier.
_ADB_KEY = re.compile(r"-([A-Z]+\d+)$")
# AfDB's CRS project numbers carry the finance-type id its transactions are tagged with.
_AFDB_FINANCE_TYPE = "{http://afdb.org/iati/}finance-type-id"
# Some publishers leave stray `<` in free text, which makes the whole file ill-formed,
# so each activity is parsed alone and a bad one costs only itself. The prefixes the
# root element declares are copied onto each activity, which would not parse without.
_ACTIVITY = re.compile(rb"<iati-activity\b.*?</iati-activity>", re.DOTALL)
_ROOT = re.compile(rb"<iati-activities\b[^>]*>")
_NAMESPACE = re.compile(rb'xmlns:(\w+)="([^"]*)"')
_FRAME_SCHEMA = {**LEDGER_SCHEMA, "cumulative": pl.Boolean}
_DUPLICATE_KEY = [
    "publisher",
    "activity_id",
    "key",
    "transaction_type",
    "date",
    "currency",
    "amount",
]


def read_iati_ledger(path: Path, *, publisher: str) -> pl.DataFrame:
    """Commitment, disbursement and expenditure transactions by activity and date.

    BMZ and the European Commission report most of their spend as expenditure, so
    type 4 stays beside type 3. Amounts are as published, in each transaction's
    currency, dated by the transaction date (the value date when there is none).
    `key` is the id the CRS project number carries for that publisher, upper case and
    without spaces; `other_ids` lists the activity's other identifiers.

    A commitment whose description names co-financing is left out: the GCF lists
    its co-financiers' commitments under its own activities, USD 57.7bn of its
    77.8bn. A transaction repeated exactly within an activity is counted once: 186 IDB
    activities repeat transactions, which would add 4.8% to IDB's 2023 disbursements.
    GEF declares its disbursements cumulative, but the figures fall as well as rise.
    For an activity that declares it, each disbursement row becomes the rise in the
    running peak of its values, so a year's rows add up to the rise in the peak over
    the year. GEF's 2021 to 2024 disbursements are USD 4.5bn as published, 2.2bn as
    running peaks and 1.9bn in the CRS, whose GEF rows carry agency ids and so allow
    no comparison by activity.

    Transactions are skipped, and counted in the log, when they have no amount, no
    full date, no currency or no key; an activity that does not parse costs only
    itself.

    Args:
        path: One IATI XML file, or a directory of them.
        publisher: The publisher's short name on the IATI Bulk Data Service, such as
            `asdb` or `gef`. It sets the key rule and is kept in `publisher`.

    Raises:
        SourceUnavailableError: when `path` holds no XML file.
    """
    skipped: Counter[str] = Counter()
    rows = [
        row
        for file in _xml_files(path)
        for activity in _activities(file, skipped)
        for row in _transactions(activity, publisher, skipped)
    ]
    if skipped:
        logger.warning("IATI ledger rows skipped for %s: %s", publisher, dict(skipped))
    frame = pl.DataFrame(rows, schema=_FRAME_SCHEMA).unique(
        subset=_DUPLICATE_KEY, keep="first", maintain_order=True
    )
    return complete_ledger(_to_period_flows(frame).select(list(LEDGER_SCHEMA))).sort(
        "key", "date", "transaction_type", "amount"
    )


def _to_period_flows(frame: pl.DataFrame) -> pl.DataFrame:
    """Replace cumulative disbursements with the rise of their running peak."""
    cumulative = pl.col("cumulative") & (pl.col("transaction_type") == "disbursement")
    group = ["activity_id", "currency"]
    peak = pl.col("amount").cum_max().clip(lower_bound=0).over(group)
    flows = (
        frame.filter(cumulative)
        .sort(*group, "date", "amount")
        .with_columns(peak.alias("peak"))
        .with_columns(
            amount=pl.col("peak") - pl.col("peak").shift(1).over(group).fill_null(0)
        )
        .drop("peak")
    )
    return pl.concat([frame.filter(~cumulative), flows])


def _xml_files(path: Path) -> list[Path]:
    files = [path] if path.is_file() else sorted(path.glob("*.xml"))
    if not files:
        raise SourceUnavailableError(f"No IATI XML file at {path}.")
    return files


def _activities(file: Path, skipped: Counter[str]) -> Iterator[ET.Element]:
    content = file.read_bytes()
    root = _ROOT.search(content)
    declared = _NAMESPACE.findall(root[0]) if root else []
    for fragment in _ACTIVITY.findall(content):
        own = fragment[: fragment.index(b">")]
        inherited = b"".join(
            b' xmlns:%b="%b"' % (prefix, uri)
            for prefix, uri in declared
            if b"xmlns:" + prefix + b"=" not in own
        )
        try:
            yield ET.fromstring(
                fragment.replace(b"<iati-activity", b"<iati-activity" + inherited, 1)
            )
        except ET.ParseError:
            skipped["activity not parsed"] += 1


def _transactions(
    activity: ET.Element, publisher: str, skipped: Counter[str]
) -> Iterator[dict[str, object]]:
    activity_id = _text(activity.find("iati-identifier"))
    if activity_id is None:
        skipped["activity without identifier"] += 1
        return
    title = _text(activity.find("title/narrative")) or _text(activity.find("title"))
    default_currency = activity.get("default-currency")
    other_ids = [
        ref
        for other in activity.findall("other-identifier")
        if (ref := other.get("ref"))
    ]
    cumulative = any(
        _CUMULATIVE_NOTE.search("".join(narrative.itertext()))
        for narrative in activity.iterfind("description/narrative")
    )
    for transaction in activity.findall("transaction"):
        type_code = transaction.find("transaction-type")
        kind = _TRANSACTION_TYPES.get(
            type_code.get("code", "") if type_code is not None else ""
        )
        if kind is None:
            continue
        if kind == "commitment" and _co_financing(transaction):
            skipped["co-financing commitment"] += 1
            continue
        value = transaction.find("value")
        currency = (
            (
                (value.get("currency") if value is not None else None)
                or default_currency
                or ""
            )
            .strip()
            .upper()
        )
        amount = _number(_text(value))
        when = _date(transaction)
        key = _key(publisher, activity_id, transaction)
        missing = [
            name
            for name, found in (
                ("amount", amount),
                ("date", when),
                ("currency", currency),
                ("key", key),
            )
            if found is None or found == ""
        ]
        if missing:
            skipped[f"transaction without {' or '.join(missing)}"] += 1
            continue
        yield {
            "publisher": publisher,
            "key": key,
            "activity_id": activity_id,
            "transaction_type": kind,
            "date": when,
            "currency": currency,
            "amount": amount,
            "title": title,
            "other_ids": other_ids,
            "cumulative": cumulative,
        }


def _co_financing(transaction: ET.Element) -> bool:
    return any(
        _CO_FINANCING.search("".join(narrative.itertext()))
        for narrative in transaction.iterfind("description/narrative")
    )


def _key(publisher: str, activity_id: str, transaction: ET.Element) -> str | None:
    match publisher:
        case "afdb":
            raw = transaction.get(_AFDB_FINANCE_TYPE)
        case "asdb":
            found = _ADB_KEY.search(activity_id.upper())
            raw = found[1] if found else None
        case _:
            raw = activity_id.removeprefix(_ID_PREFIXES.get(publisher, ""))
    return re.sub(r"\s+", "", raw.upper()) if raw else None


def _date(transaction: ET.Element) -> date | None:
    stated = transaction.find("transaction-date")
    value = transaction.find("value")
    for text in (
        stated.get("iso-date") if stated is not None else None,
        value.get("value-date") if value is not None else None,
    ):
        try:
            return date.fromisoformat((text or "").strip())
        except ValueError:
            continue
    return None


def _text(element: ET.Element | None) -> str | None:
    if element is None:
        return None
    return (element.text or "").strip() or None


def _number(text: str | None) -> float | None:
    try:
        return float(text) if text else None
    except ValueError:
        return None

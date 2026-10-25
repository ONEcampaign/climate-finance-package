"""Listings: `cf.providers`, `cf.recipients`, `cf.flows` and `cf.methods`.

Provider names come from `data/oecd_classification.csv`, the OECD's donor list.
Recipient names come from `data/recipients.csv`: DAC recipient codes and their English
names as the OECD CRS bulk download spells them, carried over from climate-finance
1.2.3 (`oecd/cleaning_tools/recipient_names.feather`, built from the 2021 CRS).
`cf-build listings` adds every other code the OECD files carry to both, and fills
`data/agencies.csv`, with the names those files give them.

Provider and recipient names resolve through `resolve`, which reads `data/names.csv`:
every common form of a provider, agency or recipient name (listing names, French
provider names, ISO codes, resolvekit aliases and agency names), baked by
`cf-build names`. A name matches ignoring case, accents and surrounding spaces. A name
of several entries, or of none, raises with candidates or close names as suggestions,
which are never applied. Flow and method names match their listing exactly, ignoring
case and accents.
"""

import unicodedata
from collections.abc import Iterable, Sequence
from difflib import get_close_matches
from functools import cache
from importlib.resources import files
from numbers import Integral
from typing import Literal

import polars as pl
from pydantic import BaseModel, ConfigDict

from climate_finance.errors import (
    UnknownNameError,
    UnknownProviderError,
    UnknownRecipientError,
)

type Kind = Literal["providers", "recipients", "flows", "methods"]
type Coded = Literal["providers", "recipients"]

_SUGGESTIONS = 5
# Close keys to collect before folding them into at most `_SUGGESTIONS` names, since
# many keys share one target.
_CLOSE_KEYS = 50
_FLOWS = {
    "commitments": "CRDF commitments at face value, leaving out repeats across "
    "vintages and aggregate rows rebuilt into their parts.",
    "disbursements": "CRS disbursements allocated to the climate commitments they "
    "pay, weighted by the commitment's markers or climate share, and Rio-marked CRS "
    "disbursements that reach no commitment, weighted by their own markers.",
    "grant_equivalent": "The grant equivalent of those disbursements; records "
    "without one stay missing.",
}


class Agency(BaseModel):
    """One agency of a provider, by DAC provider and agency code, such as the European
    Investment Bank, agency 3 of the EU Institutions (918)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    provider: int
    agency: int


type Target = int | Agency
type Entry = tuple[int, int | None, str]


def resolve(
    values: str | int | Agency | Iterable[str | int | Agency] | None, *, kind: Coded
) -> tuple[Target, ...] | None:
    """The DAC codes and agencies `values` name, sorted by provider and agency and
    without duplicates, or None for None.

    A string is a name from `data/names.csv`, matched ignoring case, accents and
    surrounding spaces, or a string of digits, read as a DAC code. An integer is a DAC
    code and an `Agency` an agency, both passed through. An agency whose provider is
    also named is dropped, since the provider covers it. Recipients have no agencies.

    Raises:
        UnknownProviderError: for a provider name of no entry, or of several.
        UnknownRecipientError: for a recipient name of no entry, or of several.
        TypeError: for a value of another type, a bool included, or for an
            `Agency` among recipients.
    """
    if values is None:
        return None
    if isinstance(values, str | Integral | Agency) or not isinstance(values, Iterable):
        targets = {_target(values, kind=kind)}
    else:
        targets = {_target(value, kind=kind) for value in values}
    covered = {target for target in targets if isinstance(target, int)}
    return tuple(
        sorted(
            (
                target
                for target in targets
                if not (isinstance(target, Agency) and target.provider in covered)
            ),
            key=_order,
        )
    )


class Catalogue:
    """One listing, with `.list()`, `.info(name)` and `.search(text)`."""

    def __init__(self, *, kind: Kind) -> None:
        self.kind = kind

    def __repr__(self) -> str:
        return (
            f"cf.{self.kind}: {self.list().height} entries; "
            ".list(), .info(name), .search(text)"
        )

    def list(self) -> pl.DataFrame:
        """Every entry as a table."""
        return _table(self.kind)

    def info(self, name: str | int | Agency) -> str:
        """A description of one entry, by name or, for providers and recipients, by
        DAC code. A provider name of an agency, or an `Agency`, describes the agency
        and names its provider.

        Raises:
            UnknownNameError: when no entry has that name or code. The provider and
                recipient listings raise its subclasses `UnknownProviderError` and
                `UnknownRecipientError`, also for a name of several entries.
        """
        if self.kind not in ("providers", "recipients"):
            if not isinstance(name, str):
                raise self._unknown(str(name), suggestions=())
            return self.list()["description"][self._index(name)]
        target = self.code(name)
        if isinstance(target, Agency):
            agency = _agency_names().get((target.provider, target.agency))
            if agency is None:
                raise self._unknown(repr(target), suggestions=())
            provider = self._row(target.provider)
            return (
                f"{agency} (agency {target.agency} of {provider['name']}, "
                f"DAC code {target.provider})."
            )
        row = self._row(target)
        if self.kind == "providers":
            return f"{row['name']} (DAC code {row['code']}): {row['type']}."
        return f"{row['name']} (DAC code {row['code']})."

    def search(self, text: str) -> pl.DataFrame:
        """Entries whose names contain `text`, ignoring case."""
        return self.list().filter(
            pl.col("name").str.to_lowercase().str.contains(text.lower(), literal=True)
        )

    def code(self, name: str | int | Agency) -> Target:
        """The DAC code of a provider or recipient name, or the `Agency` a provider
        name names, as `resolve` reads it.

        Raises:
            UnknownProviderError: for a provider name of no entry, or of several.
            UnknownRecipientError: for a recipient name of no entry, or of several.
            TypeError: on `cf.flows` and `cf.methods`, which have no codes.
        """
        if self.kind not in ("providers", "recipients"):
            raise TypeError(f"cf.{self.kind} has names, not codes")
        return _target(name, kind=self.kind)

    def _row(self, code: int) -> dict[str, object]:
        rows = self.list().filter(pl.col("code") == code)
        if rows.is_empty():
            raise self._unknown(str(code), suggestions=())
        return rows.row(0, named=True)

    def _index(self, name: str) -> int:
        """The row of `name`, matched ignoring case and accents, or an error whose
        suggestions list close spellings first, then names that contain `name`."""
        names = self.list()["name"].to_list()
        folded = [_fold(entry) for entry in names]
        key = _fold(name)
        if key in folded:
            return folded.index(key)
        close = [
            names[folded.index(match)]
            for match in get_close_matches(key, folded, n=_SUGGESTIONS)
        ]
        containing = [
            entry for entry, low in zip(names, folded, strict=True) if key in low
        ]
        suggestions = list(dict.fromkeys([*close, *containing]))[:_SUGGESTIONS]
        raise self._unknown(name, suggestions=suggestions)

    def _unknown(self, name: str, *, suggestions: Sequence[str]) -> UnknownNameError:
        return _unknown(name, kind=self.kind, suggestions=suggestions)


def _unknown(
    name: str, *, kind: Kind, suggestions: Sequence[str], note: str | None = None
) -> UnknownNameError:
    if kind == "providers":
        return UnknownProviderError(name, suggestions=suggestions, note=note)
    if kind == "recipients":
        return UnknownRecipientError(name, suggestions=suggestions, note=note)
    return UnknownNameError(name, kind=kind[:-1], suggestions=suggestions, note=note)


def _target(value: object, *, kind: Coded) -> Target:
    """The code or agency one value names."""
    if kind == "recipients" and isinstance(value, Agency):
        raise TypeError(f"recipients takes names or DAC codes, not {value!r}")
    if isinstance(value, bool) or not isinstance(value, str | Integral | Agency):
        raise TypeError(
            f"{kind} takes names, DAC codes or cf.Agency values, not "
            f"{value!r} ({type(value).__name__})"
        )
    if isinstance(value, Agency):
        return value
    if isinstance(value, Integral):
        return int(value)
    key = _fold(value).strip()
    if key.isdigit():
        return int(key)
    entries = _aliases(kind).get(key)
    if entries is None:
        raise _unknown(value, kind=kind, suggestions=_close_names(key, kind=kind))
    if len(entries) > 1:
        raise _unknown(
            value,
            kind=kind,
            suggestions=[_label(entry) for entry in entries],
            note=_several(value, entries=entries, kind=kind),
        )
    [(code, agency, _)] = entries
    return code if agency is None else Agency(provider=code, agency=agency)


def _order(target: Target) -> tuple[int, int]:
    if isinstance(target, Agency):
        return target.provider, target.agency
    return target, -1


def _label(entry: Entry) -> str:
    """How an error names an entry: its name, and an agency's provider."""
    code, agency, name = entry
    if agency is None:
        return name
    return f"{name} ({providers._row(code)['name']})"


def _several(name: str, *, entries: Sequence[Entry], kind: Coded) -> str:
    """The opening of the error for a name of several entries."""
    owners = {code for code, _, _ in entries}
    if all(agency is not None for _, agency, _ in entries):
        if len(owners) > 1:
            return f"{name!r} names agencies of {len(owners)} providers."
        return f"{name!r} names {len(entries)} agencies of one provider."
    return f"{name!r} names {len(entries)} {kind}."


def _close_names(key: str, *, kind: Coded) -> list[str]:
    """Names of the entries whose keys contain `key`, shortest key first, then of
    entries whose keys are close to it, closest first."""
    aliases = _aliases(kind)
    containing = sorted((alias for alias in aliases if key in alias), key=len)
    close = get_close_matches(key, aliases, n=_CLOSE_KEYS)
    labels = [
        _label(entry) for alias in [*containing, *close] for entry in aliases[alias]
    ]
    return list(dict.fromkeys(labels))[:_SUGGESTIONS]


def _fold(name: str) -> str:
    """`name` without case, accents or typographic apostrophes, for matching."""
    decomposed = unicodedata.normalize("NFKD", name.replace("\u2019", "'"))
    return "".join(
        char for char in decomposed if not unicodedata.combining(char)
    ).casefold()


@cache
def _aliases(kind: Coded) -> dict[str, tuple[Entry, ...]]:
    """`data/names.csv` for `kind`: each key's code, agency and name."""
    found: dict[str, list[Entry]] = {}
    for key, code, agency, name in (
        _names().filter(kind=kind[:-1]).select("key", "code", "agency", "name")
    ).iter_rows():
        found.setdefault(key, []).append((code, agency, name))
    return {key: tuple(entries) for key, entries in found.items()}


@cache
def _agency_names() -> dict[tuple[int, int], str]:
    agencies = _names().filter(pl.col("agency").is_not_null())
    return {
        (code, agency): name
        for code, agency, name in agencies.select("code", "agency", "name").iter_rows()
    }


@cache
def _names() -> pl.DataFrame:
    return pl.read_csv(
        (files("climate_finance") / "data" / "names.csv").read_bytes(),
        schema_overrides={"code": pl.Int32, "agency": pl.Int32},
    )


@cache
def _table(kind: Kind) -> pl.DataFrame:
    data = files("climate_finance") / "data"
    if kind == "providers":
        return pl.read_csv(
            data.joinpath("oecd_classification.csv").read_bytes(),
            columns=["code", "name", "type"],
            schema_overrides={"code": pl.Int32},
        )
    if kind == "recipients":
        return pl.read_csv(
            data.joinpath("recipients.csv").read_bytes(),
            schema_overrides={"code": pl.Int32},
        )
    if kind == "flows":
        return pl.DataFrame({"name": [*_FLOWS], "description": [*_FLOWS.values()]})
    return _methods()


def _methods() -> pl.DataFrame:
    # Imported here: methods.py binds its listing from this module at import time.
    from climate_finance import methods

    named = {"ONE": methods.ONE, "OECD": methods.OECD, "PROVIDER": methods.PROVIDER}
    rows = [
        {"name": name, "description": methods.describe(method), **method.model_dump()}
        for name, method in named.items()
    ]
    rows.append({"name": "RANGE", "description": methods.describe(methods.RANGE)})
    return pl.DataFrame(rows)


providers = Catalogue(kind="providers")
recipients = Catalogue(kind="recipients")
flows = Catalogue(kind="flows")

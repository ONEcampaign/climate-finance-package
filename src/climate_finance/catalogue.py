"""Listings: `cf.providers`, `cf.recipients`, `cf.flows` and `cf.methods`.

Provider names come from `data/oecd_classification.csv`, the OECD's donor list.
Recipient names come from `data/recipients.csv`: DAC recipient codes and their English
names as the OECD CRS bulk download spells them, carried over from climate-finance
1.2.3 (`oecd/cleaning_tools/recipient_names.feather`, built from the 2021 CRS).

Names resolve to codes by exact match, ignoring case and accents; close names are
offered as suggestions in the error, never used in their place.
"""

import unicodedata
from collections.abc import Sequence
from difflib import get_close_matches
from functools import cache
from importlib.resources import files
from typing import Literal

import polars as pl

from climate_finance.errors import (
    UnknownNameError,
    UnknownProviderError,
    UnknownRecipientError,
)

type Kind = Literal["providers", "recipients", "flows", "methods"]

_SUGGESTIONS = 5
_FLOWS = {
    "commitments": "CRDF commitments at face value, leaving out repeats across "
    "vintages and aggregate rows rebuilt into their parts.",
    "disbursements": "CRS disbursements allocated to the climate commitments they "
    "pay, weighted by the commitment's markers or climate share.",
    "grant_equivalent": "The grant equivalent of those disbursements; records "
    "without one stay missing.",
}


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

    def info(self, name: str | int) -> str:
        """A description of one entry, by name or, for providers and recipients, by
        DAC code.

        Raises:
            UnknownNameError: when no entry has that name or code. The provider and
                recipient listings raise its subclasses `UnknownProviderError` and
                `UnknownRecipientError`.
        """
        if isinstance(name, int):
            rows = self.list().filter(pl.col("code") == name)
            if rows.is_empty():
                raise self._unknown(str(name), suggestions=())
            row = rows.row(0, named=True)
        else:
            row = self.list().row(self._index(name), named=True)
        if self.kind == "providers":
            return f"{row['name']} (DAC code {row['code']}): {row['type']}."
        if self.kind == "recipients":
            return f"{row['name']} (DAC code {row['code']})."
        return row["description"]

    def search(self, text: str) -> pl.DataFrame:
        """Entries whose names contain `text`, ignoring case."""
        return self.list().filter(
            pl.col("name").str.to_lowercase().str.contains(text.lower(), literal=True)
        )

    def code(self, name: str) -> int:
        """The DAC code of a provider or recipient name.

        Raises:
            UnknownProviderError: for a provider name not in the listing.
            UnknownRecipientError: for a recipient name not in the listing.
        """
        return self.list()["code"][self._index(name)]

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
        if self.kind == "providers":
            return UnknownProviderError(name, suggestions=suggestions)
        if self.kind == "recipients":
            return UnknownRecipientError(name, suggestions=suggestions)
        return UnknownNameError(name, kind=self.kind[:-1], suggestions=suggestions)


def _fold(name: str) -> str:
    """`name` without case, accents or typographic apostrophes, for matching."""
    decomposed = unicodedata.normalize("NFKD", name.replace("\u2019", "'"))
    return "".join(
        char for char in decomposed if not unicodedata.combining(char)
    ).casefold()


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

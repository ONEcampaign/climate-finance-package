"""Listings: `cf.providers`, `cf.flows` and `cf.methods`."""

from typing import Literal

import polars as pl


class Catalogue:
    """One listing, with `.list()`, `.info(name)` and `.search(text)`."""

    def __init__(self, *, kind: Literal["providers", "flows", "methods"]) -> None:
        self.kind = kind

    def list(self) -> pl.DataFrame:
        """Every entry as a table."""
        raise NotImplementedError("slice S09b")

    def info(self, name: str) -> str:
        """A description of one entry."""
        raise NotImplementedError("slice S09b")

    def search(self, text: str) -> pl.DataFrame:
        """Entries whose names match `text`."""
        raise NotImplementedError("slice S09b")


providers = Catalogue(kind="providers")
flows = Catalogue(kind="flows")

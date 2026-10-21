"""Queries as lazy polars plans over release tables, weighted at query time."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING

from climate_finance.result import Quality, Result
from climate_finance.spec import Spec

if TYPE_CHECKING:
    from climate_finance.snapshot import Snapshot


def spending(snapshot: Snapshot, *, spec: Spec) -> Result:
    """Climate finance for `spec` from `snapshot`'s release tables."""
    raise NotImplementedError("slice S09a")


def quality(
    snapshot: Snapshot,
    *,
    providers: Sequence[str | int] | None = None,
    years: Iterable[int] | None = None,
) -> Quality:
    """Coverage and status statistics; the precision interval comes from the gate."""
    raise NotImplementedError("slice S09a (interval: slice S17)")

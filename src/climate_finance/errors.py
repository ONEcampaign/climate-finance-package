"""Errors raised by the analysis layer and shared with the build."""

from collections.abc import Mapping, Sequence


class ClimateFinanceError(Exception):
    """Base class for every climate_finance error."""


class SourceUnavailableError(ClimateFinanceError):
    """A source file or service could not be read."""


class UnknownProviderError(ClimateFinanceError, LookupError):
    """A provider name or code is not in the release."""

    def __init__(self, name: str, *, suggestions: Sequence[str] = ()) -> None:
        self.name = name
        self.suggestions = tuple(suggestions)
        hint = f" Did you mean: {', '.join(self.suggestions)}?" if suggestions else ""
        super().__init__(f"Unknown provider {name!r}.{hint}")


class ReleaseIntegrityError(ClimateFinanceError):
    """A release table is missing or its sha256 differs from release.json."""


class SchemaError(ClimateFinanceError):
    """A table does not match its schema; `problems` maps each bad column to why."""

    def __init__(self, table: str, problems: Mapping[str, str]) -> None:
        self.table = table
        self.problems = dict(problems)
        detail = "; ".join(f"{column}: {why}" for column, why in self.problems.items())
        super().__init__(f"Table {table!r} fails its schema: {detail}")

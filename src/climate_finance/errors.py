"""Errors raised by the analysis layer and shared with the build."""

from collections.abc import Mapping, Sequence


class ClimateFinanceError(Exception):
    """Base class for every climate_finance error."""


class SourceUnavailableError(ClimateFinanceError):
    """A source file or service could not be read."""


class UnknownNameError(ClimateFinanceError, LookupError):
    """A name is not in its listing. `suggestions` holds close names, if any."""

    def __init__(
        self, name: str, *, kind: str, suggestions: Sequence[str] = ()
    ) -> None:
        self.name = name
        self.kind = kind
        self.suggestions = tuple(suggestions)
        hint = f" Did you mean: {', '.join(self.suggestions)}?" if suggestions else ""
        super().__init__(f"Unknown {kind} {name!r}.{hint}")


class UnknownProviderError(UnknownNameError):
    """A provider name is not in `cf.providers`."""

    def __init__(self, name: str, *, suggestions: Sequence[str] = ()) -> None:
        super().__init__(name, kind="provider", suggestions=suggestions)


class UnknownRecipientError(UnknownNameError):
    """A recipient name is not in `cf.recipients`."""

    def __init__(self, name: str, *, suggestions: Sequence[str] = ()) -> None:
        super().__init__(name, kind="recipient", suggestions=suggestions)


class ReleaseIntegrityError(ClimateFinanceError):
    """A release table is missing, its sha256 differs from release.json, or two of
    its tables disagree."""


class SchemaError(ClimateFinanceError):
    """A table does not match its schema. `problems` maps each bad column to why."""

    def __init__(self, table: str, problems: Mapping[str, str]) -> None:
        self.table = table
        self.problems = dict(problems)
        detail = "; ".join(f"{column}: {why}" for column, why in self.problems.items())
        super().__init__(f"Table {table!r} fails its schema: {detail}")

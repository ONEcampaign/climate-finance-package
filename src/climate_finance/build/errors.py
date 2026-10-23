"""Errors raised by the build."""

from pathlib import Path

from climate_finance.errors import ClimateFinanceError


class BuildError(ClimateFinanceError):
    """Base class for build errors."""


class ConfigError(BuildError):
    """`build.toml` is missing or invalid."""


class StageInputMissingError(BuildError):
    """A stage reads a table that no earlier stage has written."""

    def __init__(self, *, stage: str, table: str, path: Path) -> None:
        self.stage = stage
        self.table = table
        self.path = path
        super().__init__(
            f"Stage {stage!r} has not written {table!r} ({path}); run it first."
        )


class StageOutputError(BuildError):
    """A stage returned tables other than the ones its schema declares."""


class CostCapExceededError(BuildError):
    """A batch of model calls is estimated to cost more than the per-build cap."""

    def __init__(self, *, estimate_usd: float, cap_usd: float) -> None:
        self.estimate_usd = estimate_usd
        self.cap_usd = cap_usd
        super().__init__(
            f"Estimated cost USD {estimate_usd:.2f} exceeds the cap of "
            f"USD {cap_usd:.2f}."
        )


class AllocationInputError(BuildError):
    """Links that allocation cannot split without breaking a face cap or a total."""


class LinkInputError(BuildError):
    """Link-stage inputs that a linking route cannot use."""


class ReviewDecisionsError(BuildError):
    """A reviewed CSV whose decided rows cannot be recorded."""


class ReleaseInvariantError(BuildError):
    """Release tables break `schema.release_invariants`."""

    def __init__(self, problems: list[str]) -> None:
        self.problems = problems
        shown = "; ".join(problems[:10])
        super().__init__(f"{len(problems)} release invariant problems: {shown}")


class GateFailedError(BuildError):
    """A release fails one or more of its gates (`build.gate.run_gates`)."""

    def __init__(self, failures: list[str]) -> None:
        self.failures = failures
        super().__init__(f"{len(failures)} gate rows failed: {'; '.join(failures)}")

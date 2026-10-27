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


class StaleStageError(BuildError):
    """A stage reads an earlier stage built from other inputs or other settings."""

    def __init__(self, *, stage: str, reason: str) -> None:
        self.stage = stage
        self.reason = reason
        super().__init__(f"Stage {stage!r} {reason}; rerun the build from {stage!r}.")


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


class LinkThresholdError(BuildError):
    """F1 of the links at the fixed link threshold falls more than
    `link.score.F1_TOLERANCE` below its best on the threshold grid. The links are the
    classifier's on its training pairs (`link.score`) or the Jev band's on the
    labelled pairs of single-id donors (`link.jev.best_link_threshold`)."""

    def __init__(
        self, *, threshold: float, f1: float, best_threshold: float, best_f1: float
    ) -> None:
        self.threshold = threshold
        self.f1 = f1
        self.best_threshold = best_threshold
        self.best_f1 = best_f1
        super().__init__(
            f"Training F1 is {f1:.4f} at the link threshold {threshold:.2f} and "
            f"{best_f1:.4f} at {best_threshold:.2f}; refit the link threshold."
        )


class ReviewDecisionsError(BuildError):
    """Decisions that cannot be recorded, such as a labels CSV whose pairs do not
    match the packet it answers."""


class SampleDesignError(BuildError):
    """A blind sample's design that no precision estimate can read, such as one
    with a NaN weight."""


class LabelPacketError(BuildError):
    """A labelling packet that cannot be rendered as asked."""


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


class GhError(BuildError):
    """The GitHub CLI, `gh`, is not installed, exits with an error or runs out of
    time."""


class RepositoryNameError(BuildError):
    """A repository to publish to that is not `owner/name`."""


class ReleaseNameError(BuildError):
    """A release name that is not a published name, such as `2026a` or `2026a.1`, or
    a pack whose file is named for another release."""


class AlreadyPublishedError(BuildError):
    """A release the published index already lists. A name names one pack."""


class DriftBaselineError(BuildError):
    """A release whose drift gate compared it with a release other than the one
    publishing needs, or with a copy from another pack."""


class PublishedIndexError(BuildError):
    """The published `releases.json` is not JSON, not an index this package reads,
    or names a latest release that is not a published name."""


class MissingIndexError(BuildError):
    """The `data` release holds packs but no `releases.json`, as after a publish
    that stopped before its index was in place."""


class IndexReplacedError(BuildError):
    """The `releases.json` read back after a publish differs from the one it wrote,
    so another publish replaced it meanwhile."""


class DraftReleaseError(BuildError):
    """The `data` release is a draft, whose assets only maintainers can download."""


class MissingAssetError(BuildError):
    """The `data` release lacks an asset a publish has just uploaded."""

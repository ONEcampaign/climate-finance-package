"""The fetch stage: oda-reader calls, local archives, and `raw_manifest`."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from climate_finance.build.stages import StageContext, StageOutput


def run_fetch(context: StageContext) -> StageOutput:
    """Fetch every source into the cache and write `raw_manifest`.

    Raises:
        SourceUnavailableError: when a challenged file needs a browser download
            (after printing the `register_bulk_file` call for each), or when
            `context.offline` is set and a source is not cached.
    """
    raise NotImplementedError("slice S04a")

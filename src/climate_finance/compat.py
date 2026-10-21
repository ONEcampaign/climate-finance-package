"""`ClimateData`: the 1.x interface as a thin, deprecated wrapper over `spending()`."""

from __future__ import annotations

from typing import TYPE_CHECKING, Self

if TYPE_CHECKING:
    import pandas as pd


class ClimateData:
    """The 1.2.3 constructor and methods, returning pandas; needs [compat]."""

    def __init__(
        self,
        years: list[int] | range,
        providers: list[int | str] | int | None = None,
        recipients: list[int | str] | int | None = None,
        currency: str = "USD",
        prices: str = "current",
        base_year: int | None = None,
    ) -> None:
        raise NotImplementedError("slice S09b")

    def set_only_oda(self) -> Self:
        """Count ODA only."""
        raise NotImplementedError("slice S09b")

    def set_update_data(self) -> Self:
        """Fetch the newest release index on load."""
        raise NotImplementedError("slice S09b")

    def set_oecd_spending_methodology(self) -> Self:
        """Use `methods.OECD`."""
        raise NotImplementedError("slice S09b")

    def set_one_spending_methodology(self) -> Self:
        """Use `methods.ONE`."""
        raise NotImplementedError("slice S09b")

    def set_custom_spending_methodology(
        self,
        coefficients: tuple[float, float],
        highest_marker: bool = True,
    ) -> Self:
        """Use a custom `Method` with (significant, principal) coefficients."""
        raise NotImplementedError("slice S09b")

    def load_spending_data(
        self,
        methodology: str = "ONE",
        flows: str | list[str] = "gross_disbursements",
        source: str | list[str] = "OECD_CRS",
    ) -> Self:
        """Run `spending()` for the configured arguments."""
        raise NotImplementedError("slice S09b")

    def get_data(self) -> pd.DataFrame:
        """The loaded data as pandas."""
        raise NotImplementedError("slice S09b")

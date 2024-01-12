from climate_finance.data.enums import (
    ValidPrices,
    ValidCurrencies,
    ValidPerspective,
    SpendingMethodologies,
)


class ClimateData:
    def __init__(
        self,
        years: list[int] | range,
        providers: list[int | str] | int | None = None,
        recipients: list[int | str] | int | None = None,
        currency: ValidCurrencies | str = "USD",
        prices: ValidPrices | str = "current",
        base_year: int | None = None,
    ):
        """
        Args:
            years: A list of integers or a range. Not all years may be available

            providers: Optional. A list of integers or strings, an integer, or None.
            The data is filtered to include only the providers specified. If None,
            all available providers are included.

            recipients: Optional. A list of integers or strings, an integer, or None.
            The data is filtered to include only the recipients specified. If None,
            all available recipients are included.

            currency: Optional. A string specifying the currency to use. Defaults to
            USD. Only USD, EUR, CAN, and GBP are supported.

            prices: Optional. A string specifying the price type to use. Defaults to
            current. Only current and constant are supported.

            base_year: Optional. An integer specifying the base year to use. Defaults
            to None. Only used when prices are set to constant.

        """
        self.years = list(years) if isinstance(years, range) else years
        self.providers = [providers] if isinstance(providers, int) else providers
        self.recipients = [recipients] if isinstance(recipients, int) else recipients
        self.currency: ValidCurrencies | str = ValidCurrencies(currency)
        self.prices: ValidPrices | str = ValidPrices(prices)
        self.base_year = base_year

        if self.prices == "constant" and self.base_year is None:
            raise ValueError("You must provide a base year when using constant prices")

        if self.prices == "current" and self.base_year is not None:
            raise ValueError("You cannot provide a base year when using current prices")

        # By default, the data is not loaded
        self.data = None

        # Other data attributes
        self.spending_args: dict = {
            "methodology": SpendingMethodologies("ONE"),
            "oda_only": False,
        }

    @property
    def _years_str(self):
        years_min = min(self.years)
        years_max = max(self.years)
        return f"{years_min}-{years_max}" if years_min != years_max else f"{years_min}"

    def __repr__(self):
        message: str = f"ClimateData object for {self._years_str}. "

        if self.data is None:
            message += "No data has been loaded yet. "

        return message

    def _update_spending_args(self, **kwargs):
        if "methodology" in kwargs:
            kwargs["methodology"] = ValidCurrencies(kwargs["methodology"])

        if "perspective" in kwargs:
            kwargs["perspective"] = ValidPerspective(kwargs["perspective"])

        self.spending_args.update(kwargs)

    def get_spending(
        self,
        perspective: ValidPerspective | str = "provider",
        methodology: SpendingMethodologies | str = "ONE",
        oda_only: bool = False,
    ):
        self._update_spending_args(
            perspective=perspective, methodology=methodology, oda_only=oda_only
        )


if __name__ == "__main__":
    climate = ClimateData(years=range(2015, 2020))

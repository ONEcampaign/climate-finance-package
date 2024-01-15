import pandas as pd

from climate_finance.core import loaders
from climate_finance.core.enums import (
    ValidPrices,
    ValidCurrencies,
    ValidPerspective,
    SpendingMethodologies,
    ValidFlows,
    ValidSources,
    Coefficients,
)
from climate_finance.core.validation import (
    validate_prices_and_base_year,
    validate_methodology,
    validate_list_of_str,
    validate_source,
)
from climate_finance.methodologies.bilateral.bilateral_methodologies import (
    transform_markers_into_indicators,
    base_oecd_transform_markers_into_indicators,
    process_crs_climate_indicators,
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
            USD.

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

        # Validate the prices and base year
        validate_prices_and_base_year(prices=self.prices, base_year=self.base_year)

        # By default, the data is not loaded
        self.data: dict[str, pd.DataFrame] = {}

        # By default, raw data sources are not loaded
        self._data: dict[str, pd.DataFrame] = {}

        # Other data attributes
        self.spending_args: dict = {
            "flows": ValidFlows("gross_disbursements"),
            "oda_only": False,
        }

        # By default, the custom methodology is not set
        self._custom_methodology_set: bool = False

        # By default, the data is not updated
        self._update_data: bool = False

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
        """Update the spending_args dictionary with the provided kwargs

        This method also handles validation of the provided arguments.
        """

        # Check if methodology is provided and validate
        if "methodology" in kwargs:
            # Convert the methodology to a SpendingMethodologies enum
            kwargs["methodology"] = SpendingMethodologies(kwargs["methodology"])

            # Validate the settings
            validate_methodology(
                methodology=kwargs["methodology"],
                custom_methodology=self._custom_methodology_set,
            )
            # Set the spending methodology if needed
            if kwargs["methodology"] == "OECD":
                self.set_oecd_spending_methodology()
            elif kwargs["methodology"] == "ONE":
                self.set_one_spending_methodology()

        # Check if flows is provided and validate
        if "perspective" in kwargs:
            kwargs["perspective"] = ValidPerspective(kwargs["perspective"])

        # Check if flows is provided and validate. This includes converting a string
        # to a list of validated enums.
        if "flows" in kwargs:
            kwargs["flows"] = validate_list_of_str(
                values=kwargs["flows"], valid_enum=ValidFlows
            )

        # Check if source is provided and validate. This includes converting a string
        # to a list of validated enums.
        if "source" in kwargs:
            kwargs["source"] = validate_list_of_str(
                values=kwargs["source"], valid_enum=ValidSources
            )
            validate_source(source=kwargs["source"])

        # Update the spending_args dictionary
        self.spending_args.update(kwargs)

    def _load_sources(self) -> None:
        """
        Load sources into the _data object.
        """
        for source in self.spending_args["source"]:
            if source not in self._data:
                self._data[source] = loaders.get_data(
                    source_name=source,
                    settings={
                        "years": self.years,
                        "providers": self.providers,
                        "recipients": self.recipients,
                        "update": self._update_data,
                    },
                )

    def _validate_transformation_conditions(self, source: str) -> None:
        """
        Validates the conditions required for data transformation.
        Raises AttributeError if conditions are not met.
        """
        # Get the spending_args
        methodology = self.spending_args["methodology"]

        # Raise an error when custom methodologies are not valid
        if methodology in ["ONE", "custom"] and "CRDF" in source:
            raise AttributeError("Custom methodologies cannot be applied on CRDF data")

    def _get_transform_function(self, source: str) -> callable:
        """
        Determines the appropriate transformation function based on spending_args.
        Returns a callable function or None if no transformation is applicable.
        """

        # Get the spending_args
        methodology = self.spending_args["methodology"]

        if source == "OECD_CRS":
            return transform_markers_into_indicators

    def _transform_to_climate(self, source: str) -> None:
        """
        Transforms climate data based on the methodology and data source specified in spending_args.
        Updates self.data with the transformed data.
        """
        # Apply guard conditions to validate the transformation
        self._validate_transformation_conditions(source=source)

        # Get the transformation function
        transform_function = self._get_transform_function(source=source)

        # Transform the data, if a transformation function is available
        if transform_function:
            self.data[source] = transform_function(
                df=self._data[source],
                percentage_significant=self.spending_args["coefficients"][0],
                percentage_principal=self.spending_args["coefficients"][1],
                highest_marker=self.spending_args["highest_marker"],
            )

    def set_only_oda(self) -> "ClimateData":
        """
        Sets the spending arguments to only consider ODA
        (Official Development Assistance) data.
        """
        self._update_spending_args(oda_only=True)

        return self

    def set_update_data(self) -> "ClimateData":
        """Force the loaded data to be updated from the source."""
        self._update_data = True

        return self

    def set_oecd_spending_methodology(self) -> "ClimateData":
        """Set the required parameters when choosing the OECD spending methodology."""

        self._update_spending_args(coefficients=(1, 1), highest_marker=False)

        return self

    def set_one_spending_methodology(self) -> "ClimateData":
        """Set the required parameters when choosing the ONE spending methodology."""

        self._update_spending_args(coefficients=(0.4, 1), highest_marker=True)

        return self

    def set_custom_spending_methodology(
        self,
        coefficients: Coefficients | tuple[int | float, int | float],
        highest_marker: bool = True,
    ) -> "ClimateData":
        """Set the required parameters when choosing a custom spending methodology.

        Args:
            coefficients: A tuple of coefficients (significant, principal).
            highest_marker: Optional. A boolean specifying whether to use the highest
            marker when calculating the custom methodology. Defaults to True.

        """
        # Flag that the custom methodology has been set
        self._custom_methodology_set = True

        # Update the spending_args dictionary
        self._update_spending_args(
            coefficients=coefficients,
            highest_marker=highest_marker,
        )

        return self

    def load_spending_data(
        self,
        perspective: ValidPerspective | str = "recipient",
        methodology: SpendingMethodologies | str = "ONE",
        flows: ValidFlows | str | list[ValidFlows | str] = "gross_disbursements",
        source: ValidSources | str | list[ValidSources | str] = "OECD_CRS",
    ) -> "ClimateData":
        # update the configuration to load the right data into the object.
        # This process also handles validation.
        self._update_spending_args(
            perspective=perspective,
            methodology=methodology,
            flows=flows,
            source=source,
        )

        # Load the data
        self._load_sources()

        # Transform to climate
        for source in self._data:
            self._transform_to_climate(source=source)

        return self


if __name__ == "__main__":
    climate = ClimateData(
        years=[2021], prices="constant", base_year=2021, providers=[4], recipients=[248]
    ).load_spending_data(perspective="recipient", source="OECD_CRS", methodology="ONE")

    df = climate.data["OECD_CRS"].loc[lambda d: d.indicator != "Not climate relevant"]

import pandas as pd
from oda_data import set_data_path
from oda_data.clean_data.channels import add_multi_channel_codes

from climate_finance.config import logger, ClimateDataPath
from climate_finance.core import loaders
from climate_finance.core.deflators import oecd_deflator
from climate_finance.core.enums import (
    ValidPrices,
    ValidCurrencies,
    SpendingMethodologies,
    ValidFlows,
    ValidSources,
    Coefficients,
)
from climate_finance.core.tools import (
    standard_shareby,
    groupby_none,
    data_to_share,
    filter_flows,
    clean_multi_contributions,
    align_oda_data_names,
    validate_multi_shares_groupers,
    validate_multi_groupby,
    merge_spending_and_contributions,
    calculate_imputations,
    remove_channel_name_from_spending_data,
    groupby_sum,
    get_oecd_classification,
    get_available_providers,
    match_providers,
)
from climate_finance.core.validation import (
    validate_prices_and_base_year,
    validate_methodology,
    validate_list_of_str,
    validate_source,
)
from climate_finance.methodologies.spending.crdf import (
    transform_crdf_into_indicators,
)
from climate_finance.methodologies.spending.crdf_crs import (
    transform_crs_crdf_into_indicators,
)
from climate_finance.methodologies.spending.crs import (
    transform_markers_into_indicators,
)
from climate_finance.oecd.cleaning_tools.tools import (
    multi_flows_to_indicators,
    get_contributions_data,
)

# set oda_data path
set_data_path(ClimateDataPath.raw_data)

DEFLATOR: callable = oecd_deflator


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
        Access Climate Finance data using data from the OECD or UNFCCC.

        Args:
            years: A list of integers or a range. Not all years may be available.

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

        # Validate providers
        if not all(isinstance(provider, int) for provider in self.providers):
            self.providers = match_providers(self.providers)

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

        # By default, respect prices and currency request
        self._ignore_prices_and_currency: bool = False

    @property
    def _years_str(self):
        years_min = min(self.years)
        years_max = max(self.years)
        return f"{years_min}-{years_max}" if years_min != years_max else f"{years_min}"

    @staticmethod
    def available_methodologies():
        """Print all the valid, available methodologies"""
        SpendingMethodologies.print_available(name="methodologies")

    @staticmethod
    def available_flows():
        """Print all the valid, available flows"""
        ValidFlows.print_available(name="flows")

    @staticmethod
    def available_sources():
        """Print all the valid, available flows"""
        ValidSources.print_available(name="sources")

    @staticmethod
    def available_prices():
        """Print all the valid, available flows"""
        ValidPrices.print_available(name="prices")

    @staticmethod
    def available_currencies():
        """Print all the valid, available flows"""
        ValidCurrencies.print_available(name="currencies")

    @staticmethod
    def available_providers(include_private: bool = False):
        """return a list of available providers"""

        return get_available_providers(include_private=include_private)

    @staticmethod
    def get_dac_providers(include_eui: bool = False) -> dict:
        """return a list of DAC providers

        Args:
            include_eui: Optional. A boolean specifying whether to include the EU institutions.
            Defaults to False.

        """
        dac = get_oecd_classification()["DAC member"]

        if not include_eui:
            dac = {k: v for k, v in dac.items() if k != 918}

        return dac

    @staticmethod
    def get_non_dac_providers() -> dict:
        """return a list of non DAC providers"""
        return get_oecd_classification()["Non-DAC member"]

    @staticmethod
    def get_multilateral_providers(include_eui: bool = True) -> dict:
        """return a list of multilateral providers"""
        multi = get_oecd_classification()["Multilateral donor"]

        if include_eui:
            return multi | {918: "EU Institutions"}

        return multi

    @staticmethod
    def get_private_providers() -> dict:
        """return a list of private providers"""
        return get_oecd_classification()["Private donor"]

    def __repr__(self):
        message: str = f"ClimateData object for {self._years_str}. "

        if self.data is None:
            message += "No data has been loaded yet. "

        return message

    def _validate_loaded_data(self, message: str) -> None:
        """Validate the loaded data and raise an error if it is not valid."""
        if len(self.data) < 1:
            raise AttributeError(message)

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
        if "OECD_CRDF_CRS" in self.spending_args["source"]:
            self.spending_args["source"].remove("OECD_CRDF_CRS")
            self.spending_args["source"].extend(["OECD_CRS_RAW", "OECD_CRDF"])

        if "OECD_CRDF_CRS_ALLOCABLE" in self.spending_args["source"]:
            self.spending_args["source"].remove("OECD_CRDF_CRS_ALLOCABLE")
            self.spending_args["source"].extend(["OECD_CRS_RAW_ALLOCABLE", "OECD_CRDF"])

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

        # Inform custom methodologies cannot always be applied
        if methodology in ["ONE", "custom"] and "CRDF" in source:
            logger.info(
                "Custom methodologies cannot be applied to"
                "climate components data, which may be included in the CRDF dataset"
                " for multilateral organisations."
            )

    @staticmethod
    def _get_transform_function(source: str) -> callable:
        """
        Determines the appropriate transformation function based on spending_args.
        Returns a callable function or None if no transformation is applicable.
        """

        if source == "OECD_CRS" or source == "OECD_CRS_ALLOCABLE":
            return transform_markers_into_indicators
        elif source == "OECD_CRDF":
            return transform_crdf_into_indicators

    def _transform_to_climate(self, source: str) -> None:
        """
        Transforms climate data based on the methodology and data source specified in spending_args.
        Updates self.data with the transformed data.
        """
        # Check if the source is OECD_CRDF_CRS as that requires a separate methodology.
        if source == "OECD_CRDF_CRS":
            logger.info(f"Creating {source} data...")
            self.data["OECD_CRDF_CRS"] = transform_crs_crdf_into_indicators(
                crdf=self._data["OECD_CRDF"], crs=self._data["OECD_CRS_RAW"]
            ).pipe(filter_flows, flows=self.spending_args["flows"])
            # Return to end this step
            return

        # Check if the source is OECD_CRDF_CRS_ALLOCABLE as that requires a separate methodology.
        if source == "OECD_CRDF_CRS_ALLOCABLE":
            logger.info(f"Creating {source} data...")
            self.data["OECD_CRDF_CRS_ALLOCABLE"] = transform_crs_crdf_into_indicators(
                crdf=self._data["OECD_CRDF"], crs=self._data["OECD_CRS_RAW_ALLOCABLE"]
            ).pipe(filter_flows, flows=self.spending_args["flows"])
            # Return to end this step
            return

        # Otherwise convert all the loaded sources to climate
        for loaded_source in self._data:
            logger.info(f"Processing {loaded_source} data...")

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
                ).pipe(filter_flows, flows=self.spending_args["flows"])

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
        methodology: SpendingMethodologies | str = "ONE",
        flows: ValidFlows | str | list[ValidFlows | str] = "gross_disbursements",
        source: ValidSources | str | list[ValidSources | str] = "OECD_CRS",
    ) -> "ClimateData":
        """
        Loads spending data based on the methodology specified, from the specified
        source. Gross disbursements are loaded by default, but one or more different
        ones can be loaded.

        Args:
            methodology: one of the methodologies supported: ONE, OECD, or “custom”. In
            the future, support for UNFCCC will be added. Call `.available_methodologies()`
            for a full list of available methodologies.
            flows: one, or a list of supported flows: gross_disbursements, commitments,
            grant_equivalent, net_disbursements. Call `.available_flows()` for a full list
            of available flows.
            source: the dataset used for climate data (e.g. OECD_CRS_ALLOCABLE, OECD_CRDF,
            OECD_CRDF_DONOR, OECD_CRDF_CRS, UNFCCC, etc). Call `.available_sources()` for
            a full list of available sources.
        """
        # update the configuration to load the right data into the object.
        # This process also handles validation.
        self._update_spending_args(
            methodology=methodology,
            flows=flows,
            source=source,
        )

        # Load the data
        self._load_sources()

        # transform to climate
        self._transform_to_climate(source=source)

        return self

    def _get_multilateral_contributions_data(
        self,
        flows: ValidFlows | str | list[ValidFlows | str] = "gross_disbursements",
    ) -> pd.DataFrame:
        """
        Get the multilateral imputations data based on the flows specified.
        Args:
            flows: one, or a list of supported flows: gross_disbursements, commitments.

        Returns:
            pd.DataFrame: The loaded data.

        """
        # Validate flows as oda-data indicators
        indicators = multi_flows_to_indicators(flows=flows)

        # get contributions data and clean it
        contributions_data = get_contributions_data(
            providers=self.providers,
            recipients=self.recipients,
            years=self.years,
            currency=self.currency,
            prices=self.prices,
            base_year=self.base_year,
            indicators=indicators,
        )

        # clean data
        contributions_data = contributions_data.pipe(clean_multi_contributions)

        return contributions_data

    def _get_multilateral_spending_shares(
        self, methodology, flows, source, rolling_years_spending, groupby, shareby
    ):
        """Private pipeline method to load spending data and convert it to shares.
           Args:
            methodology (str): The methodology to use for loading spending data.
            flows (str or list[str]): The flows to consider when loading spending data.
            source (str or list[str]): The source(s) to load spending data from.
            rolling_years_spending (int): The number of years to use for the rolling total share.
            groupby (str or list[str] or None): The columns to group by when converting to shares.
            shareby (str or list[str] or None): The columns to calculate the shares by
            when converting to shares.

        Returns:
            pd.DataFrame: The loaded data converted to shares.

        """
        from oda_data import donor_groupings

        # Temporarily store requested providers
        providers = self.providers
        self.providers = donor_groupings()["multilateral"].keys()

        # Temporarily shift the years to account for rolling
        years = self.years
        self.years = [
            y
            for y in range(
                min(self.years) - rolling_years_spending + 1, max(self.years) + 1
            )
        ]

        # Load the spending data
        self.load_spending_data(methodology=methodology, flows=flows, source=source)

        # For each source, add channel names
        for source in self.data:
            self.data[source] = (
                self.data[source]
                .pipe(align_oda_data_names)
                .pipe(add_multi_channel_codes)
                .pipe(align_oda_data_names, to_climate_names=True)
                .pipe(remove_channel_name_from_spending_data)
            )

        # remove providers and agencies from groupers, in favor for name and code
        groupby = validate_multi_shares_groupers(groupby)
        shareby = validate_multi_shares_groupers(shareby)

        # convert them to spending shares
        self.convert_to_shares(
            rolling_years=rolling_years_spending, groupby=groupby, shareby=shareby
        )

        # Revert providers
        self.providers = providers

        # Revert years
        self.years = years

        return self.get_data()

    def load_multilateral_imputations_data(
        self,
        spending_methodology: SpendingMethodologies | str = "OECD",
        rolling_years_spending: int = 1,
        flows: ValidFlows | str | list[ValidFlows | str] = "gross_disbursements",
        source: ValidSources | str | list[ValidSources | str] = "OECD_CRDF_CRS",
        groupby: list[str] | str | None = None,
        shareby: list[str] | str | None = None,
    ) -> "ClimateData":
        """
        Loads multilateral imputations data based on the methodology specified, from the specified
        source. Gross disbursements are loaded by default, but one or more different
        ones can be loaded.

        Args:
            spending_methodology: one of the methodologies supported: ONE, OECD, or “custom”. In
            the future, support for UNFCCC will be added. Call `.available_methodologies()`
            for a full list of available methodologies.
            rolling_years_spending: Optional. An integer specifying the number of years to use
            for the rolling total share. Defaults to 1.
            flows: one, or a list of supported flows: gross_disbursements, commitments,
            grant_equivalent, net_disbursements. Call `.available_flows()` for a full list
            of available flows.
            source: the dataset used for climate data (e.g. OECD_CRS_ALLOCABLE, OECD_CRDF,
            OECD_CRDF_DONOR, OECD_CRDF_CRS, UNFCCC, etc). Call `.available_sources()` for
            a full list of available sources.
            groupby: Optional. A list of strings or a string specifying the columns to
            group by. Defaults to None which means the data is kept at the highest level
            of detail.
            shareby: Optional. A list of strings or a string specifying the columns to
            calculate the shares by. Defaults to `None` which means the shares are
            calculated for a year/provider/agency/flow_type level.

        """

        # Load the indicators and get the data
        contributions = self._get_multilateral_contributions_data(flows=flows)

        # Validate groupby
        groupby = validate_multi_groupby(groupby)

        # Get spending shares
        spending_shares = self._get_multilateral_spending_shares(
            methodology=spending_methodology,
            flows=flows,
            source=source,
            rolling_years_spending=rolling_years_spending,
            groupby=groupby,
            shareby=shareby,
        )

        # Merge the contributions and spending shares data
        data = merge_spending_and_contributions(
            spending_data=spending_shares, contributions_data=contributions
        )

        # Calculate imputations as the 'value' column
        data = calculate_imputations(data)

        # Group to the desired level
        data = groupby_sum(data=data, groupby=groupby)

        self.data = {"Multilateral Imputations": data}

        return self

    def convert_to_shares(
        self,
        rolling_years: int = 1,
        groupby: list[str] | str | None = None,
        shareby: list[str] | str | None = None,
    ) -> "ClimateData":
        """
        Convert the loaded data to shares of total spending.

        Args:
            rolling_years: Optional. An integer specifying the number of years to use
            for the rolling total share. Defaults to 1.
            groupby: Optional. A list of strings or a string specifying the columns to
            group by. Defaults to None which means the data is kept at the highest level
            of detail.
            shareby: Optional. A list of strings or a string specifying the columns to
            calculate the shares by. Defaults to `None` which means the shares are
            calculated for a year/provider/agency/flow_type level.

        """

        # If no data has been loaded, raise an error
        self._validate_loaded_data(
            "No data has been loaded. In order to convert to shares,"
            " spending data must be loaded first."
        )

        # Indicate what is happening
        logger.info(
            f"Converting to shares using a rolling window of {rolling_years} years."
        )

        # If groupby columns is a string, convert to a list
        if isinstance(groupby, str):
            groupby = [groupby]

        # If shareby columns is a string, convert to a list
        if isinstance(shareby, str):
            shareby = [shareby]

        # for each source, convert the data to shares
        for source in self.data:
            # Get the right grouper for the data
            if groupby is None:
                groupby = groupby_none(data=self.data[source])

            if shareby is None:
                shareby = standard_shareby(data=self.data[source])

            # Convert the data to shares
            self.data[source] = data_to_share(
                data=self.data[source],
                groupby=groupby,
                shareby=shareby,
                rolling_years=rolling_years,
            )

        # Force ignoring prices and currency
        self._ignore_prices_and_currency = True

        return self

    def get_data(self) -> pd.DataFrame:
        """Return the loaded data. If more than one data source has been loaded,
        the data is concatenated into a single dataframe.

        Returns:
            pd.DataFrame: The loaded data.

        """

        # If no data has been loaded, raise an error
        self._validate_loaded_data("No data has been loaded.")

        # Add the source to the data and put all dataframes into a list
        loaded_dataframes = [d.assign(source=source) for source, d in self.data.items()]

        # Concatenate the dataframes
        loaded_data = pd.concat(loaded_dataframes, ignore_index=True)

        if self._ignore_prices_and_currency:
            return loaded_data

        # Convert to the right currency and prices, if needed
        if self.prices != "current" or self.currency != "USD":
            loaded_data = DEFLATOR(
                data=loaded_data,
                target_currency=self.currency,
                prices=self.prices,
                base_year=self.base_year,
            )
        else:
            loaded_data = loaded_data.assign(
                currency=self.currency,
                prices=self.prices,
            )

        return loaded_data

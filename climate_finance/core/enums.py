from enum import Enum
from typing import NamedTuple


class ValidatedEnum(Enum):
    @classmethod
    def _missing_(cls, value):
        raise ValueError(
            f"'{value}' is not a valid {cls.__name__}.\nPlease choose from: {cls.valid()}"
        )

    @classmethod
    def valid(cls) -> str:
        return ", ".join([repr(v.value) for v in cls])

    @classmethod
    def print_available(cls, name: str):
        print(f"The following {name} are available:\n{cls.valid()}")

    def __repr__(self):
        return f"{self.value}"

    def __eq__(self, other):
        if isinstance(other, str):
            return self.value == other
        if isinstance(other, ValidatedEnum):
            return self.value == other.value
        if isinstance(other, (int, float)):
            return self.value == other

        return super().__eq__(other)

    def __hash__(self):
        return hash(self.value)

    def __str__(self):
        return str(self.value)

    def __bool__(self):
        return bool(self.value)

    def __int__(self):
        return int(self.value)

    def __contains__(self, item):
        """
        Check if a string is in the value of the enum instance.

        Args:
            item (str): A string to check against the enum's value.

        Returns:
            bool: True if the string is in the enum's value, False otherwise.
        """
        return item in self.value


class ValidCurrencies(ValidatedEnum):
    USA = "USD"
    EUI = "EUR"
    CAN = "CAN"
    GBR = "GBP"


class ValidPrices(ValidatedEnum):
    CURRENT = "current"
    CONSTANT = "constant"


class SpendingMethodologies(ValidatedEnum):
    ONE = "ONE"
    OECD = "OECD"
    CUSTOM = "custom"


class ValidFlows(ValidatedEnum):
    GROSS_DISBURSEMENTS = "gross_disbursements"
    COMMITMENTS = "commitments"
    GRANT_EQUIVALENT = "grant_equivalent"
    NET_DISBURSEMENTS = "net_disbursements"


class ValidSources(ValidatedEnum):
    OECD_CRS = "OECD_CRS"
    OECD_CRS_ALLOCABLE = "OECD_CRS_ALLOCABLE"
    OECD_CRDF_RP = "OECD_CRDF"
    OECD_CRDF_DP = "OECD_CRDF_DONOR"
    OECD_CRDF_CRS = "OECD_CRDF_CRS"
    OECD_CRDF_CRS_ALLOCABLE = "OECD_CRDF_CRS_ALLOCABLE"
    UNFCCC = "UNFCCC"


CoefficientTuple = NamedTuple(
    "CoefficientTuple", [("significant", float), ("principal", float)]
)


class Coefficients:
    """Takes in a tuple of coefficients and validates them.
    The order should be (significant, principal).
    """

    def __init__(self, coefficients: CoefficientTuple):
        if isinstance(coefficients, tuple):
            coefficients = CoefficientTuple(*coefficients)
        if not (0 <= coefficients.principal <= 1):
            raise ValueError("Principal coefficient must be between 0 and 1.")
        if not (0 <= coefficients.significant <= 1):
            raise ValueError("Significant coefficient must be between 0 and 1.")

        self.coefficients = coefficients

    def __repr__(self):
        return (
            f"Principal: {self.coefficients.principal},"
            f" Significant: {self.coefficients.significant}"
        )

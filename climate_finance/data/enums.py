from enum import Enum


class ValidatedEnum(Enum):
    @classmethod
    def _missing_(cls, value):
        valid_values = ", ".join([repr(v.value) for v in cls])
        raise ValueError(
            f"'{value}' is not a valid {cls.__name__}.\nPlease choose from: {valid_values}"
        )

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


class ValidCurrencies(ValidatedEnum):
    USD = "USD"
    EUR = "EUR"
    CAN = "CAN"
    GBP = "GBP"


class ValidPrices(ValidatedEnum):
    CURRENT = "current"
    CONSTANT = "constant"


class ValidPerspective(ValidatedEnum):
    PROVIDER = "provider"
    RECIPIENT = "recipient"


class SpendingMethodologies(ValidatedEnum):
    ONE = "ONE"
    OECD = "OECD"
    CUSTOM = "custom"

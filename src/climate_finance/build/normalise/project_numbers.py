"""Project numbers as keys: the form every rule that joins on one compares."""

import polars as pl

# Project numbers that name no project, as `normalise_pn` leaves them.
PLACEHOLDER_PNS = (
    "NOTAPPLICABLE",
    "N.A.",
    "N.A",
    "NA",
    "N/A",
    "NONE",
    "NULL",
    "TBD",
    "UNKNOWN",
    "0",
    "-",
    ".",
    "..",
    "X",
)
# A numeric project number at least this long can be one a spreadsheet rounded.
ROUNDED_MIN_DIGITS = 10
_LONG_NUMBER = rf"^\d{{{ROUNDED_MIN_DIGITS},}}$"
_SCIENTIFIC = r"^(\d)\.(\d+)E\+(\d+)$"


def normalise_pn(project_number: pl.Expr) -> pl.Expr:
    """Upper-case, drop a float artefact `.0` and every space. Placeholders, numbers
    a spreadsheet rounded (`rounded_digits`) and values shorter than two characters
    or holding no letter or digit become null.

    A rounded number such as the AfDB's `2.0002E+12` stands for hundreds of
    projects across dozens of recipients, so no key may join on it."""
    pn = _cleaned(project_number)
    usable = (
        ~pn.is_in(PLACEHOLDER_PNS)
        & pn.str.contains("[A-Z0-9]")
        & (pn.str.len_chars() >= 2)
        & rounded_digits(pn).is_null()
    )
    return pl.when(usable).then(pn)


def rounded_digits(project_number: pl.Expr) -> pl.Expr:
    """For a project number a spreadsheet rounded, as `2.10015E+12` or
    `2100150000000`, the digits it kept (`prefix`) and the length of the number it
    stands for (`digits`), or null for other numbers."""
    project_number = _cleaned(project_number)
    scientific = project_number.str.extract_groups(_SCIENTIFIC)
    lead, rest, exponent = (scientific.struct.field(name) for name in ("1", "2", "3"))
    zeros = project_number.str.contains(_LONG_NUMBER) & project_number.str.ends_with(
        "0000"
    )
    prefix = (
        pl.when(lead.is_not_null())
        .then(lead + rest)
        .when(zeros)
        .then(project_number.str.strip_chars_end("0"))
    )
    digits = (
        pl.when(lead.is_not_null())
        .then(exponent.cast(pl.Int32) + 1)
        .when(zeros)
        .then(project_number.str.len_chars().cast(pl.Int32))
    )
    return pl.when(prefix.is_not_null()).then(pl.struct(prefix=prefix, digits=digits))


def _cleaned(project_number: pl.Expr) -> pl.Expr:
    return (
        project_number.str.to_uppercase()
        .str.strip_chars()
        .str.replace(r"\.0$", "")
        .str.replace_all(r"\s+", "")
    )

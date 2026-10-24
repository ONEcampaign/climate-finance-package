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


def normalise_pn(project_number: pl.Expr) -> pl.Expr:
    """Upper-case, drop a float artefact `.0` and every space. Placeholders and
    values shorter than two characters or holding no letter or digit become null."""
    pn = (
        project_number.str.to_uppercase()
        .str.strip_chars()
        .str.replace(r"\.0$", "")
        .str.replace_all(r"\s+", "")
    )
    usable = (
        ~pn.is_in(PLACEHOLDER_PNS)
        & pn.str.contains("[A-Z0-9]")
        & (pn.str.len_chars() >= 2)
    )
    return pl.when(usable).then(pn)

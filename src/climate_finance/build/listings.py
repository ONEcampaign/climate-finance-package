"""The listings: every provider, agency and recipient code the OECD files carry.

Three CSVs in `data/` ship in the wheel and are committed, so the package and CI read
them without raw data:

- `oecd_classification.csv`: providers, as `code`, `name`, `name_fr`, `type` and
  `mdb`.
- `agencies.csv`: agencies, as the provider's `code`, the `agency` code and `name`.
- `recipients.csv`: recipients, as `code`, `name` and `type`.

`cf-build listings RAW` refreshes them from the OECD files in `RAW/oecd/`, the raw
layout of the fetch stage or an archive of it. It keeps every listed row as it stands
and adds each code the files carry that its listing lacks, with the name the files give
it in the latest year they carry it, the name on most rows within that year, then
the first in sort order. An added provider has no French name and no type. Two columns
are set for every row from the files' own classifications:

- A provider's `mdb` is true when the CRDF's `provider_type` is "Multilateral
  development bank" on every row of the latest year that holds the provider. EU
  Institutions (918) carries the type on the EIB's rows alone, so it is false.
- A recipient's `type` is `country`, `regional` or `unspecified`. The CRDF and the
  CRS put every recipient code that is not one country in the income group "Part I
  unallocated by income". Of those, the codes in the region "Developing countries
  unspecified" (CRDF) or "Regional and Unspecified" (CRS) are `unspecified`, and the
  rest, each in a geographic region, are `regional`. Each code takes the type of the
  latest year that gives it an income group, on most rows within that year. A
  recipient the files do not type keeps its listed type, and one typed nowhere
  stops the refresh before it writes.

The files:

- `CRS*.parquet`: donors, agencies and recipients.
- `crdf_recipient*.parquet`: providers, agencies and recipients.
- `crdf_provider*.parquet`: providers and agencies. Its recipient codes are
  multilateral pools, so it adds no recipient.
- `multisystem*.parquet`: donors, leaving out the donor groups from
  `AGGREGATE_DONOR_FLOOR`.

CRS and MULTI rows count from the first year the recipient-perspective CRDF holds,
the years a build reading every CRDF year reads. Agency 0 and a null agency stand for
the provider itself, so the agency listing leaves them out.
"""

from collections.abc import Mapping
from importlib.resources import files
from pathlib import Path
from typing import Literal

import polars as pl

from climate_finance.build.errors import BuildError
from climate_finance.build.readers.multisystem import AGGREGATE_DONOR_FLOOR

type Listing = Literal["providers", "agencies", "recipients"]

DATA_DIR = Path(__file__).parents[1] / "data"
LISTING_FILES: dict[Listing, str] = {
    "providers": "oecd_classification.csv",
    "agencies": "agencies.csv",
    "recipients": "recipients.csv",
}
LISTING_KEYS: dict[Listing, tuple[str, ...]] = {
    "providers": ("code",),
    "agencies": ("code", "agency"),
    "recipients": ("code",),
}

_CRS = {
    "code": "donor_code",
    "name": "donor_name",
    "agency": "agency_code",
    "agency_name": "agency_name",
    "recipient": "recipient_code",
    "recipient_name": "recipient_name",
    "income_group": "incomegroup_name",
    "region": "region_name",
}
_CRDF = {
    "code": "provider_code",
    "name": "provider_name",
    "agency": "agency_code",
    "agency_name": "agency_name",
    "recipient": "recipient_code",
    "recipient_name": "recipient_name",
    "income_group": "recipient_income_group",
    "region": "recipient_region",
    "provider_type": "provider_type",
}
_MULTI = {"code": "donor_code", "name": "donor_name"}
# The columns `_scan` reads as integer codes. The rest are text.
_CODES = ("code", "agency", "recipient")
_MDB = "Multilateral development bank"
# The OECD's income group of every recipient code that is not one country.
_UNALLOCATED = "Part I unallocated by income"
# The region names the CRDF and the CRS give to recipient codes in no geographic region.
_UNSPECIFIED_REGIONS = ("Developing countries unspecified", "Regional and Unspecified")


class ListingsError(BuildError):
    """The listings cannot be refreshed from the raw files as they stand."""


def read_listing(listing: Listing) -> pl.DataFrame:
    """The committed `listing`, as the installed package holds it."""
    return pl.read_csv(
        (files("climate_finance") / "data" / LISTING_FILES[listing]).read_bytes(),
        schema_overrides=dict.fromkeys(LISTING_KEYS[listing], pl.Int64),
    )


def observed_codes(raw: Path) -> dict[Listing, pl.DataFrame]:
    """Every code the OECD files in `raw/oecd/` carry, with its latest name, and for
    recipients its `type`, null where no row gives the recipient an income group.

    Raises:
        ListingsError: when a file kind is missing, or naming each code that no
            file names.
    """
    recipient_crdf = _scan(raw, "crdf_recipient*.parquet", _CRDF)
    first_year = recipient_crdf.select(pl.min("year")).collect().item()
    crs = _scan(raw, "CRS*.parquet", _CRS).filter(pl.col("year") >= first_year)
    provider_crdf = _scan(raw, "crdf_provider*.parquet", _CRDF)
    multi = _scan(raw, "multisystem*.parquet", _MULTI).filter(
        pl.col("year") >= first_year, pl.col("code") < AGGREGATE_DONOR_FLOOR
    )
    coded = (crs, recipient_crdf, provider_crdf)
    names: dict[Listing, pl.LazyFrame] = {
        "providers": pl.concat(
            [frame.select("code", "name", "year") for frame in (*coded, multi)]
        ),
        "agencies": pl.concat(
            frame.select("code", "agency", "year", name="agency_name").filter(
                pl.col("agency") != 0
            )
            for frame in coded
        ),
        "recipients": pl.concat(
            frame.select("year", code="recipient", name="recipient_name")
            for frame in (crs, recipient_crdf)
        ),
    }
    observed = {
        listing: _latest_names(frame, key=list(LISTING_KEYS[listing]))
        for listing, frame in names.items()
    }
    if unnamed := [
        f"{listing} {'/'.join(map(str, key))}"
        for listing, frame in observed.items()
        for *key, name in frame.iter_rows()
        if name is None
    ]:
        raise ListingsError("No OECD file names these codes:\n" + "\n".join(unnamed))
    types = _latest_names(
        pl.concat(
            frame.select("year", code="recipient", name=_recipient_type())
            for frame in (crs, recipient_crdf)
        ),
        key=["code"],
    ).rename({"name": "type"})
    observed["recipients"] = observed["recipients"].join(
        types, on="code", how="left", maintain_order="left"
    )
    return observed


def mdb_codes(raw: Path) -> list[int]:
    """The provider codes the CRDF in `raw/oecd/` types "Multilateral development
    bank" on every row of the latest year that holds them, sorted."""
    rows = pl.concat(
        _scan(raw, pattern, _CRDF).select("code", "year", "provider_type")
        for pattern in ("crdf_recipient*.parquet", "crdf_provider*.parquet")
    )
    return (
        rows.drop_nulls("code")
        .group_by("code", "year")
        # A blank type is no bank, which `all()` alone would skip.
        .agg(mdb=(pl.col("provider_type") == _MDB).fill_null(False).all())
        .group_by("code")
        .agg(pl.col("mdb").sort_by("year").last())
        .filter("mdb")
        .sort("code")
        .collect()["code"]
        .to_list()
    )


def refresh_listings(
    raw: Path, *, data_dir: Path = DATA_DIR
) -> dict[Listing, pl.DataFrame]:
    """Add every code the OECD files in `raw/oecd/` carry to the listing in
    `data_dir` that lacks it, and set each provider's `mdb` and each recipient's
    `type` from the files, keeping the listed type where the files give none.

    Returns:
        The rows added to each listing.

    Raises:
        ListingsError: from `observed_codes`, or naming each recipient that neither
            the files nor `recipients.csv` type, before any listing is written.
    """
    mdb = mdb_codes(raw)
    added: dict[Listing, pl.DataFrame] = {}
    refreshed: dict[Listing, pl.DataFrame] = {}
    for listing, seen in observed_codes(raw).items():
        key = list(LISTING_KEYS[listing])
        listed = pl.read_csv(
            data_dir / LISTING_FILES[listing],
            schema_overrides=dict.fromkeys(key, pl.Int64),
        )
        added[listing] = seen.join(listed, on=key, how="anti")
        frame = pl.concat([listed, added[listing]], how="diagonal").sort(key)
        if listing == "providers":
            frame = frame.with_columns(mdb=pl.col("code").is_in(mdb))
        if listing == "recipients":
            # `update` skips the files' nulls, so a listed type stands where they
            # give none.
            frame = frame.update(seen.select("code", "type"), on="code")
        refreshed[listing] = frame
    untyped = refreshed["recipients"].filter(pl.col("type").is_null())["code"]
    if untyped.len():
        raise ListingsError(
            f"No OECD file gives these recipients an income group, and "
            f"{LISTING_FILES['recipients']} gives them no type: "
            f"{', '.join(map(str, untyped))}. Add each to "
            f"{LISTING_FILES['recipients']} with its type (country, regional or "
            "unspecified) and rerun."
        )
    for listing, frame in refreshed.items():
        (data_dir / LISTING_FILES[listing]).write_text(
            frame.write_csv(), encoding="utf-8"
        )
    return added


def unlisted_codes(tables: Mapping[str, pl.LazyFrame]) -> list[str]:
    """The provider, agency and recipient codes in `tables` that the listings lack,
    as `provider 1058`, `agency 3/5` or `recipient 434`.

    Agency 0, 1 and null stand for the provider itself, so only other agency codes
    need a row in the agency listing.
    """
    providers = _held(tables, "provider_code")
    agencies = _held(tables, "provider_code", "agency_code").filter(
        ~pl.col("agency").is_in([0, 1])
    )
    recipients = _held(tables, "recipient_code")
    missing = {
        "provider": providers.join(read_listing("providers"), on="code", how="anti"),
        "agency": agencies.join(
            read_listing("agencies"), on=["code", "agency"], how="anti"
        ),
        "recipient": recipients.join(read_listing("recipients"), on="code", how="anti"),
    }
    return [
        f"{kind} {'/'.join(map(str, key))}"
        for kind, frame in missing.items()
        for key in frame.sort(pl.all()).iter_rows()
    ]


def held_providers(tables: Mapping[str, pl.LazyFrame]) -> int:
    """How many provider codes `tables` hold."""
    return _held(tables, "provider_code").height


def _held(tables: Mapping[str, pl.LazyFrame], *columns: str) -> pl.DataFrame:
    """The distinct non-null values of `columns` over the tables that carry them,
    named as the listings name them."""
    names = {"provider_code": "code", "agency_code": "agency", "recipient_code": "code"}
    held = [
        pl.LazyFrame(
            schema=dict.fromkeys((names[column] for column in columns), pl.Int64)
        ),
        *(
            frame.select(
                pl.col(column).cast(pl.Int64).alias(names[column]) for column in columns
            )
            for frame in tables.values()
            if set(columns) <= set(frame.collect_schema().names())
        ),
    ]
    return pl.concat(held).drop_nulls().unique().collect()


def _scan(raw: Path, pattern: str, columns: dict[str, str]) -> pl.LazyFrame:
    paths = sorted((raw / "oecd").glob(pattern))
    if not paths:
        raise ListingsError(f"{raw / 'oecd'} holds no {pattern}")
    return pl.concat(
        [
            pl.scan_parquet(path).select(
                pl.col("year").cast(pl.Int64),
                *(
                    pl.col(source)
                    .cast(pl.Int64 if name in _CODES else pl.String)
                    .alias(name)
                    for name, source in columns.items()
                ),
            )
            for path in paths
        ]
    )


def _recipient_type() -> pl.Expr:
    """Each row's recipient type from its income group and region, null where the row
    has no income group."""
    income, region = pl.col("income_group"), pl.col("region")
    return (
        pl.when(income.is_null())
        .then(pl.lit(None, pl.String))
        .when(income != _UNALLOCATED)
        .then(pl.lit("country"))
        .when(region.is_in(_UNSPECIFIED_REGIONS))
        .then(pl.lit("unspecified"))
        .otherwise(pl.lit("regional"))
    )


def _latest_names(frame: pl.LazyFrame, *, key: list[str]) -> pl.DataFrame:
    """Each non-null `key` with its name in the latest year that names it, the name
    on most rows within that year, then the first in sort order; null where no row
    names it."""
    named = (
        frame.drop_nulls([*key, "name"])
        .group_by(*key, "name", "year")
        .agg(rows=pl.len())
        .sort(
            [*key, "year", "rows", "name"],
            descending=[*(False for _ in key), True, True, False],
        )
        .group_by(key, maintain_order=True)
        .agg(pl.col("name").first())
    )
    return (
        frame.drop_nulls(key)
        .select(key)
        .unique()
        .join(named, on=key, how="left")
        .sort(key)
        .collect()
    )

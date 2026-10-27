"""The listings: every provider, agency and recipient code the OECD files carry.

Three CSVs in `data/` ship in the wheel and are committed, so the package and CI read
them without raw data:

- `oecd_classification.csv`: providers, as `code`, `name`, `name_fr` and `type`.
- `agencies.csv`: agencies, as the provider's `code`, the `agency` code and `name`.
- `recipients.csv`: recipients, as `code` and `name`.

`cf-build listings RAW` refreshes them from the OECD files in `RAW/oecd/`, the raw
layout of the fetch stage or an archive of it. It keeps every listed row as it stands
and adds each code the files carry that its listing lacks, with the name the files give
it in the latest year they carry it, the name on most rows within that year, then
the first in sort order. An added provider has no French name and no type. The files:

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
}
_CRDF = {
    "code": "provider_code",
    "name": "provider_name",
    "agency": "agency_code",
    "agency_name": "agency_name",
    "recipient": "recipient_code",
    "recipient_name": "recipient_name",
}
_MULTI = {"code": "donor_code", "name": "donor_name"}


class ListingsError(BuildError):
    """The listings cannot be refreshed from the raw files as they stand."""


def read_listing(listing: Listing) -> pl.DataFrame:
    """The committed `listing`, as the installed package holds it."""
    return pl.read_csv(
        (files("climate_finance") / "data" / LISTING_FILES[listing]).read_bytes(),
        schema_overrides=dict.fromkeys(LISTING_KEYS[listing], pl.Int64),
    )


def observed_codes(raw: Path) -> dict[Listing, pl.DataFrame]:
    """Every code the OECD files in `raw/oecd/` carry, with its latest name.

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
    return observed


def refresh_listings(
    raw: Path, *, data_dir: Path = DATA_DIR
) -> dict[Listing, pl.DataFrame]:
    """Add every code the OECD files in `raw/oecd/` carry to the listing in
    `data_dir` that lacks it.

    Returns:
        The rows added to each listing.
    """
    added: dict[Listing, pl.DataFrame] = {}
    for listing, seen in observed_codes(raw).items():
        path = data_dir / LISTING_FILES[listing]
        key = list(LISTING_KEYS[listing])
        listed = pl.read_csv(path, schema_overrides=dict.fromkeys(key, pl.Int64))
        added[listing] = seen.join(listed, on=key, how="anti")
        refreshed = pl.concat([listed, added[listing]], how="diagonal").sort(key)
        path.write_text(refreshed.write_csv(), encoding="utf-8")
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
                    .cast(pl.String if "name" in name else pl.Int64)
                    .alias(name)
                    for name, source in columns.items()
                ),
            )
            for path in paths
        ]
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

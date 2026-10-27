"""The alias table: every common form of a provider, agency or recipient name, and the
DAC codes it stands for.

`cf-build names` writes `data/names.csv`, which ships in the wheel, so the analyst
package resolves names without resolvekit. One row per key and target:

- `kind`: `provider` or `recipient`.
- `key`: the name, folded as `catalogue._fold` folds it, and stripped.
- `code`: the DAC provider or recipient code.
- `agency`: the DAC agency code when the key names an agency, else null.
- `name`: the listing name of the provider, agency or recipient.
- `source`: where the key came from, one of `SOURCES`.

The sources, best first:

1. `listing`: the listing's English name.
2. `listing_fr`: the provider listing's French name.
3. `curated`: `aliases.toml`'s providers and agencies.
4. `group`: `aliases.toml`'s groups, each a name with one row per provider it names.
5. `iso3`, then `iso2`: the ISO codes of the resolvekit place carrying the listing
   code as its `oecd:provider` or `oecd:recipient` code.
6. `resolvekit`: every name of the resolvekit places and organisations carrying the
   listing code.
7. `agency`: the agency listing's names, and every name of the `org.governments`
   agencies whose `oecd:agency` code is the provider (a DAC code, or an ISO 3 code
   mapped through `geo.provider_code`) and the agency. Only agencies the agency
   listing holds, of providers the provider listing holds, have rows, so a name
   resolves only to an agency a release table can carry.

Keys of digits alone are left out, since those read as DAC codes. Each key keeps its
rows at its best source only. A key with one row resolves, and so does a group, to
all its rows. A key with several rows at any other source is ambiguous. The bake
refuses a listing name that does not resolve to its own code, a curated name or group
member the listings lack, a curated name or group a listing name hides, a group of
fewer than two providers, a key that is both a curated name and a group,
and a key whose best rows widen one of a provider's agencies to the whole provider
(an ISO code or resolvekit alias of the provider that is also the agency's name)
unless `aliases.toml` settles the key. The table is sorted by `kind`, `key`, `code`
and `agency`, so a rebake from the same inputs gives the same bytes.
"""

import tomllib
from collections.abc import Iterator
from importlib.resources import files
from pathlib import Path

import polars as pl
from resolvekit.core.model.entity import EntityRecord

from climate_finance.build.errors import BuildError
from climate_finance.build.geo import provider_code, resolver
from climate_finance.build.listings import read_listing
from climate_finance.catalogue import _fold, providers, recipients

SOURCES = (
    "listing",
    "listing_fr",
    "curated",
    "group",
    "iso3",
    "iso2",
    "resolvekit",
    "agency",
)
ALIASES_TOML = Path(__file__).with_name("aliases.toml")
NAMES_CSV = Path(__file__).parents[1] / "data" / "names.csv"

_SCHEMA = {
    "kind": pl.String,
    "key": pl.String,
    "code": pl.Int64,
    "agency": pl.Int64,
    "name": pl.String,
    "source": pl.String,
}
_SYSTEMS = {"provider": "oecd:provider", "recipient": "oecd:recipient"}

type Row = dict[str, str | int | None]
# An agency's name and every alias, by provider and agency code.
type Agencies = dict[tuple[int, int], tuple[str, list[str]]]


class NamesError(BuildError):
    """The alias table cannot be baked as its inputs stand."""


def bake_names(*, aliases: Path = ALIASES_TOML) -> pl.DataFrame:
    """The alias table, from the listings, resolvekit's bundled packs and `aliases`.

    Raises:
        NamesError: listing every key the bake refuses, and why.
    """
    listings = {
        "provider": dict(providers.list().select("code", "name").iter_rows()),
        "recipient": dict(recipients.list().select("code", "name").iter_rows()),
    }
    agencies = _agencies(listings["provider"])
    rows = pl.DataFrame(
        [
            *_listing_rows(listings),
            *_curated_rows(aliases, listings=listings, agencies=agencies),
            *_entity_rows(listings),
            *_agency_rows(agencies),
        ],
        schema=_SCHEMA,
        orient="row",
    )
    # A string of digits reads as a DAC code at query time, so no alias may be one.
    rows = rows.filter(~pl.col("key").str.contains(r"^\d*$")).with_columns(
        rank=pl.col("source").replace_strict(SOURCES, range(len(SOURCES)))
    )
    best = rows.filter(pl.col("rank") == pl.col("rank").min().over("kind", "key"))
    problems = [
        *_unresolved_listing_names(best, listings=listings),
        *_shadowed_curated_names(rows),
        *_widened_agencies(rows, best),
    ]
    if problems:
        raise NamesError("The alias table cannot be baked:\n" + "\n".join(problems))
    return (
        best.drop("rank")
        .unique()
        .sort("kind", "key", "code", "agency", nulls_last=False)
    )


def write_names(path: Path = NAMES_CSV) -> pl.DataFrame:
    """Bake the alias table and write it to `path`."""
    table = bake_names()
    path.write_text(table.write_csv(), encoding="utf-8", newline="\n")
    return table


def names_are_current(path: Path = NAMES_CSV) -> bool:
    """Whether `path` holds the table a fresh bake writes."""
    return (
        path.is_file() and path.read_text(encoding="utf-8") == bake_names().write_csv()
    )


def _key(name: str) -> str:
    return _fold(name).strip()


def _listing_rows(listings: dict[str, dict[int, str]]) -> Iterator[Row]:
    for kind, listing in listings.items():
        for code, name in listing.items():
            yield _row(kind, name, code=code, name=name, source="listing")
    french = pl.read_csv(
        (files("climate_finance") / "data" / "oecd_classification.csv").read_bytes(),
        columns=["code", "name_fr"],
    ).drop_nulls()
    for code, name_fr in french.iter_rows():
        yield _row(
            "provider",
            name_fr,
            code=code,
            name=listings["provider"][code],
            source="listing_fr",
        )


def _curated_rows(
    aliases: Path,
    *,
    listings: dict[str, dict[int, str]],
    agencies: Agencies,
) -> Iterator[Row]:
    curated = tomllib.loads(aliases.read_text(encoding="utf-8"))
    unknown = []
    for alias, code in curated.get("providers", {}).items():
        if code not in listings["provider"]:
            unknown.append(f"{alias!r}: provider {code} is not in the listing")
            continue
        yield _row(
            "provider",
            alias,
            code=code,
            name=listings["provider"][code],
            source="curated",
        )
    for alias, (code, agency) in curated.get("agencies", {}).items():
        if (code, agency) not in agencies:
            unknown.append(f"{alias!r}: the listing has no agency {agency} of {code}")
            continue
        yield _row(
            "provider",
            alias,
            code=code,
            agency=agency,
            name=agencies[code, agency][0],
            source="curated",
        )
    for alias, codes in curated.get("groups", {}).items():
        if len(set(codes)) < 2:
            unknown.append(f"{alias!r}: a group names two providers or more")
        for code in codes:
            if code not in listings["provider"]:
                unknown.append(f"{alias!r}: provider {code} is not in the listing")
                continue
            yield _row(
                "provider",
                alias,
                code=code,
                name=listings["provider"][code],
                source="group",
            )
    if unknown:
        raise NamesError(
            f"{aliases.name} names targets that do not exist:\n" + "\n".join(unknown)
        )


def _entity_rows(listings: dict[str, dict[int, str]]) -> Iterator[Row]:
    """The ISO codes and names of every resolvekit place or organisation that
    carries a listing code."""
    for domain in ("geo", "org"):
        store = resolver().store_for_domain(domain)
        for entity in store.bulk_get_entities(sorted(store.all_entity_ids())).values():
            codes = {code.system: code.value for code in entity.codes}
            for kind, system in _SYSTEMS.items():
                code = int(codes[system]) if system in codes else None
                if code not in listings[kind]:
                    continue
                name = listings[kind][code]
                for iso in ("iso3", "iso2"):
                    if iso in codes:
                        yield _row(kind, codes[iso], code=code, name=name, source=iso)
                for alias in _names(entity):
                    yield _row(kind, alias, code=code, name=name, source="resolvekit")


def _agencies(listing: dict[int, str]) -> Agencies:
    """The agency listing's agencies of listed providers, named as it names them,
    each with every name of the `org.governments` agency carrying its code. An agency
    the listing lacks is left out, since no release table holds it."""
    agencies: Agencies = {
        (code, agency): (name, [name])
        for code, agency, name in read_listing("agencies").iter_rows()
        if code in listing
    }
    store = resolver().store_for_domain("org")
    for entity in store.bulk_get_entities(sorted(store.all_entity_ids())).values():
        codes = {code.system: code.value for code in entity.codes}
        if "oecd:agency" not in codes:
            continue
        provider, agency = codes["oecd:agency"].split(":")
        code = int(provider) if provider.isdigit() else provider_code(provider)
        if code is not None and (key := (code, int(agency))) in agencies:
            agencies[key][1].extend(_names(entity))
    return agencies


def _agency_rows(agencies: Agencies) -> Iterator[Row]:
    for (code, agency), (name, aliases) in agencies.items():
        for alias in aliases:
            yield _row(
                "provider",
                alias,
                code=code,
                agency=agency,
                name=name,
                source="agency",
            )


def _names(entity: EntityRecord) -> list[str]:
    return [entity.canonical_name, *(record.value for record in entity.names)]


def _row(
    kind: str, key: str, *, code: int, name: str, source: str, agency: int | None = None
) -> Row:
    return {
        "kind": kind,
        "key": _key(key),
        "code": code,
        "agency": agency,
        "name": name,
        "source": source,
    }


def _unresolved_listing_names(
    best: pl.DataFrame, *, listings: dict[str, dict[int, str]]
) -> Iterator[str]:
    """Listing names whose best rows are anything but their own code."""
    targets: dict[tuple[str, str], set[tuple[int, int | None]]] = {}
    for kind, key, code, agency in best.select(
        "kind", "key", "code", "agency"
    ).iter_rows():
        targets.setdefault((kind, key), set()).add((code, agency))
    for kind, listing in listings.items():
        for code, name in listing.items():
            found = targets[kind, _key(name)]
            if found != {(code, None)}:
                yield f"{kind} {name!r} ({code}) resolves to {sorted(found, key=str)}"


def _shadowed_curated_names(rows: pl.DataFrame) -> Iterator[str]:
    """Curated names and groups that a listing name hides, and curated names that
    name two targets or are also a group."""
    curated = rows.filter(pl.col("source").is_in(["curated", "group"]))
    listed = rows.filter(pl.col("source").is_in(["listing", "listing_fr"]))
    hidden = curated.join(listed, on=["kind", "key"], how="semi")
    for key in hidden["key"].unique().sort():
        yield f"curated {key!r} is a listing name, which outranks it"
    named = (pl.col("source") == "curated").sum().over("kind", "key")
    twice = curated.filter(
        (named > 1) | ((named > 0) & (pl.len().over("kind", "key") > named))
    )
    for key in twice["key"].unique().sort():
        yield f"curated {key!r} appears twice in {ALIASES_TOML.name}"


def _widened_agencies(rows: pl.DataFrame, best: pl.DataFrame) -> Iterator[str]:
    """Keys whose best rows are an ISO code or resolvekit alias of a provider, where
    the same key names one of that provider's agencies."""
    agencies = rows.filter(source="agency").select("kind", "key", "code").unique()
    widened = (
        best.filter(pl.col("source").is_in(["iso3", "iso2", "resolvekit"]))
        .join(agencies, on=["kind", "key", "code"])
        .select("key", "code", "source")
        .unique()
        .sort("key", "code")
    )
    for key, code, source in widened.iter_rows():
        yield (
            f"{key!r} is a {source} name of provider {code} and the name of one of its "
            f"agencies; settle it in {ALIASES_TOML.name}"
        )

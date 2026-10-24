"""Provider, agency and channel names resolved to DAC codes, on exact matches only.

Providers come from resolvekit's geo pack (countries carry an `oecd:provider` code
there), then its org pack. Agencies come from the org pack, within their donor only.
In the org pack a provider, its sole agency and a lender often share a name, so a
name comes back ambiguous; the one candidate with the wanted id prefix is taken when
it matches the name exactly. A hand-built pack adds development finance institutions
the org pack misses. A CRS channel string without a code takes the code its coded
rows carry, else the channel whose codelist name it equals. Fuzzy and full-text
matches are not taken, because on channel names they were 15% correct.
"""

import re
import unicodedata
from collections.abc import Iterable
from functools import cache

import polars as pl
import resolvekit as rk
from resolvekit.core.engine.router import RoutingMode

from climate_finance import schema

_PROVIDER = "org/oecd:provider:"
_AGENCY = "org/oecd:agency:"
_EXACT_TIERS = ("exact_code", "exact_name")
# CRS channel codes that name no channel.
_NO_CHANNEL = (0, 99999)
# A channel string takes a code when this share of its coded CRS rows carry it.
_PROPAGATION_SHARE = 0.9
# The resolvekit modules shipped inside the package, in resolvekit's own registry
# order. The remote admin and city packs hold places that share country names. With
# them, "Russia" comes back ambiguous between the country and two places, and with
# these modules alone it resolves to provider 87. The order matters. With
# `org.companies` or `org.lenders` ahead of `org.providers`, the org pack makes
# no fuzzy candidates, and "H&M Foundation" finds nothing.
RESOLVER_MODULES = (
    "geo.countries",
    "geo.regions",
    "geo.continental_unions",
    "geo.continents",
    "org.providers",
    "org.lenders",
    "org.political_parties",
    "org.companies",
    "org.governments",
    "org.data_sources",
)

# Development finance institutions and agencies the org pack misses or finds only
# by full-text search: (donor code, agency code, name, aliases). Each code is one
# the CRS reports from 2010 on.
DFI_PACK: tuple[tuple[int, int, str, tuple[str, ...]], ...] = (
    (
        8,
        8,
        "Norfund",
        (
            "Norwegian Investment Fund for Developing Countries",
            "Statens investeringsfond for næringsvirksomhet i utviklingsland",
        ),
    ),
    (
        302,
        34,
        "International Development Finance Corporation",
        ("DFC", "US International Development Finance Corporation"),
    ),
    (302, 22, "Overseas Private Investment Corporation", ("OPIC",)),
    (
        4,
        44,
        "Proparco",
        ("Société de promotion et de participation pour la coopération économique",),
    ),
    (4, 3, "French Development Agency", ("AFD", "Agence Française de Développement")),
    (
        5,
        4,
        "DEG",
        (
            "Deutsche Investitions- und Entwicklungsgesellschaft",
            "Deutsche Investitions- und Entwicklungsgesellschaft mbH",
        ),
    ),
    (
        5,
        2,
        "KfW",
        (
            "KfW Bankengruppe",
            "Kreditanstalt für Wiederaufbau",
            "Kreditanstalt fuer Wiederaufbau",
            "KfW Development Bank",
        ),
    ),
    (
        12,
        2,
        "British International Investment",
        ("BII", "CDC Group", "CDC", "Commonwealth Development Corporation"),
    ),
    (10, 22, "Swedfund", ("Swedish Development Finance Institution",)),
    (
        10,
        6,
        "Sida",
        (
            "Swedish International Development Authority",
            "Swedish International Development Cooperation Agency",
        ),
    ),
    (
        18,
        2,
        "Finnfund",
        (
            "Finnish Fund for Industrial Cooperation",
            "Finnish Fund for Industrial Cooperation Ltd",
        ),
    ),
    (
        2,
        22,
        "BIO",
        (
            "Belgian Investment Company for Developing Countries",
            "Belgische Investeringsmaatschappij voor Ontwikkelingslanden",
        ),
    ),
    (50, 59, "COFIDES", ("Compañía Española de Financiación del Desarrollo",)),
    (
        6,
        16,
        "Cassa Depositi e Prestiti",
        ("CDP", "Cassa Depositi e Prestiti S.p.A.", "CDP SpA"),
    ),
    (11, 22, "SIFEM", ("Swiss Investment Fund for Emerging Markets",)),
    (1, 13, "OeEB", ("Oesterreichische Entwicklungsbank", "Austrian Development Bank")),
    (
        3,
        3,
        "Impact Fund Denmark",
        (
            "IFU",
            "Investment Fund for Developing Countries",
            "Investeringsfonden for Udviklingslande",
        ),
    ),
    (
        701,
        8,
        "JICA",
        (
            "Japan International Cooperation Agency",
            "Japanese International Co-operation Agency",
        ),
    ),
    (742, 4, "KOICA", ("Korea International Cooperation Agency",)),
)


def org_names(*, crs: pl.LazyFrame, crdf: Iterable[pl.LazyFrame]) -> pl.LazyFrame:
    """Every organisation name the raw CRS and CRDF print, once per role and donor.

    Agency names keep the donor they were printed under (`scope_code`). Channel
    names are the CRDF's, and the CRS's reported channel names on rows without a
    channel code.

    Args:
        crs: The bulk CRS file as fetch wrote it.
        crdf: The CRDF files as fetch wrote them.
    """
    crdf = list(crdf)
    uncoded = pl.col("channel_code").is_null() | pl.col("channel_code").is_in(
        _NO_CHANNEL
    )
    providers = pl.concat(
        [
            crs.select(source_name="donor_name"),
            *(frame.select(source_name="provider_name") for frame in crdf),
        ]
    )
    agencies = pl.concat(
        [
            crs.select(
                source_name="agency_name",
                scope_code=pl.col("donor_code").cast(pl.Int32),
            ),
            *(
                frame.select(
                    source_name="agency_name",
                    scope_code=pl.col("provider_code").cast(pl.Int32),
                )
                for frame in crdf
            ),
        ]
    )
    channels = pl.concat(
        [
            crs.filter(uncoded).select(source_name="channel_reported_name"),
            *(frame.select(source_name="channel_name") for frame in crdf),
        ]
    )
    return (
        pl.concat(
            [
                providers.with_columns(role=pl.lit("provider")),
                agencies.with_columns(role=pl.lit("agency")),
                channels.with_columns(role=pl.lit("channel")),
            ],
            how="diagonal",
        )
        .with_columns(pl.col("source_name").str.strip_chars())
        .filter(pl.col("source_name").str.len_chars() > 1)
        .select("role", "source_name", "scope_code")
        .unique()
    )


def resolve_orgs(names: pl.LazyFrame, *, channels: pl.LazyFrame) -> pl.DataFrame:
    """Resolve names to DAC codes into the `orgs` schema.

    Args:
        names: `role`, `source_name` and `scope_code`, the donor an agency name was
            printed under (null when unknown), as `org_names` builds them.
        channels: CRS `channel_code`, `channel_name` and `channel_reported_name`,
            one row per record. Coded rows give the channel codelist and the codes
            that propagate to uncoded strings.

    Returns:
        One row per resolved name; names that resolve to nothing are left out.
    """
    named = names.unique().sort("role", "source_name", "scope_code").collect()
    providers = [
        {"role": "provider", "source_name": name} | resolved
        for name in named.filter(role="provider")["source_name"]
        if (resolved := _provider(name)) is not None
    ]
    agencies = [
        {"role": "agency", "source_name": name} | resolved
        for name, scope in named.filter(role="agency")
        .select("source_name", "scope_code")
        .iter_rows()
        if (resolved := _agency(name, scope)) is not None
    ]
    channel_names = named.filter(role="channel")["source_name"]
    rows = [*providers, *agencies, *_channels(channel_names, channels=channels)]
    return pl.DataFrame(rows, schema=schema.ORGS.dtypes).unique()


def _provider(name: str) -> dict[str, object] | None:
    result = _resolver().resolve(
        name, as_result=True, include_entity=True, domain="geo"
    )
    tier = result.match_tier.value if result.match_tier else None
    if result.status.value == "resolved" and tier in _EXACT_TIERS and result.entity:
        codes = {code.system: code.value for code in result.entity.codes}
        if "oecd:provider" in codes:
            return {"donor_code": int(codes["oecd:provider"]), "method": tier}
    match = _unique_exact(name, prefix=_PROVIDER)
    if match is None:
        return None
    provider, method = match
    return {"donor_code": int(provider), "method": method}


def _agency(name: str, scope: int | None) -> dict[str, object] | None:
    """An agency of donor `scope`, or of any donor in the DFI pack when unknown."""
    if scope is not None:
        key = _scope_key(scope)
        country = re.fullmatch("[A-Z]{3}", key)
        match = _unique_exact(
            name,
            prefix=f"{_AGENCY}{key}:",
            context=rk.ResolutionContext(country=key) if country else None,
        )
        if match is not None:
            agency, method = match
            return {"donor_code": scope, "agency_code": int(agency), "method": method}
    pack = _dfi_pack().get(_normal(name))
    if pack is None or (scope is not None and pack[0] != scope):
        return None
    return {"donor_code": pack[0], "agency_code": pack[1], "method": "pack"}


def _unique_exact(
    name: str, *, prefix: str, context: rk.ResolutionContext | None = None
) -> tuple[str, str] | None:
    """The one org-pack candidate under `prefix` that matches `name` exactly.

    Returns the id after the prefix and the match method. resolvekit labels some exact
    matches full-text, so a candidate whose canonical name equals `name`, case, accents
    and punctuation set aside, counts as an exact name.
    """
    result = _resolver().resolve(name, as_result=True, domain="org", context=context)
    exact: dict[str, str] = {}
    for candidate in result.candidates:
        if not candidate.entity_id.startswith(prefix):
            continue
        tier = candidate.match_tier.value if candidate.match_tier else None
        if tier in _EXACT_TIERS:
            exact[candidate.entity_id] = tier
        elif _normal(candidate.canonical_name or "") == _normal(name):
            exact[candidate.entity_id] = "exact_name"
    if len(exact) != 1:
        return None
    ((entity_id, method),) = exact.items()
    return entity_id.removeprefix(prefix), method


def _channels(names: pl.Series, *, channels: pl.LazyFrame) -> list[dict[str, object]]:
    """Channel strings resolved by propagation, else by exact codelist name."""
    coded = (
        channels.filter(
            pl.col("channel_code").is_not_null(),
            ~pl.col("channel_code").is_in(_NO_CHANNEL),
        )
        .with_columns(pl.col("channel_code").cast(pl.Int32))
        .collect()
    )
    propagated = (
        coded.select(
            "channel_code",
            source_name=pl.col("channel_reported_name").str.strip_chars(),
        )
        .drop_nulls()
        .group_by("source_name", "channel_code")
        .len()
        .filter(
            pl.col("len")
            >= _PROPAGATION_SHARE * pl.col("len").sum().over("source_name")
        )
    )
    by_string = dict(propagated.select("source_name", "channel_code").iter_rows())
    codelist = (
        coded.drop_nulls("channel_name")
        .group_by("channel_code", "channel_name")
        .len()
        .sort("len", "channel_name", descending=[True, False])
        .unique("channel_code", keep="first")
    )
    by_name: dict[str, set[int]] = {}
    for code, label in codelist.select("channel_code", "channel_name").iter_rows():
        by_name.setdefault(_normal(label), set()).add(code)

    rows: list[dict[str, object]] = []
    for name in names:
        if name in by_string:
            rows.append(_channel_row(name, by_string[name], "propagated"))
        elif len(codes := by_name.get(_normal(name), set())) == 1:
            rows.append(_channel_row(name, next(iter(codes)), "exact_name"))
    return rows


def _channel_row(name: str, code: int, method: str) -> dict[str, object]:
    return {
        "role": "channel",
        "source_name": name,
        "channel_code": code,
        "method": method,
    }


def _normal(text: str) -> str:
    """Lower case, accents dropped, punctuation and runs of spaces as one space."""
    plain = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", " ", plain.lower()).strip()


@cache
def _resolver() -> rk.Resolver:
    """resolvekit's bundled packs, routed by the domain each call names.

    The module list is fixed so the result does not depend on which remote packs
    (the admin tiers and cities) the machine has cached.
    """
    return rk.Resolver.from_modules(
        module_ids=list(RESOLVER_MODULES), routing_mode=RoutingMode.EXPLICIT
    )


@cache
def _scope_key(donor_code: int) -> str:
    """The org pack's key for a donor's agencies, ISO3 for a country else the
    provider code."""
    iso3 = _resolver().resolve(str(donor_code), from_system="oecd:provider", to="iso3")
    return iso3 if isinstance(iso3, str) else str(donor_code)


@cache
def _dfi_pack() -> dict[str, tuple[int, int]]:
    return {
        _normal(label): (donor, agency)
        for donor, agency, name, aliases in DFI_PACK
        for label in (name, *aliases)
    }

"""resolvekit's bundled packs, and the DAC codes of places and providers they give.

Names resolve on exact matches only: an exact tier, never a fuzzy or full-text hit.
"""

from functools import cache

import resolvekit as rk
from resolvekit.core.engine.router import RoutingMode

EXACT_TIERS = ("exact_code", "exact_name")
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


@cache
def resolver() -> rk.Resolver:
    """resolvekit's bundled packs, routed by the domain each call names.

    The module list is fixed so the result does not depend on which remote packs
    (the admin tiers and cities) the machine has cached.
    """
    return rk.Resolver.from_modules(
        module_ids=list(RESOLVER_MODULES), routing_mode=RoutingMode.EXPLICIT
    )


@cache
def recipient_code(name: str) -> int | None:
    """The DAC recipient code of the place `name` names exactly, or None when the
    geo pack finds no exact match or the place has no recipient code (a region such
    as "Africa")."""
    result = resolver().resolve(name, as_result=True, include_entity=True, domain="geo")
    tier = result.match_tier.value if result.match_tier else None
    if result.status.value != "resolved" or tier not in EXACT_TIERS:
        return None
    if result.entity is None:
        return None
    codes = {code.system: code.value for code in result.entity.codes}
    code = codes.get("oecd:recipient")
    return None if code is None else int(code)


@cache
def provider_code(iso3: str) -> int | None:
    """The DAC provider code of the country with ISO 3166 alpha-3 code `iso3`."""
    code = resolver().resolve(iso3, from_system="iso3", to="oecd:provider")
    return None if code is None else int(code)

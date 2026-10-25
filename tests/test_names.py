"""The committed alias table, `data/names.csv`, that `cf-build names` bakes.

These tests read the committed file alone. The lint job's `cf-build names --check`
compares it with a fresh bake, since only the locked resolvekit gives the same table.
"""

from importlib.resources import files

import polars as pl
import pytest
from make_fixture import FIXTURE_DIR

import climate_finance as cf
from climate_finance.build.readers.btr import _PARTY_PROVIDERS
from climate_finance.catalogue import Catalogue, _fold

NAMES = pl.read_csv(
    (files("climate_finance") / "data" / "names.csv").read_bytes(),
    schema_overrides={"code": pl.Int64, "agency": pl.Int64},
)


def _targets(kind: str, name: str) -> set[tuple[int, int | None]]:
    rows = NAMES.filter(kind=kind, key=_fold(name).strip())
    return set(rows.select("code", "agency").iter_rows())


@pytest.mark.parametrize(
    ("kind", "name", "target"),
    [
        ("provider", "DEU", (5, None)),
        ("provider", "DE", (5, None)),
        ("provider", "Allemagne", (5, None)),
        ("provider", "Deutschland", (5, None)),
        ("provider", "IDA", (905, None)),
        ("provider", "GCF", (1016, None)),
        ("provider", "EUU", (918, None)),
        ("provider", "EIB", (918, 3)),
        ("provider", "USAID", (302, 1)),
        ("provider", "KfW", (5, 2)),
        ("provider", "EC", (918, 1)),
        ("provider", "Sida", (10, 6)),
        ("recipient", "EC", (440, None)),
        ("recipient", "Congo", (234, None)),
        ("recipient", "Kenia", (248, None)),
    ],
)
def test_common_forms_resolve_to_one_target(
    kind: str, name: str, target: tuple[int, int | None]
) -> None:
    assert _targets(kind, name) == {target}


@pytest.mark.parametrize(
    ("kind", "listing"), [("provider", cf.providers), ("recipient", cf.recipients)]
)
def test_every_listing_name_resolves_to_its_own_code(
    kind: str, listing: Catalogue
) -> None:
    wrong = [
        (code, name)
        for code, name in listing.list().select("code", "name").iter_rows()
        if _targets(kind, name) != {(code, None)}
    ]
    assert wrong == []


def test_btr_parties_resolve_to_their_provider_codes() -> None:
    fixture = pl.read_parquet(FIXTURE_DIR / "btr_totals.parquet")
    parties = {
        **dict(fixture.select("party", "provider_code").unique().iter_rows()),
        **_PARTY_PROVIDERS,
    }
    assert {party: _targets("provider", party) for party in parties} == {
        party: {(code, None)} for party, code in parties.items()
    }


def test_table_is_sorted_with_each_key_from_one_source() -> None:
    assert NAMES.equals(NAMES.sort("kind", "key", "code", "agency", nulls_last=False))
    sources = NAMES.group_by("kind", "key").agg(pl.col("source").n_unique())
    assert sources.filter(pl.col("source") > 1).is_empty()

import json
import re
import socket
import sys
from importlib.metadata import version
from pathlib import Path
from types import SimpleNamespace
from typing import Literal

import httpx
import oda_reader
import oda_reader.crs
import pandas as pd
import polars as pl
import pytest
from reads import record_reads
from stage_runs import BUILD_TOML, stage_context, write_fetched_years

import climate_finance.build
from climate_finance.build import BuildConfig, Stage, build, fetch, load_config
from climate_finance.build.config import Ledgers, LocalFile
from climate_finance.build.errors import ConfigError
from climate_finance.build.sources import files
from climate_finance.build.sources.gcf import PROJECTS_URL, READINESS_URL
from climate_finance.build.stages import REGISTRY, raw_dir
from climate_finance.errors import SourceUnavailableError
from climate_finance.schema import sha256_file

FIXTURES = Path(__file__).parent / "fixtures" / "raw"
CRS_URL = "https://webfs-dcd.oecd.org/files/dotStat/DSD_CRS/CRS.parquet"
CRDF_FILES = {"recipient": "CRDF-RP_2024.xlsx", "provider": "CRDF-PP_2024.xlsx"}
MANIFEST = Path("fetch/raw_manifest.parquet")
# A row for every year, as the bulk CRS holds.
CRS_YEARS = range(1973, 2026)


# The DAC exchange rates the pydeflate stand-in gives, by target currency, DAC code
# (France, the DAC total) and year.
RATES = {
    currency: {
        (entity, year): rate * (1 + 0.1 * (year - 2021)) * (1 + 0.01 * (entity == 4))
        for entity in (4, 20001)
        for year in (2021, 2022)
    }
    for currency, rate in {"USD": 1.0, "EUR": 0.9, "GBP": 0.8, "CAD": 1.3}.items()
}


def deflator(entity: int, year: int, *, base_year: int, currency: str) -> float | None:
    """The stand-in's DAC deflator: none for France before the base year."""
    if entity == 4 and year < base_year:
        return None
    rate = RATES[currency][(20001, base_year)]
    return 100 * (1 + 0.05 * (year - base_year) + 0.01 * (entity == 4)) / rate


class FakeSources:
    """Stand-ins for the oda-reader, pydeflate and GCF calls, recording each call.

    The CRDF and MULTI downloads hold a row for each year asked, MULTI from
    `fetch.MULTISYSTEM_FIRST_YEAR`, less the years `lacking` names by source.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.lacking: dict[str, set[int]] = {}
        self.gcf_response = httpx.Response(
            200,
            content=(FIXTURES / "gcf" / "projects.json").read_bytes(),
            headers={"etag": '"gcf-1"'},
        )

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(oda_reader, "bulk_download_crs", self.bulk_download_crs)
        monkeypatch.setattr(oda_reader.crs, "get_full_crs_parquet_url", lambda: CRS_URL)
        monkeypatch.setattr(oda_reader, "list_crdf_files", self.list_crdf_files)
        monkeypatch.setattr(oda_reader, "download_crdf", self.download_crdf)
        monkeypatch.setattr(oda_reader, "download_multisystem", self.multisystem)
        monkeypatch.setattr(oda_reader, "download_crdf_file", self.crdf_file)
        # A stand-in module, so this file never imports pydeflate (and wbgapi).
        pydeflate = SimpleNamespace(
            get_oecd_dac_exchange_rates=self.rates,
            get_oecd_dac_deflators=self.deflators,
        )
        monkeypatch.setitem(sys.modules, "pydeflate", pydeflate)
        transport = httpx.MockTransport(self.gcf)
        monkeypatch.setattr(
            fetch, "new_client", lambda: httpx.Client(transport=transport)
        )

    def bulk_download_crs(self, *, save_to_path: Path) -> None:
        self.calls.append(("bulk_download_crs", {}))
        pd.DataFrame({"year": CRS_YEARS, "usd_grant_equiv": 1.0}).to_parquet(
            Path(save_to_path) / "CRS.parquet"
        )

    def list_crdf_files(self) -> pd.DataFrame:
        self.calls.append(("list_crdf_files", {}))
        return pd.DataFrame(
            [
                ("CRDF-RP_2000-2009.xlsx", "recipient", 2000, 2009),
                ("CRDF-RP_2024.xlsx", "recipient", 2024, 2024),
                ("CRDF-PP_2013-2014.xlsx", "provider", 2013, 2014),
                ("CRDF-PP_2024.xlsx", "provider", 2024, 2024),
                ("Imputed_multilateral_shares.xlsx", None, None, None),
            ],
            columns=["name", "perspective", "start_year", "end_year"],
        ).astype({"start_year": "Int32", "end_year": "Int32"})

    def download_crdf(self, perspective: str, **kwargs: object) -> pd.DataFrame:
        self.calls.append(("download_crdf", {"perspective": perspective, **kwargs}))
        return pd.DataFrame(
            {
                "year": self._years(f"crdf_{perspective}", kwargs),
                "source_file": CRDF_FILES[perspective],
                "file_version": f'"{perspective}-etag"',
            }
        )

    def multisystem(self, **kwargs: object) -> pd.DataFrame:
        self.calls.append(("download_multisystem", kwargs))
        return pd.DataFrame(
            {
                "year": self._years(
                    "multisystem", kwargs, first=fetch.MULTISYSTEM_FIRST_YEAR
                ),
                "value": 1.0,
            }
        )

    def _years(
        self, source: str, kwargs: dict[str, object], *, first: int = 0
    ) -> list[int]:
        start, end = kwargs["start_year"], kwargs["end_year"]
        assert isinstance(start, int) and isinstance(end, int)
        lacking = self.lacking.get(source, set())
        return [
            year for year in range(max(start, first), end + 1) if year not in lacking
        ]

    def crdf_file(self, name: str, save_to_path: Path) -> Path:
        self.calls.append(("download_crdf_file", {"name": name}))
        Path(save_to_path).write_bytes(b"workbook")
        return Path(save_to_path)

    def rates(self, **kwargs: object) -> pd.DataFrame:
        """Local currency per US dollar by ISO code, or with `use_source_codes`
        `RATES` of the target currency by DAC code."""
        self.calls.append(("get_oecd_dac_exchange_rates", kwargs))
        if not kwargs.get("use_source_codes"):
            return pd.DataFrame(
                {"iso_code": ["FRA"], "year": [2022], "exchange_rate": [0.9509]}
            )
        rates = RATES[str(kwargs["target_currency"])]
        return pd.DataFrame(
            [(entity, year, rate) for (entity, year), rate in rates.items()],
            columns=["entity_code", "year", "exchange_rate"],
        )

    def deflators(self, **kwargs: object) -> pd.DataFrame:
        """`deflator` by DAC code, None for France before the base year."""
        self.calls.append(("get_oecd_dac_deflators", kwargs))
        base_year, currency = kwargs["base_year"], str(kwargs["target_currency"])
        assert isinstance(base_year, int)
        return pd.DataFrame(
            [
                (
                    entity,
                    year,
                    deflator(entity, year, base_year=base_year, currency=currency),
                )
                for entity, year in RATES[currency]
            ],
            columns=["entity_code", "year", "deflator"],
        )

    def gcf(self, request: httpx.Request) -> httpx.Response:
        if str(request.url) == READINESS_URL:
            self.calls.append(("gcf_readiness", {}))
            grants = json.loads((FIXTURES / "ledgers/gcf/readiness.json").read_text())
            page = {"props": {"pageProps": {"data": {"data": grants}}}}
            return httpx.Response(
                200,
                text='<script id="__NEXT_DATA__" type="application/json">'
                f"{json.dumps(page)}</script>",
            )
        self.calls.append(("gcf", {"url": str(request.url)}))
        return self.gcf_response

    def names(self) -> list[str]:
        return [name for name, _ in self.calls]


@pytest.fixture
def sources(monkeypatch: pytest.MonkeyPatch) -> FakeSources:
    fakes = FakeSources()
    fakes.install(monkeypatch)
    return fakes


@pytest.fixture
def crs_file(tmp_path: Path) -> LocalFile:
    path = tmp_path / "archive" / "CRS_v1.parquet"
    path.parent.mkdir()
    pd.DataFrame({"year": CRS_YEARS, "usd_grant_equiv": 2.0}).to_parquet(path)
    return LocalFile(path=path, sha256=sha256_file(path))


def _config(
    *,
    crdf_years: tuple[int, int] | Literal["all"] = (2015, 2024),
    crs_years: tuple[int, int] | None = None,
    **sources: object,
) -> BuildConfig:
    config = load_config(BUILD_TOML)
    update = {
        "iati_archive": FIXTURES / "iati",
        "iati_publishers": ("caf", "gef"),
        "btr_archive": None,
        **sources,
    }
    return config.model_copy(
        update={
            "sources": config.sources.model_copy(update=update),
            "years": config.years.model_copy(
                update={"crdf": crdf_years, "crs": crs_years}
            ),
        }
    )


def _fetch(config: BuildConfig, out_dir: Path, *, offline: bool = False) -> Path:
    build("r", config=config, out_dir=out_dir, stages=[Stage.FETCH], offline=offline)
    return out_dir / "r"


@pytest.mark.parametrize(
    ("crdf_years", "crs_years", "resolved", "multisystem_held"),
    [
        (
            (2015, 2024),
            None,
            {
                "crs": (2014, 2025),
                "crdf_recipient": (2015, 2024),
                "crdf_provider": (2015, 2024),
                "multisystem": (2015, 2024),
            },
            (2015, 2024),
        ),
        (
            "all",
            None,
            {
                "crs": (2000, 2025),
                "crdf_recipient": (2000, 2024),
                "crdf_provider": (2013, 2024),
                "multisystem": (2000, 2024),
            },
            (2011, 2024),
        ),
        (
            (2010, 2024),
            (1990, 2024),
            {
                "crs": (1990, 2024),
                "crdf_recipient": (2010, 2024),
                "crdf_provider": (2013, 2024),
                "multisystem": (2010, 2024),
            },
            (2011, 2024),
        ),
    ],
)
def test_online_fetch_writes_the_raw_layout_and_one_manifest_row_per_file(
    sources: FakeSources,
    crs_file: LocalFile,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crdf_years: tuple[int, int] | Literal["all"],
    crs_years: tuple[int, int] | None,
    resolved: dict[str, tuple[int, int]],
    multisystem_held: tuple[int, int],
) -> None:
    config = _config(crs_file=crs_file, crdf_years=crdf_years, crs_years=crs_years)
    registry, reads = record_reads(monkeypatch, config, REGISTRY)
    monkeypatch.setattr(climate_finance.build, "REGISTRY", registry)

    release = _fetch(config, tmp_path / "out")
    raw = raw_dir(release)
    manifest = pl.read_parquet(release / MANIFEST)

    iati = [
        f"external/iati/{publisher}/{xml.name}"
        for publisher in ("caf", "gef")
        for xml in sorted((FIXTURES / "iati" / publisher).glob("*.xml"))
    ]
    assert sorted(manifest["path"]) == sorted(
        [
            "oecd/CRS.parquet",
            "oecd/crdf_recipient.parquet",
            "oecd/crdf_provider.parquet",
            "oecd/multisystem.parquet",
            "oecd/Imputed_multilateral_shares.xlsx",
            "external/gcf/projects.json",
            "dac_exchange.parquet",
            "conversion_factors.parquet",
            *iati,
        ]
    )
    on_disk = sorted(
        path.relative_to(raw).as_posix() for path in raw.rglob("*") if path.is_file()
    )
    assert on_disk == sorted(manifest["path"])
    for path, sha256 in manifest.select("path", "sha256").iter_rows():
        assert sha256_file(raw / path) == sha256
    methods = dict(manifest.select("path", "method").iter_rows())
    versions = dict(manifest.select("source", "version").iter_rows())
    assert versions["conversion_factors"] == f"pydeflate {version('pydeflate')}"
    assert methods["oecd/CRS.parquet"] == "manual"
    assert all(methods[path] == "manual" for path in iati)
    assert methods["oecd/multisystem.parquet"] == "http"
    years = manifest.filter(pl.col("first_year").is_not_null())
    assert {
        source: (first, last)
        for source, first, last in years.select(
            "source", "first_year", "last_year"
        ).iter_rows()
    } == resolved
    # MULTI holds nothing before 2011; every other file holds the years asked.
    held = resolved | {"multisystem": multisystem_held}
    assert {
        source: (first, last)
        for source, first, last in years.select(
            "source", "held_first_year", "held_last_year"
        ).iter_rows()
    } == held
    assert fetch.read_years(manifest) == held
    assert (raw / "oecd/CRS.parquet").read_bytes() == crs_file.path.read_bytes()
    assert (raw / "external/gcf/projects.json").read_bytes() == (
        FIXTURES / "gcf" / "projects.json"
    ).read_bytes()
    assert not (release / "raw.partial").exists()
    # The adaptation labels are read with jev_labels on.
    assert reads[Stage.FETCH] == set(fetch.SETTINGS) - {"sources.adaptation_labels"}

    assert "bulk_download_crs" not in sources.names()
    # The recipient file starts a year early, where the listing holds that year,
    # for the first share year's span.
    first, last = resolved["crdf_recipient"]
    asked = {
        "recipient": (max(first - 1, 2000), last),
        "provider": resolved["crdf_provider"],
    }
    for perspective, (first, last) in asked.items():
        assert (
            "download_crdf",
            {
                "perspective": perspective,
                "start_year": first,
                "end_year": last,
                "pre_process": True,
            },
        ) in sources.calls
    first, last = resolved["multisystem"]
    assert (
        "download_multisystem",
        {
            "start_year": first,
            "end_year": last,
            "pre_process": True,
            "dotstat_codes": True,
        },
    ) in sources.calls


@pytest.mark.usefixtures("sources")
def test_the_conversion_factors_hold_every_target_as_pydeflate_gives_it() -> None:
    factors = fetch.conversion_factors()

    def target(currency: str, base_year: int | None) -> dict[tuple[int, int], float]:
        rows = factors.filter(
            pl.col("currency") == currency, pl.col("base_year").eq_missing(base_year)
        )
        return {
            (entity, year): f
            for entity, year, f in rows.select(
                "entity_code", "year", "factor"
            ).iter_rows()
        }

    assert set(factors["currency"]) == {"USD", "EUR", "GBP", "CAD"}
    base_years = dict(
        factors.group_by("currency")
        .agg(pl.col("base_year").unique().sort())
        .iter_rows()
    )
    # USD at current prices is the identity and has no rows.
    assert base_years == {
        "USD": [2021, 2022],
        **{key: [None, 2021, 2022] for key in ("EUR", "GBP", "CAD")},
    }
    assert target("EUR", None) == pytest.approx(RATES["EUR"])
    # A missing deflator, France's 2021 in 2022 prices, gives no row.
    assert target("GBP", 2022) == pytest.approx(
        {
            key: 1 / value
            for key in RATES["GBP"]
            if (value := deflator(*key, base_year=2022, currency="GBP")) is not None
        }
    )


def test_offline_fetch_rewrites_its_stage_unchanged_with_no_network_call(
    sources: FakeSources,
    crs_file: LocalFile,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(crs_file=crs_file, ledgers=LEDGERS)
    release = _fetch(config, tmp_path / "out")
    online = sha256_file(release / MANIFEST)
    # The fetch stage's identity, which every later stage records.
    record = (release / "fetch" / "stage.json").read_bytes()
    calls = len(sources.calls)

    def refuse(*args: object, **kwargs: object) -> None:
        raise AssertionError("an offline fetch opened a connection")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "getaddrinfo", refuse)
    # Every download, ledgers included, runs on the client `run_fetch` builds.
    monkeypatch.setattr(fetch, "new_client", refuse)

    _fetch(config, tmp_path / "out", offline=True)

    assert len(sources.calls) == calls
    assert sha256_file(release / MANIFEST) == online
    assert (release / "fetch" / "stage.json").read_bytes() == record


def test_a_fetch_that_fails_before_its_manifest_keeps_the_last_fetch(
    sources: FakeSources,
    crs_file: LocalFile,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(crs_file=crs_file)
    release = _fetch(config, tmp_path / "out")
    gcf = raw_dir(release) / "external/gcf/projects.json"
    first = gcf.read_bytes()
    sources.gcf_response = httpx.Response(200, json=[{"ProjectsID": 2}])

    def full_disk(*args: object) -> str:
        raise OSError("No space left on device")

    with monkeypatch.context() as patch:
        patch.setattr(climate_finance.build, "write_table", full_disk)
        with pytest.raises(OSError, match="No space"):
            _fetch(config, tmp_path / "out")

    assert gcf.read_bytes() == first
    _fetch(config, tmp_path / "out", offline=True)


@pytest.mark.parametrize(
    ("crdf_years", "crs"),
    [
        # The recipient CRDF holds 2014, which normalise reads into the first
        # share year's span, so the CRS starts then too.
        ((2015, 2024), (2014, 2025)),
        # The CRDF starts in its first year, so the span is empty and the first
        # share year's CRS commitments are its own year's alone.
        ((2000, 2024), (2000, 2025)),
        ("all", (2000, 2025)),
    ],
)
def test_the_crs_starts_with_the_first_share_years_span(
    crdf_years: tuple[int, int] | Literal["all"], crs: tuple[int, int]
) -> None:
    years = fetch.resolve_years(
        _config(crdf_years=crdf_years).years,
        held={
            "crs": (1973, 2025),
            "crdf_recipient": (2000, 2024),
            "crdf_provider": (2013, 2024),
        },
        recipient_first=2000,
        source="source",
    )
    assert years["crs"] == crs


@pytest.mark.parametrize(
    ("crdf_years", "crs_years", "recorded"),
    [
        ((2015, 2024), None, (2014, 2025)),
        ((2022, 2024), None, (2021, 2025)),
        ((2022, 2024), (2016, 2024), (2016, 2024)),
        ((2010, 2024), None, "crdf_recipient"),
        ((2015, 2024), (2013, 2025), "crs"),
        ("all", None, "years.crdf"),
    ],
)
def test_offline_fetch_narrows_to_years_the_raw_files_hold_and_refuses_others(
    sources: FakeSources,
    crs_file: LocalFile,
    tmp_path: Path,
    crdf_years: tuple[int, int] | Literal["all"],
    crs_years: tuple[int, int] | None,
    recorded: tuple[int, int] | str,
) -> None:
    release = _fetch(_config(crs_file=crs_file), tmp_path / "out")
    config = _config(crs_file=crs_file, crdf_years=crdf_years, crs_years=crs_years)

    if isinstance(recorded, str):
        with pytest.raises(SourceUnavailableError, match=re.escape(recorded)):
            _fetch(config, tmp_path / "out", offline=True)
        return
    _fetch(config, tmp_path / "out", offline=True)
    manifest = pl.read_parquet(release / MANIFEST).filter(
        pl.col("first_year").is_not_null()
    )
    assert dict(
        zip(
            manifest["source"],
            zip(manifest["first_year"], manifest["last_year"], strict=True),
            strict=True,
        )
    ) == {
        "crs": recorded,
        "crdf_recipient": crdf_years,
        "crdf_provider": crdf_years,
        "multisystem": crdf_years,
    }


@pytest.mark.parametrize(
    ("source", "lacking"),
    [
        # MULTI starts in 2011, so a file from 2012 lost a year.
        ("multisystem", {2011}),
        ("multisystem", {2018}),
        ("multisystem", {2024}),
        # The CRDF listing holds recipient years from 2000 and provider from 2013.
        ("crdf_recipient", {2000}),
        ("crdf_provider", {2013, 2014}),
    ],
)
def test_an_online_fetch_refuses_a_download_short_of_the_years_the_source_holds(
    sources: FakeSources,
    crs_file: LocalFile,
    tmp_path: Path,
    source: str,
    lacking: set[int],
) -> None:
    sources.lacking = {source: lacking}
    config = _config(crs_file=crs_file, crdf_years="all")

    with pytest.raises(SourceUnavailableError) as raised:
        _fetch(config, tmp_path / "out")

    assert re.search(
        rf"{source} \(\d+ to 2024 asked\) lacks {', '.join(map(str, sorted(lacking)))}"
        r"(;|$)",
        str(raised.value),
    )
    assert not (tmp_path / "out" / "r" / MANIFEST).exists()


@pytest.mark.parametrize(
    ("crdf_years", "multisystem"),
    [("all", (2011, 2024)), ((2010, 2024), (2011, 2024)), ((2012, 2024), (2012, 2024))],
)
def test_an_offline_fetch_reads_the_years_asked_that_the_files_hold(
    sources: FakeSources,
    crs_file: LocalFile,
    tmp_path: Path,
    crdf_years: tuple[int, int] | Literal["all"],
    multisystem: tuple[int, int],
) -> None:
    release = _fetch(_config(crs_file=crs_file, crdf_years="all"), tmp_path / "out")
    config = _config(crs_file=crs_file, crdf_years=crdf_years)

    _fetch(config, tmp_path / "out", offline=True)

    manifest = pl.read_parquet(release / MANIFEST)
    asked = crdf_years if crdf_years != "all" else (2000, 2024)
    assert fetch.read_years(manifest) == {
        # The recipient file holds 2000 on, so a span precedes 2010 and 2012 alone.
        "crs": (asked[0] - 1 if asked[0] > 2000 else asked[0], 2025),
        "crdf_recipient": asked,
        "crdf_provider": (max(asked[0], 2013), 2024),
        "multisystem": multisystem,
    }
    # The years held stay those the online fetch found.
    assert manifest.filter(source="multisystem").select(
        "held_first_year", "held_last_year"
    ).row(0) == (2011, 2024)


def test_an_offline_fetch_of_all_years_reads_an_earlier_fetch_of_all_years(
    sources: FakeSources, crs_file: LocalFile, tmp_path: Path
) -> None:
    config = _config(crs_file=crs_file, crdf_years="all")
    release = _fetch(config, tmp_path / "out")
    online = sha256_file(release / MANIFEST)

    _fetch(config, tmp_path / "out", offline=True)

    assert sha256_file(release / MANIFEST) == online


def test_a_provider_perspective_without_the_crdf_years_stops_the_fetch(
    sources: FakeSources, crs_file: LocalFile, tmp_path: Path
) -> None:
    config = _config(crs_file=crs_file, crdf_years=(2005, 2009))

    with pytest.raises(SourceUnavailableError, match="provider-perspective"):
        _fetch(config, tmp_path / "out")


@pytest.mark.parametrize("change", ["missing", "changed", "unrecorded"])
def test_offline_fetch_refuses_raw_files_that_differ_from_the_manifest(
    sources: FakeSources, crs_file: LocalFile, tmp_path: Path, change: str
) -> None:
    config = _config(crs_file=crs_file)
    raw = raw_dir(_fetch(config, tmp_path / "out"))
    target = raw / "external/gcf/projects.json"
    match change:
        case "missing":
            target.unlink()
        case "changed":
            target.write_bytes(b"[]")
        case "unrecorded":
            target = raw / "external/iati/gef/extra.xml"
            target.write_bytes(b"<iati-activities/>")

    with pytest.raises(SourceUnavailableError, match=re.escape(str(target))):
        _fetch(config, tmp_path / "out", offline=True)


@pytest.mark.parametrize(
    ("publishers", "named"),
    [(("caf",), "iati_gef"), (("caf", "gef", "eib"), "iati_eib")],
)
def test_offline_fetch_refuses_a_manifest_for_other_sources(
    sources: FakeSources,
    crs_file: LocalFile,
    tmp_path: Path,
    publishers: tuple[str, ...],
    named: str,
) -> None:
    _fetch(_config(crs_file=crs_file), tmp_path / "out")
    config = _config(crs_file=crs_file, iati_publishers=publishers)

    with pytest.raises(SourceUnavailableError, match=named):
        _fetch(config, tmp_path / "out", offline=True)


LEDGERS = Ledgers(
    years=(2021, 2022),
    publishers=("ibrd", "ida", "asdb", "gcf", "gcf_readiness"),
    archive=FIXTURES / "ledgers",
)


def test_online_fetch_copies_the_ledger_archive_with_a_source_per_ledger(
    sources: FakeSources, crs_file: LocalFile, tmp_path: Path
) -> None:
    release = _fetch(_config(crs_file=crs_file, ledgers=LEDGERS), tmp_path / "out")
    manifest = pl.read_parquet(release / MANIFEST).filter(
        pl.col("path").str.starts_with("ledgers/")
    )

    snapshots = [
        (f"ledger_finances_one_{dataset}_{year}", f"{dataset}_31-Dec-{year}.json")
        for dataset in ("DS00975", "DS00976")
        for year in (2020, 2021, 2022)
    ]
    assert sorted(manifest.select("source", "path").rows()) == sorted(
        [
            *((source, f"ledgers/finances_one/{name}") for source, name in snapshots),
            ("ledger_iati_asdb", "ledgers/iati/asdb/asdb-sample.xml"),
            ("ledger_gcf_readiness", "ledgers/gcf/readiness.json"),
        ]
    )
    assert set(manifest["method"]) == {"manual"}
    # The GCF ledger reads the one projects file every fetch downloads.
    assert sources.names().count("gcf") == 1


def test_online_fetch_copies_the_btr_archive_it_lists(
    sources: FakeSources, crs_file: LocalFile, tmp_path: Path
) -> None:
    archive = tmp_path / "btr"
    archive.mkdir()
    name = "Norway__NLD-table-1.xlsx"
    (archive / name).write_bytes((FIXTURES / "btr" / "NLD-table-1.xlsx").read_bytes())
    (archive / "unlisted.xlsx").write_bytes(b"draft")
    entry = {"party": "Netherlands", "name": name, "ok": True}
    (archive / "manifest.json").write_text(
        json.dumps([entry | {"bytes": (archive / name).stat().st_size}])
    )
    config = _config(crs_file=crs_file, btr_archive=archive)

    release = _fetch(config, tmp_path / "out")
    manifest = pl.read_parquet(release / MANIFEST).filter(source="unfccc_btr1")

    assert sorted(manifest["path"]) == [
        "unfccc/btr1/Norway__NLD-table-1.xlsx",
        "unfccc/btr1/manifest.json",
    ]
    assert set(manifest["method"]) == {"manual"}
    # Offline, the copied archive matches its manifest rows.
    _fetch(config, tmp_path / "out", offline=True)


def test_online_fetch_downloads_ledgers_without_an_archive(
    sources: FakeSources, crs_file: LocalFile, tmp_path: Path
) -> None:
    ledgers = Ledgers(years=(2021, 2022), publishers=("gcf", "gcf_readiness"))

    release = _fetch(_config(crs_file=crs_file, ledgers=ledgers), tmp_path / "out")

    readiness = (
        pl.read_parquet(release / MANIFEST)
        .filter(source="ledger_gcf_readiness")
        .row(0, named=True)
    )
    assert (readiness["path"], readiness["method"], readiness["url"]) == (
        "ledgers/gcf/readiness.json",
        "http",
        READINESS_URL,
    )
    assert sources.names().count("gcf_readiness") == 1


def test_the_afdb_ledger_brings_the_imf_sdr_rate(
    sources: FakeSources, crs_file: LocalFile, tmp_path: Path
) -> None:
    ledgers = LEDGERS.model_copy(update={"publishers": ("afdb",)})

    release = _fetch(_config(crs_file=crs_file, ledgers=ledgers), tmp_path / "out")

    manifest = pl.read_parquet(release / MANIFEST).filter(
        pl.col("path").str.starts_with("ledgers/")
    )
    # The AfDB's ledger is in SDR, which the DAC gives no rate for.
    assert sorted(manifest.select("source", "path").rows()) == [
        ("ledger_iati_afdb", "ledgers/iati/afdb/afdb-sample.xml"),
        ("ledger_imf_sdr", "ledgers/imf/sdr_usd.xml"),
    ]


@pytest.mark.parametrize(
    ("fetched", "offline", "named"),
    [
        (None, LEDGERS, "ledger_iati_asdb"),
        (LEDGERS, None, "ledger_iati_asdb"),
        (
            LEDGERS,
            LEDGERS.model_copy(update={"publishers": ("ibrd", "asdb")}),
            "ledger_finances_one_DS00976_2020",
        ),
        (
            LEDGERS,
            LEDGERS.model_copy(update={"years": (2021, 2023)}),
            "ledger_finances_one_DS00975_2023",
        ),
        (
            LEDGERS,
            LEDGERS.model_copy(update={"publishers": (*LEDGERS.publishers, "afdb")}),
            "ledger_imf_sdr",
        ),
    ],
    ids=["added", "dropped", "fewer-publishers", "more-years", "sdr"],
)
def test_offline_fetch_refuses_a_manifest_for_other_ledgers(
    sources: FakeSources,
    crs_file: LocalFile,
    tmp_path: Path,
    fetched: Ledgers | None,
    offline: Ledgers | None,
    named: str,
) -> None:
    _fetch(_config(crs_file=crs_file, ledgers=fetched), tmp_path / "out")

    with pytest.raises(SourceUnavailableError, match=named):
        _fetch(
            _config(crs_file=crs_file, ledgers=offline), tmp_path / "out", offline=True
        )


def test_offline_fetch_refuses_without_an_earlier_manifest(tmp_path: Path) -> None:
    with pytest.raises(SourceUnavailableError, match="raw_manifest"):
        _fetch(_config(), tmp_path / "out", offline=True)


def test_offline_fetch_refuses_a_manifest_for_another_crs_file(
    sources: FakeSources, crs_file: LocalFile, tmp_path: Path
) -> None:
    _fetch(_config(crs_file=crs_file), tmp_path / "out")
    other = LocalFile(path=crs_file.path, sha256="0" * 64)

    with pytest.raises(SourceUnavailableError, match="CRS file"):
        _fetch(_config(crs_file=other), tmp_path / "out", offline=True)


@pytest.mark.parametrize(
    ("setting", "value"),
    [
        ("crs_version", "v20270101"),
        ("crdf_etags", {"CRDF-RP_2024.xlsx": '"other-etag"'}),
        ("iati_archive", FIXTURES),
    ],
)
def test_offline_fetch_refuses_settings_the_earlier_fetch_ran_without(
    sources: FakeSources,
    crs_file: LocalFile,
    tmp_path: Path,
    setting: str,
    value: object,
) -> None:
    _fetch(_config(crs_file=crs_file), tmp_path / "out")

    with pytest.raises(SourceUnavailableError, match=re.escape(f"sources.{setting}")):
        _fetch(
            _config(crs_file=crs_file, **{setting: value}),
            tmp_path / "out",
            offline=True,
        )


def test_offline_fetch_refuses_an_earlier_fetch_without_its_settings(
    sources: FakeSources, crs_file: LocalFile, tmp_path: Path
) -> None:
    config = _config(crs_file=crs_file)
    release = _fetch(config, tmp_path / "out")
    (release / "fetch" / "stage.json").unlink()

    with pytest.raises(SourceUnavailableError, match=re.escape("stage.json")):
        _fetch(config, tmp_path / "out", offline=True)


def test_local_crs_file_with_another_sha256_stops_the_fetch(
    sources: FakeSources, crs_file: LocalFile, tmp_path: Path
) -> None:
    wrong = LocalFile(path=crs_file.path, sha256="0" * 64)

    with pytest.raises(SourceUnavailableError, match="0" * 64):
        _fetch(_config(crs_file=wrong), tmp_path / "out")
    assert sources.calls == []


def test_without_a_local_file_the_crs_is_downloaded(
    sources: FakeSources, tmp_path: Path
) -> None:
    release = _fetch(_config(crs_file=None), tmp_path / "out")
    crs = pl.read_parquet(release / MANIFEST).filter(source="crs").row(0, named=True)

    assert crs["method"] == "http"
    assert crs["url"] == CRS_URL
    assert sha256_file(raw_dir(release) / crs["path"]) == crs["sha256"]


def test_a_challenged_crs_download_stops_with_the_registration_call(
    sources: FakeSources, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def challenged(**kwargs: object) -> None:
        raise oda_reader.BulkDownloadChallengeError(
            status_code=403, url=CRS_URL, body="", cf_ray=None, dataset="crs"
        )

    monkeypatch.setattr(oda_reader, "bulk_download_crs", challenged)

    with pytest.raises(SourceUnavailableError, match="register_bulk_file") as error:
        _fetch(_config(crs_file=None), tmp_path / "out")
    assert isinstance(error.value.__cause__, oda_reader.BulkDownloadChallengeError)
    assert sources.calls == []
    assert not (tmp_path / "out" / "r" / "fetch").exists()


@pytest.mark.parametrize(
    "response",
    [httpx.Response(503), httpx.Response(200, json={"error": "maintenance"})],
    ids=["http-error", "not-a-list"],
)
def test_a_bad_gcf_response_stops_the_fetch(
    sources: FakeSources,
    crs_file: LocalFile,
    tmp_path: Path,
    response: httpx.Response,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sources.gcf_response = response
    monkeypatch.setattr(files.time, "sleep", lambda seconds: None)

    with pytest.raises(SourceUnavailableError, match=PROJECTS_URL):
        _fetch(_config(crs_file=crs_file), tmp_path / "out")


def test_crdf_workbooks_must_match_their_pinned_etags(
    sources: FakeSources, crs_file: LocalFile, tmp_path: Path
) -> None:
    pins = {"CRDF-RP_2024.xlsx": '"recipient-etag"', "CRDF-PP_2024.xlsx": '"old"'}

    with pytest.raises(SourceUnavailableError, match=re.escape("CRDF-PP_2024.xlsx")):
        _fetch(_config(crs_file=crs_file, crdf_etags=pins), tmp_path / "out")


def test_jev_labels_bring_the_pinned_adaptation_labels_into_raw(
    sources: FakeSources, crs_file: LocalFile, tmp_path: Path
) -> None:
    path = crs_file.path.with_name("wk.csv")
    path.write_text("text,level\nSeawalls,2\n")
    labels = LocalFile(path=path, sha256=sha256_file(path))
    config = _config(crs_file=crs_file, adaptation_labels=labels).model_copy(
        update={"jev_labels": True}
    )

    release = _fetch(config, tmp_path / "out")

    row = (
        pl.read_parquet(release / MANIFEST)
        .filter(source="adaptation_labels")
        .row(0, named=True)
    )
    assert (row["path"], row["method"], row["sha256"]) == (
        "labels/adaptation_labels.csv",
        "manual",
        labels.sha256,
    )
    assert (raw_dir(release) / row["path"]).read_bytes() == path.read_bytes()
    other = _config(
        crs_file=crs_file,
        adaptation_labels=LocalFile(path=path, sha256="0" * 64),
    ).model_copy(update={"jev_labels": True})
    with pytest.raises(SourceUnavailableError, match="adaptation label file"):
        _fetch(other, tmp_path / "out", offline=True)


@pytest.mark.parametrize(
    ("crs_years", "refused"), [((2010, 2024), False), ((2010, 2023), True)]
)
def test_later_stages_refuse_a_last_complete_year_outside_the_fetched_crs_years(
    tmp_path: Path, crs_years: tuple[int, int], refused: bool
) -> None:
    write_fetched_years(tmp_path, crs=crs_years)
    context = stage_context(tmp_path)

    if refused:
        with pytest.raises(ConfigError, match="crs_last_complete 2024"):
            fetch.last_complete_year(context)
    else:
        assert fetch.last_complete_year(context) == 2024

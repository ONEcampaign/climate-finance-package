import re
import socket
import sys
from pathlib import Path
from types import SimpleNamespace

import httpx
import oda_reader
import oda_reader.crs
import pandas as pd
import polars as pl
import pytest

import climate_finance.build
from climate_finance.build import BuildConfig, Stage, build, fetch, load_config
from climate_finance.build.config import LocalFile
from climate_finance.build.stages import raw_dir
from climate_finance.errors import SourceUnavailableError
from climate_finance.schema import sha256_file

BUILD_TOML = Path(__file__).parents[1] / "build.toml"
FIXTURES = Path(__file__).parent / "fixtures" / "raw"
CRS_URL = "https://webfs-dcd.oecd.org/files/dotStat/DSD_CRS/CRS.parquet"
CRDF_FILES = {"recipient": "CRDF-RP_2024.xlsx", "provider": "CRDF-PP_2024.xlsx"}
MANIFEST = Path("fetch/raw_manifest.parquet")


class FakeSources:
    """Stand-ins for the oda-reader, pydeflate and GCF calls, recording each call."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.gcf_response = httpx.Response(
            200,
            content=(FIXTURES / "gcf" / "projects.json").read_bytes(),
            headers={"etag": '"gcf-1"'},
        )

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(oda_reader, "bulk_download_crs", self.bulk_download_crs)
        monkeypatch.setattr(oda_reader.crs, "get_full_crs_parquet_url", lambda: CRS_URL)
        monkeypatch.setattr(oda_reader, "download_crdf", self.download_crdf)
        monkeypatch.setattr(oda_reader, "download_multisystem", self.multisystem)
        monkeypatch.setattr(oda_reader, "download_crdf_file", self.crdf_file)
        # A stand-in module, so this file never imports pydeflate (and wbgapi).
        pydeflate = SimpleNamespace(get_oecd_dac_exchange_rates=self.rates)
        monkeypatch.setitem(sys.modules, "pydeflate", pydeflate)
        transport = httpx.MockTransport(self.gcf)
        monkeypatch.setattr(
            fetch, "_http_client", lambda: httpx.Client(transport=transport)
        )

    def bulk_download_crs(self, *, save_to_path: Path) -> None:
        self.calls.append(("bulk_download_crs", {}))
        pd.DataFrame({"usd_grant_equiv": [1.0]}).to_parquet(
            Path(save_to_path) / "CRS.parquet"
        )

    def download_crdf(self, perspective: str, **kwargs: object) -> pd.DataFrame:
        self.calls.append(("download_crdf", {"perspective": perspective, **kwargs}))
        return pd.DataFrame(
            {
                "year": [2024],
                "source_file": [CRDF_FILES[perspective]],
                "file_version": [f'"{perspective}-etag"'],
            }
        )

    def multisystem(self, **kwargs: object) -> pd.DataFrame:
        self.calls.append(("download_multisystem", kwargs))
        return pd.DataFrame({"year": [2022], "value": [1.0]})

    def crdf_file(self, name: str, save_to_path: Path) -> Path:
        self.calls.append(("download_crdf_file", {"name": name}))
        Path(save_to_path).write_bytes(b"workbook")
        return Path(save_to_path)

    def rates(self, **kwargs: object) -> pd.DataFrame:
        self.calls.append(("get_oecd_dac_exchange_rates", kwargs))
        return pd.DataFrame(
            {"iso_code": ["FRA"], "year": [2022], "exchange_rate": [0.9509]}
        )

    def gcf(self, request: httpx.Request) -> httpx.Response:
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
    pd.DataFrame({"usd_grant_equiv": [2.0, 3.0]}).to_parquet(path)
    return LocalFile(path=path, sha256=sha256_file(path))


def _config(
    *, crdf_years: tuple[int, int] = (2015, 2024), **sources: object
) -> BuildConfig:
    config = load_config(BUILD_TOML)
    update = {
        "iati_archive": FIXTURES / "iati",
        "iati_publishers": ("caf", "gef"),
        **sources,
    }
    return config.model_copy(
        update={
            "sources": config.sources.model_copy(update=update),
            "years": config.years.model_copy(update={"crdf": crdf_years}),
        }
    )


def _fetch(config: BuildConfig, out_dir: Path, *, offline: bool = False) -> Path:
    build("r", config=config, out_dir=out_dir, stages=[Stage.FETCH], offline=offline)
    return out_dir / "r"


def test_build_toml_names_the_bulk_crs_file() -> None:
    crs = load_config(BUILD_TOML).sources.crs_file

    assert crs is not None
    assert crs.path == BUILD_TOML.parent / "data/raw/oecd/CRS_v20261006.parquet"
    assert crs.sha256.startswith("d24840a5")


def test_online_fetch_writes_the_raw_layout_and_one_manifest_row_per_file(
    sources: FakeSources, crs_file: LocalFile, tmp_path: Path
) -> None:
    release = _fetch(_config(crs_file=crs_file), tmp_path / "out")
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
    assert methods["oecd/CRS.parquet"] == "manual"
    assert all(methods[path] == "manual" for path in iati)
    assert methods["oecd/multisystem.parquet"] == "http"
    years = manifest.filter(pl.col("first_year").is_not_null())
    assert sorted(years["source"]) == sorted(fetch.YEAR_SOURCES)
    assert set(years.select("first_year", "last_year").rows()) == {(2015, 2024)}
    assert (raw / "oecd/CRS.parquet").read_bytes() == crs_file.path.read_bytes()
    assert (raw / "external/gcf/projects.json").read_bytes() == (
        FIXTURES / "gcf" / "projects.json"
    ).read_bytes()
    assert not (release / "raw.partial").exists()

    assert "bulk_download_crs" not in sources.names()
    assert (
        "download_crdf",
        {
            "perspective": "recipient",
            "start_year": 2015,
            "end_year": 2024,
            "pre_process": True,
        },
    ) in sources.calls
    assert (
        "download_multisystem",
        {
            "start_year": 2015,
            "end_year": 2024,
            "pre_process": True,
            "dotstat_codes": True,
        },
    ) in sources.calls


def test_offline_fetch_rewrites_the_manifest_with_no_network_call(
    sources: FakeSources,
    crs_file: LocalFile,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(crs_file=crs_file)
    release = _fetch(config, tmp_path / "out")
    online = sha256_file(release / MANIFEST)
    calls = len(sources.calls)

    def refuse(*args: object, **kwargs: object) -> None:
        raise AssertionError("an offline fetch opened a connection")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "getaddrinfo", refuse)

    _fetch(config, tmp_path / "out", offline=True)

    assert len(sources.calls) == calls
    assert sha256_file(release / MANIFEST) == online


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
    ("crdf_years", "refused"),
    [((2015, 2024), False), ((2022, 2024), False), ((2010, 2024), True)],
)
def test_offline_fetch_refuses_crdf_years_the_raw_files_lack(
    sources: FakeSources,
    crs_file: LocalFile,
    tmp_path: Path,
    crdf_years: tuple[int, int],
    refused: bool,
) -> None:
    _fetch(_config(crs_file=crs_file), tmp_path / "out")
    config = _config(crs_file=crs_file, crdf_years=crdf_years)

    if refused:
        with pytest.raises(SourceUnavailableError, match="crdf_provider"):
            _fetch(config, tmp_path / "out", offline=True)
    else:
        _fetch(config, tmp_path / "out", offline=True)


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
) -> None:
    sources.gcf_response = response

    with pytest.raises(SourceUnavailableError, match=fetch.GCF_URL):
        _fetch(_config(crs_file=crs_file), tmp_path / "out")


def test_crdf_workbooks_must_match_their_pinned_etags(
    sources: FakeSources, crs_file: LocalFile, tmp_path: Path
) -> None:
    pins = {"CRDF-RP_2024.xlsx": '"recipient-etag"', "CRDF-PP_2024.xlsx": '"old"'}

    with pytest.raises(SourceUnavailableError, match=re.escape("CRDF-PP_2024.xlsx")):
        _fetch(_config(crs_file=crs_file, crdf_etags=pins), tmp_path / "out")

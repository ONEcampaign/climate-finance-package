import shutil
from pathlib import Path

import polars as pl
import pytest
from normalised import IATI_PUBLISHERS, record_fetch, write_raw
from reads import record_reads
from stage_runs import BUILD_TOML

import climate_finance.build
from climate_finance.build import (
    BuildConfig,
    build,
    load_config,
    measure,
    run_stages,
)
from climate_finance.build.errors import (
    BuildError,
    ConfigError,
    StageInputMissingError,
    StageOutputError,
)
from climate_finance.build.stages import (
    REGISTRY,
    Stage,
    StageContext,
    StageDef,
    raw_dir,
)
from climate_finance.schema import STAGE_TABLES, Column, TableSchema

TABLE = TableSchema(
    name="numbers",
    columns={"key": Column(pl.String()), "value": Column(pl.Float64())},
    sort_key=("key",),
)


@pytest.fixture
def config() -> BuildConfig:
    return load_config(BUILD_TOML)


def _stage(name: str, run) -> StageDef:
    return StageDef(name=name, run=run, tables={"numbers": TABLE})


def _numbers(keys: list[str], values: list[float]) -> dict[str, pl.DataFrame]:
    return {"numbers": pl.DataFrame({"key": keys, "value": values})}


def test_registry_holds_the_nine_stages_in_run_order() -> None:
    names = [stage.name for stage in REGISTRY]

    assert names == list(Stage) == list(STAGE_TABLES)
    assert names[0] == "fetch"
    assert names[-1] == "assemble"


def test_build_runs_selected_stages_in_registry_order(
    config: BuildConfig, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []

    def recorder(name: str):
        def run(context: StageContext) -> dict[str, pl.DataFrame]:
            calls.append(name)
            return _numbers(["a"], [1.0])

        return run

    registry = tuple(_stage(name, recorder(name)) for name in ("fetch", "link", "fill"))
    monkeypatch.setattr(climate_finance.build, "REGISTRY", registry)

    manifest = build(
        "r1", config=config, out_dir=tmp_path, stages=[Stage.FILL, Stage.FETCH]
    )

    assert calls == ["fetch", "fill"]
    assert list(manifest.tables) == ["fetch", "fill"]
    assert (tmp_path / "r1" / "fill" / "numbers.parquet").exists()
    assert not (tmp_path / "r1" / "link").exists()


@pytest.mark.parametrize(("argument", "configured"), [(True, False), (False, True)])
def test_offline_reaches_every_stage(
    config: BuildConfig,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    argument: bool,
    configured: bool,
) -> None:
    seen: list[tuple[bool, bool]] = []

    def run(context: StageContext) -> dict[str, pl.DataFrame]:
        seen.append((context.offline, context.config.offline))
        return _numbers(["a"], [1.0])

    monkeypatch.setattr(
        climate_finance.build, "REGISTRY", (_stage("fetch", run), _stage("link", run))
    )

    build(
        "r1",
        config=config.model_copy(update={"offline": configured}),
        out_dir=tmp_path,
        offline=argument,
    )

    assert seen == [(True, True), (True, True)]


# Settings read only on paths the raw fixtures cannot take: embeddings and the Jev
# step need models and a truth group, the fill needs bank ledgers, and review and
# the Jev labels are off in build.toml.
UNREACHED = {
    "link": {"embedding.model", "embedding.revision", "jev.band", "jev.model"},
    "fill": {"sources.iati_publishers", "sources.ledgers", "years.crs_last_complete"},
    "review": {"jev.band", "sources.ledgers", "sources.review_decisions"},
    "label": {"jev.model"},
}


@pytest.mark.parametrize("switches", [False, True])
def test_every_stage_reads_the_settings_it_declares_and_no_others(
    config: BuildConfig,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    switches: bool,
) -> None:
    # An earlier fetch laid the raw fixtures out, so the build runs offline from
    # fetch on, with no BTR archive, which the raw fixtures leave out. Measure reads
    # no agency and models no provider, since the fixtures hold too few, and the
    # fixtures hold no truth group to set Jev's scale.
    config = config.model_copy(
        update={
            "offline": True,
            "key_repair": switches,
            "recoveries": switches,
            "jev_links": False,
            "sources": config.sources.model_copy(
                update={
                    "crs_file": None,
                    "iati_publishers": IATI_PUBLISHERS,
                    "btr_archive": None,
                }
            ),
        }
    )
    manifest = write_raw(raw_dir(tmp_path))
    record_fetch(tmp_path, manifest, config=config)
    facts = measure.load_provider_facts()
    monkeypatch.setattr(
        measure,
        "load_provider_facts",
        lambda: facts.model_copy(update={"agencies": (), "modelled": ()}),
    )
    registry, reads = record_reads(monkeypatch, config, REGISTRY)

    run_stages(registry, release="r", release_dir=tmp_path, config=config)

    # Recoveries read the IATI publishers and the EIB routes to skip.
    recoveries = set() if switches else {"sources.iati_publishers", "skip_eib_routes"}
    unreached = UNREACHED | {"link": UNREACHED["link"] | recoveries}
    assert reads == {
        stage.name: set(stage.settings) - unreached.get(stage.name, set())
        for stage in REGISTRY
    }


@pytest.mark.parametrize(
    "keys",
    [
        # Repeated keys sort by their values too.
        ["b", "a", "b", "c", "a"],
        # Unique keys alone decide the order.
        ["b", "a", "e", "c", "d"],
    ],
)
def test_sorted_writes_hash_identically_whatever_the_row_order(
    config: BuildConfig, tmp_path: Path, keys: list[str]
) -> None:
    values = [2.0, 1.0, 0.5, 3.0, -1.0]
    shuffled = [4, 2, 0, 3, 1]

    first = run_stages(
        [_stage("s", lambda _: _numbers(keys, values))],
        release="r",
        release_dir=tmp_path / "one",
        config=config,
    )
    second = run_stages(
        [
            _stage(
                "s",
                lambda _: _numbers(
                    [keys[i] for i in shuffled], [values[i] for i in shuffled]
                ),
            )
        ],
        release="r",
        release_dir=tmp_path / "two",
        config=config,
    )

    assert first.tables == second.tables
    written = pl.read_parquet(tmp_path / "one" / "s" / "numbers.parquet")
    assert written.rows() == sorted(zip(keys, values, strict=True))


def test_a_stage_reads_an_earlier_stage_from_disk(
    config: BuildConfig, tmp_path: Path
) -> None:
    def double(context: StageContext) -> dict[str, pl.LazyFrame]:
        upstream = context.read("first", "numbers")
        return {"numbers": upstream.with_columns(pl.col("value") * 2)}

    registry = [
        _stage("first", lambda _: _numbers(["a"], [1.5])),
        _stage("second", double),
    ]
    with pytest.raises(StageInputMissingError, match="first"):
        run_stages(
            registry,
            release="r",
            release_dir=tmp_path,
            config=config,
            stages=["second"],
        )

    run_stages(
        registry, release="r", release_dir=tmp_path, config=config, stages=["first"]
    )
    run_stages(
        registry, release="r", release_dir=tmp_path, config=config, stages=["second"]
    )

    doubled = pl.read_parquet(tmp_path / "second" / "numbers.parquet")
    assert doubled["value"].to_list() == [3.0]


@pytest.mark.parametrize("replaced", [False, True])
def test_a_build_recovers_a_stage_write_or_swap_that_died_halfway(
    config: BuildConfig, tmp_path: Path, replaced: bool
) -> None:
    registry = [
        _stage("first", lambda _: _numbers(["a"], [1.5])),
        _stage("second", lambda context: {"numbers": context.read("first", "numbers")}),
    ]
    run_stages(
        registry, release="r", release_dir=tmp_path, config=config, stages=["first"]
    )
    # The build died with the last good stage moved aside, before or after the new
    # one moved into its place.
    first, old = tmp_path / "first", tmp_path / "first.old"
    killed_write = tmp_path / ".second-k3x9q2ab"
    killed_write.mkdir()
    (killed_write / "numbers.parquet").write_bytes(b"half")
    if replaced:
        shutil.copytree(first, old)
    else:
        first.rename(old)

    run_stages(
        registry, release="r", release_dir=tmp_path, config=config, stages=["second"]
    )

    assert not old.exists()
    assert not killed_write.exists()
    assert pl.read_parquet(first / "numbers.parquet")["value"].to_list() == [1.5]
    assert pl.read_parquet(tmp_path / "second" / "numbers.parquet").height == 1


def test_a_stage_returning_undeclared_tables_writes_nothing(
    config: BuildConfig, tmp_path: Path
) -> None:
    def wrong(context: StageContext) -> dict[str, pl.DataFrame]:
        return {**_numbers(["a"], [1.0]), "extra": pl.DataFrame({"x": [1]})}

    with pytest.raises(StageOutputError, match="extra"):
        run_stages(
            [_stage("s", wrong)], release="r", release_dir=tmp_path, config=config
        )

    assert not (tmp_path / "s").exists()


@pytest.mark.parametrize(("returned", "left_out"), [("crdf", None), ("orgs", "crdf")])
def test_only_optional_tables_may_be_left_out(
    config: BuildConfig, tmp_path: Path, returned: str, left_out: str | None
) -> None:
    stage = StageDef(
        name="normalise",
        run=lambda _: {returned: _numbers(["a"], [1.0])["numbers"]},
        tables={"crdf": TABLE, "orgs": TABLE},
    )

    if left_out is None:
        manifest = run_stages([stage], release="r", release_dir=tmp_path, config=config)
        assert list(manifest.tables["normalise"]) == [returned]
        return
    with pytest.raises(StageOutputError, match=left_out):
        run_stages([stage], release="r", release_dir=tmp_path, config=config)


def test_unknown_stage_is_refused(config: BuildConfig, tmp_path: Path) -> None:
    with pytest.raises(BuildError, match="nonsense"):
        run_stages(
            [_stage("s", lambda _: _numbers(["a"], [1.0]))],
            release="r",
            release_dir=tmp_path,
            config=config,
            stages=["nonsense"],
        )


def test_config_resolves_paths_against_its_file(config: BuildConfig) -> None:
    assert config.key_repair
    assert config.recoveries
    assert not config.offline
    assert config.jev.band == (0.3, 0.9)
    assert config.sources.btr_archive == BUILD_TOML.parent / "data/raw/unfccc_btr1"


@pytest.mark.parametrize(
    ("line", "replacement", "match"),
    [
        (
            'crdf = "all"',
            "crdf = [2015, 2024]\ncrs = [2010, 2023]",
            "crs_last_complete",
        ),
        ('crdf = "all"', "crdf = [2025, 2026]", "crs_last_complete"),
        ('crdf = "all"', 'crdf = "every"', "crdf"),
        # Normalise reads only raw/, so no source names a table outside it.
        (
            'btr_archive = "data/raw/unfccc_btr1"',
            'btr_archive = "data/raw/unfccc_btr1"\nbtr_table = "btr.parquet"',
            "btr_table",
        ),
    ],
)
def test_invalid_config_is_refused(
    tmp_path: Path, line: str, replacement: str, match: str
) -> None:
    text = BUILD_TOML.read_text()
    assert line in text
    (tmp_path / "build.toml").write_text(text.replace(line, replacement))

    with pytest.raises(ConfigError, match=match):
        load_config(tmp_path / "build.toml")


def test_missing_config_raises_config_error(tmp_path: Path) -> None:
    with pytest.raises(ConfigError):
        load_config(tmp_path / "absent.toml")

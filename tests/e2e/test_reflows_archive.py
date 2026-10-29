"""Net flows on the archive release: the reflows tables against the bulk CRS, the
gross flows beside a release whose `reflows` table is empty, and the repayments of
the DAC's bilateral providers, 2015 to 2024, under ONE. The `release` fixture in
conftest.py builds the archive.

The pins marked "pinned from the first archive run" hold a sentinel until the first
run prints the figures.
"""

from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import climate_finance as cf
from climate_finance.build.fetch import CRS_FILE
from climate_finance.build.gate import load_gates, reflow_conservation_gate
from climate_finance.build.stages import raw_dir, table_path
from climate_finance.schema import (
    BILATERAL,
    EU_INSTITUTIONS,
    ReleaseManifest,
    TableEntry,
    sha256_file,
)

pytestmark = pytest.mark.slow

YEARS = range(2015, 2025)
GERMANY, FRANCE, SPAIN, JAPAN = 5, 4, 50, 701
# Principal repaid in the normalise `reflows` table, all providers, in USD billion.
RECEIVED_BOUNDS = {2015: (50, 70), 2024: (80, 105)}
# DAC bilateral providers, 2015 to 2024, under ONE in the recipient view, in USD
# billion: gross, repaid (gross less net), net, and the share of principal tied to
# a disbursement the release counts.
PINNED = {"gross": 202.08, "repaid": 20.07, "net": 182.01, "measured_share": 0.0911}
PINNED_TOLERANCE = 0.01
MAJOR_HOLDERS_SHARE = 0.7


@pytest.fixture(scope="module")
def loaded(release: Path) -> cf.Release:
    return cf.load(path=release)


@pytest.fixture(scope="module")
def emptied(release: Path, tmp_path_factory: pytest.TempPathFactory) -> cf.Release:
    """The release with its `reflows` table emptied: every other table linked from
    `release`, and `release.json` rehashed, so no second archive build runs."""
    target = tmp_path_factory.mktemp("emptied")
    for path in release.iterdir():
        if path.name not in ("reflows.parquet", "release.json"):
            (target / path.name).symlink_to(path)
    reflows = target / "reflows.parquet"
    pl.read_parquet(release / "reflows.parquet").clear().write_parquet(reflows)
    manifest = ReleaseManifest.model_validate_json(
        (release / "release.json").read_text(encoding="utf-8")
    )
    tables = dict(manifest.tables) | {
        "reflows": TableEntry(sha256=sha256_file(reflows), rows=0)
    }
    (target / "release.json").write_text(
        manifest.model_copy(update={"tables": tables}).model_dump_json(indent=2),
        encoding="utf-8",
    )
    return cf.load(path=target)


@pytest.fixture(scope="module")
def dac() -> list[int]:
    """The DAC members' provider codes, EU Institutions included."""
    members = cf.providers.list().filter(type="DAC member")
    return members["code"].to_list()


def _bilateral(result: cf.Result) -> pl.DataFrame:
    return result.table.filter(pl.col("bi_multi").is_in(BILATERAL))


def _value(frame: pl.DataFrame) -> float:
    return frame["value"].sum()


def test_reflows_are_conserved_from_the_bulk_crs(
    loaded: cf.Release, release: Path
) -> None:
    release_dir = release.parent
    first = loaded.manifest.years["crs"][0]
    last = loaded.manifest.settings["years.crs_last_complete"]
    assert isinstance(last, int)

    results = reflow_conservation_gate(
        raw=pl.scan_parquet(raw_dir(release_dir) / CRS_FILE),
        staged=pl.scan_parquet(table_path(release_dir, "normalise", "reflows")),
        released=loaded.table("reflows"),
        years=(first, last),
        config=load_gates().reflow_conservation,
    )
    print(results.select("subject", "value", "threshold", "passed", "detail"))

    assert sorted(results["subject"]) == ["interest", "received"]
    assert results["passed"].all(), results["detail"].to_list()


def test_principal_repaid_rises_from_2015_to_2024(release: Path) -> None:
    reflows = pl.read_parquet(
        table_path(release.parent, "normalise", "reflows"),
        columns=["year", "received_usd"],
    )
    by_year = dict(
        reflows.group_by("year").agg(pl.col("received_usd").sum()).iter_rows()
    )
    print({year: round(by_year[year] / 1e9, 2) for year in sorted(by_year)})

    for year, (low, high) in RECEIVED_BOUNDS.items():
        assert low < by_year[year] / 1e9 < high, year


@pytest.mark.parametrize("flow", ["disbursements", "grant_equivalent"])
@pytest.mark.parametrize("view", ["recipient", "provider"])
def test_gross_flows_ignore_the_reflows_table(
    loaded: cf.Release, emptied: cf.Release, flow: str, view: str
) -> None:
    """Gross totals under ONE for every provider, 2015 to 2024, equal those of the
    release with no reflows."""
    query = {"flow": flow, "view": view, "years": YEARS}

    assert_frame_equal(loaded.spending(**query).table, emptied.spending(**query).table)


def test_a_net_flow_is_gross_less_its_repayment_rows(loaded: cf.Release) -> None:
    gross = loaded.spending(years=YEARS)
    net = loaded.spending(flow="net_disbursements", years=YEARS)
    repayments = net.rows(origin="repayment")

    assert repayments["value"].sum() < 0
    assert _value(net.table) == pytest.approx(
        _value(gross.table) + repayments["value"].sum(), rel=1e-9
    )


@pytest.fixture(scope="module")
def dac_results(loaded: cf.Release, dac: list[int]) -> dict[str, pl.DataFrame]:
    """The DAC providers' bilateral rows, 2015 to 2024, under ONE, recipient view."""
    return {
        flow: _bilateral(loaded.spending(providers=dac, years=YEARS, flow=flow))
        for flow in ("disbursements", "net_disbursements")
    }


def test_the_dac_repays_a_pinned_share_of_its_bilateral_climate_finance(
    loaded: cf.Release, dac: list[int], dac_results: dict[str, pl.DataFrame]
) -> None:
    gross = _value(dac_results["disbursements"]) / 1e9
    net = _value(dac_results["net_disbursements"]) / 1e9
    repayments = loaded.quality(providers=dac, years=YEARS).repayments
    measured = repayments.select(
        pl.col("commitment_usd").sum() + pl.col("untraced_usd").sum(),
        pl.col("received_usd").sum(),
    ).row(0)
    figures = {
        "gross": gross,
        "repaid": gross - net,
        "net": net,
        "measured_share": measured[0] / measured[1],
    }
    print({name: round(figure, 4) for name, figure in figures.items()})

    assert figures["repaid"] > 0
    # pinned from the first archive run
    assert figures == pytest.approx(PINNED, rel=PINNED_TOLERANCE)


def test_germany_japan_and_france_hold_most_repayments(
    dac_results: dict[str, pl.DataFrame],
) -> None:
    def by_provider(flow: str) -> dict[int, float]:
        table = dac_results[flow]
        return dict(table.group_by("provider_code").agg(pl.col("value").sum()).rows())

    gross, net = by_provider("disbursements"), by_provider("net_disbursements")
    repaid = {code: gross[code] - net.get(code, 0.0) for code in gross}
    share = sum(repaid.get(code, 0.0) for code in (GERMANY, JAPAN, FRANCE)) / sum(
        repaid.values()
    )
    print(f"Germany, Japan and France hold {share:.3f} of the repayments")

    assert share > MAJOR_HOLDERS_SHARE


def test_spain_repays_no_more_than_it_disbursed(loaded: cf.Release) -> None:
    spain = loaded.spending(
        providers=SPAIN, years=YEARS, flow="net_disbursements"
    ).table
    print(f"Spain's net, 2015 to 2024: {_value(spain) / 1e9:.4f} billion")

    assert _value(spain) >= 0


def test_provider_view_net_less_gross_is_the_bilateral_repayments(
    loaded: cf.Release, dac: list[int]
) -> None:
    """For the DAC providers but EU Institutions, which the provider view counts
    once through its channels."""
    members = [code for code in dac if code != EU_INSTITUTIONS]
    query = {"providers": members, "years": YEARS}
    net = _value(
        loaded.spending(view="provider", flow="net_disbursements", **query).table
    )
    gross = _value(loaded.spending(view="provider", **query).table)
    repaid = _value(_bilateral(loaded.spending(**query))) - _value(
        _bilateral(loaded.spending(flow="net_disbursements", **query))
    )

    assert repaid > 0
    assert net - gross == pytest.approx(-repaid, rel=1e-9)

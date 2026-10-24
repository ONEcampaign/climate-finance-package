"""`spending()` and `quality()` on the fixture release.

Golden tables live in tests/golden/; set CF_UPDATE_GOLDEN=1 to rewrite them after a
deliberate change, then review the diff.
"""

import itertools
import os
import tomllib
from pathlib import Path

import polars as pl
import pytest
from make_fixture import FIXTURE_DIR
from polars.testing import assert_frame_equal
from releases import copy_release

import climate_finance as cf
from climate_finance.methods import OECD, RANGE, Method, MethodRange, label
from climate_finance.query import BILATERAL, EU_CHANNELS, EU_INSTITUTIONS
from climate_finance.result import AMOUNTS
from climate_finance.snapshot import Snapshot
from climate_finance.spec import Spec

GOLDEN_DIR = Path(__file__).parent / "golden"
SNAP = cf.load(path=FIXTURE_DIR)
FLOWS = ("commitments", "disbursements", "grant_equivalent")
VIEWS = ("recipient", "provider")


def _golden(name: str, frame: pl.DataFrame) -> None:
    path = GOLDEN_DIR / f"{name}.csv"
    if os.environ.get("CF_UPDATE_GOLDEN") == "1":
        frame.write_csv(path)
    expected = pl.read_csv(path, schema=frame.schema)
    assert_frame_equal(frame, expected, check_exact=False, rel_tol=1e-12)


GOLDEN_SPECS = {
    "one_disbursements_recipient": Spec(),
    "oecd_commitments_recipient": Spec(method=OECD, flow="commitments"),
    "range_disbursements_provider": Spec(method=RANGE, view="provider"),
    "one_grant_equivalent_recipient": Spec(flow="grant_equivalent"),
}
# France's 2020 core contribution to AfDB, whose 2020 OECD share has no split.
CLIMATE_ONLY_SPEC = Spec(view="provider", method=RANGE, providers=(4,), years=(2020,))


@pytest.mark.parametrize(("name", "spec"), GOLDEN_SPECS.items())
def test_method_outputs_match_their_golden_tables(name: str, spec: Spec) -> None:
    _golden(name, SNAP.spending(spec=spec).table.to_polars())


@pytest.mark.parametrize("spec", [*GOLDEN_SPECS.values(), CLIMATE_ONLY_SPEC])
def test_value_is_the_objectives_plus_unsplit(spec: Spec) -> None:
    result = SNAP.spending(spec=spec)
    # Under the highest-marker rule the objectives are disjoint; otherwise adaptation
    # and mitigation each include cross_cutting.
    sign = pl.col("method").replace_strict(
        {label(method): 1 if method.highest_marker else -1 for method in spec.methods}
    )

    for frame in (result.rows(), result.table.to_polars()):
        parts = frame.select(
            pl.col("adaptation")
            + pl.col("mitigation")
            + sign * pl.col("cross_cutting")
            + pl.col("unsplit")
        ).to_series()
        assert frame.height
        assert parts.to_list() == pytest.approx(frame["value"].to_list())


def test_quality_summary_matches_its_golden_table() -> None:
    _golden("quality_summary", SNAP.quality().summary())


GRID = [
    Method(significant=significant, principal=principal, highest_marker=highest)
    for significant, principal, highest in itertools.product(
        (0.0, 0.4, 1.0), (0.5, 1.0), (True, False)
    )
]


@pytest.mark.parametrize("flow", FLOWS)
@pytest.mark.parametrize("view", VIEWS)
def test_value_stays_within_face_and_between_its_bounds(flow: str, view: str) -> None:
    grid = MethodRange(methods=tuple(GRID))
    result = SNAP.spending(method=grid, flow=flow, view=view)

    for frame in (result.rows(), result.table.to_polars()):
        known = frame.drop_nulls("amount")
        assert known.height
        assert (known["value"] <= known["amount"] * (1 + 1e-12)).all()
        assert (known["value"] >= 0).all()
        assert (known["low"] <= known["value"]).all()
        assert (known["value"] <= known["high"]).all()


def _without_code_2(tmp_path: Path) -> Snapshot:
    code_2 = SNAP.table("commitments").filter(pl.col("bi_multi") == 2).collect()
    assert code_2.height, "the fixture needs a Bi_Multi 2 commitment"
    keep = ~pl.col("row_id").is_in(code_2["row_id"].implode())
    release = copy_release(
        tmp_path / "release",
        edits={
            "commitments": lambda frame: frame.filter(keep),
            "disbursements": lambda frame: frame.filter(keep),
        },
    )
    return cf.load(path=release)


@pytest.mark.parametrize("flow", FLOWS)
@pytest.mark.parametrize("view", VIEWS)
def test_code_2_rows_leave_every_total_unchanged(
    tmp_path: Path, flow: str, view: str
) -> None:
    spec = Spec(method=cf.methods.RANGE, flow=flow, view=view)
    stripped = _without_code_2(tmp_path)

    with_code_2 = SNAP.spending(spec=spec).table.to_polars()

    assert_frame_equal(with_code_2, stripped.spending(spec=spec).table.to_polars())
    bilateral = with_code_2.filter(pl.col("bi_multi").is_in(BILATERAL))
    assert bilateral.height


@pytest.mark.parametrize(
    "spec",
    [
        Spec(),
        Spec(
            providers=(5, 905),
            years=(2021, 2022, 2023),
            method=cf.methods.RANGE,
            flow="commitments",
            view="provider",
            oda_only=True,
        ),
        Spec(method=cf.methods.ONE.replace(significant=0.25), flow="grant_equivalent"),
        Spec(method=cf.methods.ONE.replace(eu="provider"), view="provider"),
    ],
)
def test_a_spec_round_trips_through_json_and_reruns_identically(spec: Spec) -> None:
    first = SNAP.spending(spec=spec)

    restored = Spec.model_validate_json(first.spec.model_dump_json())
    rerun = SNAP.spending(spec=restored)

    assert restored == spec
    assert_frame_equal(first.table.to_polars(), rerun.table.to_polars())
    assert_frame_equal(first.rows(), rerun.rows())


def test_keyword_arguments_build_the_same_spec() -> None:
    result = SNAP.spending(providers=5, years=range(2021, 2023), view="provider")

    assert result.spec == Spec(providers=(5,), years=(2021, 2022), view="provider")


@pytest.mark.parametrize("flow", FLOWS)
def test_the_provider_view_attributes_core_contributions_of_its_flow(
    flow: str,
) -> None:
    # OECD has a share for every agency and year with a core contribution.
    rows = SNAP.spending(view="provider", flow=flow, method=OECD).rows(
        origin="attribution"
    )
    contributions = (
        SNAP.table("core_contributions").filter(pl.col("flow") == flow).collect()
    )

    assert sorted(rows["amount"]) == sorted(contributions["contribution_usd"])
    assert set(rows["bi_multi"]) == {2}
    assert rows["recipient_code"].is_null().all()


# Fixture inputs: Germany gives IDA (channel 44002) 60m in 2021; IDA's ONE shares are
# adaptation 0.18, mitigation 0.22 and cross-cutting 0.04, its OECD shares 0.20,
# 0.25 and 0.05, each of adaptation and mitigation including the cross-cutting share.
@pytest.mark.parametrize(
    ("method", "expected"),
    [
        # The highest-marker rule takes cross-cutting out of both objectives.
        (
            cf.methods.ONE,
            (60e6 * 0.14, 60e6 * 0.18, 60e6 * 0.04, 60e6 * (0.18 + 0.22 - 0.04)),
        ),
        (
            cf.methods.OECD,
            (60e6 * 0.20, 60e6 * 0.25, 60e6 * 0.05, 60e6 * (0.20 + 0.25 - 0.05)),
        ),
    ],
)
def test_attribution_reads_agency_shares_as_inclusive_of_cross_cutting(
    method: Method, expected: tuple[float, float, float, float]
) -> None:
    result = SNAP.spending(view="provider", method=method, providers=5, years=2021)
    rows = result.rows(origin="attribution").select(
        "adaptation", "mitigation", "cross_cutting", "value"
    )

    assert rows.height == 1
    assert rows.row(0) == pytest.approx(expected)


def test_a_climate_only_share_attributes_value_as_unsplit() -> None:
    # France gives AfDB (channel 46002) 25m in 2020. AfDB's 2020 OECD share is 0.35
    # in total, with no split by objective, and it has no ONE share that year.
    result = SNAP.spending(spec=CLIMATE_ONLY_SPEC)
    columns = (
        "adaptation", "mitigation", "cross_cutting", "unsplit", "value", "low", "high",
    )  # fmt: skip
    expected = (0.0, 0.0, 0.0, *[pytest.approx(25e6 * 0.35)] * 4)

    rows = result.rows(origin="attribution")
    assert rows["method"].to_list() == ["OECD"]
    assert rows.select(columns).row(0) == expected
    assert result.table.to_polars().select(columns).row(0) == expected
    # France's 2020 code 2 flows total 1.5 times its contribution.
    share = result.share_of_total().table.to_polars()["value"].item()
    assert share == pytest.approx(0.35 / 1.5)
    assert "Channels 46002: the climate share has no split" in result.explain()
    assert "value sits in unsplit" in result.explain()


@pytest.mark.parametrize("method", [cf.methods.ONE, cf.methods.OECD])
def test_a_component_row_counts_its_climate_share_of_face_value_disbursed(
    method: Method,
) -> None:
    rows = SNAP.spending(method=method).rows(row_id="IDA-2")

    # IDA-2: face 200m, half of it disbursed, climate share 0.30.
    assert rows["value"].sum() == pytest.approx(200e6 * 0.5 * 0.30)


@pytest.mark.parametrize("method", [cf.methods.ONE, cf.methods.OECD])
def test_a_fully_disbursed_component_counts_all_of_its_climate_value(
    method: Method,
) -> None:
    rows = SNAP.spending(method=method).rows(row_id="IDA-1")

    # IDA-1: face 150m, climate share 0.45, disbursed in full over three years.
    assert rows["year"].n_unique() == 3
    assert rows["value"].sum() == pytest.approx(67.5e6)
    assert rows["high"].sum() == pytest.approx(67.5e6)


def test_a_split_record_counts_its_grant_equivalent_once() -> None:
    rows = SNAP.spending(flow="grant_equivalent").rows(record_id="JPN-SPLIT")

    # JPN-SPLIT disburses 30m with an 18m grant equivalent: 12m to JPN-1, 10m to
    # JPN-3, 8m excess. Each allocation takes its pro-rata grant equivalent.
    by_commitment = dict(zip(rows["row_id"], rows["amount"], strict=True))
    assert by_commitment == pytest.approx(
        {"JPN-1": 18e6 * 12e6 / 30e6, "JPN-3": 18e6 * 10e6 / 30e6}
    )
    # Both commitments are principal mitigation, so value equals amount.
    assert rows["value"].sum() == pytest.approx(18e6 * (12e6 + 10e6) / 30e6)


def test_bounds_follow_each_allocation_in_its_own_year() -> None:
    rows = SNAP.spending(flow="disbursements").rows(row_id="DEU-1").sort("year")

    # DEU-1: face 40m, 95% disbursed in three equal records, a principal marker.
    # The 2021 link is certain. The 2022 and 2023 links have probability 0.85, so
    # they leave the low bound. Each high bound is 1.05 times the allocation.
    allocated = 40e6 * 0.95 / 3
    assert rows["year"].to_list() == [2021, 2022, 2023]
    assert rows["value"].to_list() == pytest.approx([allocated] * 3)
    assert rows["low"].to_list() == pytest.approx([allocated, 0.0, 0.0])
    assert rows["high"].to_list() == pytest.approx([allocated * 1.05] * 3)


def test_the_recipient_view_counts_outflows_and_no_attribution() -> None:
    rows = SNAP.spending().rows()

    assert set(rows["origin"]) == {"disbursement"}
    assert 4 in set(rows["bi_multi"])
    assert set(rows["bi_multi"]) <= {*BILATERAL, 4}


def test_oecd_attribution_uses_the_imputed_shares() -> None:
    def attributed(method: Method) -> float:
        rows = SNAP.spending(view="provider", method=method).rows(origin="attribution")
        return rows["value"].sum()

    assert attributed(cf.methods.OECD) > attributed(cf.methods.ONE)


def test_grant_equivalent_gaps_stay_missing() -> None:
    rows = SNAP.spending(flow="grant_equivalent").rows()

    assert rows["amount"].null_count() > 0
    assert rows.filter(pl.col("amount").is_null())["value"].is_null().all()


def test_oda_only_drops_rows_outside_oda_flow_codes() -> None:
    rows = SNAP.spending(flow="commitments", oda_only=True).rows()

    assert set(rows["flow_code"]) <= {11, 13, 19}
    assert SNAP.spending(oda_only=True).modelled.to_polars().is_empty()


def test_rows_filter_by_equality_and_membership() -> None:
    result = SNAP.spending()

    assert set(result.rows(year=2022)["year"]) == {2022}
    assert set(result.rows(provider_code=[5, 8])["provider_code"]) == {5, 8}
    with pytest.raises(KeyError, match="no column"):
        result.rows(donor="Germany")


def test_a_record_shows_its_jev_label_before_its_markers() -> None:
    row = SNAP.spending().rows(record_id="JPN-1-R1")

    assert row.select("objective_label", "label_source").row(0) == ("adaptation", "jev")


def test_the_modelled_line_stays_out_of_traced_totals() -> None:
    result = SNAP.spending()

    modelled = result.modelled.to_polars()
    assert set(modelled["provider_code"]) == {903}
    assert 903 not in set(result.table.to_polars()["provider_code"])


def test_explain_names_the_method_view_and_units() -> None:
    text = SNAP.spending(method=cf.methods.OECD, view="provider").explain()

    assert "Method OECD" in text
    assert "EU Institutions count as a provider" in text
    assert "Provider view" in text
    assert "USD at current prices" in text


def test_share_of_total_divides_each_key_by_its_total_flows() -> None:
    result = SNAP.spending(view="provider", providers=5, years=2021)

    shares = result.share_of_total().table.to_polars()

    by_key = {
        (row["recipient_code"], row["bi_multi"]): row["value"]
        for row in shares.iter_rows(named=True)
    }
    # Kenya: a principal-marked disbursement over four times its amount in all flows.
    assert by_key[(248, 1)] == pytest.approx(0.25)
    # Attribution, no recipient: 60m to IDA at a 0.36 ONE climate share, over every
    # code 2 disbursement (DEU-3's 2021 record of 60m * 0.95 / 3 times four, plus
    # 1.5 times the 60m contribution).
    assert by_key[(None, 2)] == pytest.approx(
        60e6 * 0.36 / (4 * 60e6 * 0.95 / 3 + 1.5 * 60e6)
    )
    assert_frame_equal(shares.drop(AMOUNTS), result.table.to_polars().drop(AMOUNTS))


@pytest.mark.parametrize(
    ("row_id", "significant"),
    # Germany's 0.5 is confirmed. Japan's 0.6 is not, so Japan takes ONE's 0.4.
    [("DEU-2", 0.5), ("JPN-2", 0.4)],
)
def test_provider_coefficients_replace_the_significant_coefficient_once_confirmed(
    row_id: str, significant: float
) -> None:
    def ratio(method: Method) -> float:
        rows = SNAP.spending(method=method).rows(row_id=row_id)
        return rows["value"].sum() / rows["amount"].sum()

    assert ratio(cf.methods.PROVIDER) == pytest.approx(significant)
    assert ratio(cf.methods.ONE) == pytest.approx(0.4)


def test_explain_names_providers_without_a_confirmed_coefficient() -> None:
    text = SNAP.spending(method=cf.methods.PROVIDER).explain()

    assert "providers 4, 8, 701 have no confirmed coefficient and take 0.4" in text


def _repeat_jpn_2(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.with_columns(
        repeat_of=pl.when(pl.col("row_id") == "JPN-2")
        .then(pl.lit("JPN-1"))
        .otherwise(pl.col("repeat_of"))
    )


@pytest.mark.parametrize("flow", FLOWS)
def test_a_repeat_and_the_disbursements_allocated_to_it_stay_out(
    tmp_path: Path, flow: str
) -> None:
    release = copy_release(tmp_path / "release", edits={"commitments": _repeat_jpn_2})

    rows = cf.load(path=release).spending(flow=flow).rows()

    assert rows.height
    left_out = set(SNAP.spending(flow=flow).rows()["row_id"]) - set(rows["row_id"])
    assert left_out == {"JPN-2"}


def _split_norway_bilateral_totals(frame: pl.DataFrame) -> pl.DataFrame:
    # Norway's 2021 commitments to Bangladesh in all flows, half under Bi_Multi 3.
    key = (
        (pl.col("flow") == "commitments")
        & (pl.col("provider_code") == 8)
        & (pl.col("year") == 2021)
    )
    halves = frame.filter(key).with_columns(pl.col("amount_usd") / 2)
    return pl.concat(
        [frame.filter(~key), halves, halves.with_columns(bi_multi=pl.lit(3, pl.Int8))]
    )


def test_a_bilateral_commitment_divides_by_every_bilateral_code(
    tmp_path: Path,
) -> None:
    release = copy_release(
        tmp_path / "release", edits={"total_flows": _split_norway_bilateral_totals}
    )
    spec = Spec(flow="commitments", providers=(8,), years=(2021,))

    shares = cf.load(path=release).spending(spec=spec).share_of_total()

    expected = SNAP.spending(spec=spec).share_of_total().table.to_polars()
    assert expected.height
    assert_frame_equal(shares.table.to_polars(), expected)


ONE_EU_AS_PROVIDER = cf.methods.ONE.replace(eu="provider")
# France gives the EU budget (channel 42001) 30m a year in 2021 and 2022 in every
# flow, whose ONE climate share is 0.30. EU Institutions' own commitment EU-1 copies
# Norway's NOR-1: committed in 2021, disbursed in 2021, 2022 and 2023.
EU_CONTRIBUTION, EU_SHARE = 30e6, 0.30
EU_ATTRIBUTION_YEARS = (2021, 2022)


def _with_eu_commitment(frame: pl.DataFrame) -> pl.DataFrame:
    eu = frame.filter(pl.col("row_id") == "NOR-1").with_columns(
        row_id=pl.lit("EU-1"), provider_code=pl.lit(EU_INSTITUTIONS, pl.Int32)
    )
    if "record_id" in frame.columns:
        eu = eu.with_columns(pl.col("record_id").str.replace("NOR-1", "EU-1"))
    return pl.concat([frame, eu])


def _with_eu_contributions(frame: pl.DataFrame) -> pl.DataFrame:
    rows = pl.DataFrame(
        {
            "provider_code": 4,
            "channel_code": 42001,
            "year": [year for year in (2021, 2022) for _ in FLOWS],
            "flow": list(FLOWS) * 2,
            "contribution_usd": EU_CONTRIBUTION,
        }
    )
    return pl.concat([frame, rows.cast(frame.schema)])


def _with_eu_shares(frame: pl.DataFrame) -> pl.DataFrame:
    rows = pl.DataFrame(
        {
            "channel_code": 42001,
            "year": [year for year in (2021, 2022) for _ in range(3)],
            "objective": ["adaptation", "mitigation", "cross_cutting"] * 2,
            "share": [0.10, 0.25, 0.05] * 2,
            "source": "one",
        }
    )
    # The OECD's share for the same channel, which OECD ignores.
    oecd = rows.with_columns(source=pl.lit("oecd"))
    return pl.concat([frame, rows.cast(frame.schema), oecd.cast(frame.schema)])


@pytest.fixture(scope="module")
def eu_snap(tmp_path_factory: pytest.TempPathFactory) -> Snapshot:
    release = copy_release(
        tmp_path_factory.mktemp("eu") / "release",
        edits={
            "commitments": _with_eu_commitment,
            "disbursements": _with_eu_commitment,
            "core_contributions": _with_eu_contributions,
            "agency_shares": _with_eu_shares,
        },
    )
    return cf.load(path=release)


def test_the_eu_constants_match_the_channels_the_build_maps_to_eu_institutions() -> (
    None
):
    toml = Path(cf.__file__).parent / "build" / "providers.toml"
    agencies = tomllib.loads(toml.read_text())["agencies"]

    channels = {
        row["channel_code"]
        for row in agencies
        if row["provider_code"] == EU_INSTITUTIONS
    }
    assert channels == set(EU_CHANNELS)


@pytest.mark.parametrize("flow", FLOWS)
@pytest.mark.parametrize(
    ("method", "eu_own_rows", "eu_attributed"),
    [
        # As a channel, France's contribution carries the EU's share and the EU's
        # own rows leave the all-provider total.
        (cf.methods.ONE, False, EU_SHARE),
        (ONE_EU_AS_PROVIDER, True, 0.0),
        (cf.methods.OECD, True, 0.0),
    ],
)
def test_an_all_provider_total_counts_eu_money_once(
    eu_snap: Snapshot,
    method: Method,
    flow: str,
    eu_own_rows: bool,
    eu_attributed: float,
) -> None:
    rows = eu_snap.spending(view="provider", method=method, flow=flow).rows()

    own = rows.filter(
        pl.col("provider_code") == EU_INSTITUTIONS,
        pl.col("year").is_in(EU_ATTRIBUTION_YEARS),
    )
    attributed = rows.filter(pl.col("channel_code").is_in(EU_CHANNELS))
    assert (own.height > 0) == eu_own_rows
    assert attributed.filter(pl.col("provider_code") == EU_INSTITUTIONS).is_empty()
    assert attributed["value"].sum() == pytest.approx(
        2 * EU_CONTRIBUTION * eu_attributed
    )


@pytest.mark.parametrize("view", VIEWS)
@pytest.mark.parametrize("flow", FLOWS)
def test_eu_institutions_alone_return_their_own_spending_under_either_setting(
    eu_snap: Snapshot, flow: str, view: str
) -> None:
    def eu_alone(method: Method) -> pl.DataFrame:
        result = eu_snap.spending(
            view=view, method=method, flow=flow, providers=EU_INSTITUTIONS
        )
        return result.table.to_polars().drop("method")

    as_channel = eu_alone(cf.methods.ONE)
    assert as_channel.height
    assert_frame_equal(as_channel, eu_alone(ONE_EU_AS_PROVIDER))


@pytest.mark.parametrize(
    ("providers", "eu_own_rows"),
    [
        ((EU_INSTITUTIONS,), True),
        # IDA attributes nothing to an EU channel, so no EU money counts twice.
        ((EU_INSTITUTIONS, 905), True),
        ((EU_INSTITUTIONS, 4), False),
        (None, False),
    ],
)
def test_eu_own_rows_leave_a_result_only_beside_eu_attribution(
    eu_snap: Snapshot, providers: tuple[int, ...] | None, eu_own_rows: bool
) -> None:
    rows = eu_snap.spending(
        view="provider", providers=providers, years=EU_ATTRIBUTION_YEARS
    ).rows(provider_code=EU_INSTITUTIONS)

    assert (rows.height > 0) == eu_own_rows


def test_a_member_counts_its_eu_share_only_with_the_eu_as_a_channel(
    eu_snap: Snapshot,
) -> None:
    def france(method: Method) -> float:
        result = eu_snap.spending(
            view="provider", method=method, providers=4, years=2021
        )
        return result.rows(origin="attribution")["value"].sum()

    assert france(cf.methods.ONE) - france(ONE_EU_AS_PROVIDER) == pytest.approx(
        EU_CONTRIBUTION * EU_SHARE
    )


def test_eu_own_rows_stay_in_a_year_without_eu_attribution(eu_snap: Snapshot) -> None:
    # EU-1 disburses in 2023, a year in which France attributes nothing to the EU.
    def eu_rows(method: Method) -> pl.DataFrame:
        rows = eu_snap.spending(view="provider", method=method).rows(
            provider_code=EU_INSTITUTIONS
        )
        return rows.drop("method")

    as_channel = eu_rows(cf.methods.ONE)

    assert set(as_channel["year"]) == {2023}
    assert_frame_equal(
        as_channel, eu_rows(ONE_EU_AS_PROVIDER).filter(pl.col("year") == 2023)
    )

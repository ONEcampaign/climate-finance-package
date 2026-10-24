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
from make_fixture import FIXTURE_DIR, LEDGER_RECORD
from polars.testing import assert_frame_equal
from releases import copy_release

import climate_finance as cf
from climate_finance.methods import OECD, ONE, RANGE, Method, MethodRange, label
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


def _with_top_channel_remainders(frame: pl.DataFrame) -> pl.DataFrame:
    # Every provider, year and flow keeps 1m its leaves do not itemise on 40000.
    remainders = (
        frame.select("provider_code", "year", "flow")
        .unique()
        .with_columns(
            channel_code=pl.lit(40000, pl.Int32), contribution_usd=pl.lit(1e6)
        )
    )
    return pl.concat([frame, remainders.select(frame.columns)])


@pytest.mark.parametrize("method", [ONE, OECD, RANGE])
def test_a_channel_with_no_agency_share_attributes_nothing(
    tmp_path: Path, method: MethodRange | Method
) -> None:
    release = copy_release(
        tmp_path / "release",
        edits={"core_contributions": _with_top_channel_remainders},
    )

    spec = Spec(view="provider", method=method)
    result = cf.load(path=release).spending(spec=spec).table.to_polars()

    assert_frame_equal(result, SNAP.spending(spec=spec).table.to_polars())


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
    rows = SNAP.spending(flow="disbursements").rows(row_id="DEU-2").sort("year")

    # DEU-2: face 25m, half disbursed in three equal records. The 2022 record's
    # parent-id link is certain; the 2023 and 2024 records' project-number links
    # are estimated, so they leave the low bound. Each high bound is 1.05 times
    # the allocation.
    value = rows["value"][0]
    assert rows["year"].to_list() == [2022, 2023, 2024]
    assert rows["value"].to_list() == pytest.approx([value] * 3)
    assert rows["low"].to_list() == pytest.approx([value, 0.0, 0.0])
    assert rows["high"].to_list() == pytest.approx([value * 1.05] * 3)
    # DEU-2 is Germany's only 2022 commitment. Under OECD its disbursements' value
    # is quality's disbursed climate value, low and high alike.
    oecd = SNAP.spending(flow="disbursements", method=OECD).rows(row_id="DEU-2")
    quality = SNAP.table("quality").filter(provider_code=5, year=2022).collect()
    assert (oecd["low"].sum(), oecd["high"].sum()) == pytest.approx(
        quality.select("certain_disbursed_climate_usd", "disbursed_climate_usd").row(0)
    )


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


def _hold_afdb_2022(frame: pl.DataFrame) -> pl.DataFrame:
    # AfDB's 2022 ONE share repeats an earlier year's.
    key = (
        (pl.col("channel_code") == 46002)
        & (pl.col("year") == 2022)
        & (pl.col("source") == "one")
    )
    return frame.with_columns(
        share_source=pl.when(key).then(pl.lit("held")).otherwise("share_source")
    )


def test_attributed_value_shows_and_sums_by_share_source(tmp_path: Path) -> None:
    snap = cf.load(
        path=copy_release(
            tmp_path / "release", edits={"agency_shares": _hold_afdb_2022}
        )
    )
    rows = snap.spending(view="provider").rows(origin="attribution")
    by_source = rows.group_by("provider_code", "year", "share_source").agg(
        pl.col("value").sum()
    )

    attribution = snap.quality().attribution.to_polars().filter(flow="disbursements")
    columns = attribution.unpivot(
        index=["provider_code", "year"],
        on=["computed_usd", "held_usd", "oecd_usd"],
        variable_name="share_source",
    ).filter(pl.col("value") != 0)

    assert set(rows["share_source"]) == {"computed", "held"}
    assert sorted(
        columns.with_columns(pl.col("share_source").str.strip_suffix("_usd")).rows()
    ) == pytest.approx(sorted(by_source.select(columns.columns).rows()))
    oecd_rows = snap.spending(view="provider", method=OECD).rows(origin="attribution")
    assert set(oecd_rows["share_source"]) == {"oecd"}


def test_grant_equivalent_gaps_stay_missing() -> None:
    rows = SNAP.spending(flow="grant_equivalent").rows()

    assert rows["amount"].null_count() > 0
    assert rows.filter(pl.col("amount").is_null())["value"].is_null().all()


def test_oda_only_counts_commitments_of_the_oda_flow_category() -> None:
    oda = (
        pl.read_parquet(FIXTURE_DIR / "commitments.parquet")
        .filter(pl.col("flow_category") == 10, pl.col("bi_multi") != 2)
        .sort("row_id")
    )
    rows = SNAP.spending(flow="commitments", method=OECD, oda_only=True).rows()

    assert rows["row_id"].sort().to_list() == oda["row_id"].to_list()
    assert rows["value"].sum() > 0
    assert rows["value"].sum() == pytest.approx(oda["climate_usd"].sum())


def test_oda_only_counts_disbursements_by_flow_code_and_fills_by_category() -> None:
    result = SNAP.spending(oda_only=True)
    rows = result.rows()
    fills = rows.filter(pl.col("flow_code").is_null())

    assert set(rows["flow_code"].drop_nulls()) <= {11, 13, 19}
    # A bank-ledger fill has no CRS flow code, and counts by its commitment's.
    assert fills["record_id"].to_list() == [LEDGER_RECORD]
    assert fills["flow_category"].to_list() == [10]
    assert result.modelled.to_polars().is_empty()


def _publish_deu_2_at_40_percent(frame: pl.DataFrame) -> pl.DataFrame:
    # As IFAD publishes its significant rows: climate value 40% of face value.
    is_row = pl.col("row_id") == "DEU-2"
    return frame.with_columns(
        pl.when(is_row).then(pl.col(name) * 0.4).otherwise(pl.col(name)).alias(name)
        for name in ("climate_usd", "adaptation_usd", "mitigation_usd", "overlap_usd")
    )


def test_a_marker_row_counts_at_most_its_published_climate_share(
    tmp_path: Path,
) -> None:
    release = copy_release(
        tmp_path / "release", edits={"commitments": _publish_deu_2_at_40_percent}
    )

    result = cf.load(path=release).spending(flow="commitments", method=OECD)
    row = result.rows(row_id="DEU-2")

    assert row["value"].item() == pytest.approx(0.4 * 25e6)


def test_rows_filter_by_equality_and_membership() -> None:
    result = SNAP.spending()

    assert set(result.rows(year=2022)["year"]) == {2022}
    assert set(result.rows(provider_code=[5, 8])["provider_code"]) == {5, 8}
    with pytest.raises(KeyError, match="no column"):
        result.rows(donor="Germany")


def test_a_record_shows_its_jev_label_before_its_markers() -> None:
    row = SNAP.spending().rows(record_id="JPN-2-R1")

    assert row.select("objective_label", "label_source", "jev_level").row(0) == (
        "adaptation",
        "jev",
        2,
    )


JEV_ONE = ONE.replace(relevance="jev")


@pytest.mark.parametrize(
    ("method", "adaptation", "cross_cutting", "value"),
    [
        # JPN-2 marks both objectives significant: cross-cutting at 0.4.
        (ONE, 0.0, 0.4, 0.4),
        # Jev's principal adaptation level outranks the significant mitigation one.
        (JEV_ONE, 1.0, 0.0, 1.0),
        # Each marker counts at the method's own coefficients: 0.5 and 1.0.
        (
            Method(
                significant=0.5, principal=1.0, highest_marker=False, relevance="jev"
            ),
            1.0,
            0.5,
            1.0,
        ),
    ],
)
def test_jev_relevance_weighs_a_jev_level_like_a_marker(
    method: Method, adaptation: float, cross_cutting: float, value: float
) -> None:
    row = SNAP.spending(method=method).rows(record_id="JPN-2-R1")

    amount = row["amount"].item()
    assert row.select("adaptation", "cross_cutting", "value").row(0) == (
        pytest.approx(adaptation * amount),
        pytest.approx(cross_cutting * amount),
        pytest.approx(value * amount),
    )


def test_jev_relevance_leaves_records_jev_did_not_label_alone() -> None:
    def unlabelled(method: Method) -> pl.DataFrame:
        return (
            SNAP.spending(method=method)
            .rows()
            .filter(pl.col("record_id").ne_missing("JPN-2-R1"))
            .drop("method")
        )

    assert_frame_equal(unlabelled(JEV_ONE), unlabelled(ONE))


def test_explain_says_what_jev_relevance_replaces() -> None:
    text = SNAP.spending(method=JEV_ONE).explain()

    assert "read Jev's adaptation level (none, significant or principal)" in text
    assert "Jev labels only records whose commitment is marked adaptation" in text


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


SURVEY = "Fixture survey, p. 8"


def _ratio(snapshot: Snapshot, method: Method, row_id: str) -> tuple[float, str]:
    """The share of `row_id`'s commitment that counts, and its coefficient basis."""
    rows = snapshot.spending(method=method, flow="commitments").rows(row_id=row_id)
    return rows["value"].sum() / rows["amount"].sum(), rows["coefficient_basis"][0]


@pytest.mark.parametrize(
    ("row_id", "method", "ratio", "basis"),
    [
        # Germany counts an activity significant for both objectives at 1.0, under
        # either marker rule.
        ("DEU-2", cf.methods.PROVIDER, 1.0, "provider"),
        ("DEU-2", cf.methods.PROVIDER.replace(highest_marker=False), 1.0, "provider"),
        # Japan's principal coefficient is 0.85 up to 2021 and 1.0 from 2022.
        ("JPN-1", cf.methods.PROVIDER, 0.85, "provider"),
        ("JPN-2", cf.methods.PROVIDER, 0.6, "provider"),
        ("JPN-3", cf.methods.PROVIDER, 1.0, "provider"),
        # France's agency 1 states no flat coefficient, so it takes PROVIDER's 0.4
        # rather than France's 0.3.
        ("FRA-2", cf.methods.PROVIDER, 0.4, "case_by_case"),
        # Norway's default covers significant activities; principal ones take 1.0.
        ("NOR-2", cf.methods.PROVIDER, 0.3, "default"),
        ("NOR-1", cf.methods.PROVIDER, 1.0, "default"),
        ("DEU-2", cf.methods.ONE, 0.4, None),
    ],
)
def test_provider_coefficients_apply_by_year_agency_and_marker(
    row_id: str, method: Method, ratio: float, basis: str | None
) -> None:
    assert _ratio(SNAP, method, row_id) == (pytest.approx(ratio), basis)


def _jev_rules_out_adaptation(frame: pl.DataFrame) -> pl.DataFrame:
    jev = pl.DataFrame(
        {
            "record_id": "DEU-2-R0",
            "objective": "adaptation",
            "level": 0,
            "label_source": "jev",
            "probability": 0.2,
        }
    )
    return pl.concat([frame, jev.cast(frame.schema)])


def test_both_significant_reads_the_jev_level(tmp_path: Path) -> None:
    # DEU-2 marks both objectives significant, which Germany counts at 1.0. Jev
    # finds DEU-2-R0 not adaptation, so under Jev relevance it is significant for
    # mitigation alone and counts at Germany's 0.5.
    release = copy_release(
        tmp_path / "release", edits={"objectives": _jev_rules_out_adaptation}
    )
    snapshot = cf.load(path=release)

    def ratio(method: Method) -> float:
        row = snapshot.spending(method=method).rows(record_id="DEU-2-R0")
        return row["value"].item() / row["amount"].item()

    assert ratio(cf.methods.PROVIDER) == pytest.approx(1.0)
    assert ratio(cf.methods.PROVIDER.replace(relevance="jev")) == pytest.approx(0.5)


def test_an_agency_without_its_own_row_takes_the_providers(tmp_path: Path) -> None:
    def move_agency(frame: pl.DataFrame) -> pl.DataFrame:
        return frame.with_columns(
            agency_code=pl.when(pl.col("agency_code") == 1).then(6)
        )

    release = copy_release(
        tmp_path / "release", edits={"provider_coefficients": move_agency}
    )

    assert _ratio(cf.load(path=release), cf.methods.PROVIDER, "FRA-2") == (
        pytest.approx(0.3),
        "provider",
    )


def test_explain_names_each_providers_coefficient_source() -> None:
    lines = SNAP.spending(method=cf.methods.PROVIDER).explain().splitlines()

    assert "Coefficients of providers 701 (year 2021): Fixture report, footnote 4." in (
        lines
    )
    assert f"Coefficients of providers 5, 701 (years 2022, 2023, 2024): {SURVEY}." in (
        lines
    )
    assert (
        "Providers 8 assess each activity case by case; where an activity's share is "
        "unknown they take the default share in Fixture survey, p. 5." in lines
    )
    assert (
        "Providers 4 assess each activity case by case (Fixture survey, p. 9, note f) "
        "and state no flat coefficient, so they take significant 0.4 and principal 1."
        in lines
    )


def test_explain_names_providers_without_coefficients(tmp_path: Path) -> None:
    def drop_norway(frame: pl.DataFrame) -> pl.DataFrame:
        return frame.filter(pl.col("provider_code") != 8)

    release = copy_release(
        tmp_path / "release", edits={"provider_coefficients": drop_norway}
    )
    text = cf.load(path=release).spending(method=cf.methods.PROVIDER).explain()

    assert (
        "Providers 8 have no coefficient in the release and take significant 0.4 and "
        "principal 1." in text.splitlines()
    )


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
            "share_source": "computed",
        }
    )
    # The OECD's share for the same channel, which OECD ignores.
    oecd = rows.with_columns(source=pl.lit("oecd"), share_source=pl.lit("oecd"))
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


def _with_eib_and_no_2022_budget_share(frame: pl.DataFrame) -> pl.DataFrame:
    # France also gives the EIB (42004), with an EIB share in 2021 and 2022, and the
    # EU budget's 2022 share is missing.
    eib = _with_eu_shares(frame.clear()).with_columns(
        channel_code=pl.lit(42004, pl.Int32)
    )
    budget = _with_eu_shares(frame).filter(
        ~((pl.col("channel_code") == 42001) & (pl.col("year") == 2022))
    )
    return pl.concat([budget, eib])


def _with_eib_contributions(frame: pl.DataFrame) -> pl.DataFrame:
    with_budget = _with_eu_contributions(frame)
    eib = with_budget.filter(channel_code=42001).with_columns(
        channel_code=pl.lit(42004, pl.Int32)
    )
    return pl.concat([with_budget, eib])


def test_a_year_without_a_budget_share_keeps_the_budgets_own_rows(
    tmp_path: Path,
) -> None:
    release = copy_release(
        tmp_path / "release",
        edits={
            "commitments": _with_eu_commitment,
            "disbursements": _with_eu_commitment,
            "core_contributions": _with_eib_contributions,
            "agency_shares": _with_eib_and_no_2022_budget_share,
        },
    )

    # EU-1 is a budget row (agency 1). In 2022 only the EIB's channel attributes.
    rows = cf.load(path=release).spending(view="provider").rows(row_id="EU-1")

    assert set(rows["year"]) == {2022, 2023}


@pytest.mark.parametrize("flow", FLOWS)
def test_quality_reports_the_eu_rows_a_provider_total_leaves_out(
    eu_snap: Snapshot, flow: str
) -> None:
    own = (
        eu_snap.spending(
            view="provider",
            method=ONE_EU_AS_PROVIDER,
            flow=flow,
            providers=EU_INSTITUTIONS,
            years=EU_ATTRIBUTION_YEARS,
        )
        .rows()
        .group_by("year")
        .agg(pl.col("value").sum())
    )

    left_out = eu_snap.quality().eu_left_out.to_polars().filter(flow=flow)

    assert own.height
    assert set(left_out.select("agency_code", "channel_code").rows()) == {(1, 42001)}
    assert dict(left_out.select("year", "value").rows()) == pytest.approx(
        dict(own.rows())
    )
    assert eu_snap.quality(method=ONE_EU_AS_PROVIDER).eu_left_out.to_polars().is_empty()
    assert eu_snap.quality(providers=4).eu_left_out.to_polars().is_empty()

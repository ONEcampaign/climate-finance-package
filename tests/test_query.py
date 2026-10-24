"""`spending()` and `quality()` on the fixture release.

Golden tables live in tests/golden/; set CF_UPDATE_GOLDEN=1 to rewrite them after a
deliberate change, then review the diff.
"""

import inspect
import itertools
import math
import os
import tomllib
from pathlib import Path

import polars as pl
import pytest
from make_fixture import FIXTURE_DIR
from polars.testing import assert_frame_equal
from releases import copy_release

import climate_finance as cf
from climate_finance import convert as convert_module
from climate_finance.methods import OECD, ONE, RANGE, Method, MethodRange, label
from climate_finance.query import (
    BILATERAL,
    EIB_AGENCY,
    EIB_CHANNEL,
    EU_CHANNELS,
    EU_INSTITUTIONS,
    _aggregate,
)
from climate_finance.result import AMOUNTS, ROW_KEYS
from climate_finance.schema import ReleaseManifest
from climate_finance.snapshot import Snapshot
from climate_finance.spec import Spec
from climate_finance.sums import ordered_sum

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


def test_certain_route_share_is_certain_route_value_over_climate_value() -> None:
    table = SNAP.table("quality").collect()
    summary = SNAP.quality().summary()

    expected = table.group_by("provider_code").agg(
        share=pl.col("certain_route_usd").sum() / pl.col("climate_usd").sum()
    )
    joined = summary.join(expected, on="provider_code", validate="1:1")
    assert joined.height == table["provider_code"].n_unique()
    assert joined["certain_route_share"].to_list() == pytest.approx(
        joined["share"].to_list()
    )
    assert (joined["certain_route_share"] < joined["link_rate"]).any()


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


@pytest.fixture(scope="module")
def without_code_2(tmp_path_factory: pytest.TempPathFactory) -> Snapshot:
    """The fixture release without its Bi_Multi 2 commitments and their rows."""
    code_2 = SNAP.table("commitments").filter(pl.col("bi_multi") == 2).collect()
    assert code_2.height, "the fixture needs a Bi_Multi 2 commitment"
    keep = ~pl.col("row_id").is_in(code_2["row_id"].implode())
    release = copy_release(
        tmp_path_factory.mktemp("without_code_2") / "release",
        edits={
            "commitments": lambda frame: frame.filter(keep),
            "disbursements": lambda frame: frame.filter(keep),
        },
    )
    return cf.load(path=release)


@pytest.mark.parametrize("flow", FLOWS)
@pytest.mark.parametrize("view", VIEWS)
def test_code_2_rows_leave_every_total_unchanged(
    without_code_2: Snapshot, flow: str, view: str
) -> None:
    spec = Spec(method=cf.methods.RANGE, flow=flow, view=view)
    with_code_2 = SNAP.spending(spec=spec).table.to_polars()

    assert_frame_equal(
        with_code_2, without_code_2.spending(spec=spec).table.to_polars()
    )
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
        Spec(view="provider", providers=(5,), measure="share_of_total"),
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


def test_spending_defaults_match_the_spec_defaults() -> None:
    parameters = inspect.signature(Snapshot.spending).parameters
    fields = Spec.model_fields

    assert set(fields) <= set(parameters)
    assert {name: parameters[name].default for name in fields} == {
        name: field.default for name, field in fields.items()
    }


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


# Fixture inputs: Germany gives IDA (channel 44002) 60m in 2021. IDA's commitments,
# as fractions of its spending, are 0.06 principal adaptation, 0.2 significant
# adaptation, 0.1 principal mitigation, 0.2 significant mitigation and 0.1
# significant on both, so its ONE shares are adaptation 0.18, mitigation 0.22 and
# cross-cutting 0.04. Its OECD shares are 0.20, 0.25 and 0.05, each of adaptation
# and mitigation including the cross-cutting share.
ONE_IDA = (60e6 * 0.14, 60e6 * 0.18, 60e6 * 0.04, 60e6 * (0.18 + 0.22 - 0.04))
# Significant markers at 1.0: adaptation 0.06 + 0.2, mitigation 0.1 + 0.2, and 0.1
# cross-cutting.
SIGNIFICANT_IDA = (60e6 * 0.26, 60e6 * 0.30, 60e6 * 0.10, 60e6 * 0.66)


@pytest.mark.parametrize(
    ("method", "expected"),
    [
        # The highest-marker rule takes cross-cutting out of both objectives.
        (cf.methods.ONE, ONE_IDA),
        (
            cf.methods.OECD,
            (60e6 * 0.20, 60e6 * 0.25, 60e6 * 0.05, 60e6 * (0.20 + 0.25 - 0.05)),
        ),
        # The EU setting leaves the share source alone.
        (
            cf.methods.OECD.replace(eu="channel"),
            (60e6 * 0.20, 60e6 * 0.25, 60e6 * 0.05, 60e6 * (0.20 + 0.25 - 0.05)),
        ),
        # Agency shares follow the method's coefficients.
        (cf.methods.ONE.replace(significant=1.0), SIGNIFICANT_IDA),
        # Agencies take the method's significant coefficient, not Germany's 0.5.
        (cf.methods.PROVIDER, ONE_IDA),
        (cf.methods.PROVIDER.replace(significant=1.0), SIGNIFICANT_IDA),
        # Each marker counts: adaptation 0.06 + 0.4 x 0.3, mitigation 0.1 + 0.4 x
        # 0.3, overlap 0.4 x 0.1.
        (
            cf.methods.ONE.replace(highest_marker=False),
            (60e6 * 0.18, 60e6 * 0.22, 60e6 * 0.04, 60e6 * 0.36),
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
    # ONE and PROVIDER hold no AfDB share for 2020, so the range's low bound is 0.
    oecd = pl.col("method") == "OECD"
    table = result.table.to_polars().filter(oecd)
    assert table.select(columns).row(0) == (*expected[:5], 0.0, expected[6])
    # France's 2020 code 2 flows total 1.5 times its contribution.
    share = result.share_of_total().table.to_polars().filter(oecd)["value"].item()
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


def test_a_zero_central_allocation_keeps_its_upper_bound(tmp_path: Path) -> None:
    def zero_central(frame: pl.DataFrame) -> pl.DataFrame:
        record = pl.col("record_id") == "DEU-1-R0"
        return frame.with_columns(
            pl.when(record).then(0.0).otherwise(pl.col(name)).alias(name)
            for name in ("allocated_low_usd", "allocated_usd")
        )

    release = copy_release(tmp_path / "release", edits={"disbursements": zero_central})
    row = cf.load(path=release).spending().rows(record_id="DEU-1-R0")

    # DEU-1 is principal-marked, so the 13.3m upper allocation counts in full. The
    # record allocates nothing at the centre, so its allocation weight is unknown.
    assert row.select("value", "low", "high").row(0) == pytest.approx(
        (0.0, 0.0, 13.3e6)
    )
    assert row["allocation_weight"].to_list() == [None]


def test_a_range_bounds_every_key_under_every_method(tmp_path: Path) -> None:
    # Without ONE shares or spending for AfDB, France's attribution to it counts under
    # OECD only.
    def oecd_only_for_afdb(frame: pl.DataFrame) -> pl.DataFrame:
        return frame.filter(
            (pl.col("channel_code") != 46002) | (pl.col("source") == "oecd")
        )

    release = copy_release(
        tmp_path / "release",
        edits={
            "agency_shares": oecd_only_for_afdb,
            "agency_spending": lambda frame: frame.filter(
                pl.col("channel_code") != 46002
            ),
        },
    )
    result = cf.load(path=release).spending(method=RANGE, view="provider")
    table = result.table.to_polars()

    assert set(result.rows(origin="attribution", provider_code=4)["method"]) == {"OECD"}
    by_method = table.partition_by("method", as_dict=True, include_key=False)
    keys = [frame.select(ROW_KEYS).sort(pl.all()) for frame in by_method.values()]
    for other in keys[1:]:
        assert_frame_equal(keys[0], other)
    bounds = table.group_by("method").agg(pl.col("low").sum(), pl.col("high").sum())
    assert bounds["low"].n_unique() == bounds["high"].n_unique() == 1
    assert table.filter(pl.col("provider_code") == 4)["low"].min() == 0.0


def test_sums_do_not_depend_on_row_order() -> None:
    # Summed in the order given, 1e16 + 1 - 1e16 is 0 and 1e16 - 1e16 + 1 is 1.
    values = pl.Series([1e16, 1.0, -1e16])
    rows = pl.DataFrame(
        {
            "year": pl.Series([2021] * 3, dtype=pl.Int16),
            "provider_code": pl.Series([5] * 3, dtype=pl.Int32),
            "recipient_code": pl.Series([248] * 3, dtype=pl.Int32),
            "bi_multi": pl.Series([1] * 3, dtype=pl.Int8),
            "method": ["ONE"] * 3,
            **dict.fromkeys(AMOUNTS, values),
        }
    )

    def summed(order: list[int]) -> pl.DataFrame:
        return _aggregate(rows[order].lazy(), methods=["ONE"]).collect()

    first = summed([0, 1, 2])
    for order in ([0, 2, 1], [1, 0, 2], [2, 0, 1]):
        assert_frame_equal(first, summed(order), check_exact=True)


@pytest.mark.parametrize(
    "groups", [None, [1, 1], [1, 2]], ids=["select", "one_group", "several_groups"]
)
def test_an_empty_sum_is_positive_zero(groups: list[int] | None) -> None:
    frame = pl.LazyFrame({"group": groups or [1, 1], "value": [1.0, 2.0]})
    total = ordered_sum(pl.col("value").filter(pl.col("value") < 0))
    sums = frame.select(total) if groups is None else frame.group_by("group").agg(total)

    signs = [math.copysign(1.0, value) for value in sums.collect()["value"]]
    assert signs == [1.0] * len(signs)


def test_a_range_of_close_methods_counts_each_member_once() -> None:
    close = (ONE.replace(significant=0.5000001), ONE.replace(significant=0.5000002))
    spec = Spec(providers=(5,), recipients=(248,), years=(2022,))

    together = SNAP.spending(spec=spec.replace(method=MethodRange(methods=close)))

    values = together.table.to_polars().group_by("method").agg(pl.col("value").sum())
    assert values.height == 2
    for method in close:
        alone = SNAP.spending(spec=spec.replace(method=method)).table.to_polars()
        expected = alone["value"].sum()
        assert values.filter(pl.col("method") == label(method))["value"].item() == (
            pytest.approx(expected)
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


def test_agency_shares_stand_in_where_agency_spending_measures_none(
    tmp_path: Path,
) -> None:
    # AfDB (channel 46002) loses its measured 2022 share and gains a `one` share in
    # its place; IDA's `one` share in 2021 sits beside a measured one and is unused.
    def stand_in(frame: pl.DataFrame) -> pl.DataFrame:
        added = frame.filter(pl.col("year") == 2021).with_columns(
            source=pl.lit("one"),
            year=pl.when(pl.col("channel_code") == 46002).then(2022).otherwise(2021),
            share=pl.col("share") / 10,
        )
        return pl.concat([frame, added.cast(frame.schema)])

    release = copy_release(
        tmp_path / "release",
        edits={
            "agency_shares": stand_in,
            "agency_spending": lambda frame: frame.filter(
                (pl.col("channel_code") != 46002) | (pl.col("year") != 2022)
            ),
        },
    )
    rows = cf.load(path=release).spending(view="provider").rows(origin="attribution")
    totals = rows.group_by("channel_code", "year").agg(pl.col("value").sum())
    value = {(channel, year): total for channel, year, total in totals.iter_rows()}
    # France gives AfDB 25m a year: in 2021 at its components, 0.27 + 0.30 - 0.08;
    # in 2022 at its OECD shares over 10, 0.055. Germany and Norway give IDA 75m a
    # year at its measured climate share, 0.36.
    assert value == pytest.approx(
        {
            (46002, 2021): 25e6 * 0.49,
            (46002, 2022): 25e6 * 0.055,
            (44002, 2021): 75e6 * 0.36,
            (44002, 2022): 75e6 * 0.36,
        }
    )


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


@pytest.mark.parametrize(
    ("spec", "converted"),
    [({}, False), ({"currency": "EUR"}, True), ({"prices": 2024}, True)],
)
def test_converted_results_carry_conversion_provenance(
    monkeypatch: pytest.MonkeyPatch, spec: dict[str, object], converted: bool
) -> None:
    factors = pl.DataFrame(
        {"entity_code": [5, 20001], "year": [2022, 2022], "factor": [0.8, 0.5]},
        schema={"entity_code": pl.Int32, "year": pl.Int16, "factor": pl.Float64},
    )
    monkeypatch.setattr(convert_module, "dac_factors", lambda **_: factors)

    result = SNAP.spending(years=2022, **spec)

    assert (result.meta.conversion is not None) is converted
    if converted:
        assert result.meta.conversion.factors_sha256 in result.explain()
    else:
        assert "Conversion factors" not in result.explain()


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


def test_a_share_result_reruns_from_its_own_spec() -> None:
    shares = SNAP.spending(view="provider", providers=5, years=2021).share_of_total()

    assert shares.spec.measure == "share_of_total"
    assert_frame_equal(
        SNAP.spending(spec=shares.spec).table.to_polars(), shares.table.to_polars()
    )
    assert_frame_equal(
        shares.share_of_total().table.to_polars(), shares.table.to_polars()
    )


@pytest.mark.parametrize(
    ("measure", "expected", "absent"),
    [
        ("amount", "Amounts in USD at current prices.", "Values are shares"),
        ("share_of_total", "Values are shares of all CRS flows", "Amounts in USD"),
    ],
)
def test_explain_and_title_name_the_measure(
    measure: str, expected: str, absent: str
) -> None:
    result = SNAP.spending(providers=5, years=2021, measure=measure)

    assert expected in result.explain()
    assert absent not in result.explain()
    title = repr(result).splitlines()[0]
    assert ("share of total" in title) == (measure == "share_of_total")


def test_an_amount_spec_serialises_without_a_measure() -> None:
    assert "measure" not in Spec(flow="commitments").model_dump_json()
    assert "share_of_total" in Spec(measure="share_of_total").model_dump_json()


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
# France gives the EU budget (channel 42001) 30m and the EIB (42004) 10m a year in
# 2021 and 2022 in every flow. Their ONE climate shares are 0.30 and 0.20, and the
# EIB's OECD share is 0.40. EU Institutions' own commitments EU-1, EU-2 and EU-3 copy
# Norway's NOR-1 for the EU budget, the EDF and the EIB (agencies 1, 2 and 3):
# committed in 2021, disbursed in 2021, 2022 and 2023.
EU_CONTRIBUTION, EU_SHARE = 30e6, 0.30
EIB_CONTRIBUTION, EIB_ONE_SHARE, EIB_OECD_SHARE = 10e6, 0.20, 0.40
EU_ATTRIBUTION_YEARS = (2021, 2022)
EU_AGENCIES = {"EU-1": 1, "EU-2": 2, "EU-3": EIB_AGENCY}


def _with_eu_commitments(frame: pl.DataFrame) -> pl.DataFrame:
    norway = frame.filter(pl.col("row_id") == "NOR-1")
    copies = []
    for row_id, agency in EU_AGENCIES.items():
        eu = norway.with_columns(
            row_id=pl.lit(row_id),
            provider_code=pl.lit(EU_INSTITUTIONS, pl.Int32),
            agency_code=pl.lit(agency, pl.Int32),
        )
        if "record_id" in frame.columns:
            eu = eu.with_columns(pl.col("record_id").str.replace("NOR-1", row_id))
        copies.append(eu)
    return pl.concat([frame, *copies])


def _with_eu_contributions(frame: pl.DataFrame) -> pl.DataFrame:
    rows = pl.DataFrame(
        {
            "provider_code": 4,
            "channel_code": [
                channel
                for channel in (42001, EIB_CHANNEL)
                for _ in EU_ATTRIBUTION_YEARS
                for _ in FLOWS
            ],
            "year": [year for year in EU_ATTRIBUTION_YEARS for _ in FLOWS] * 2,
            "flow": list(FLOWS) * 4,
            "contribution_usd": [EU_CONTRIBUTION] * 6 + [EIB_CONTRIBUTION] * 6,
        }
    )
    return pl.concat([frame, rows.cast(frame.schema)])


def _with_eu_shares(frame: pl.DataFrame) -> pl.DataFrame:
    def shares(channel: int, source: str, split: list[float]) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "channel_code": channel,
                "year": [year for year in EU_ATTRIBUTION_YEARS for _ in range(3)],
                "objective": ["adaptation", "mitigation", "cross_cutting"] * 2,
                "share": split * 2,
                "source": source,
            }
        ).cast(frame.schema)

    return pl.concat(
        [
            frame,
            shares(42001, "one", [0.10, 0.25, 0.05]),
            # The OECD's share for the EU budget, which OECD ignores.
            shares(42001, "oecd", [0.10, 0.25, 0.05]),
            shares(EIB_CHANNEL, "one", [0.05, 0.15, 0.0]),
            shares(EIB_CHANNEL, "oecd", [0.10, 0.30, 0.0]),
        ]
    )


@pytest.fixture(scope="module")
def eu_snap(tmp_path_factory: pytest.TempPathFactory) -> Snapshot:
    release = copy_release(
        tmp_path_factory.mktemp("eu") / "release",
        edits={
            "commitments": _with_eu_commitments,
            "disbursements": _with_eu_commitments,
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
        row["channel_code"]: row["agency_codes"]
        for row in agencies
        if row["provider_code"] == EU_INSTITUTIONS
    }
    assert channels.keys() == set(EU_CHANNELS)
    assert channels[EIB_CHANNEL] == [EIB_AGENCY]


@pytest.mark.parametrize("flow", FLOWS)
@pytest.mark.parametrize(
    ("method", "eu_own_agencies", "eu_attributed"),
    [
        # As a channel, France's contributions carry the EU's shares and the EU's
        # own rows leave the all-provider total.
        (
            cf.methods.ONE,
            set(),
            EU_CONTRIBUTION * EU_SHARE + EIB_CONTRIBUTION * EIB_ONE_SHARE,
        ),
        # As a provider, the EU budget and the EDF keep their own rows and the EIB
        # is a channel.
        (ONE_EU_AS_PROVIDER, {1, 2}, EIB_CONTRIBUTION * EIB_ONE_SHARE),
        (cf.methods.OECD, {1, 2}, EIB_CONTRIBUTION * EIB_OECD_SHARE),
    ],
)
def test_an_all_provider_total_counts_eu_money_once(
    eu_snap: Snapshot,
    method: Method,
    flow: str,
    eu_own_agencies: set[int],
    eu_attributed: float,
) -> None:
    rows = eu_snap.spending(view="provider", method=method, flow=flow).rows()

    own = rows.filter(
        pl.col("provider_code") == EU_INSTITUTIONS,
        pl.col("year").is_in(EU_ATTRIBUTION_YEARS),
    )
    attributed = rows.filter(pl.col("channel_code").is_in(EU_CHANNELS))
    assert set(own["agency_code"]) == eu_own_agencies
    assert attributed.filter(pl.col("provider_code") == EU_INSTITUTIONS).is_empty()
    assert attributed["value"].sum() == pytest.approx(2 * eu_attributed)


@pytest.mark.parametrize("view", VIEWS)
@pytest.mark.parametrize("flow", FLOWS)
def test_eu_institutions_alone_return_their_own_spending(
    eu_snap: Snapshot, flow: str, view: str
) -> None:
    def eu_alone(method: Method) -> pl.DataFrame:
        result = eu_snap.spending(
            view=view, method=method, flow=flow, providers=EU_INSTITUTIONS
        )
        return result.rows().drop("method")

    as_channel = eu_alone(cf.methods.ONE)
    eib = pl.col("agency_code") == EIB_AGENCY
    assert as_channel.filter(eib).height
    # As a provider, the EU leaves out the EIB's own rows in the provider view.
    if view == "provider":
        as_channel = as_channel.filter(~eib)
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
    # The EU's commitments disburse in 2023, a year in which France attributes
    # nothing to the EU.
    def eu_rows(method: Method) -> pl.DataFrame:
        rows = eu_snap.spending(view="provider", method=method).rows(
            provider_code=EU_INSTITUTIONS
        )
        return rows.drop("method")

    as_channel = eu_rows(cf.methods.ONE)

    assert set(as_channel["year"]) == {2023}
    assert set(as_channel["agency_code"]) == set(EU_AGENCIES.values())
    assert_frame_equal(
        as_channel.filter(pl.col("agency_code") != EIB_AGENCY),
        eu_rows(ONE_EU_AS_PROVIDER).filter(pl.col("year") == 2023),
    )


def _unmeasured_deu_2(frame: pl.DataFrame) -> pl.DataFrame:
    # As normalise writes a commitment of a year the CRDF measures no adaptation in.
    unmeasured = pl.col("row_id") != "DEU-2"
    return frame.with_columns(
        pl.when(unmeasured).then(pl.col(name)).alias(name)
        for name in ("rio_adaptation", "adaptation_usd", "overlap_usd")
    )


def _unmeasured_deu_2_markers(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.with_columns(
        inherited_rio_adaptation=pl.when(pl.col("row_id") != "DEU-2").then(
            pl.col("inherited_rio_adaptation")
        )
    )


def _unmeasured_2022_spending(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.with_columns(adaptation_measured=pl.col("year") != 2022)


@pytest.mark.parametrize("flow", FLOWS)
def test_unmeasured_adaptation_stays_null_and_value_counts_mitigation(
    tmp_path: Path, flow: str
) -> None:
    release = copy_release(
        tmp_path / "release",
        edits={
            "commitments": _unmeasured_deu_2,
            "disbursements": _unmeasured_deu_2_markers,
            "agency_spending": _unmeasured_2022_spending,
        },
    )
    spec = Spec(providers=(5,), view="provider", flow=flow, method=ONE)
    result = cf.load(path=release).spending(spec=spec)
    original = SNAP.spending(spec=spec)

    # DEU-2's own rows and 2022's attribution; disbursements in 2022 of DEU-1, a
    # 2021 commitment, keep their adaptation.
    rows = result.rows(year=2022).filter(
        (pl.col("row_id") == "DEU-2") | (pl.col("origin") == "attribution")
    )
    assert set(rows["origin"]) == {"attribution"} | (
        {"commitment"} if flow == "commitments" else {"disbursement"}
    )
    assert rows["adaptation"].is_null().all()
    assert rows["cross_cutting"].is_null().all()
    assert rows["value"].to_list() == pytest.approx(rows["mitigation"].to_list())
    table = result.table.to_polars()
    # The table rows these rows reach, by recipient and Bi_Multi code.
    reached = table.join(
        rows.select("recipient_code", "bi_multi").unique(),
        on=["recipient_code", "bi_multi"],
        nulls_equal=True,
    ).filter(year=2022)
    assert reached.height == rows.select("recipient_code", "bi_multi").n_unique()
    assert reached.select("adaptation", "cross_cutting").null_count().row(0) == (
        reached.height,
        reached.height,
    )
    assert table.filter(year=2021).equals(original.table.to_polars().filter(year=2021))
    years = "2022" if flow == "commitments" else "2022 to 2024"
    assert f"not measured for rows in {years}," in result.explain()
    assert "not measured" not in original.explain()


_SPENDING_COVERS = (
    "Under {}, attributed finance covers core contributions in 2021 to 2022 and "
    "agency spending in 2022 to 2023; the provider view attributes none in "
)
_IMPUTED_COVERS = (
    "Under OECD, attributed finance covers core contributions in 2021 to 2022 and "
    "the OECD's imputed shares in 2020 to 2022; the provider view attributes none in "
)


@pytest.mark.parametrize(
    ("view", "method", "years", "named"),
    [
        (
            "provider",
            ONE,
            None,
            [_SPENDING_COVERS.format("ONE") + "2015 to 2021 and 2023 to 2024."],
        ),
        (
            "provider",
            OECD,
            None,
            [_IMPUTED_COVERS + "2015 to 2020 and 2023 to 2024."],
        ),
        (
            "provider",
            ONE,
            (2020, 2021),
            [_SPENDING_COVERS.format("ONE") + "2020 to 2021."],
        ),
        ("provider", OECD, (2021, 2022), []),
        (
            "provider",
            RANGE,
            (2020, 2021),
            [
                _SPENDING_COVERS.format("ONE and PROVIDER") + "2020 to 2021.",
                _IMPUTED_COVERS + "2020.",
            ],
        ),
        ("provider", RANGE, (2022,), []),
        ("recipient", ONE, None, []),
    ],
)
def test_explain_names_queried_years_without_attributed_finance(
    tmp_path: Path,
    view: str,
    method: Method | MethodRange,
    years: tuple[int, ...] | None,
    named: list[str],
) -> None:
    release = copy_release(tmp_path / "release")
    path = release / "release.json"
    manifest = ReleaseManifest.model_validate_json(path.read_text())
    # Each method's line names the years of the share table it reads.
    covered = {
        "crdf_recipient": (2015, 2024),
        "core_contributions": (2021, 2022),
        "agency_shares": (2020, 2022),
        "agency_spending": (2022, 2023),
    }
    path.write_text(manifest.model_copy(update={"years": covered}).model_dump_json())

    text = cf.load(path=release).spending(view=view, method=method, years=years)
    lines = [
        line
        for line in text.explain().splitlines()
        if "attributed finance covers" in line
    ]

    assert lines == named

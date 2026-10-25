"""`spending()` and `quality()` on the fixture release.

Golden tables live in tests/golden/; set CF_UPDATE_GOLDEN=1 to rewrite them after a
deliberate change, then review the diff.
"""

import itertools
import json
import math
import os
import tomllib
import warnings
from collections.abc import Callable
from importlib.resources import files
from pathlib import Path

import numpy as np
import polars as pl
import pyarrow as pa
import pytest
from make_fixture import DEU, FIXTURE_DIR, FRA, GEF, JPN, LEDGER_RECORD
from polars.testing import assert_frame_equal
from releases import copy_release

import climate_finance as cf
from climate_finance import convert as convert_module
from climate_finance.catalogue import Agency
from climate_finance.errors import (
    CoverageWarning,
    InvalidInputError,
    MissingLabelsError,
    UnknownRecipientError,
)
from climate_finance.methods import OECD, ONE, RANGE, Method, MethodRange, label
from climate_finance.query import (
    BILATERAL,
    EIB_AGENCY,
    EIB_CHANNEL,
    EU_BUDGET_AND_EDF,
    EU_CHANNELS,
    EU_INSTITUTIONS,
    _aggregate,
)
from climate_finance.release import Release
from climate_finance.result import AMOUNTS, ROW_KEYS
from climate_finance.schema import ReleaseManifest
from climate_finance.spec import Spec, SpecInputs
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
    _golden(name, SNAP.run(spec).table)


def _tables() -> list[tuple[object, str]]:
    quality = SNAP.quality()
    return [
        (SNAP.spending(), "table"),
        (SNAP.spending(), "modelled"),
        (quality, "table"),
        (quality, "untraced"),
        (SNAP.reconcile(), "table"),
        (cf.compare(SNAP.spending(), SNAP.spending(flow="commitments")), "table"),
    ]


@pytest.mark.parametrize(("owner", "name"), _tables())
def test_a_result_table_is_a_polars_frame_that_arrow_readers_accept(
    owner: object, name: str
) -> None:
    table = getattr(owner, name)

    assert isinstance(table, pl.DataFrame)
    assert pl.DataFrame(table).equals(table)
    assert pa.table(table).num_rows == len(table)
    assert list(table.columns) == pa.table(table).column_names


@pytest.mark.parametrize(("owner", "name"), _tables())
def test_a_result_table_runs_its_query_once(
    owner: object, name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    collect = pl.LazyFrame.collect
    scans = []

    def counting(self: pl.LazyFrame) -> pl.DataFrame:
        scans.append(1)
        return collect(self)

    monkeypatch.setattr(pl.LazyFrame, "collect", counting)
    first = getattr(owner, name)
    scans_for_first = len(scans)
    second = getattr(owner, name)

    assert second is first
    assert scans_for_first
    assert len(scans) == scans_for_first


@pytest.mark.parametrize("spec", [*GOLDEN_SPECS.values(), CLIMATE_ONLY_SPEC])
def test_value_is_the_objectives_plus_unsplit(spec: Spec) -> None:
    result = SNAP.run(spec)
    # Under the highest-marker rule the objectives are disjoint; otherwise adaptation
    # and mitigation each include cross_cutting.
    sign = pl.col("method").replace_strict(
        {label(method): 1 if method.highest_marker else -1 for method in spec.methods}
    )

    for frame in (result.rows(), result.table):
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

    for frame in (result.rows(), result.table):
        known = frame.drop_nulls("amount")
        assert known.height
        assert (known["value"] <= known["amount"] * (1 + 1e-12)).all()
        assert (known["value"] >= 0).all()
        assert (known["low"] <= known["value"]).all()
        assert (known["value"] <= known["high"]).all()


@pytest.fixture(scope="module")
def without_code_2(tmp_path_factory: pytest.TempPathFactory) -> Release:
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
    without_code_2: Release, flow: str, view: str
) -> None:
    spec = Spec(method=cf.methods.RANGE, flow=flow, view=view)
    with_code_2 = SNAP.run(spec).table

    assert_frame_equal(with_code_2, without_code_2.run(spec).table)
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
        Spec(providers=(5, Agency(provider=918, agency=3)), recipients=(248,)),
    ],
)
def test_a_spec_round_trips_through_json_and_reruns_identically(spec: Spec) -> None:
    first = SNAP.run(spec)

    restored = Spec.model_validate_json(first.spec.model_dump_json())
    rerun = SNAP.run(restored)

    assert restored == spec
    assert_frame_equal(first.table, rerun.table)
    assert_frame_equal(first.rows(), rerun.rows())


def test_a_spec_holds_sorted_codes_and_agencies_whatever_names_them() -> None:
    eib = Agency(provider=918, agency=3)
    spec = Spec(providers=["EIB", "Japan", eib, 701], recipients=["KEN", 248])

    assert Spec(providers=["DEU", 5]) == Spec(providers=[5])
    assert SNAP.spending(providers="Japan").spec == SNAP.spending(providers=701).spec
    assert (spec.providers, spec.recipients) == ((701, eib), (248,))
    assert json.loads(spec.model_dump_json())["providers"] == [
        701,
        {"provider": 918, "agency": 3},
    ]
    # A provider covers its agencies.
    assert Spec(providers=["EU Institutions", "EIB"]).providers == (EU_INSTITUTIONS,)


def test_keyword_arguments_build_the_same_spec() -> None:
    result = SNAP.spending(providers=5, years=range(2021, 2023), view="provider")

    assert result.spec == Spec(providers=(5,), years=(2021, 2022), view="provider")


def test_spec_inputs_name_every_spec_field() -> None:
    assert set(SpecInputs.__annotations__) == set(Spec.model_fields)


@pytest.mark.parametrize(
    ("years", "expected"),
    [
        (2022, (2022,)),
        (np.int64(2022), (2022,)),
        (range(2021, 2023), (2021, 2022)),
        ([2023, np.int32(2021), 2023], (2021, 2023)),
    ],
)
def test_years_hold_sorted_distinct_ints(years: object, expected: tuple) -> None:
    assert Spec.model_validate({"years": years}).years == expected


@pytest.mark.parametrize("years", ["2022", ["2022"], True, 2022.0])
def test_years_refuse_anything_but_ints(years: object) -> None:
    with pytest.raises(TypeError, match="years takes an int or ints"):
        Spec.model_validate({"years": years})


@pytest.mark.parametrize(
    ("method", "expected"),
    [
        ("oecd", OECD),
        (" Range ", RANGE),
        (["one", OECD], MethodRange(methods=(ONE, OECD))),
    ],
)
def test_a_method_reads_from_its_name_ignoring_case(
    method: object, expected: Method | MethodRange
) -> None:
    spec = Spec.model_validate({"method": method})

    assert spec.method == expected
    assert Spec.model_validate_json(spec.model_dump_json()) == spec


def test_an_unknown_method_name_suggests_close_ones() -> None:
    with pytest.raises(cf.UnknownNameError) as raised:
        SNAP.spending(method="OECDD")

    assert raised.value.suggestions == ("OECD",)


@pytest.mark.parametrize(
    ("field", "value"),
    [("method", 1), ("method", ["RANGE"]), ("prices", "constant"), ("prices", True)],
)
def test_a_method_or_prices_of_another_form_raises_one_type_error(
    field: str, value: object
) -> None:
    with pytest.raises(TypeError, match=f"^{field} takes"):
        Spec.model_validate({field: value})


def test_spending_takes_keywords_only_and_run_reruns_a_spec() -> None:
    spec = Spec(providers="Japan")

    with pytest.raises(InvalidInputError, match=r"release\.run\(spec\)"):
        SNAP.spending(spec=spec, providers="France")
    with pytest.raises(TypeError):
        SNAP.spending(spec)  # ty: ignore[too-many-positional-arguments]
    assert_frame_equal(
        SNAP.run(spec).table,
        SNAP.spending(providers="Japan").table,
    )


@pytest.mark.parametrize(
    ("inputs", "message"),
    [
        ({"provider": "Japan"}, "no keyword 'provider'; did you mean 'providers'?"),
        ({"flow": "commitment"}, "flow takes 'commitments', 'disbursements' or"),
        ({"years": []}, "years takes at least one year"),
        ({"oda_only": "maybe"}, "oda_only: input should be a valid boolean"),
    ],
)
def test_spec_restates_every_refusal_as_one_package_error(
    inputs: dict, message: str
) -> None:
    with pytest.raises(InvalidInputError, match=message):
        Spec.model_validate(inputs)
    with pytest.raises(InvalidInputError, match=message):
        SNAP.spending(**inputs)


COVERED = {
    "spending": lambda: SNAP.spending(years=range(2015, 2023)),
    "commitments": lambda: SNAP.spending(flow="commitments", years=[2022, 2023]),
    "quality": lambda: SNAP.quality(years=[2019, 2022, 2023]),
    "reconcile": lambda: SNAP.reconcile(years=2030),
}


@pytest.mark.parametrize(
    ("call", "message"),
    [
        (
            COVERED["spending"],
            "holds disbursements for 2021 to 2024; years 2015 to 2020 return no rows",
        ),
        (
            COVERED["commitments"],
            "holds commitments for 2021 to 2022; year 2023 returns no rows",
        ),
        (
            COVERED["quality"],
            "quality statistics for 2021 to 2022; years 2019 and 2023 return",
        ),
        (COVERED["reconcile"], "BTR totals for 2021 to 2022; year 2030 returns"),
    ],
    ids=list(COVERED),
)
def test_years_a_table_lacks_warn_once_at_the_caller(
    call: Callable[[], object], message: str
) -> None:
    with pytest.warns(CoverageWarning, match=message) as record:
        call()

    assert [warning.filename for warning in record] == [__file__]


@pytest.mark.parametrize(
    "call",
    [
        lambda: SNAP.spending(years=range(2021, 2025)),
        lambda: SNAP.spending(view="provider", years=[2021, 2024]),
        lambda: SNAP.spending(),
        lambda: SNAP.quality(years=2022),
        lambda: SNAP.reconcile(years=[2021, 2022]),
    ],
)
def test_covered_years_warn_nothing(call: Callable[[], object]) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error", CoverageWarning)
        call()


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
    result = cf.load(path=release).run(spec).table

    assert_frame_equal(result, SNAP.run(spec).table)


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
    rows = result.rows(origin="attribution")

    assert rows.height == 1
    assert rows.select("adaptation", "mitigation", "cross_cutting", "value").row(
        0
    ) == pytest.approx(expected)
    # The shares take no coefficient of Germany's, so the row names none.
    assert rows.select("coefficient_basis", "coefficient_source").row(0) == (
        None,
        None,
    )


def test_a_climate_only_share_attributes_value_as_unsplit() -> None:
    # France gives AfDB (channel 46002) 25m in 2020. AfDB's 2020 OECD share is 0.35
    # in total, with no split by objective, and it has no ONE share that year.
    result = SNAP.run(CLIMATE_ONLY_SPEC)
    columns = (
        "adaptation", "mitigation", "cross_cutting", "unsplit", "value", "low", "high",
    )  # fmt: skip
    expected = (0.0, 0.0, 0.0, *[pytest.approx(25e6 * 0.35)] * 4)

    rows = result.rows(origin="attribution")
    assert rows["method"].to_list() == ["OECD"]
    assert rows.select(columns).row(0) == expected
    # ONE and PROVIDER hold no AfDB share for 2020, so the range's low bound is 0.
    oecd = pl.col("method") == "OECD"
    table = result.table.filter(oecd)
    assert table.select(columns).row(0) == (*expected[:5], 0.0, expected[6])
    # France's 2020 code 2 flows total 1.5 times its contribution.
    share = result.share_of_total().table.filter(oecd)["value"].item()
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
    # JPN-3, 8m excess, which is untraced. Each part takes its pro-rata grant
    # equivalent.
    by_commitment = dict(zip(rows["row_id"], rows["amount"], strict=True))
    assert by_commitment == pytest.approx(
        {
            "JPN-1": 18e6 * 12e6 / 30e6,
            "JPN-3": 18e6 * 10e6 / 30e6,
            None: 18e6 * 8e6 / 30e6,
        }
    )
    # Both commitments and the record are principal mitigation, so the record's
    # whole grant equivalent counts once.
    assert rows["value"].sum() == pytest.approx(18e6)


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
    table = result.table

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

    together = SNAP.run(spec.replace(method=MethodRange(methods=close)))

    values = together.table.group_by("method").agg(pl.col("value").sum())
    assert values.height == 2
    for method in close:
        alone = SNAP.run(spec.replace(method=method)).table
        expected = alone["value"].sum()
        assert values.filter(pl.col("method") == label(method))["value"].item() == (
            pytest.approx(expected)
        )


def test_the_recipient_view_counts_outflows_and_no_attribution() -> None:
    rows = SNAP.spending().rows()

    assert set(rows["origin"]) == {"disbursement", "untraced"}
    assert 4 in set(rows["bi_multi"])
    assert set(rows["bi_multi"]) <= {*BILATERAL, 4}


def test_oecd_attribution_uses_the_imputed_shares() -> None:
    def attributed(method: Method) -> float:
        rows = SNAP.spending(view="provider", method=method).rows(origin="attribution")
        return rows["value"].sum()

    assert attributed(cf.methods.OECD) > attributed(cf.methods.ONE)


def _hold_afdb_2022(frame: pl.DataFrame) -> pl.DataFrame:
    # AfDB's 2022 spending repeats an earlier year's.
    key = (pl.col("channel_code") == 46002) & (pl.col("year") == 2022)
    return frame.with_columns(
        share_source=pl.when(key).then(pl.lit("held")).otherwise("share_source")
    )


def test_attributed_value_shows_and_sums_by_share_source(tmp_path: Path) -> None:
    release = cf.load(
        path=copy_release(
            tmp_path / "release", edits={"agency_spending": _hold_afdb_2022}
        )
    )
    rows = release.spending(view="provider").rows(origin="attribution")
    by_source = rows.group_by("provider_code", "year", "share_source").agg(
        pl.col("value").sum()
    )

    attribution = release.quality().attribution.filter(flow="disbursements")
    columns = attribution.unpivot(
        index=["provider_code", "year"],
        on=["computed_usd", "held_usd", "oecd_usd"],
        variable_name="share_source",
    ).filter(pl.col("value") != 0)

    assert set(rows["share_source"]) == {"computed", "held"}
    assert sorted(
        columns.with_columns(pl.col("share_source").str.strip_suffix("_usd")).rows()
    ) == pytest.approx(sorted(by_source.select(columns.columns).rows()))
    oecd_rows = release.spending(view="provider", method=OECD).rows(
        origin="attribution"
    )
    assert set(oecd_rows["share_source"]) == {"oecd"}


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
    assert result.modelled.is_empty()


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


# Each name column with the listing that names it, keyed as the tables key it.
LISTINGS = {
    "provider": cf.providers.list().select(provider_code="code", provider="name"),
    "recipient": cf.recipients.list().select(recipient_code="code", recipient="name"),
    "agency": pl.read_csv(
        (files("climate_finance") / "data" / "agencies.csv").read_bytes(),
        schema_overrides={"code": pl.Int32, "agency": pl.Int32},
    ).select(provider_code="code", agency_code="agency", agency="name"),
}


def _named_tables() -> list[tuple[str, pl.DataFrame, tuple[str, ...]]]:
    quality = SNAP.quality()
    provider = SNAP.spending(view="provider")
    return [
        ("table", SNAP.spending().table, ("provider", "recipient")),
        ("rows", provider.rows(), ("provider", "agency", "recipient")),
        ("modelled", SNAP.spending().modelled, ("provider", "recipient")),
        ("quality", quality.table, ("provider",)),
        ("status", quality.status, ("provider",)),
        ("attribution", quality.attribution, ("provider",)),
        ("untraced", quality.untraced, ("provider",)),
        ("summary", quality.summary(), ("provider",)),
        ("reconcile", SNAP.reconcile().table, ("provider",)),
        (
            "compare",
            cf.compare(provider, SNAP.spending()).table,
            ("provider", "recipient"),
        ),
    ]


@pytest.mark.parametrize(
    ("frame", "names"),
    [(frame, names) for _, frame, names in _named_tables()],
    ids=[name for name, _, _ in _named_tables()],
)
def test_names_sit_before_their_codes_and_match_the_listings(
    frame: pl.DataFrame, names: tuple[str, ...]
) -> None:
    for name in names:
        listing = LISTINGS[name]
        keys = [column for column in listing.columns if column != name]
        expected = frame.select(keys).join(
            listing, on=keys, how="left", maintain_order="left"
        )[name]

        assert frame.columns.index(name) < frame.columns.index(keys[-1])
        assert frame[name].drop_nulls().len() > 0
        assert frame[name].equals(expected)


def test_a_result_table_prints_names_first_and_bounds_last() -> None:
    result = SNAP.spending()
    rows = result.rows()

    assert result.table.columns[:4] == ["year", "provider", "recipient", "method"]
    assert result.table.columns[-4:] == ["value", "low", "high", "untraced"]
    assert rows.columns[:4] == ["origin", "year", "record_id", "link_method"]
    assert rows.columns[-4:] == ["value", "low", "high", "untraced"]
    assert set(rows.columns) == set(result._rows.collect_schema().names())


@pytest.mark.parametrize("kenya", ["kenya", "KEN", 248])
def test_rows_take_a_recipient_in_any_form(kenya: str | int) -> None:
    result = SNAP.spending()

    assert result.rows(recipient=kenya).equals(result.rows(recipient_code=248))
    assert result.rows(recipient=kenya).height > 0
    assert set(result.rows(recipient=["kenya", "Bangladesh"])["recipient"]) == {
        "Kenya",
        "Bangladesh",
    }


def test_an_unknown_recipient_in_rows_raises_with_suggestions() -> None:
    with pytest.raises(UnknownRecipientError) as error:
        SNAP.spending().rows(recipient="kenyaa")

    assert "Kenya" in error.value.suggestions


def test_rows_filter_by_equality_and_membership() -> None:
    result = SNAP.spending()

    assert set(result.rows(year=2022)["year"]) == {2022}
    assert set(result.rows(provider_code=[5, 8])["provider_code"]) == {5, 8}
    assert set(result.rows(provider=["Germany", 8])["provider_code"]) == {5, 8}
    with pytest.raises(cf.UnknownNameError) as raised:
        result.rows(yeer=2022)
    assert raised.value.suggestions[0] == "year"


@pytest.mark.parametrize(
    ("filters", "message"),
    [
        ({"provider_code": "Japan"}, "DAC codes, not 'Japan'; .* provider='Japan'"),
        ({"recipient_code": ["Kenya"]}, "filter names with recipient='Kenya'"),
        ({"agency_code": "KfW"}, "agency codes, not 'KfW'; .* provider='KfW'"),
        ({"agency": "EIB"}, "through provider, as provider='EIB'"),
        ({"year": [2022, "2023"]}, "year holds integers, not '2023'"),
        ({"origin": 1}, "origin holds text, not 1"),
    ],
)
def test_rows_refuse_a_filter_its_column_cannot_hold(
    filters: dict, message: str
) -> None:
    with pytest.raises(InvalidInputError, match=message):
        SNAP.spending().rows(**filters)


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
            .filter(~pl.col("record_id").is_in(["JPN-2-R1", "DEU-UNLINKED"]))
            .drop("method")
        )

    assert_frame_equal(unlabelled(JEV_ONE), unlabelled(ONE))


def test_explain_says_what_jev_relevance_replaces() -> None:
    text = SNAP.spending(method=JEV_ONE).explain()

    assert "read Jev's adaptation level (none, significant or principal)" in text
    assert "Jev labels only records whose commitment is marked adaptation" in text


def _without_jev_labels(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.filter(pl.col("label_source") != "jev")


def test_jev_relevance_refuses_a_release_without_jev_labels(tmp_path: Path) -> None:
    release = copy_release(
        tmp_path / "release", edits={"objectives": _without_jev_labels}
    )

    with pytest.raises(MissingLabelsError, match="jev_labels"):
        cf.load(path=release).spending(method=JEV_ONE)


def test_the_modelled_line_stays_out_of_traced_totals() -> None:
    result = SNAP.spending()

    modelled = result.modelled
    assert set(modelled["provider_code"]) == {903}
    assert 903 not in set(result.table["provider_code"])


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

    shares = result.share_of_total().table

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
    assert_frame_equal(shares.drop(AMOUNTS), result.table.drop(AMOUNTS))


def test_a_share_result_reruns_from_its_own_spec() -> None:
    shares = SNAP.spending(view="provider", providers=5, years=2021).share_of_total()

    assert shares.spec.measure == "share_of_total"
    assert_frame_equal(SNAP.run(shares.spec).table, shares.table)
    assert_frame_equal(shares.share_of_total().table, shares.table)


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


def test_the_title_names_providers_agencies_recipients_and_year_runs() -> None:
    result = SNAP.spending(
        providers=["EIB", 701], recipients="KEN", years=[2015, 2021, 2022, 2023]
    )

    assert (
        "providers Japan, European Investment Bank (EU Institutions); recipients Kenya;"
        " years 2015, 2021 to 2023"
    ) in repr(result).splitlines()[0]


@pytest.mark.parametrize(
    ("field", "value"), [("measure", "share_of_total"), ("untraced", False)]
)
def test_a_spec_serialises_only_the_later_fields_it_changes(
    field: str, value: object
) -> None:
    assert f'"{field}"' not in Spec(flow="commitments").model_dump_json()
    assert f'"{field}"' in Spec(**{field: value}).model_dump_json()


SURVEY = "Fixture survey, p. 8"


def _ratio(release: Release, method: Method, row_id: str) -> tuple[float, str]:
    """The share of `row_id`'s commitment that counts, and its coefficient basis."""
    rows = release.spending(method=method, flow="commitments").rows(row_id=row_id)
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
    path = copy_release(
        tmp_path / "release", edits={"objectives": _jev_rules_out_adaptation}
    )
    release = cf.load(path=path)

    def ratio(method: Method) -> float:
        row = release.spending(method=method).rows(record_id="DEU-2-R0")
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

    shares = cf.load(path=release).run(spec).share_of_total()

    expected = SNAP.run(spec).share_of_total().table
    assert expected.height
    assert_frame_equal(shares.table, expected)


ONE_EU_AS_PROVIDER = cf.methods.ONE.replace(eu="provider")
# France gives the EU budget (channel 42001) and the EDF (42003) 30m each and the EIB
# (42004) 10m a year in 2021 and 2022 in every flow. Their ONE climate shares are
# 0.30, 0.30 and 0.20, and the EIB's OECD share is 0.40. EU Institutions' own
# commitments EU-1, EU-2 and EU-3 copy Norway's NOR-1 for the EU budget, the EDF and
# the EIB (agencies 1, 2 and 3):
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
                for channel in (*EU_BUDGET_AND_EDF, EIB_CHANNEL)
                for _ in EU_ATTRIBUTION_YEARS
                for _ in FLOWS
            ],
            "year": [year for year in EU_ATTRIBUTION_YEARS for _ in FLOWS] * 3,
            "flow": list(FLOWS) * 6,
            "contribution_usd": [EU_CONTRIBUTION] * 12 + [EIB_CONTRIBUTION] * 6,
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
            *(
                shares(channel, "one", [0.10, 0.25, 0.05])
                for channel in EU_BUDGET_AND_EDF
            ),
            # The OECD's shares for the EU budget and the EDF, which OECD ignores.
            *(
                shares(channel, "oecd", [0.10, 0.25, 0.05])
                for channel in EU_BUDGET_AND_EDF
            ),
            shares(EIB_CHANNEL, "one", [0.05, 0.15, 0.0]),
            shares(EIB_CHANNEL, "oecd", [0.10, 0.30, 0.0]),
        ]
    )


# Each EU agency's CRS flows to NOR-1's recipient in every year it disburses, under
# NOR-1's Bi_Multi codes: 100m for the EU budget, 200m for the EDF, 300m for the EIB.
EU_FLOW_USD = 100e6


def _with_eu_total_flows(frame: pl.DataFrame) -> pl.DataFrame:
    rows = pl.DataFrame(
        [
            {
                "provider_code": EU_INSTITUTIONS,
                "agency_code": agency,
                "recipient_code": 666,
                "year": year,
                "bi_multi": 1 if flow == "commitments" else 3,
                "flow": flow,
                "rio_adaptation": 0,
                "rio_mitigation": 0,
                "amount_usd": agency * EU_FLOW_USD,
            }
            for agency in EU_AGENCIES.values()
            for year in (2021, 2022, 2023)
            for flow in FLOWS
        ]
    ).select(frame.columns)
    return pl.concat([frame, rows.cast(frame.schema)])


@pytest.fixture(scope="module")
def eu_release(tmp_path_factory: pytest.TempPathFactory) -> Release:
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
            len(EU_BUDGET_AND_EDF) * EU_CONTRIBUTION * EU_SHARE
            + EIB_CONTRIBUTION * EIB_ONE_SHARE,
        ),
        # As a provider, the EU budget and the EDF keep their own rows and the EIB
        # is a channel.
        (ONE_EU_AS_PROVIDER, {1, 2}, EIB_CONTRIBUTION * EIB_ONE_SHARE),
        (cf.methods.OECD, {1, 2}, EIB_CONTRIBUTION * EIB_OECD_SHARE),
    ],
)
def test_an_all_provider_total_counts_eu_money_once(
    eu_release: Release,
    method: Method,
    flow: str,
    eu_own_agencies: set[int],
    eu_attributed: float,
) -> None:
    rows = eu_release.spending(view="provider", method=method, flow=flow).rows()

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
    eu_release: Release, flow: str, view: str
) -> None:
    def eu_alone(method: Method) -> pl.DataFrame:
        result = eu_release.spending(
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


@pytest.mark.parametrize("view", VIEWS)
@pytest.mark.parametrize("flow", FLOWS)
def test_an_agency_returns_its_own_rows_and_no_modelled_line(
    eu_release: Release, flow: str, view: str
) -> None:
    eib = eu_release.spending(view=view, flow=flow, providers="EIB")
    eu = eu_release.spending(view=view, flow=flow, providers=EU_INSTITUTIONS)

    rows = eib.rows()
    assert rows.height
    assert set(rows.select("provider_code", "agency_code").rows()) == {
        (EU_INSTITUTIONS, EIB_AGENCY)
    }
    assert_frame_equal(rows, eu.rows(provider="EIB"))
    assert_frame_equal(rows, eu.rows(provider=Agency(provider=918, agency=3)))
    assert eib.modelled.is_empty()


def test_under_oecd_the_provider_view_holds_no_eib_rows(eu_release: Release) -> None:
    """OECD counts EU Institutions as a provider, whose provider view leaves the
    EIB's own rows out and attributes members' contributions to it."""
    result = eu_release.spending(view="provider", method=OECD, providers="EIB")

    assert result.rows().is_empty()
    assert result.table.is_empty()


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
    eu_release: Release, providers: tuple[int, ...] | None, eu_own_rows: bool
) -> None:
    rows = eu_release.spending(
        view="provider", providers=providers, years=EU_ATTRIBUTION_YEARS
    ).rows(provider_code=EU_INSTITUTIONS)

    assert (rows.height > 0) == eu_own_rows


# Germany gives each EU channel three times what France gives, so France holds a
# quarter of every EU channel's contributions.
FRANCE_EU_SHARE = 0.25


def _with_two_eu_members(frame: pl.DataFrame) -> pl.DataFrame:
    france = _with_eu_contributions(frame.clear())
    germany = france.with_columns(
        provider_code=pl.lit(5, pl.Int32),
        contribution_usd=pl.col("contribution_usd") * 3,
    )
    return pl.concat([frame, france, germany])


@pytest.fixture(scope="module")
def two_member_release(tmp_path_factory: pytest.TempPathFactory) -> Release:
    release = copy_release(
        tmp_path_factory.mktemp("eu-members") / "release",
        edits={
            "commitments": _with_eu_commitments,
            "disbursements": _with_eu_commitments,
            "core_contributions": _with_two_eu_members,
            "agency_shares": _with_eu_shares,
            "total_flows": _with_eu_total_flows,
        },
    )
    return cf.load(path=release)


@pytest.mark.parametrize("flow", FLOWS)
def test_eu_own_rows_count_less_the_contributions_of_queried_members(
    two_member_release: Release, flow: str
) -> None:
    def value(providers: int | tuple[int, ...] | None, provider: int) -> float:
        result = two_member_release.spending(
            view="provider", flow=flow, providers=providers, years=EU_ATTRIBUTION_YEARS
        )
        return result.rows(provider_code=provider)["value"].sum()

    eu_alone = value(EU_INSTITUTIONS, EU_INSTITUTIONS)

    assert eu_alone > 0
    assert value((EU_INSTITUTIONS, 4), EU_INSTITUTIONS) == pytest.approx(
        eu_alone * (1 - FRANCE_EU_SHARE)
    )
    assert value((EU_INSTITUTIONS, 4), 4) == pytest.approx(value(4, 4))
    assert value(None, EU_INSTITUTIONS) == 0


# Untraced records of the EU budget (agency 1) and of the EIB (agency 3) in 2022,
# 10m each and principal for mitigation.
EU_UNTRACED_USD = 10e6


def _with_eu_untraced(frame: pl.DataFrame) -> pl.DataFrame:
    rows = pl.DataFrame(
        [
            {
                "record_id": f"EU-UNTRACED-{agency}",
                "year": 2022,
                "provider_code": EU_INSTITUTIONS,
                "agency_code": agency,
                "recipient_code": 666,
                "bi_multi": 1,
                "rio_adaptation": 0,
                "rio_mitigation": 2,
                "untraced_usd": EU_UNTRACED_USD,
                "reason": "unlinked",
                "record_disbursement_usd": EU_UNTRACED_USD,
            }
            for agency in (1, EIB_AGENCY)
        ],
        schema_overrides=frame.schema,
    )
    return pl.concat([frame, rows], how="diagonal").select(frame.columns)


@pytest.fixture(scope="module")
def eu_untraced_release(tmp_path_factory: pytest.TempPathFactory) -> Release:
    release = copy_release(
        tmp_path_factory.mktemp("eu-untraced") / "release",
        edits={
            "core_contributions": _with_two_eu_members,
            "agency_shares": _with_eu_shares,
            "untraced": _with_eu_untraced,
        },
    )
    return cf.load(path=release)


@pytest.mark.parametrize(
    ("method", "providers", "kept"),
    [
        # France funds a quarter of each EU channel, so a query naming it keeps three
        # quarters of the EU's own untraced rows, and one naming every member none.
        (cf.methods.ONE, (EU_INSTITUTIONS,), {1: 1.0, EIB_AGENCY: 1.0}),
        (cf.methods.ONE, (EU_INSTITUTIONS, 4), {1: 0.75, EIB_AGENCY: 0.75}),
        (cf.methods.ONE, None, {}),
        # As a provider, the EU keeps the EU budget's rows whole and drops the EIB's.
        (ONE_EU_AS_PROVIDER, (EU_INSTITUTIONS, 4), {1: 1.0}),
    ],
)
def test_eu_untraced_rows_count_once_as_the_eus_own_rows_do(
    eu_untraced_release: Release,
    method: Method,
    providers: tuple[int, ...] | None,
    kept: dict[int, float],
) -> None:
    rows = eu_untraced_release.spending(
        view="provider", method=method, providers=providers, years=(2022,)
    ).rows(origin="untraced", provider_code=EU_INSTITUTIONS)

    assert dict(rows.select("agency_code", "value").rows()) == pytest.approx(
        {agency: share * EU_UNTRACED_USD for agency, share in kept.items()}
    )
    assert rows["high"].to_list() == pytest.approx(rows["value"].to_list())
    assert set(rows["low"]) <= {0.0}


# NOR-1's records carry no grant equivalent, so the EU rows have none to divide.
@pytest.mark.parametrize("flow", ["commitments", "disbursements"])
@pytest.mark.parametrize("providers", [(EU_INSTITUTIONS,), (EU_INSTITUTIONS, 4)])
def test_eu_shares_of_total_divide_by_the_rows_each_method_counts(
    two_member_release: Release, flow: str, providers: tuple[int, ...]
) -> None:
    result = two_member_release.spending(
        view="provider", method=RANGE, flow=flow, providers=providers
    )
    eu = pl.col("provider_code") == EU_INSTITUTIONS
    amounts = result.table.filter(eu)
    shares = result.share_of_total().table.filter(eu)

    # ONE and PROVIDER count the EU as a channel: all three agencies' flows, scaled
    # as the EU's own rows are by the part France's contributions do not fund. OECD
    # counts the EU as a provider and leaves the EIB's own rows and flows out.
    def total(method: str, year: int) -> float:
        if method == "OECD":
            return 3 * EU_FLOW_USD
        named = 4 in providers and year in EU_ATTRIBUTION_YEARS
        return 6 * EU_FLOW_USD * (1 - FRANCE_EU_SHARE if named else 1)

    assert set(amounts["method"]) == {"ONE", "OECD", "PROVIDER"}
    assert amounts["value"].min() > 0
    assert shares["value"].to_list() == pytest.approx(
        [
            value / total(method, year)
            for method, year, value in amounts.select(
                "method", "year", "value"
            ).iter_rows()
        ]
    )


def test_range_share_bounds_span_each_methods_own_share(
    two_member_release: Release,
) -> None:
    spec = Spec(
        view="provider",
        flow="disbursements",
        providers=(EU_INSTITUTIONS, 4),
        measure="share_of_total",
    )
    eu = pl.col("provider_code") == EU_INSTITUTIONS
    shares = two_member_release.run(spec.replace(method=RANGE)).table.filter(eu)
    alone = pl.concat(
        two_member_release.run(spec.replace(method=method)).table.filter(eu)
        for method in RANGE.methods
    )
    expected = alone.group_by(ROW_KEYS).agg(pl.col("low").min(), pl.col("high").max())

    assert shares.height == 3 * expected.height
    joined = shares.join(expected, on=ROW_KEYS, suffix="_expected")
    assert joined["low"].to_list() == pytest.approx(joined["low_expected"].to_list())
    assert joined["high"].to_list() == pytest.approx(joined["high_expected"].to_list())


def test_a_member_counts_its_eu_share_only_with_the_eu_as_a_channel(
    eu_release: Release,
) -> None:
    def france(method: Method) -> float:
        result = eu_release.spending(
            view="provider", method=method, providers=4, years=2021
        )
        return result.rows(origin="attribution")["value"].sum()

    assert france(cf.methods.ONE) - france(ONE_EU_AS_PROVIDER) == pytest.approx(
        len(EU_BUDGET_AND_EDF) * EU_CONTRIBUTION * EU_SHARE
    )


def test_eu_own_rows_stay_in_a_year_without_eu_attribution(eu_release: Release) -> None:
    # The EU's commitments disburse in 2023, a year in which France attributes
    # nothing to the EU.
    def eu_rows(method: Method) -> pl.DataFrame:
        rows = eu_release.spending(view="provider", method=method).rows(
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


def _without_a_2022_budget_share(frame: pl.DataFrame) -> pl.DataFrame:
    # The EIB keeps its share in 2021 and 2022; the EU budget's 2022 share is
    # missing.
    return _with_eu_shares(frame).filter(
        ~((pl.col("channel_code") == 42001) & (pl.col("year") == 2022))
    )


def test_a_year_without_a_budget_share_keeps_the_budgets_own_rows(
    tmp_path: Path,
) -> None:
    release = copy_release(
        tmp_path / "release",
        edits={
            "commitments": _with_eu_commitments,
            "disbursements": _with_eu_commitments,
            "core_contributions": _with_eu_contributions,
            "agency_shares": _without_a_2022_budget_share,
        },
    )

    # EU-1 is a budget row (agency 1). In 2022 only the EIB's channel attributes.
    rows = cf.load(path=release).spending(view="provider").rows(row_id="EU-1")

    assert set(rows["year"]) == {2022, 2023}


@pytest.mark.parametrize("method", ["RANGE", RANGE, ["ONE", "OECD"]])
def test_quality_refuses_a_range_of_methods(method: object) -> None:
    with pytest.raises(TypeError, match=r"quality\(\) reads one method; .* holds ONE"):
        # A range is outside the type quality() takes; the test checks the run time.
        SNAP.quality(method=method)  # ty: ignore[invalid-argument-type]


def test_quality_reads_a_method_by_name() -> None:
    by_name = SNAP.quality(method="oecd").untraced

    assert by_name.equals(SNAP.quality(method=OECD).untraced)


@pytest.mark.parametrize("flow", FLOWS)
def test_quality_reports_the_eu_rows_a_provider_total_leaves_out(
    eu_release: Release, flow: str
) -> None:
    # As a provider, the EU keeps its budget and EDF rows; the EIB's leave as a
    # channel's own rows under either method.
    own = (
        eu_release.spending(
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

    left_out = eu_release.quality().eu_left_out.filter(flow=flow)

    assert own.height
    listed = LISTINGS["agency"].filter(provider_code=EU_INSTITUTIONS)
    names = dict(listed.select("agency_code", "agency").iter_rows())
    assert {code: names[code] for code in left_out["agency_code"]} == dict(
        left_out.select("agency_code", "agency").iter_rows()
    )
    assert set(left_out.select("agency_code", "channel_code").rows()) == {
        (1, 42001),
        (2, 42003),
        (EIB_AGENCY, EIB_CHANNEL),
    }
    budget_and_edf = (
        left_out.filter(pl.col("agency_code") != EIB_AGENCY)
        .group_by("year")
        .agg(pl.col("value").sum())
    )
    assert dict(budget_and_edf.rows()) == pytest.approx(dict(own.rows()))
    assert eu_release.quality(method=ONE_EU_AS_PROVIDER).eu_left_out.is_empty()
    assert eu_release.quality(providers=4).eu_left_out.is_empty()


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
    result = cf.load(path=release).run(spec)
    original = SNAP.run(spec)

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
    table = result.table
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
    assert table.filter(year=2021).equals(original.table.filter(year=2021))
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


@pytest.mark.parametrize(
    ("record_id", "method", "weight"),
    [
        # Germany's unlinked record is principal for adaptation.
        ("DEU-UNLINKED", ONE, 1.0),
        ("DEU-UNLINKED", OECD, 1.0),
        # France's is significant for mitigation; its agency assesses case by case
        # and states no default, so PROVIDER takes ONE's 0.4.
        ("FRA-UNLINKED", ONE, 0.4),
        ("FRA-UNLINKED", OECD, 1.0),
        ("FRA-UNLINKED", cf.methods.PROVIDER, 0.4),
        # JPN-SPLIT's 8m excess keeps the record's principal mitigation marker.
        ("JPN-SPLIT", cf.methods.PROVIDER, 1.0),
    ],
)
def test_untraced_rows_weigh_by_the_records_own_markers_in_value_and_high(
    record_id: str, method: Method, weight: float
) -> None:
    row = SNAP.spending(method=method).rows(record_id=record_id, origin="untraced")

    amount = {"DEU-UNLINKED": 2.5e6, "FRA-UNLINKED": 3e6, "JPN-SPLIT": 8e6}[record_id]
    assert row.select("amount", "value", "low", "high", "untraced").row(0) == (
        pytest.approx(amount),
        pytest.approx(weight * amount),
        0.0,
        pytest.approx(weight * amount),
        pytest.approx(weight * amount),
    )
    assert row["row_id"].to_list() == [None]


def test_traced_only_leaves_the_untraced_rows_out() -> None:
    spec = Spec(method=RANGE)
    full = SNAP.run(spec)
    traced = SNAP.run(spec.replace(untraced=False))

    assert set(traced.rows()["origin"]) == {"disbursement"}
    assert traced.table["untraced"].sum() == 0.0
    sums = [
        result._sums.collect().select(pl.col("value", "low", "untraced").sum()).row(0)
        for result in (full, traced)
    ]
    # Untraced value moves out of value and leaves low where it was.
    assert sums[1] == pytest.approx((sums[0][0] - sums[0][2], sums[0][1], 0.0))
    assert "traced only" in repr(traced).splitlines()[0]
    assert "Untraced disbursement" in traced.explain()
    assert "is left out" in traced.explain()


def test_explain_names_the_untraced_rows_of_disbursement_flows_only() -> None:
    line = "Untraced: CRS disbursement with a Rio marker that reaches no commitment"

    assert line in SNAP.spending().explain()
    assert line in SNAP.spending(flow="grant_equivalent").explain()
    assert "Untraced" not in SNAP.spending(flow="commitments").explain()


def test_jev_relevance_reads_a_jev_level_on_an_untraced_record() -> None:
    row = SNAP.spending(method=JEV_ONE).rows(record_id="DEU-UNLINKED")

    # Jev reads the record as significant for adaptation, where its own marker is
    # principal.
    assert row["value"].item() == pytest.approx(0.4 * 2.5e6)


def _before_adaptation(frame: pl.DataFrame) -> pl.DataFrame:
    # In 2009, as the release writes it: no adaptation marker.
    is_row = pl.col("record_id") == "DEU-UNLINKED"
    return frame.with_columns(
        year=pl.when(is_row).then(pl.lit(2009, pl.Int16)).otherwise(pl.col("year")),
        rio_adaptation=pl.when(~is_row).then(pl.col("rio_adaptation")),
    )


@pytest.mark.parametrize("method", [ONE, JEV_ONE])
def test_an_untraced_record_before_the_adaptation_marker_counts_mitigation(
    tmp_path: Path, method: Method
) -> None:
    release = copy_release(tmp_path / "release", edits={"untraced": _before_adaptation})

    row = cf.load(path=release).spending(method=method).rows(record_id="DEU-UNLINKED")

    # DEU-UNLINKED marks adaptation alone, and its Jev label reads adaptation, which
    # the CRS does not measure in 2009.
    assert row.select("year", "adaptation", "cross_cutting", "value").row(0) == (
        2009,
        None,
        None,
        0.0,
    )


# JPN-SPLIT, with its traced rows on commitments marked 0 for both objectives and its
# 8m excess marked adaptation principal alone, which Jev reads as significant.
def _unmark_split_commitments(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.with_columns(
        inherited_rio_mitigation=pl.when(pl.col("record_id") == "JPN-SPLIT")
        .then(pl.lit(0, pl.Int8))
        .otherwise(pl.col("inherited_rio_mitigation"))
    )


def _mark_split_excess_adaptation(frame: pl.DataFrame) -> pl.DataFrame:
    is_row = pl.col("record_id") == "JPN-SPLIT"
    return frame.with_columns(
        rio_adaptation=pl.when(is_row)
        .then(pl.lit(2, pl.Int8))
        .otherwise(pl.col("rio_adaptation")),
        rio_mitigation=pl.when(is_row)
        .then(pl.lit(0, pl.Int8))
        .otherwise(pl.col("rio_mitigation")),
    )


def _label_split_record(frame: pl.DataFrame) -> pl.DataFrame:
    label = pl.DataFrame(
        {
            "record_id": ["JPN-SPLIT"],
            "objective": ["adaptation"],
            "level": [1],
            "label_source": ["jev"],
            "probability": [0.6],
        }
    )
    return pl.concat([frame, label.cast(frame.schema)])


def test_a_jev_label_reaches_only_the_rows_a_record_weighs_as_adaptation(
    tmp_path: Path,
) -> None:
    path = copy_release(
        tmp_path / "release",
        edits={
            "disbursements": _unmark_split_commitments,
            "untraced": _mark_split_excess_adaptation,
            "objectives": _label_split_record,
        },
    )
    release = cf.load(path=path)

    def values(method: Method) -> dict[str | None, float]:
        rows = release.spending(method=method).rows(record_id="JPN-SPLIT")
        return dict(rows.select("row_id", "value").rows())

    # The traced rows weigh by their commitments' 0 and count nothing, label or not.
    # The excess weighs by its own adaptation marker, which the label replaces.
    assert values(ONE) == pytest.approx({"JPN-1": 0.0, "JPN-3": 0.0, None: 8e6})
    assert values(JEV_ONE) == pytest.approx(
        {"JPN-1": 0.0, "JPN-3": 0.0, None: 0.4 * 8e6}
    )


def test_quality_reports_the_untraced_and_left_out_marked_disbursement() -> None:
    untraced = SNAP.quality(method=OECD).untraced
    parts = ("excess_usd", "unlinked_marked_usd", "outside_release_usd")

    # The provider-years with Rio-marked disbursement outside `disbursements`.
    assert untraced.filter(pl.sum_horizontal(*parts, "fund_unlinked_usd") > 0).select(
        "provider_code",
        "year",
        *parts,
        "fund_unlinked_usd",
        "excess_climate_usd",
        "unlinked_climate_usd",
    ).rows() == [
        (FRA, 2021, 0.0, 3e6, 0.0, 0.0, 0.0, 3e6),
        (DEU, 2022, 0.0, 2.5e6, 4e6, 0.0, 0.0, 2.5e6),
        (JPN, 2023, 8e6, 0.0, 0.0, 0.0, 8e6, 0.0),
        (GEF, 2021, 0.0, 0.0, 0.0, 1.5e6, 0.0, 0.0),
    ]
    one = SNAP.quality(providers=["France"]).untraced
    assert one.select("provider_code", "year", "unlinked_climate_usd").rows() == [
        (FRA, 2021, pytest.approx(1.2e6)),
        (FRA, 2023, 0.0),
        (FRA, 2024, 0.0),
    ]

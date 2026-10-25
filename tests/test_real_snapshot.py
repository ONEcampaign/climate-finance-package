"""The query layer on a sample of a real release, when one has been drawn.

`uv run python tests/make_fixture.py --from-release <dir>` writes the sample to the
gitignored tests/fixtures/real_snapshot/; without it these tests skip.
"""

import polars as pl
import pytest
from make_fixture import REAL_SNAPSHOT_DIR, SAMPLE_PROVIDERS

import climate_finance as cf
from climate_finance.schema import RELEASE_TABLES, STATUSES, release_invariants

pytestmark = pytest.mark.skipif(
    not (REAL_SNAPSHOT_DIR / "release.json").is_file(),
    reason="no sample of a real release in tests/fixtures/real_snapshot",
)
FLOWS = ("commitments", "disbursements", "grant_equivalent")


@pytest.fixture(scope="module")
def release() -> cf.Release:
    return cf.load(path=REAL_SNAPSHOT_DIR)


def test_the_sample_holds_the_invariants_and_the_cases_it_was_drawn_for(
    release: cf.Release,
) -> None:
    tables = {name: release.table(name).collect() for name in RELEASE_TABLES}
    commitments = tables["commitments"]
    allocated_rows = tables["disbursements"]["row_id"].implode()

    assert release_invariants(tables) == []
    assert set(commitments["provider_code"]) == set(SAMPLE_PROVIDERS)
    assert set(commitments["status"]) == set(STATUSES) - {"pending_review"}
    assert commitments.filter(pl.col("repeat_of").is_in(allocated_rows)).height
    assert commitments["parent_row_id"].drop_nulls().len()


@pytest.mark.parametrize("flow", FLOWS)
def test_repeats_stay_out_of_every_flow(release: cf.Release, flow: str) -> None:
    repeats = (
        release.table("commitments").filter(pl.col("repeat_of").is_not_null()).collect()
    )

    rows = release.spending(flow=flow).rows()

    assert rows.height
    assert not rows.filter(pl.col("row_id").is_in(repeats["row_id"].implode())).height
    assert rows.filter(pl.col("row_id").is_in(repeats["repeat_of"].implode())).height


@pytest.mark.parametrize("flow", ["commitments", "disbursements"])
def test_unspecified_recipients_answer_by_name_in_every_flow(
    release: cf.Release, flow: str
) -> None:
    rows = release.spending(
        recipients="Developing countries, unspecified", flow=flow
    ).rows()

    assert rows.height
    assert set(rows["recipient_code"]) == {998}


def test_commitment_shares_of_total_find_a_total_for_each_bilateral_key(
    release: cf.Release,
) -> None:
    shares = (
        release.spending(flow="commitments", providers=["Germany", "France", "Japan"])
        .share_of_total()
        .table.to_polars()
    )

    assert shares.height
    assert shares["value"].null_count() == 0

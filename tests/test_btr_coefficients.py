"""`btr_coefficients`: the coefficients a BTR party's linked rows imply."""

import polars as pl
import pytest

from climate_finance.build.btr_coefficients import (
    MAX_SHARE,
    MIN_COMPONENTS,
    MIN_LINKED_USD,
    btr_coefficients,
)
from climate_finance.schema import (
    BTR,
    BTR_COEFFICIENTS,
    BTR_LINKS,
    PROVIDER_COEFFICIENTS,
    validate,
)

GBR, FRA, AFD = 12, 4, 3
# Enough components, each of USD 2m, for a coefficient.
COMPONENTS = MIN_COMPONENTS
GROSS = 2e6
assert COMPONENTS * GROSS >= MIN_LINKED_USD


def _rows(
    party: str,
    provider: int,
    *,
    marker: int,
    btr_usd: float,
    commitment: float,
    disbursement: float,
    status: str = "committed",
    count: int = COMPONENTS,
    agency: int = 1,
) -> tuple[list[dict], list[dict]]:
    """`count` BTR rows of `party`, each linked to one CRS record of its own."""
    rows, links = [], []
    for index in range(count):
        row_id = f"BTR-{party}-{agency}-{marker}-{status}-{index}"
        rows.append(
            {
                "row_id": row_id,
                "party": party,
                "provider_code": provider,
                "year": 2021 + index % 2,
                "face_usd_at_dac": btr_usd,
                "status": status,
                "is_aggregate": False,
            }
        )
        links.append(
            {
                "row_id": row_id,
                "record_id": f"{row_id}-R",
                "provider_code": provider,
                "agency_code": agency,
                "commitment_usd": commitment,
                "disbursement_usd": disbursement,
                "rio_adaptation": marker,
                "rio_mitigation": 0,
            }
        )
    return rows, links


def _frames(*parts: tuple[list[dict], list[dict]]) -> tuple[pl.DataFrame, pl.DataFrame]:
    rows = [row for part in parts for row in part[0]]
    links = [link for part in parts for link in part[1]]
    btr = pl.DataFrame(rows).with_columns(
        pl.col(name).cast(BTR.columns[name].dtype) for name in ("provider_code", "year")
    )
    link_frame = pl.DataFrame(links).with_columns(
        pl.col(name).cast(BTR_LINKS.columns[name].dtype)
        for name in ("provider_code", "rio_adaptation", "rio_mitigation")
    )
    return btr, link_frame.with_columns(pl.col("agency_code").cast(pl.Int32))


def _coefficients(*rows: dict) -> pl.DataFrame:
    defaults = dict.fromkeys(PROVIDER_COEFFICIENTS.columns) | {
        "case_by_case": True,
        "source": "survey",
        "locator": "p. 8",
    }
    return pl.DataFrame(
        [defaults | row for row in rows],
        schema={name: c.dtype for name, c in PROVIDER_COEFFICIENTS.columns.items()},
    )


def _table(btr: pl.DataFrame, links: pl.DataFrame, coefficients: pl.DataFrame) -> dict:
    table = btr_coefficients(btr, links=links, coefficients=coefficients)
    validate(BTR_COEFFICIENTS, table)
    return {
        (row["provider_code"], row["agency_code"], row["marker"]): (
            row["coefficient"] and pytest.approx(row["coefficient"]),
            row["source"],
            row["reason"],
            row["components"],
        )
        for row in table.iter_rows(named=True)
    }


def test_a_case_by_case_party_takes_its_btr_share_on_the_flow_it_follows() -> None:
    # The UK labels its rows committed, but its principal components sit at 1 on
    # disbursements (2m) and at 0.2 on commitments (10m), so both markers read
    # disbursements: significant activities at 1.2m of 2m.
    btr, links = _frames(
        _rows("GBR", GBR, marker=2, btr_usd=2e6, commitment=10e6, disbursement=GROSS),
        _rows("GBR", GBR, marker=1, btr_usd=1.2e6, commitment=10e6, disbursement=GROSS),
    )

    assert _table(btr, links, _coefficients({"provider_code": GBR})) == {
        (GBR, None, "principal"): (pytest.approx(1.0), "btr", None, COMPONENTS),
        (GBR, None, "significant"): (pytest.approx(0.6), "btr", None, COMPONENTS),
    }


def test_rows_and_records_that_share_links_count_their_money_once() -> None:
    # In each group two BTR rows of 1m link to two records of 2m, one row to both
    # and one to the second, so the component holds 2m of BTR money on 4m of CRS:
    # 0.5. Summing the links one by one would count the second record twice.
    principal = _rows(
        "GBR", GBR, marker=2, btr_usd=GROSS, commitment=GROSS, disbursement=GROSS
    )
    rows, links = [], []
    for group in range(COMPONENTS):
        first, second = _rows(
            "GBR",
            GBR,
            marker=1,
            btr_usd=1e6,
            commitment=GROSS,
            disbursement=GROSS,
            count=2,
            agency=100 + group,
        )[0]
        records = [f"G{group}-R1", f"G{group}-R2"]
        rows += [first, second]
        for row_id, record in (
            (first["row_id"], records[0]),
            (first["row_id"], records[1]),
            (second["row_id"], records[1]),
        ):
            links.append(
                {
                    "row_id": row_id,
                    "record_id": record,
                    "provider_code": GBR,
                    "agency_code": 1,
                    "commitment_usd": GROSS,
                    "disbursement_usd": GROSS,
                    "rio_adaptation": 1,
                    "rio_mitigation": 0,
                }
            )
    btr, link_frame = _frames(principal, (rows, links))
    coefficients = _coefficients({"provider_code": GBR})

    assert _table(btr, link_frame, coefficients)[(GBR, None, "significant")] == (
        pytest.approx(0.5),
        "btr",
        None,
        COMPONENTS,
    )


@pytest.mark.parametrize(
    ("share", "kept"),
    [
        # BTR and CRS amounts that differ by rounding count at 1, up to the boundary.
        (1.05, True),
        (MAX_SHARE, True),
        # Above it the component is a link error or claims more than the money.
        (1.11, False),
        (2.74, False),
    ],
)
def test_a_component_above_max_share_leaves_the_evidence(
    share: float, kept: bool
) -> None:
    # 20 significant components at 0.5 and one more at `share`, each on 2m of CRS:
    # kept, it counts at 1, (20 * 1m + 2m) / 42m; dropped, 20m / 40m remain.
    rows, links = _rows(
        "GBR",
        GBR,
        marker=1,
        btr_usd=0.5 * GROSS,
        commitment=GROSS,
        disbursement=GROSS,
        count=COMPONENTS + 1,
    )
    rows[0]["face_usd_at_dac"] = share * GROSS
    btr, link_frame = _frames(
        _rows(
            "GBR", GBR, marker=2, btr_usd=GROSS, commitment=GROSS, disbursement=GROSS
        ),
        (rows, links),
    )

    table = btr_coefficients(
        btr, links=link_frame, coefficients=_coefficients({"provider_code": GBR})
    )

    columns = (
        "coefficient",
        "components",
        "linked_usd",
        "dropped",
        "dropped_usd",
        "dropped_btr_usd",
    )
    expected = (
        (22 / 42, COMPONENTS + 1, 21 * GROSS, 0, 0.0, 0.0)
        if kept
        else (0.5, COMPONENTS, COMPONENTS * GROSS, 1, GROSS, share * GROSS)
    )
    assert table.filter(marker="significant").select(columns).row(0) == pytest.approx(
        expected
    )


def test_a_component_reaches_every_scope_and_marker_of_its_records() -> None:
    # One BTR row links a significant AFD record and a principal record of France's
    # other agencies. Both scopes are reached, each on its own record's marker, and
    # the party has too few principal components to align, in any row order.
    rows, links = _rows(
        "FRA",
        FRA,
        marker=1,
        btr_usd=GROSS,
        commitment=GROSS,
        disbursement=0.0,
        count=1,
        agency=AFD,
    )
    links.append(
        links[0] | {"record_id": "FRA-R2", "agency_code": 1, "rio_adaptation": 2}
    )
    btr, link_frame = _frames((rows, links))
    coefficients = _coefficients(
        {"provider_code": FRA, "significant": 0.4, "principal": 1.0},
        {"provider_code": FRA, "agency_code": AFD},
    )
    expected = {
        (FRA, None, "principal"): (None, "stated", "unaligned", 0),
        (FRA, None, "significant"): (None, "stated", "no_links", 0),
        (FRA, AFD, "principal"): (None, "fallback", "no_links", 0),
        (FRA, AFD, "significant"): (None, "fallback", "unaligned", 0),
    }

    for seed in range(8):
        shuffled = link_frame.sample(fraction=1.0, shuffle=True, seed=seed)
        assert _table(btr, shuffled, coefficients) == expected


@pytest.mark.parametrize(
    ("principal_share", "count", "reason"),
    [
        # Principal components at twice their records say the amounts do not line
        # up with the CRS, as the US obligations against its commitments.
        (2.0, COMPONENTS, "unaligned"),
        # Too few principal components to tell.
        (1.0, 4, "unaligned"),
    ],
)
def test_a_party_whose_principal_shares_stray_from_one_gives_no_coefficient(
    principal_share: float, count: int, reason: str
) -> None:
    btr, links = _frames(
        _rows(
            "GBR",
            GBR,
            marker=2,
            btr_usd=principal_share * GROSS,
            commitment=GROSS,
            disbursement=GROSS,
            count=count,
        ),
        _rows(
            "GBR",
            GBR,
            marker=1,
            btr_usd=0.5 * GROSS,
            commitment=GROSS,
            disbursement=GROSS,
        ),
    )

    table = _table(btr, links, _coefficients({"provider_code": GBR}))

    assert table[(GBR, None, "significant")] == (None, "fallback", reason, 0)


def test_thin_evidence_and_stated_coefficients_keep_their_own() -> None:
    # France states 0.4 and 1.0, so its BTR's shares are evidence only. The AFD
    # assesses case by case: its significant components take their 0.6, and its
    # principal ones, one short of the minimum, give none.
    btr, links = _frames(
        _rows("FRA", FRA, marker=2, btr_usd=GROSS, commitment=GROSS, disbursement=0.0),
        _rows(
            "FRA",
            FRA,
            marker=1,
            btr_usd=0.5 * GROSS,
            commitment=GROSS,
            disbursement=0.0,
        ),
        _rows(
            "FRA",
            FRA,
            marker=2,
            btr_usd=GROSS,
            commitment=GROSS,
            disbursement=0.0,
            agency=AFD,
            count=COMPONENTS - 1,
        ),
        _rows(
            "FRA",
            FRA,
            marker=1,
            btr_usd=0.6 * GROSS,
            commitment=GROSS,
            disbursement=0.0,
            agency=AFD,
        ),
    )
    coefficients = _coefficients(
        {
            "provider_code": FRA,
            "significant": 0.4,
            "principal": 1.0,
            "case_by_case": False,
        },
        {"provider_code": FRA, "agency_code": AFD},
    )

    assert _table(btr, links, coefficients) == {
        (FRA, None, "principal"): (pytest.approx(1.0), "stated", None, COMPONENTS),
        (FRA, None, "significant"): (pytest.approx(0.5), "stated", None, COMPONENTS),
        (FRA, AFD, "principal"): (None, "fallback", "thin", COMPONENTS - 1),
        (FRA, AFD, "significant"): (pytest.approx(0.6), "btr", None, COMPONENTS),
    }


def test_a_build_without_btr_rows_has_no_coefficients() -> None:
    btr = pl.DataFrame(schema=BTR.dtypes)
    links = pl.DataFrame(schema=BTR_LINKS.dtypes).with_columns(
        agency_code=pl.lit(None, pl.Int32)
    )

    table = btr_coefficients(
        btr, links=links, coefficients=_coefficients({"provider_code": GBR})
    )

    assert table.is_empty()
    validate(BTR_COEFFICIENTS, table)

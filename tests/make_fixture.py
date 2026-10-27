"""Write the synthetic release snapshot in tests/fixtures/snapshot/.

Run `uv run python tests/make_fixture.py [out_dir]`. The snapshot is schema-valid,
passes `release_invariants`, and covers six providers, a Bi_Multi 2 row, Bi_Multi 4
rows, an id-less multilateral row, a modelled-line provider, every status, every link
method, a CRS record split across two commitments with excess, a bank-ledger gap
fill, cross-cutting agency shares measured from markers and from components, core
contributions for every flow, provider coefficients that change between years, split
by agency, hold a both-significant coefficient or a case-by-case default, commitments
reached through certain and estimated CRDF routes, and unallocated disbursements of
each kind. Amounts are invented.
"""

import itertools
import sys
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from climate_finance import __version__
from climate_finance.build import write_table
from climate_finance.schema import (
    CERTAIN_ROUTES,
    FLOWS,
    LINK_METHODS,
    RELEASE_TABLES,
    ReleaseManifest,
    TableEntry,
    release_invariants,
    validate,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "snapshot"
RELEASE = "fixture"

# DAC codes: Germany, France, Norway, Japan, IDA, AfDB; IFC has only a modelled line.
DEU, FRA, NOR, JPN, IDA, AFDB, IFC = 5, 4, 8, 701, 905, 913, 903
IDA_CH, AFDB_CH = 44002, 46002
KEN, BGD, IRQ, UNS = 248, 666, 543, 998
NO_RECORD = "no_disbursement_record"
# FRA-1's project pays earlier tranches; AFDB-2 is the id-less multilateral row.
PROJECT_DISBURSING = {"FRA-1"}
FULLY_DISBURSED = {"IDA-1"}
ID_LESS = {"AFDB-2"}

# (row_id, provider, agency, recipient, channel, bi_multi, year, rio_adaptation,
#  rio_mitigation, climate_share, face_usd, status)
COMMITMENT_SPECS = [
    ("DEU-1", DEU, 52, KEN, None, 1, 2021, 2, 0, None, 40e6, "disbursed"),
    ("DEU-2", DEU, 52, BGD, None, 1, 2022, 1, 1, None, 25e6, "partly_disbursed"),
    ("DEU-3", DEU, 52, UNS, IDA_CH, 2, 2021, 1, 1, None, 60e6, "disbursed"),
    ("FRA-1", FRA, 1, IRQ, None, 1, 2021, 0, 2, None, 30e6, NO_RECORD),
    ("FRA-2", FRA, 1, KEN, None, 1, 2022, 1, 0, None, 12e6, "pending_review"),
    ("NOR-1", NOR, 1, BGD, None, 1, 2021, 2, 1, None, 18e6, "disbursed"),
    ("NOR-2", NOR, 1, IRQ, None, 3, 2022, 0, 1, None, 9e6, NO_RECORD),
    ("JPN-1", JPN, 2, BGD, None, 1, 2021, 0, 2, None, 80e6, "partly_disbursed"),
    ("JPN-2", JPN, 2, KEN, None, 1, 2022, 1, 1, None, 50e6, "disbursed"),
    ("JPN-3", JPN, 2, BGD, None, 1, 2022, 0, 2, None, 30e6, "partly_disbursed"),
    ("IDA-1", IDA, 1, KEN, IDA_CH, 4, 2021, None, None, 0.45, 150e6, "disbursed"),
    (
        "IDA-2",
        IDA,
        1,
        BGD,
        IDA_CH,
        4,
        2022,
        None,
        None,
        0.30,
        200e6,
        "partly_disbursed",
    ),
    (
        "AFDB-1",
        AFDB,
        1,
        KEN,
        AFDB_CH,
        4,
        2021,
        None,
        None,
        0.60,
        70e6,
        "partly_disbursed",
    ),
    ("AFDB-2", AFDB, 1, IRQ, AFDB_CH, 4, 2022, None, None, 0.50, 45e6, "not_traceable"),
]

# Share of face value disbursed through each commitment's own three records, by
# status. IDA-1 disburses its whole face value, which fills its high bound to face.
DISBURSED_SHARE = {
    "disbursed": 0.95,
    "partly_disbursed": 0.5,
    "pending_review": 0.3,
    "no_disbursement_record": 0.0,
    "not_traceable": 0.0,
}
# An allocation counts in the low bound when its link is at least this probable,
# and its high bound is this multiple of the allocation, capped so the commitment's
# high bounds stay within its face value.
CERTAIN_PROBABILITY = 0.9
HIGH_FACTOR = 1.05
# A record's grant equivalent as a share of its disbursement, by flow code.
GRANT_EQUIV_RATIO = {11: 1.0, 13: 0.6}

# One CRS record split across JPN-1 and JPN-3. The rest of its disbursement is
# excess, which reaches no commitment and so has no row.
SPLIT_RECORD = "JPN-SPLIT"
SPLIT_YEAR = 2023
SPLIT_DISBURSEMENT = 30e6
SPLIT_GRANT_EQUIV = 18e6
# (row_id, allocated_usd, link_method, link_probability)
SPLIT_ALLOCATIONS = (
    ("JPN-1", 12e6, "crs_id", 1.0),
    ("JPN-3", 10e6, "pn_recipient", 0.85),
)
# A bank-ledger gap fill on AFDB-1: no CRS link, so no method or probability.
LEDGER_RECORD = "AFDB-1-LEDGER"
LEDGER_ALLOCATED = 1e6

# The OECD's imputed agency shares of climate spending by objective, (adaptation,
# mitigation, cross_cutting), adaptation and mitigation each including cross_cutting.
AGENCY_SHARES = {IDA_CH: (0.20, 0.25, 0.05), AFDB_CH: (0.30, 0.35, 0.10)}
# Each agency's commitments by Rio markers as fractions of its spending, with the
# components of rows that report them: (rio_adaptation, rio_mitigation, face_share,
# (adaptation_share, mitigation_share, overlap_share) or None). Under ONE the IDA's
# markers give shares of 0.18, 0.22 and 0.04: significant markers at 0.4 of 0.2 + 0.2
# + 0.1, principal ones at 1.0 of 0.06 + 0.1. The AfDB reports components alone.
AGENCY_SPENDING = {
    IDA_CH: (
        (2, 0, 0.06, None),
        (1, 0, 0.2, None),
        (0, 2, 0.1, None),
        (0, 1, 0.2, None),
        (1, 1, 0.1, None),
    ),
    AFDB_CH: ((None, None, 0.6, (0.27, 0.30, 0.08)),),
}
# Core contributions disbursed each year, and each flow's amount as a multiple.
CORE_CONTRIBUTIONS = ((DEU, IDA_CH, 60e6), (FRA, AFDB_CH, 25e6), (NOR, IDA_CH, 15e6))
CORE_FLOW_FACTOR = {"commitments": 1.2, "disbursements": 1.0, "grant_equivalent": 1.0}
# (provider, agency, first_year, last_year, significant, principal, both_significant,
#  case_by_case, source, locator). Germany counts an activity significant for both
# objectives at 1.0. Japan's coefficients change in 2022. France's agency 1 assesses
# case by case and states no default, so its rows take the method's coefficients
# rather than France's 0.3. Norway assesses case by case with a default of 0.3 for
# significant activities and none for principal ones.
SURVEY, REPORT = "Fixture survey", "Fixture report"
PROVIDER_COEFFICIENTS = (
    (DEU, None, None, None, 0.5, 1.0, 1.0, False, SURVEY, "p. 8"),
    (JPN, None, None, 2021, 0.5, 0.85, None, False, REPORT, "footnote 4"),
    (JPN, None, 2022, None, 0.6, 1.0, None, False, SURVEY, "p. 8"),
    (FRA, None, None, None, 0.3, 1.0, None, False, SURVEY, "p. 8"),
    (FRA, 1, None, None, None, None, None, True, SURVEY, "p. 9, note f"),
    (NOR, None, None, None, 0.3, None, None, True, SURVEY, "p. 5"),
)
# The route by which a commitment's CRDF row reaches its CRS commitment, where it is
# not `within_year`.
CRDF_ROUTES = {"NOR-1": "crosswalk_amount", "JPN-3": "title_recipient"}
# Rio-marked disbursements that reach no commitment: (provider, year,
# outside_release_usd, unlinked_marked_usd). Excess comes from the records' rows.
UNALLOCATED = ((DEU, 2022, 4e6, 2.5e6), (FRA, 2021, 0.0, 3e6))
# The build settings the fixture claims, as `release.json` records them.
SETTINGS = {
    "offline": True,
    "key_repair": True,
    "recoveries": True,
    "years": {"crdf": [2021, 2022], "crs": [2021, 2023]},
}


def _climate_split(
    face: float, rio_a: int | None, rio_m: int | None, share: float | None
) -> tuple[float, float, float, float]:
    """(climate, adaptation, mitigation, overlap) in USD."""
    if share is not None:
        climate = face * share
        return climate, climate * 0.5, climate * 0.5, 0.0
    adaptation = face if rio_a else 0.0
    mitigation = face if rio_m else 0.0
    overlap = face if rio_a and rio_m else 0.0
    return adaptation + mitigation - overlap, adaptation, mitigation, overlap


def _allocation(
    commitment: dict[str, object],
    *,
    record_id: str,
    year: int,
    allocated: float,
    allocated_high: float,
    record_disbursement: float,
    record_grant_equiv: float | None,
    link_method: str | None,
    link_probability: float | None,
    rule: str,
    disbursement_source: str = "crs",
) -> dict[str, object]:
    """A disbursements row allocating `allocated` of `record_id` to `commitment`.

    `record_disbursement` and `record_grant_equiv` are the whole CRS record's.
    Unlinked rows (a bank-ledger fill) count in full in the low bound.
    """
    certain = link_probability is None or link_probability >= CERTAIN_PROBABILITY
    first = record_id.endswith("-R0")
    rio_a, rio_m = commitment["rio_adaptation"], commitment["rio_mitigation"]
    return {
        "row_id": commitment["row_id"],
        "record_id": record_id,
        "year": year,
        "provider_code": commitment["provider_code"],
        "agency_code": commitment["agency_code"],
        "recipient_code": commitment["recipient_code"],
        "channel_code": commitment["channel_code"],
        "bi_multi": commitment["bi_multi"],
        "purpose_code": 41010,
        "flow_code": _flow_code(commitment),
        "project_number": f"PN-{commitment['row_id']}",
        "title": f"Climate project {commitment['row_id']}",
        "allocated_usd": allocated,
        "allocated_low_usd": allocated if certain else 0.0,
        "allocated_high_usd": allocated_high,
        "record_disbursement_usd": record_disbursement,
        "record_grant_equiv_usd": record_grant_equiv,
        "inherited_rio_adaptation": rio_a,
        "inherited_rio_mitigation": rio_m,
        "record_rio_adaptation": None if first else rio_a,
        "record_rio_mitigation": rio_m,
        "marker_source": "record" if first else "commitment",
        "climate_share": commitment["climate_share"],
        "link_method": link_method,
        "link_probability": link_probability,
        "rule": rule,
        "disbursement_source": disbursement_source,
        "ledger_check": "confirmed" if commitment["provider_code"] == IDA else None,
    }


def _flow_code(commitment: dict[str, object]) -> int:
    return 11 if commitment["provider_code"] in (DEU, NOR) else 13


def _objectives(
    commitment: dict[str, object], record_id: str
) -> list[dict[str, object]]:
    """Objective labels for a record: bank climate components carry CRDF objectives,
    other records their Rio markers."""
    labels = (
        (
            ("adaptation", commitment["rio_adaptation"], "marker"),
            ("mitigation", commitment["rio_mitigation"], "marker"),
        )
        if commitment["climate_share"] is None
        else (("adaptation", 2, "crdf"), ("mitigation", 2, "crdf"))
    )
    return [
        {
            "record_id": record_id,
            "objective": objective,
            "level": level,
            "label_source": source,
            "probability": None,
        }
        for objective, level, source in labels
    ]


def build_tables() -> dict[str, pl.DataFrame]:
    """Every release table of the synthetic snapshot."""
    methods = itertools.cycle(LINK_METHODS)
    commitments: dict[str, dict[str, object]] = {}
    disbursement_rows: list[dict[str, object]] = []
    objective_rows: list[dict[str, object]] = []
    high_used: dict[str, float] = {}

    def allocate(
        commitment: dict[str, object], *, allocated: float, reserved: float = 0.0, **row
    ) -> None:
        """Add an allocation whose high bound leaves `reserved` of the commitment's
        face value for the allocations still to come."""
        row_id = str(commitment["row_id"])
        headroom = float(commitment["face_usd"]) - high_used.get(row_id, 0.0) - reserved
        high = min(allocated * HIGH_FACTOR, headroom)
        high_used[row_id] = high_used.get(row_id, 0.0) + high
        disbursement_rows.append(
            _allocation(commitment, allocated=allocated, allocated_high=high, **row)
        )

    for (
        row_id, provider, agency, recipient, channel, bi_multi, year, rio_a, rio_m,
        share, face, status,
    ) in COMMITMENT_SPECS:  # fmt: skip
        has_ids = row_id not in ID_LESS
        climate, adaptation, mitigation, overlap = _climate_split(
            face, rio_a, rio_m, share
        )
        commitment: dict[str, object] = {
            "row_id": row_id,
            "vintage": year,
            "year": year,
            "provider_code": provider,
            "agency_code": agency,
            "recipient_code": recipient,
            "channel_code": channel,
            "bi_multi": bi_multi,
            "purpose_code": 41010,
            "flow_code": 11,
            "crs_id": f"{year}00{row_id}" if has_ids else None,
            "project_number": f"PN-{row_id}" if has_ids else None,
            "rio_adaptation": rio_a,
            "rio_mitigation": rio_m,
            "climate_share": share,
            "face_usd": face,
            "climate_usd": climate,
            "adaptation_usd": adaptation,
            "mitigation_usd": mitigation,
            "overlap_usd": overlap,
            "status": status,
            "project_disbursing": row_id in PROJECT_DISBURSING,
            "repeat_of": None,
            "parent_row_id": None,
        }
        commitments[row_id] = commitment
        # Each disbursing commitment draws three records of its own, one per year
        # from the commitment year, so every link method appears across the fixture.
        if not DISBURSED_SHARE[status]:
            continue
        disbursed_share = 1.0 if row_id in FULLY_DISBURSED else DISBURSED_SHARE[status]
        allocated = face * disbursed_share / 3
        for index in range(3):
            record_id = f"{row_id}-R{index}"
            grant_equiv = allocated * GRANT_EQUIV_RATIO[_flow_code(commitment)]
            allocate(
                commitment,
                record_id=record_id,
                year=year + index,
                allocated=allocated,
                reserved=allocated * (2 - index),
                record_disbursement=allocated,
                record_grant_equiv=grant_equiv if index else None,
                link_method=next(methods),
                link_probability=1.0 if index == 0 else 0.85,
                rule="dated" if index == 0 else "fifo",
            )
            objective_rows.extend(_objectives(commitment, record_id))

    used = {row["link_method"] for row in disbursement_rows}
    if missing := set(LINK_METHODS) - used:
        raise ValueError(f"fixture misses link methods {sorted(missing)}")

    for row_id, allocated, link_method, probability in SPLIT_ALLOCATIONS:
        allocate(
            commitments[row_id],
            record_id=SPLIT_RECORD,
            year=SPLIT_YEAR,
            allocated=allocated,
            record_disbursement=SPLIT_DISBURSEMENT,
            record_grant_equiv=SPLIT_GRANT_EQUIV,
            link_method=link_method,
            link_probability=probability,
            rule="fifo",
        )
    objective_rows.extend(_objectives(commitments["JPN-1"], SPLIT_RECORD))
    allocate(
        commitments["AFDB-1"],
        record_id=LEDGER_RECORD,
        year=2023,
        allocated=LEDGER_ALLOCATED,
        record_disbursement=LEDGER_ALLOCATED,
        record_grant_equiv=None,
        link_method=None,
        link_probability=None,
        rule="bank_ledger",
        disbursement_source="bank_ledger",
    )
    disbursement_rows[-1]["ledger_check"] = None
    # A Jev objective label on one record without markers.
    objective_rows.append(
        {
            "record_id": "JPN-1-R1",
            "objective": "adaptation",
            "level": 1,
            "label_source": "jev",
            "probability": 0.72,
        }
    )

    tables = {
        "disbursements": disbursement_rows,
        "objectives": objective_rows,
        "agency_shares": [
            {
                "channel_code": channel,
                "year": year,
                "objective": objective,
                "share": share,
                "source": "oecd",
            }
            for channel, shares in AGENCY_SHARES.items()
            for year in (2021, 2022)
            for objective, share in zip(
                ("adaptation", "mitigation", "cross_cutting"), shares, strict=True
            )
        ],
        "agency_spending": [
            {
                "channel_code": channel,
                "year": year,
                "rio_adaptation": rio_a,
                "rio_mitigation": rio_m,
                "face_share": face,
                **dict(
                    zip(
                        ("adaptation_share", "mitigation_share", "overlap_share"),
                        components or (None, None, None),
                        strict=True,
                    )
                ),
            }
            for channel, rows in AGENCY_SPENDING.items()
            for year in (2021, 2022)
            for rio_a, rio_m, face, components in rows
        ],
        "modelled": [
            {
                "provider_code": IFC,
                "year": year,
                "recipient_code": recipient,
                "low_usd": low,
                "high_usd": high,
                "basis": "annual_report",
            }
            for year, recipient, low, high in (
                (2021, None, 460e6, 970e6),
                (2022, None, 500e6, 1050e6),
                (2022, IRQ, 30e6, 63e6),
            )
        ],
        "core_contributions": [
            {
                "provider_code": provider,
                "channel_code": channel,
                "year": year,
                "flow": flow,
                "contribution_usd": amount * CORE_FLOW_FACTOR[flow],
            }
            for provider, channel, amount in CORE_CONTRIBUTIONS
            for year in (2021, 2022)
            for flow in FLOWS
        ],
        "provider_coefficients": [
            dict(zip(RELEASE_TABLES["provider_coefficients"].columns, row, strict=True))
            for row in PROVIDER_COEFFICIENTS
        ],
        "btr": [
            {
                "row_id": f"BTR-{party}-{year}",
                "party": party,
                "year": year,
                "recipient": "Kenya",
                "recipient_code": KEN,
                "title": f"Adaptation programme {party} {year}",
                "amount_domestic": amount,
                "currency": currency,
                "face_usd": amount * rate,
                "grant_equiv_usd": amount * rate,
                "status": "disbursed",
                "channel": "bilateral",
                "funding_source": "ODA",
                "instrument": "grant",
                "support_type": "adaptation",
                "sector": "agriculture",
                "additional_information": None,
                "is_aggregate": False,
                "source_file": f"{party}-BTR1.xlsx",
            }
            for party, currency, rate, amount in (
                ("DEU", "EUR", 1.1, 20e6),
                ("NOR", "NOK", 0.1, 150e6),
            )
            for year in (2021, 2022)
        ],
        "disbursement_profile": [
            {
                "group": "all",
                "years_since": years_since,
                "cumulative_share": share,
                "vintages": 9 - years_since,
            }
            for years_since, share in enumerate(
                (0.23, 0.31, 0.39, 0.46, 0.52, 0.58, 0.63, 0.67, 0.70)
            )
        ],
    }
    frames = {
        name: pl.from_dicts(rows, schema=_polars_schema(name))
        for name, rows in tables.items()
    }
    frames["commitments"] = _commitments(
        list(commitments.values()), frames["disbursements"]
    )
    frames["quality"] = _quality(frames["commitments"], frames["disbursements"])
    frames["unallocated"] = _unallocated(frames["disbursements"])
    frames["total_flows"] = _total_flows(
        frames["commitments"], frames["disbursements"], frames["core_contributions"]
    )
    return frames


def _commitments(
    rows: list[dict[str, object]], disbursements: pl.DataFrame
) -> pl.DataFrame:
    """Commitments with disbursed amounts summed from their allocations."""
    disbursed = disbursements.group_by("row_id").agg(
        disbursed_usd=pl.col("allocated_usd").sum(),
        disbursed_low_usd=pl.col("allocated_low_usd").sum(),
        disbursed_high_usd=pl.col("allocated_high_usd").sum(),
    )
    return (
        pl.from_dicts(rows, infer_schema_length=None)
        .join(disbursed, on="row_id", how="left")
        .with_columns(pl.col("^disbursed_.*$").fill_null(0.0))
        .select(
            pl.col(name).cast(dtype)
            for name, dtype in _polars_schema("commitments").items()
        )
    )


def _polars_schema(table: str) -> pl.Schema:
    columns = RELEASE_TABLES[table].columns
    return pl.Schema({name: column.dtype for name, column in columns.items()})


def _total_flows(
    commitments: pl.DataFrame,
    disbursements: pl.DataFrame,
    core_contributions: pl.DataFrame,
) -> pl.DataFrame:
    """All CRS flows: four times the climate rows' amounts (0.9 of that for grant
    equivalent), plus 1.5 times each flow's core contributions to unspecified
    recipients, so every climate figure has a larger total."""
    keys = ["provider_code", "recipient_code", "year", "bi_multi"]
    committed = commitments.select(*keys, amount_usd=pl.col("face_usd") * 4)
    disbursed = disbursements.select(*keys, amount_usd=pl.col("allocated_usd") * 4)
    climate = {
        "commitments": committed,
        "disbursements": disbursed,
        "grant_equivalent": disbursed.with_columns(pl.col("amount_usd") * 0.9),
    }
    core = core_contributions.select(
        "provider_code",
        recipient_code=pl.lit(UNS, pl.Int32),
        year="year",
        bi_multi=pl.lit(2, pl.Int8),
        flow="flow",
        amount_usd=pl.col("contribution_usd") * 1.5,
    )
    return pl.concat(
        pl.concat([frame, core.filter(pl.col("flow") == flow).drop("flow")])
        .group_by(keys)
        .agg(pl.col("amount_usd").sum())
        .with_columns(flow=pl.lit(flow))
        for flow, frame in climate.items()
    ).select(list(RELEASE_TABLES["total_flows"].columns))


# Statuses of commitments that trace to at least one CRS record.
TRACED = ("disbursed", "partly_disbursed", "no_disbursement_record")


def _quality(commitments: pl.DataFrame, disbursements: pl.DataFrame) -> pl.DataFrame:
    """Climate value by provider and commitment year: all of it, the part that
    traces, and the traced part whose CRDF route is certain; and the share of the
    traced commitments' allocated value whose record has a grant equivalent."""
    allocated = pl.col("allocated_usd")
    allocations = disbursements.group_by("row_id").agg(
        allocated=allocated.sum(),
        with_grant_equiv=allocated.filter(
            pl.col("record_grant_equiv_usd").is_not_null()
        ).sum(),
    )
    climate = pl.col("climate_usd")
    traced = pl.col("status").is_in(TRACED)
    route = pl.col("row_id").replace_strict(CRDF_ROUTES, default="within_year")
    return (
        commitments.join(allocations, on="row_id", how="left")
        .group_by("provider_code", "year")
        .agg(
            commitments=pl.len().cast(pl.Int32),
            climate_usd=climate.sum(),
            linked_usd=climate.filter(traced).sum(),
            certain_route_usd=climate.filter(traced, route.is_in(CERTAIN_ROUTES)).sum(),
            allocated=pl.col("allocated").filter(traced).sum(),
            with_grant_equiv=pl.col("with_grant_equiv").filter(traced).sum(),
        )
        .with_columns(
            grant_equiv_coverage=pl.when(pl.col("allocated") > 0).then(
                pl.col("with_grant_equiv") / pl.col("allocated")
            )
        )
        .with_columns(
            model_labelled_usd=pl.lit(0.0),
            report_labelled_usd=pl.col("climate_usd"),
        )
        .select(list(RELEASE_TABLES["quality"].columns))
    )


def _unallocated(disbursements: pl.DataFrame) -> pl.DataFrame:
    """Per provider and disbursement year, each record's disbursement beyond its
    allocations, with `UNALLOCATED`'s amounts outside the release and unlinked."""
    excess = (
        disbursements.group_by("provider_code", "year", "record_id")
        .agg(
            excess_usd=pl.col("record_disbursement_usd").first()
            - pl.col("allocated_usd").sum()
        )
        .group_by("provider_code", "year")
        .agg(pl.col("excess_usd").sum())
    )
    other = pl.DataFrame(
        UNALLOCATED,
        schema=["provider_code", "year", "outside_release_usd", "unlinked_marked_usd"],
        orient="row",
    )
    columns = _polars_schema("unallocated")
    return (
        excess.join(
            other.cast({name: columns[name] for name in other.columns}),
            on=["provider_code", "year"],
            how="full",
            coalesce=True,
        )
        .with_columns(pl.col("^.*_usd$").fill_null(0.0))
        .filter(pl.sum_horizontal("^.*_usd$") > 0)
        .select(pl.col(name).cast(dtype) for name, dtype in columns.items())
        .sort("provider_code", "year")
    )


def write_fixture(out_dir: Path) -> ReleaseManifest:
    """Write every release table and `release.json` to `out_dir`."""
    tables = build_tables()
    if problems := release_invariants(tables):
        raise ValueError(f"fixture breaks release invariants: {problems}")
    entries = {}
    for name, frame in tables.items():
        table = RELEASE_TABLES[name]
        validate(table, frame)
        sha256 = write_table(frame, table, out_dir / f"{name}.parquet")
        entries[name] = TableEntry(sha256=sha256, rows=frame.height)
    manifest = ReleaseManifest(
        release=RELEASE,
        created_at=datetime(2026, 10, 21, tzinfo=UTC),
        package_version=__version__,
        tables=entries,
        source_versions={"crs": "fixture", "crdf": "fixture"},
        model_versions={"jev": "jev-1.14.0"},
        gates={},
        settings=SETTINGS,
        precision_gate_waived=False,
    )
    (out_dir / "release.json").write_text(manifest.model_dump_json(indent=2) + "\n")
    return manifest


if __name__ == "__main__":
    target = Path(sys.argv[1]) if len(sys.argv) > 1 else FIXTURE_DIR
    written = write_fixture(target)
    print(f"wrote {len(written.tables)} tables to {target}")

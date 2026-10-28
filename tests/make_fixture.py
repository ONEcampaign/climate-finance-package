"""Write the synthetic example release in src/climate_finance/data/example/, which
`cf.load_example()` opens, or a sample of a real release in
tests/fixtures/real_snapshot/.

The build extra loads only inside the functions that write a release, so the tests that
read the fixture import this module on a base install.

Run `uv run python tests/make_fixture.py [out_dir]`. The snapshot is schema-valid,
passes `release_invariants`, and covers six providers, a Bi_Multi 2 row, Bi_Multi 4
rows, an id-less multilateral row, a modelled-line provider, every status, every link
method, a CRS record split across two commitments with excess, a bank-ledger gap
fill, cross-cutting agency shares measured from markers and from components, a
climate share with no split by objective, core contributions for every flow,
provider coefficients that change between years, split by agency, hold a
both-significant coefficient or a case-by-case default, commitments reached through
certain and estimated CRDF routes, untraced excess and unlinked records, and
unallocated disbursements of each kind.
Amounts are invented.

Run `uv run python tests/make_fixture.py --from-release <dir> [out_dir]` to sample a
built release: about 300 commitments and their 2,000 or so CRS records from six
providers. The sample holds real OECD rows, so its directory is gitignored.
"""

import argparse
import itertools
from datetime import UTC, datetime
from pathlib import Path
from typing import NamedTuple

import polars as pl

from climate_finance import __version__
from climate_finance.convert import DAC_TOTAL
from climate_finance.release import EXAMPLE_DIR, SYNTHETIC_RELEASE
from climate_finance.schema import (
    CERTAIN_METHODS,
    CERTAIN_ROUTES,
    DATA_FORMAT,
    FLOWS,
    LINK_METHODS,
    RECIPIENT_VIEW,
    RELEASE_TABLES,
    BuildIdentity,
    ReleaseManifest,
    TableEntry,
    measured_adaptation,
    release_invariants,
    validate,
)

FIXTURE_DIR = EXAMPLE_DIR
REAL_SNAPSHOT_DIR = Path(__file__).parent / "fixtures" / "real_snapshot"
RELEASE = SYNTHETIC_RELEASE

# DAC codes: Germany, France, Norway, Japan, IDA, AfDB. IFC has only a modelled line.
# The EIB appears only in the sample of a real release.
DEU, FRA, NOR, JPN, IDA, AFDB, IFC, EIB = 5, 4, 8, 701, 905, 913, 903, 918
# The GEF reports only an unlinked record, in `unallocated`.
GEF = 811
IDA_CH, AFDB_CH = 44002, 46002
KEN, BGD, IRQ, UNS = 248, 666, 543, 998
ESP = 50
NO_RECORD = "no_disbursement_record"
# FRA-1's project pays earlier tranches, and AFDB-2 is the id-less multilateral row.
PROJECT_DISBURSING = {"FRA-1"}
FULLY_DISBURSED = {"IDA-1"}
# A row the allocation calls partly disbursed and the IDA ledger undisbursed, as a
# wrong repair link would leave it.
LEDGER_UNDISBURSED = {"IDA-2"}
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
    ("NOR-2", NOR, 1, IRQ, None, 1, 2022, 0, 1, None, 9e6, NO_RECORD),
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

# A later record that marks mitigation 0 where its commitment marks 1, as a CRS
# record may report its project another way than the commitment did. Its value
# still weighs by the commitment's markers.
RECORD_MITIGATION = {"JPN-2-R2": 0}

# CRDF rows carry their provider's class, Bi_Multi 1 for a bilateral provider, while
# each CRS record keeps its own code. NOR-1's records are Bi_Multi 3.
RECORD_BI_MULTI = {"NOR-1": 3}

# Share of face value disbursed through each commitment's own three records, by
# status. IDA-1 disburses its whole face value, which fills its high bound to face.
DISBURSED_SHARE = {
    "disbursed": 0.95,
    "partly_disbursed": 0.5,
    "pending_review": 0.3,
    "no_disbursement_record": 0.0,
    "not_traceable": 0.0,
}
# Commitments carry the CRDF's flow category: ODA (10) but for AFDB-2, an other
# official flow (21). Records carry CRS flow codes.
ODA_CATEGORY, OOF_CATEGORY = 10, 21
OTHER_OFFICIAL = {"AFDB-2"}
# A record's grant equivalent as a share of its disbursement, by flow code.
GRANT_EQUIV_RATIO = {11: 1.0, 13: 0.6}

# The records a certain link carries, which count in the low bound. Every other
# record's link is estimated. Each takes the next certain method, and the rest take
# the estimated methods in turn, so a new method changes no low bound. It needs a
# slot of its own, or the fixture refuses to build.
CERTAIN_RECORDS = frozenset(
    {
        "DEU-1-R0",
        "DEU-1-R1",
        "DEU-1-R2",
        "DEU-2-R0",
        "IDA-2-R0",
        "IDA-2-R1",
        "IDA-2-R2",
        "AFDB-1-R0",
    }
)

# One CRS record split across JPN-1 and JPN-3. The rest of its disbursement is
# excess, which reaches no commitment and so is untraced.
SPLIT_RECORD = "JPN-SPLIT"
SPLIT_YEAR = 2023
SPLIT_DISBURSEMENT = 30e6
SPLIT_GRANT_EQUIV = 18e6
# (row_id, allocated_usd, link_method, link_probability)
SPLIT_ALLOCATIONS = (
    ("JPN-1", 12e6, "crs_id", 1.0),
    ("JPN-3", 10e6, "pn_recipient", 0.85),
)
# A bank-ledger gap fill on AFDB-1 has no CRS link, so no method or probability.
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
# AfDB's OECD share for 2020: a climate total with no split by objective, as the
# OECD gives before 2021. AfDB has no ONE share that year.
CLIMATE_ONLY_SHARE = {
    "channel_code": AFDB_CH,
    "year": 2020,
    "objective": "climate",
    "share": 0.35,
    "source": "oecd",
}
# Core contributions disbursed in each of their years, and each flow's amount as a
# multiple.
CORE_CONTRIBUTIONS = (
    (DEU, IDA_CH, 60e6, (2021, 2022)),
    (FRA, AFDB_CH, 25e6, (2020, 2021, 2022)),
    (NOR, IDA_CH, 15e6, (2021, 2022)),
)
CORE_FLOW_FACTOR = {"commitments": 1.2, "disbursements": 1.0, "grant_equivalent": 1.0}
BTR_LINK_COLUMNS = (
    "row_id", "record_id", "link_method", "probability", "provider_code", "year",
    "project_number", "commitment_usd", "disbursement_usd",
)  # fmt: skip
# BTR rows' links to CRS records: Germany's 2021 programme reaches two components,
# Norway's 2021 row links by the model, and the 2022 rows stay UNFCCC only.
BTR_LINKS = (
    ("BTR-DEU-2021", "DEU-B-R1", "btr_id", 1.0, DEU, 2021, "300123-101", 15e6, 5e6),
    ("BTR-DEU-2021", "DEU-B-R2", "btr_id", 1.0, DEU, 2021, "300123-105", 7e6, 2e6),
    ("BTR-NOR-2021", "NOR-B-R1", "btr_splink", 0.93, NOR, 2021, None, None, 15e6),
)
# The OECD-only side of each BTR provider-year: bilateral climate-marked CRS amounts
# on records no BTR row links, in USD millions by provider, year and flow.
BTR_CRS_UNLINKED = (
    (DEU, 2021, 12, 4), (DEU, 2022, 9, 3), (NOR, 2021, 0, 0), (NOR, 2022, 6, 2),
)  # fmt: skip
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
# Rio-marked CRS records linked to no commitment: (record_id, provider, agency,
# recipient, year, flow_code, rio_adaptation, rio_mitigation, disbursement_usd,
# grant_equiv_usd). Germany's is principal for adaptation and Jev labels it
# significant; France's is significant for mitigation and has no grant equivalent.
UNLINKED = (
    ("DEU-UNLINKED", DEU, 52, KEN, 2022, 11, 2, 0, 2.5e6, 2.5e6),
    ("FRA-UNLINKED", FRA, 1, IRQ, 2021, 13, 0, 1, 3e6, None),
)
# Rio-marked disbursements that no total counts: (provider, year,
# outside_release_usd, fund_unlinked_usd). Excess and unlinked amounts come from
# `untraced`.
# The GEF, a route fund, has an unlinked record its IATI route covers.
UNALLOCATED = ((DEU, 2022, 4e6, 0.0), (GEF, 2021, 0.0, 1.5e6))


class Repayment(NamedTuple):
    """A CRS repayment record: its fields, its key group and the tier that linked it.

    `loan_id` is the key group (`tier:key`) a reflow link names, None where no tier
    reaches a loan. `rio` is the record's own Rio markers, which the loan's
    commitment markers replace in the weights.
    """

    record_id: str
    provider: int
    agency: int
    recipient: int
    year: int
    received: float
    interest: float
    loan_id: str | None
    link_method: str | None
    rio: tuple[int | None, int | None]
    bi_multi: int = 1
    channel: int | None = None


# CRS repayments. Each loan below names the CRDF rows whose CRS records it
# disbursed, or its records outright.
REPAYMENTS = (
    # Japan's loan behind JPN-1 repays through a certain crs id link. Its
    # disbursement takes in the split record, so the repayment reaches JPN-3 and the
    # split's excess too.
    Repayment("JPN-1-REPAY", JPN, 2, BGD, 2023, 2e6, 0.5e6, "crs_id:JPN-1", "crs_id",
              (0, 2)),
    # France's loan behind FRA-2 is counted at 40% (significant under ONE) and
    # repays in two years. Its disbursements carry estimated links.
    Repayment("FRA-2-REPAY-2022", FRA, 1, KEN, 2022, 0.5e6, 0.1e6, "crs_id:FRA-2",
              "crs_id", (1, 0)),
    Repayment("FRA-2-REPAY-2023", FRA, 1, KEN, 2023, 0.7e6, 0.1e6, "crs_id:FRA-2",
              "crs_id", (1, 0)),
    # Germany's loan behind DEU-1 has certain links all through.
    Repayment("DEU-1-REPAY", DEU, 52, KEN, 2022, 4e6, 0.3e6, "crs_id:DEU-1",
              "crs_id", (2, 0)),
    # The IDA repays IDA-2 under a fresh crs id, which the loan number ties back. It
    # reports no interest, and counts at the commitment's climate share.
    Repayment("IDA-2-REPAY", IDA, 1, BGD, 2023, 5e6, 0.0, "wb_loan:IDA-2",
              "wb_loan", (None, None), bi_multi=4, channel=IDA_CH),
    # Norway's repayments of NOR-1 pass what it disbursed (17.1m) in 2023.
    Repayment("NOR-1-REPAY-2022", NOR, 1, BGD, 2022, 10e6, 0.4e6, "crs_id:NOR-1",
              "crs_id", (2, 1), bi_multi=3),
    Repayment("NOR-1-REPAY-2023", NOR, 1, BGD, 2023, 10e6, 0.3e6, "crs_id:NOR-1",
              "crs_id", (2, 1), bi_multi=3),
    # France's loan behind its unlinked record repays at the record's own markers.
    Repayment("FRA-UNLINKED-REPAY", FRA, 1, IRQ, 2023, 0.6e6, 0.1e6,
              "crs_id:FRA-UNLINKED", "crs_id", (0, 1)),
    # NOR-2's key reaches its commitment, which disbursed nothing.
    Repayment("NOR-2-REPAY", NOR, 1, IRQ, 2023, 0.4e6, 0.05e6, "crs_id:NOR-2",
              "crs_id", (0, 1)),
    # Germany's under no key a tier reaches is unlinked.
    Repayment("DEU-REPAY", DEU, 52, KEN, 2022, 1e6, 0.2e6, None, None, (0, 1)),
    # Spain's loan predates the window, so no disbursement ties to its repayment, and
    # the record's own marker of 2 weighs nothing.
    Repayment("ESP-REPAY", ESP, 1, KEN, 2022, 8e6, 1e6, None, None, (2, 0)),
    # A loan whose disbursements the release leaves out in three ways.
    Repayment("DEU-OUT-REPAY", DEU, 52, KEN, 2023, 4e6, 0.2e6, "pn_recipient:DEU-OUT",
              "pn_recipient", (0, 0)),
)  # fmt: skip
# The CRDF rows whose CRS records each loan disbursed.
LOAN_ROWS = {
    "crs_id:JPN-1": ("JPN-1",),
    "crs_id:FRA-2": ("FRA-2",),
    "crs_id:DEU-1": ("DEU-1",),
    "wb_loan:IDA-2": ("IDA-2",),
    "crs_id:NOR-1": ("NOR-1",),
    "crs_id:NOR-2": ("NOR-2",),
}
# Loans reached through their disbursement records alone. DEU-OUT-A is Germany's
# Rio-marked record allocated to a commitment with no CRDF row, which `UNALLOCATED`
# holds as its 2022 outside-release amount. DEU-OUT-B is an unmarked record's
# excess and DEU-OUT-C an unmarked record linked to no commitment. A release holds
# no unmarked record's amounts, so those two sit in no table but the CRS.
LOAN_RECORDS = {
    "crs_id:FRA-UNLINKED": ("FRA-UNLINKED",),
    "pn_recipient:DEU-OUT": ("DEU-OUT-A", "DEU-OUT-B", "DEU-OUT-C"),
}
# (record_id, rule, allocated_usd) of an allocation to no CRDF row, and
# (record_id, disbursement_usd) of a record linked to no commitment.
OFF_ROW_ALLOCATIONS = (("DEU-OUT-A", "fifo", 4e6), ("DEU-OUT-B", "excess", 2e6))
OFF_ROW_RECORDS = (("DEU-OUT-C", 1e6),)
# Conversion factors for France, Germany, Norway, Japan and the DAC total in 2020 to
# 2024, at current prices and in 2022 and 2024 prices. Every currency but USD
# takes its rate per US dollar, raised 1% per provider in this order. A base year's
# factor rises 2% for each year before it.
CURRENCY_RATES = {"USD": 1.0, "EUR": 0.9, "GBP": 0.8, "CAD": 1.3}
FACTOR_ENTITIES = (FRA, DEU, NOR, JPN, DAC_TOTAL)
FACTOR_YEARS = range(2020, 2025)
FACTOR_BASE_YEARS = (None, 2022, 2024)
# The build the fixture claims, with fixed digests so the fixture stays the same
# whichever package and environment regenerate it.
BUILD = BuildIdentity(
    package_source_sha256="1" * 64,
    python="3.13.0",
    environment_sha256="2" * 64,
    dependencies={"polars": "2.0.0"},
    stage_identities={"fetch": "3" * 64},
)
# The build settings the fixture claims, as `release.json` records them.
SETTINGS = {
    "offline": True,
    "key_repair": True,
    "recoveries": True,
    "gap_fill": False,
    "review": False,
    "jev_labels": False,
    "jev_links": False,
    "embed": False,
    "jev_flags": False,
    "skip_eib_routes": [],
    "jev_flags_first_year": 2015,
    "years.crdf": [2021, 2022],
    "years.crs": [2021, 2023],
    "years.crs_last_complete": 2023,
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
    record_disbursement: float,
    record_grant_equiv: float | None,
    link_method: str | None,
    link_probability: float | None,
    rule: str,
    disbursement_source: str = "crs",
) -> dict[str, object]:
    """A disbursements row allocating `allocated` of `record_id` to `commitment`.

    `record_disbursement` and `record_grant_equiv` are the whole CRS record's.
    Its high bound is `allocated`. The row counts in
    the low bound when a `CERTAIN_METHODS` link carried it, and a bank-ledger fill
    counts in full. A fill has no CRS record, so it carries its commitment's facts,
    no flow code and no record markers, as `release.fill_disbursements` builds it.
    """
    certain = link_method is None or link_method in CERTAIN_METHODS
    fill = disbursement_source != "crs"
    first = record_id.endswith("-R0")
    rio_a, rio_m = commitment["rio_adaptation"], commitment["rio_mitigation"]
    record_rio_a = None if first or fill else rio_a
    record_rio_m = None if fill else RECORD_MITIGATION.get(record_id, rio_m)
    # As label.inherit_markers sets it.
    if commitment["climate_share"] is not None:
        marker_source = "components"
    elif fill or (record_rio_a == rio_a and record_rio_m == rio_m):
        marker_source = "commitment"
    else:
        marker_source = "record_differs"
    return {
        "row_id": commitment["row_id"],
        "record_id": record_id,
        "year": year,
        "provider_code": commitment["provider_code"],
        "agency_code": commitment["agency_code"],
        "recipient_code": commitment["recipient_code"],
        "channel_code": commitment["channel_code"],
        "bi_multi": commitment["bi_multi"]
        if fill
        else RECORD_BI_MULTI.get(str(commitment["row_id"]), commitment["bi_multi"]),
        "purpose_code": commitment["purpose_code"] if fill else 41010,
        "flow_code": None if fill else _flow_code(commitment),
        "project_number": commitment["project_number"]
        if fill
        else f"PN-{commitment['row_id']}",
        "title": f"Climate project {commitment['row_id']}",
        "allocated_usd": allocated,
        "allocated_low_usd": allocated if certain else 0.0,
        "record_disbursement_usd": record_disbursement,
        "record_grant_equiv_usd": record_grant_equiv,
        "inherited_rio_adaptation": rio_a,
        "inherited_rio_mitigation": rio_m,
        "record_rio_adaptation": record_rio_a,
        "record_rio_mitigation": record_rio_m,
        "marker_source": marker_source,
        "climate_share": commitment["climate_share"],
        "link_method": link_method,
        "link_probability": link_probability,
        "rule": rule,
        "disbursement_source": disbursement_source,
        "ledger_check": "confirmed" if commitment["provider_code"] == IDA else None,
    }


# The facts an untraced excess row takes from its record's disbursements row.
UNTRACED_FACTS = (
    "record_id",
    "year",
    "provider_code",
    "agency_code",
    "recipient_code",
    "channel_code",
    "bi_multi",
    "purpose_code",
    "flow_code",
    "project_number",
    "title",
)


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
            (
                "mitigation",
                RECORD_MITIGATION.get(record_id, commitment["rio_mitigation"]),
                "marker",
            ),
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
    from climate_finance.build.release import btr_totals

    certain_methods = itertools.cycle(CERTAIN_METHODS)
    estimated_methods = itertools.cycle(
        [method for method in LINK_METHODS if method not in CERTAIN_METHODS]
    )
    commitments: dict[str, dict[str, object]] = {}
    disbursement_rows: list[dict[str, object]] = []
    objective_rows: list[dict[str, object]] = []

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
            "flow_category": OOF_CATEGORY if row_id in OTHER_OFFICIAL else ODA_CATEGORY,
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
            "ledger_undisbursed": True if row_id in LEDGER_UNDISBURSED else None,
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
        # The CRS gives each commitment's first record no grant equivalent, and
        # `normalise_crs` gives a grant's its face value, so only loans lack one.
        flow_code = _flow_code(commitment)
        for index in range(3):
            record_id = f"{row_id}-R{index}"
            grant_equiv = allocated * GRANT_EQUIV_RATIO[flow_code]
            disbursement_rows.append(
                _allocation(
                    commitment,
                    record_id=record_id,
                    year=year + index,
                    allocated=allocated,
                    record_disbursement=allocated,
                    record_grant_equiv=grant_equiv
                    if index or flow_code == 11
                    else None,
                    link_method=next(
                        certain_methods
                        if record_id in CERTAIN_RECORDS
                        else estimated_methods
                    ),
                    link_probability=1.0 if index == 0 else 0.85,
                    rule="dated" if index == 0 else "fifo",
                )
            )
            objective_rows.extend(_objectives(commitment, record_id))

    used = {row["link_method"] for row in disbursement_rows}
    if missing := set(LINK_METHODS) - used:
        raise ValueError(f"fixture misses link methods {sorted(missing)}")

    for row_id, allocated, link_method, probability in SPLIT_ALLOCATIONS:
        disbursement_rows.append(
            _allocation(
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
        )
    objective_rows.extend(_objectives(commitments["JPN-1"], SPLIT_RECORD))
    split = next(row for row in disbursement_rows if row["record_id"] == SPLIT_RECORD)
    untraced_rows = [
        {
            **{column: split[column] for column in UNTRACED_FACTS},
            "rio_adaptation": split["record_rio_adaptation"],
            "rio_mitigation": split["record_rio_mitigation"],
            "untraced_usd": SPLIT_DISBURSEMENT
            - sum(allocated for _, allocated, *_ in SPLIT_ALLOCATIONS),
            "reason": "excess",
            "record_disbursement_usd": SPLIT_DISBURSEMENT,
            "record_grant_equiv_usd": SPLIT_GRANT_EQUIV,
        }
    ]
    for (
        record_id, provider, agency, recipient, year, flow_code, rio_a, rio_m,
        disbursement, grant_equiv,
    ) in UNLINKED:  # fmt: skip
        untraced_rows.append(
            {
                "record_id": record_id,
                "year": year,
                "provider_code": provider,
                "agency_code": agency,
                "recipient_code": recipient,
                "channel_code": None,
                "bi_multi": 1,
                "purpose_code": 41010,
                "flow_code": flow_code,
                "project_number": f"PN-{record_id}",
                "title": f"Climate project {record_id}",
                "rio_adaptation": rio_a,
                "rio_mitigation": rio_m,
                "untraced_usd": disbursement,
                "reason": "unlinked",
                "record_disbursement_usd": disbursement,
                "record_grant_equiv_usd": grant_equiv,
            }
        )
        objective_rows.extend(
            {
                "record_id": record_id,
                "objective": objective,
                "level": level,
                "label_source": "marker",
                "probability": None,
            }
            for objective, level in (("adaptation", rio_a), ("mitigation", rio_m))
        )
    disbursement_rows.append(
        _allocation(
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
    )
    disbursement_rows[-1]["ledger_check"] = None
    # A Jev label on a record of an adaptation-marked commitment: principal, where
    # JPN-2 and the record mark adaptation significant. Another on an untraced
    # record: significant, where the record marks adaptation principal.
    objective_rows.extend(
        {
            "record_id": record_id,
            "objective": "adaptation",
            "level": level,
            "label_source": "jev",
            "probability": probability,
        }
        for record_id, level, probability in (
            ("JPN-2-R1", 2, 0.72),
            ("DEU-UNLINKED", 1, 0.64),
        )
    )

    tables = {
        "disbursements": disbursement_rows,
        "untraced": untraced_rows,
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
        ]
        + [CLIMATE_ONLY_SHARE],
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
                "published": None if components else 1.0,
                "adaptation_measured": True,
                "share_source": "computed",
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
            for provider, channel, amount, years in CORE_CONTRIBUTIONS
            for year in years
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
                "provider_code": provider,
                "year": year,
                "recipient": "Kenya",
                "recipient_code": KEN,
                "title": f"Adaptation programme {party} {year}",
                "amount_domestic": amount,
                "grant_equiv_domestic": amount,
                "currency": currency,
                "face_usd": amount * rate,
                "grant_equiv_usd": amount * rate,
                "dac_rate": 1 / rate,
                "face_usd_at_dac": amount * rate,
                "grant_equiv_usd_at_dac": amount * rate,
                "status": status,
                "channel": "bilateral",
                "funding_source": "ODA",
                "instrument": "grant",
                "support_type": "adaptation",
                "sector": "agriculture",
                "additional_information": None,
                "is_aggregate": False,
                "source_file": f"{party}-BTR1.xlsx",
            }
            for party, provider, currency, rate, amount, status in (
                ("DEU", DEU, "EUR", 1.1, 20e6, "committed"),
                ("NOR", NOR, "NOK", 0.1, 150e6, "disbursed"),
            )
            for year in (2021, 2022)
        ],
        "btr_links": [
            dict(zip(BTR_LINK_COLUMNS, link, strict=True))
            | {
                "recipient_code": KEN,
                "title": f"Adaptation programme {link[1]}",
                "rio_adaptation": 2,
                "rio_mitigation": 0,
            }
            for link in BTR_LINKS
        ],
        "btr_crs_unlinked": [
            {
                "provider_code": provider,
                "year": year,
                "flow": flow,
                "amount_usd": amount * 1e6,
            }
            for provider, year, committed, disbursed in BTR_CRS_UNLINKED
            for flow, amount in (
                ("commitments", committed),
                ("disbursements", disbursed),
            )
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
        # The fixture claims a build with `jev_flags` off.
        "jev_flags": [],
        "conversion_factors": [
            {
                "currency": currency,
                "base_year": base_year,
                "entity_code": entity,
                "year": year,
                "factor": (rate * (1 + 0.01 * index) if currency != "USD" else 1.0)
                * (1 + 0.02 * (base_year - year) if base_year else 1.0),
            }
            for currency, rate in CURRENCY_RATES.items()
            for base_year in FACTOR_BASE_YEARS
            # USD at current prices needs no factor.
            if (currency, base_year) != ("USD", None)
            for index, entity in enumerate(FACTOR_ENTITIES)
            for year in FACTOR_YEARS
        ],
    }
    frames = {
        name: pl.from_dicts(rows, schema=_polars_schema(name))
        for name, rows in tables.items()
    }
    frames["commitments"] = _commitments(
        list(commitments.values()), frames["disbursements"]
    )
    frames["reflows"] = _reflows(
        frames["commitments"], frames["disbursements"], frames["untraced"]
    )
    frames["quality"] = _quality(frames["commitments"], frames["disbursements"])
    frames["btr_totals"] = btr_totals(
        frames["btr"].lazy(), links=frames["btr_links"].lazy()
    ).collect()
    frames["unallocated"] = _unallocated(frames["untraced"], frames["disbursements"])
    frames["total_flows"] = _total_flows(
        frames["commitments"],
        pl.concat(
            [
                frames["disbursements"].select(
                    *TOTAL_KEYS,
                    "allocated_usd",
                    **_markers("inherited_rio_adaptation", "inherited_rio_mitigation"),
                ),
                frames["untraced"].select(
                    *TOTAL_KEYS,
                    allocated_usd="untraced_usd",
                    **_markers("rio_adaptation", "rio_mitigation"),
                ),
            ]
        ),
        frames["core_contributions"],
    )
    return frames


def _reflows(
    commitments: pl.DataFrame, disbursements: pl.DataFrame, untraced: pl.DataFrame
) -> pl.DataFrame:
    """`REPAYMENTS` split over their loans' disbursements by the build's own
    `loan_shares`, `attribute_reflows` and `release_reflows`."""
    from climate_finance.build.reflows import (
        attribute_reflows,
        loan_shares,
        release_reflows,
    )

    crs = disbursements.filter(pl.col("disbursement_source") == "crs")
    loans = pl.DataFrame(
        [
            {"loan_id": loan_id, "record_id": record_id}
            for loan_id, row_ids in LOAN_ROWS.items()
            for record_id in crs.filter(pl.col("row_id").is_in(row_ids))["record_id"]
        ]
        + [
            {"loan_id": loan_id, "record_id": record_id}
            for loan_id, record_ids in LOAN_RECORDS.items()
            for record_id in record_ids
        ]
    ).unique()
    # The fill's allocations: the release's CRS rows without its markers, a linked
    # record's excess as a row of its own, and the allocations of records the
    # release leaves out.
    allocations = pl.concat(
        [
            crs.select(
                "record_id",
                "row_id",
                "rule",
                "allocated_usd",
                "allocated_low_usd",
                "disbursement_source",
            ),
            untraced.filter(pl.col("reason") == "excess").select(
                "record_id",
                row_id=pl.lit(None, pl.String),
                rule=pl.lit("excess"),
                allocated_usd="untraced_usd",
                allocated_low_usd=pl.lit(0.0),
                disbursement_source=pl.lit("crs"),
            ),
            pl.DataFrame(
                [
                    {
                        "record_id": record_id,
                        "row_id": None,
                        "rule": rule,
                        "allocated_usd": allocated,
                        "allocated_low_usd": 0.0,
                        "disbursement_source": "crs",
                    }
                    for record_id, rule, allocated in OFF_ROW_ALLOCATIONS
                ],
                schema_overrides={"row_id": pl.String},
            ),
        ]
    )
    unlinked = pl.concat(
        [
            untraced.filter(pl.col("reason") == "unlinked").select(
                "record_id", disbursement_usd="untraced_usd"
            ),
            pl.DataFrame(
                OFF_ROW_RECORDS,
                schema={"record_id": pl.String, "disbursement_usd": pl.Float64},
                orient="row",
            ),
        ]
    )
    shares = loan_shares(
        loans.lazy(),
        allocations=allocations.lazy(),
        untraced=untraced.lazy(),
        crs=unlinked.lazy(),
    )
    records = pl.DataFrame(
        [
            {
                "record_id": repayment.record_id,
                "year": repayment.year,
                "donor_code": repayment.provider,
                "agency_code": repayment.agency,
                "recipient_code": repayment.recipient,
                "channel_code": repayment.channel,
                "bi_multi": repayment.bi_multi,
                "purpose_code": 41010,
                "flow_code": 13,
                "finance_t": 421,
                "project_number": f"PN-{repayment.record_id}",
                "title": f"Loan repayment {repayment.record_id}",
                "rio_adaptation": repayment.rio[0],
                "rio_mitigation": repayment.rio[1],
                "received_usd": repayment.received,
                "interest_usd": repayment.interest,
            }
            for repayment in REPAYMENTS
        ],
        schema_overrides={
            "bi_multi": pl.Int8,
            "rio_adaptation": pl.Int8,
            "rio_mitigation": pl.Int8,
        },
    )
    linked = [repayment for repayment in REPAYMENTS if repayment.loan_id is not None]
    attributed = attribute_reflows(
        records.lazy(),
        reflow_units=pl.LazyFrame(
            {
                "unit_id": [repayment.record_id for repayment in REPAYMENTS],
                "record_ids": [[repayment.record_id] for repayment in REPAYMENTS],
            }
        ),
        reflow_links=pl.LazyFrame(
            {
                "reflow_unit_id": [repayment.record_id for repayment in linked],
                "group_id": [repayment.loan_id for repayment in linked],
                "link_method": [repayment.link_method for repayment in linked],
                "records_disagree": [False] * len(linked),
            }
        ),
        shares=shares,
    )
    return (
        release_reflows(attributed, reflows=records.lazy(), crdf=commitments.lazy())
        .sort("record_id", "basis", "row_id", "inherited_rio_adaptation")
        .select(
            pl.col(name).cast(dtype)
            for name, dtype in _polars_schema("reflows").items()
        )
        .collect()
    )


def _commitments(
    rows: list[dict[str, object]], disbursements: pl.DataFrame
) -> pl.DataFrame:
    """Commitments with disbursed amounts summed from their allocations."""
    disbursed = disbursements.group_by("row_id").agg(
        disbursed_usd=pl.col("allocated_usd").sum(),
        disbursed_low_usd=pl.col("allocated_low_usd").sum(),
    )
    return (
        pl.from_dicts(rows, infer_schema_length=None)
        .join(disbursed, on="row_id", how="left")
        .with_columns(pl.col("^disbursed_.*$").fill_null(0.0), withheld_usd=0.0)
        .select(
            pl.col(name).cast(dtype)
            for name, dtype in _polars_schema("commitments").items()
        )
    )


def _polars_schema(table: str) -> pl.Schema:
    return pl.Schema(RELEASE_TABLES[table].dtypes)


# The keys of `total_flows`.
TOTAL_KEYS = ("provider_code", "agency_code", "recipient_code", "year", "bi_multi")
# Norway's 2023 bilateral disbursement to Kenya, with no Rio marker, on a key no
# climate row holds, as (provider, agency, recipient, year, amount).
UNMARKED_DISBURSEMENT = (NOR, 1, KEN, 2023, 6e6)


def _total_flows(
    commitments: pl.DataFrame,
    disbursements: pl.DataFrame,
    core_contributions: pl.DataFrame,
) -> pl.DataFrame:
    """All CRS flows: four times the climate rows' amounts (0.9 of that for grant
    equivalent), plus 1.5 times each flow's core contributions to unspecified
    recipients, so every climate figure has a larger total, and
    `UNMARKED_DISBURSEMENT` among the disbursements. `disbursements` holds the
    traced and untraced disbursements' keys, Rio markers and amounts."""
    keys = [*TOTAL_KEYS, "rio_adaptation", "rio_mitigation"]
    committed = commitments.select(
        *TOTAL_KEYS,
        **_markers("rio_adaptation", "rio_mitigation"),
        amount_usd=pl.col("face_usd") * 4,
    )
    provider, agency, recipient, year, amount = UNMARKED_DISBURSEMENT
    unmarked = pl.DataFrame(
        {
            "provider_code": [provider],
            "agency_code": [agency],
            "recipient_code": [recipient],
            "year": [year],
            "bi_multi": [1],
            "rio_adaptation": [0],
            "rio_mitigation": [0],
            "amount_usd": [amount],
        },
        schema=committed.schema,
    )
    disbursed = pl.concat(
        [
            disbursements.select(*keys, amount_usd=pl.col("allocated_usd") * 4),
            unmarked,
        ]
    )
    climate = {
        "commitments": committed,
        "disbursements": disbursed,
        "grant_equivalent": disbursed.with_columns(pl.col("amount_usd") * 0.9),
    }
    core = core_contributions.select(
        "provider_code",
        agency_code=pl.lit(None, pl.Int32),
        recipient_code=pl.lit(UNS, pl.Int32),
        year="year",
        bi_multi=pl.lit(2, pl.Int8),
        rio_adaptation=pl.lit(0, pl.Int8),
        rio_mitigation=pl.lit(0, pl.Int8),
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


def _markers(adaptation: str, mitigation: str) -> dict[str, pl.Expr]:
    """The `total_flows` markers from columns `adaptation` and `mitigation`."""
    return {
        "rio_adaptation": pl.col(adaptation).fill_null(0).cast(pl.Int8),
        "rio_mitigation": pl.col(mitigation).fill_null(0).cast(pl.Int8),
    }


# Statuses of commitments that trace to at least one CRS record.
TRACED = ("disbursed", "partly_disbursed", "no_disbursement_record")


def _quality(commitments: pl.DataFrame, disbursements: pl.DataFrame) -> pl.DataFrame:
    """Climate value by provider and commitment year: all of it, the part that
    traces, the traced part whose CRDF route is certain, and that of the high- and
    low-bound disbursements; and the share of the traced commitments' allocated
    value whose record has a grant equivalent."""
    allocated = pl.col("allocated_usd")
    allocations = disbursements.group_by("row_id").agg(
        allocated=allocated.sum(),
        low=pl.col("allocated_low_usd").sum(),
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
            disbursed_climate_usd=(
                climate * pl.col("allocated") / pl.col("face_usd")
            ).sum(),
            certain_disbursed_climate_usd=(
                climate * pl.col("low") / pl.col("face_usd")
            ).sum(),
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
            repeat_climate_usd=pl.lit(0.0),
        )
        .select(list(RELEASE_TABLES["quality"].columns))
    )


def _unallocated(untraced: pl.DataFrame, disbursements: pl.DataFrame) -> pl.DataFrame:
    """Per provider and disbursement year, the untraced excess and unlinked
    amounts, with `UNALLOCATED`'s amounts outside the release and of route funds,
    and `marked_usd`, their sum with the allocations of the Rio-marked CRS records
    of `disbursements`."""
    key = ["provider_code", "year"]
    amount = pl.col("untraced_usd")
    excess = untraced.group_by(key).agg(
        excess_usd=amount.filter(pl.col("reason") == "excess").sum(),
        unlinked_marked_usd=amount.filter(pl.col("reason") == "unlinked").sum(),
    )
    other = pl.DataFrame(
        UNALLOCATED,
        schema=[*key, "outside_release_usd", "fund_unlinked_usd"],
        orient="row",
    )
    released = _marked_allocations(disbursements)
    columns = _polars_schema("unallocated")
    return (
        excess.join(
            other.cast({name: columns[name] for name in other.columns}),
            on=key,
            how="full",
            coalesce=True,
        )
        .join(released, on=key, how="full", coalesce=True)
        .with_columns(pl.col("^.*_usd$").fill_null(0.0))
        .with_columns(marked_usd=pl.sum_horizontal("^.*_usd$"))
        .filter(pl.col("marked_usd") > 0)
        .select(pl.col(name).cast(dtype) for name, dtype in columns.items())
        .sort(key)
    )


def _marked_allocations(disbursements: pl.DataFrame) -> pl.DataFrame:
    """Per provider and year, `released_usd`: the allocated amounts of the
    `disbursements` rows of Rio-marked CRS records, as `release_invariants` sums
    them."""
    marked = (measured_adaptation(pl.col("record_rio_adaptation")) > 0) | (
        pl.col("record_rio_mitigation") > 0
    )
    return (
        disbursements.filter(
            pl.col("disbursement_source") == "crs",
            pl.col("bi_multi").is_in(RECIPIENT_VIEW),
            marked.fill_null(value=False),
        )
        .group_by("provider_code", "year")
        .agg(released_usd=pl.col("allocated_usd").sum())
    )


def write_fixture(out_dir: Path) -> ReleaseManifest:
    """Write every release table and `release.json` to `out_dir`."""
    from climate_finance.build.fetch import BTR_SOURCE

    return _write_release(
        build_tables(),
        out_dir,
        manifest=ReleaseManifest(
            release=RELEASE,
            data_format=DATA_FORMAT,
            created_at=datetime(2026, 10, 21, tzinfo=UTC),
            package_version=__version__,
            build=BUILD,
            tables={},
            source_versions={
                "crs": "fixture",
                "crdf": "fixture",
                BTR_SOURCE: "fixture",
                "conversion_factors": "fixture",
            },
            model_versions={"jev": "jev-1.14.0"},
            gates={},
            settings=SETTINGS,
            precision_gate_waived=False,
        ),
    )


# The real sample: Germany, France, Japan, IDA, AfDB and the EIB, whose rows include
# repeats of an earlier vintage, children of aggregate rows, climate components and
# the modelled line.
SAMPLE_PROVIDERS = (DEU, FRA, JPN, IDA, AFDB, EIB)
SAMPLE_COMMITMENTS = 50
# Per provider: up to this many commitments of each status, repeats whose first row
# holds allocations, and children of aggregate rows. The rest of the provider's 50
# are commitments with at least `SAMPLE_MIN_RECORDS` CRS records, then any others.
# Each sampled repeat brings its first row.
SAMPLE_PER_KIND = 3
SAMPLE_MIN_RECORDS = 8
SAMPLE_SEED = 7
# Tables sampled by provider. The rest are copied whole.
BY_PROVIDER = (
    "core_contributions",
    "quality",
    "modelled",
    "total_flows",
    "untraced",
    "unallocated",
    "jev_flags",
)


def sample_release(release: Path) -> dict[str, pl.DataFrame]:
    """The release tables of `release` cut to a sample of `SAMPLE_PROVIDERS`.

    Each commitment keeps every allocation and each record its objective labels, so
    the commitments' disbursed amounts still sum from their allocations. Each repeat
    keeps the first row it repeats, which holds its allocations. `quality`,
    `modelled`, `core_contributions`, `total_flows`, `untraced`, `unallocated` and
    `jev_flags` keep the six providers' rows for the whole release, with
    `unallocated.marked_usd` less the Rio-marked allocations the sample leaves out.
    `reflows` keeps every row of the six providers' repayments whose commitment rows
    are all in the sample.
    """
    tables = {
        name: pl.read_parquet(release / f"{name}.parquet") for name in RELEASE_TABLES
    }
    disbursements = tables["disbursements"]
    records = disbursements.group_by("row_id").agg(
        records=pl.col("record_id").n_unique()
    )
    candidates = (
        tables["commitments"]
        .filter(pl.col("provider_code").is_in(SAMPLE_PROVIDERS))
        .join(records, on="row_id", how="left")
        .with_columns(
            pl.col("records").fill_null(0),
            order=pl.col("row_id").hash(SAMPLE_SEED),
        )
        .sort("order", "row_id")
    )
    kinds = [
        pl.col("status") == status for status in candidates["status"].unique().sort()
    ]
    allocated = candidates.filter(pl.col("records") > 0)["row_id"].implode()
    kinds += [
        pl.col("repeat_of").is_in(allocated),
        pl.col("parent_row_id").is_not_null(),
    ]
    chosen = []
    for provider in SAMPLE_PROVIDERS:
        rows = candidates.filter(pl.col("provider_code") == provider)
        picked = pl.concat(rows.filter(kind).head(SAMPLE_PER_KIND) for kind in kinds)
        picked = picked.unique("row_id", keep="first", maintain_order=True)
        rest = (
            rows.join(picked, on="row_id", how="anti")
            .with_columns(few=pl.col("records") < SAMPLE_MIN_RECORDS)
            .sort("few", maintain_order=True)
            .drop("few")
        )
        chosen.append(pl.concat([picked, rest]).head(SAMPLE_COMMITMENTS)["row_id"])
    picked_ids = pl.concat(chosen)
    first_rows = tables["commitments"].filter(
        pl.col("row_id").is_in(picked_ids.implode())
    )["repeat_of"]
    row_ids = pl.concat([picked_ids, first_rows.drop_nulls()]).unique().implode()

    sample = dict(tables)
    sample["commitments"] = tables["commitments"].filter(
        pl.col("row_id").is_in(row_ids)
    )
    sample["disbursements"] = disbursements.filter(pl.col("row_id").is_in(row_ids))
    sample["objectives"] = tables["objectives"].join(
        sample["disbursements"].select("record_id").unique(), on="record_id"
    )
    for name in BY_PROVIDER:
        sample[name] = tables[name].filter(
            pl.col("provider_code").is_in(SAMPLE_PROVIDERS)
        )
    # Every row of each repayment whose commitment rows the sample keeps, so its
    # rows still sum to the record's repayment.
    dropped_rows = tables["reflows"].filter(
        pl.col("row_id").is_not_null(), ~pl.col("row_id").is_in(row_ids)
    )
    sample["reflows"] = (
        tables["reflows"]
        .filter(pl.col("provider_code").is_in(SAMPLE_PROVIDERS))
        .join(dropped_rows.select("record_id"), on="record_id", how="anti")
    )
    # `marked_usd` leaves out the Rio-marked allocations the sample drops, so the
    # sample's parts still sum to it.
    key = ["provider_code", "year"]
    dropped = _marked_allocations(
        disbursements.filter(~pl.col("row_id").is_in(row_ids))
    )
    sample["unallocated"] = (
        sample["unallocated"]
        .join(dropped, on=key, how="left")
        .with_columns(
            marked_usd=pl.col("marked_usd") - pl.col("released_usd").fill_null(0.0)
        )
        .drop("released_usd")
    )
    return sample


def write_sample(release: Path, out_dir: Path) -> ReleaseManifest:
    """Write a sample of `release` to `out_dir`, named after it."""
    source = ReleaseManifest.model_validate_json(
        (release / "release.json").read_text(encoding="utf-8")
    )
    return _write_release(
        sample_release(release),
        out_dir,
        manifest=source.model_copy(
            update={"release": f"{source.release}-sample", "tables": {}}
        ),
    )


def _write_release(
    tables: dict[str, pl.DataFrame], out_dir: Path, *, manifest: ReleaseManifest
) -> ReleaseManifest:
    """Validate and write `tables` and `release.json`, `manifest` with their hashes."""
    from climate_finance.build import write_table

    for name, frame in tables.items():
        validate(RELEASE_TABLES[name], frame)
    if problems := release_invariants(tables):
        raise ValueError(f"release breaks its invariants: {problems}")
    entries = {}
    for name, frame in tables.items():
        sha256 = write_table(frame, RELEASE_TABLES[name], out_dir / f"{name}.parquet")
        entries[name] = TableEntry(sha256=sha256, rows=frame.height)
    written = manifest.model_copy(update={"tables": entries})
    (out_dir / "release.json").write_text(
        written.model_dump_json(indent=2) + "\n", encoding="utf-8"
    )
    return written


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("out_dir", nargs="?", type=Path)
    parser.add_argument("--from-release", type=Path, metavar="DIR")
    args = parser.parse_args()
    if args.from_release is None:
        target = args.out_dir or FIXTURE_DIR
        written = write_fixture(target)
    else:
        target = args.out_dir or REAL_SNAPSHOT_DIR
        written = write_sample(args.from_release, target)
    rows = {name: entry.rows for name, entry in written.tables.items()}
    print(f"wrote {len(rows)} tables to {target}: {rows}")

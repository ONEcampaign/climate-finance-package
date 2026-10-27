"""CRDF provider and recipient rows into the `crdf` schema."""

import logging
from typing import Literal

import polars as pl

from climate_finance.build.errors import BuildError
from climate_finance.build.normalise.ids import content_id
from climate_finance.schema import (
    ADAPTATION_FIRST_YEAR,
    CORE_CONTRIBUTION,
    EIB_AGENCY,
    EU_INSTITUTIONS,
)

logger = logging.getLogger(__name__)

# oda-reader's `download_crdf` gives amounts in USD thousand.
_USD_PER_UNIT = 1e3
_AMOUNTS = {
    "adaptation_usd": "adaptation_current_kusd",
    "mitigation_usd": "mitigation_current_kusd",
    "overlap_usd": "overlap_current_kusd",
    "climate_usd": "climate_current_kusd",
}
# Rio marker labels as the CRDF prints them. Rows that are not Rio-marked carry the
# label of their method in both marker columns and get null markers.
_MARKERS: dict[str, int | None] = {
    "Principal": 2,
    "Significant": 1,
    "Not targeted/Not screened": 0,
    "Climate Components": None,
    "Imputed Multilateral Contribution": None,
}
_IMPUTED = "Imputed Multilateral Contribution"
# The columns that measure adaptation, null before `ADAPTATION_FIRST_YEAR`.
_ADAPTATION_COLUMNS = ("rio_adaptation", "adaptation_usd", "overlap_usd")
# Text Excel writes in place of a formula it could not evaluate.
_EXCEL_ERRORS = ("#NAME?", "#VALUE!", "#REF!", "#DIV/0!", "#N/A", "#NUM!", "#NULL!")
# Excel read some `Donor project N°` cells (such as "1/2015" or "Aug-18") as dates,
# and they reach the parquet as midnight timestamps.
_DATE_DAMAGED = r"^\d{4}-\d{2}-\d{2} 00:00:00$"
_TEXT_COLUMNS = ("crs_id", "project_number", "project_title", "description")

# The EIB reports to the CRDF as agency 3 of EU Institutions (918). Its ids follow a
# different scheme in each period, and no transform maps one onto another: none in
# 2015, EIB serials (year and four digits, letters for components) to 2020, reporting
# sequences (year and six digits, or year, "PSI" and a number) in 2019 to 2023, and
# bulk CRS ids (year, agency 3, project number, suffix) from 2024.
_EIB_SERIAL = r"^\d{8}[a-z]*$"
_EIB_SEQUENCE = r"^\d{4}(\d{6}|PSI\d+)[a-z]*$"
ID_SCHEMES = ("none", "eib_serial", "sequence", "bulk")

_IDENTITY = (
    "perspective",
    "year",
    "provider_code",
    "agency_code",
    "crs_id",
    "project_number",
    "recipient_code",
    "purpose_code",
    "channel_code",
    "flow_type_code",
    "climate_objective",
    "adaptation_objective",
    "mitigation_objective",
    "project_title",
)


class CRDFNormaliseError(BuildError, ValueError):
    """A CRDF row that the normaliser cannot place."""


def normalise_crdf(
    raw: pl.LazyFrame,
    *,
    perspective: Literal["provider", "recipient"],
    crs: pl.LazyFrame,
) -> pl.LazyFrame:
    """One perspective's `download_crdf` output in the `crdf` schema.

    Amounts are converted to USD units at current prices. `adaptation_usd` and
    `mitigation_usd` each include `overlap_usd`, as published. Before
    `ADAPTATION_FIRST_YEAR` adaptation is not measured: `rio_adaptation`,
    `adaptation_usd` and `overlap_usd` are null, and every row's climate value is its
    mitigation value. A Rio-marked row marked for adaptation alone keeps its face
    value and objective with a climate value of 0, a component row keeps its
    published face value with a climate share of mitigation over face, and a warning
    gives the count of rows changed and their published climate value beyond
    mitigation. Rio-marked rows carry the commitment's full value as `climate_usd`
    (IFAD's own 40% for significant projects aside), and component rows
    (development banks' climate components and imputed multilateral contributions)
    carry the climate part. `face_usd` is `climate_usd` over the published climate
    share, or `climate_usd` where none is published, and `climate_share` is set on
    component rows only, as climate over face.

    `objective` is cross-cutting when a row has both an adaptation and a mitigation part
    (by amount or marker), else the one it has, and `climate` for imputed rows that
    publish only a climate total. `vintage` is the latest data year of the workbook that
    holds the row. The CRDF does not publish `bi_multi`. Imputed multilateral rows are
    core contributions (2), and every other row takes its provider's most common
    Bi_Multi among the provider's non-core records in `crs`. `id_scheme` classifies
    every EIB row's id and is null for every other row.

    Excel damage is repaired or nulled, with counts logged. Error text such as
    `#NAME?` becomes null, and a project number Excel turned into a date takes the
    record's project number in `crs` (same donor, crs id and year) when exactly one
    exists, else null. Crs ids and project numbers lose leading and trailing
    spaces, as in `normalise_crs`.

    Args:
        raw: `oda_reader.download_crdf(perspective=..., pre_process=True)` output.
        perspective: The perspective `raw` was downloaded in.
        crs: Normalised CRS records.

    Raises:
        CRDFNormaliseError: when a provider has no CRS record, a row has no climate
            part, or an EIB id follows no known scheme.
    """
    ids = pl.col("crs_id", "project_number").cast(pl.String).str.strip_chars()
    frame = (
        _repair_excel_damage(
            raw.with_columns(ids, pl.col("description").cast(pl.String)).collect(),
            crs=crs,
        )
        .with_columns(perspective=pl.lit(perspective))
        .with_columns(row_id=content_id(_IDENTITY, order_by=list(_AMOUNTS.values())))
    )
    frame = frame.with_columns(
        bi_multi=_bi_multi(frame, crs=crs),
        rio_adaptation=_marker("adaptation_objective"),
        rio_mitigation=_marker("mitigation_objective"),
        **{
            name: pl.col(source).fill_null(0.0) * _USD_PER_UNIT
            for name, source in _AMOUNTS.items()
        },
        published_share=_published_share(frame),
    ).with_columns(
        pl.when(pl.col("year") >= ADAPTATION_FIRST_YEAR).then(pl.col(name))
        for name in _ADAPTATION_COLUMNS
    )
    frame = frame.with_columns(
        face_usd=pl.when(pl.col("published_share").is_not_null())
        .then(pl.col("climate_usd") / pl.col("published_share"))
        .otherwise(pl.col("climate_usd")),
        climate_share=pl.when(_is_component()).then(
            pl.col("published_share").fill_null(1.0)
        ),
        id_scheme=_id_scheme(),
        vintage=pl.col("year").max().over("source_file").cast(pl.Int16),
    )
    frame = _mitigation_only(frame).with_columns(objective=_objective())
    _check(frame)
    return frame.lazy().select(
        "row_id",
        "vintage",
        "perspective",
        pl.col("year").cast(pl.Int16),
        pl.col("provider_code", "agency_code").cast(pl.Int32),
        "crs_id",
        "project_number",
        title=pl.col("project_title"),
        description="description",
        recipient_code=pl.col("recipient_code").cast(pl.Int32),
        channel_code=pl.col("channel_code").cast(pl.Int32),
        bi_multi="bi_multi",
        purpose_code=pl.col("purpose_code").cast(pl.Int32),
        flow_category=pl.col("flow_type_code").cast(pl.Int32),
        objective="objective",
        rio_adaptation="rio_adaptation",
        rio_mitigation="rio_mitigation",
        face_usd="face_usd",
        climate_usd="climate_usd",
        adaptation_usd="adaptation_usd",
        mitigation_usd="mitigation_usd",
        overlap_usd="overlap_usd",
        climate_share="climate_share",
        id_scheme="id_scheme",
        repeat_of=pl.lit(None, pl.String),
        parent_row_id=pl.lit(None, pl.String),
    )


def _repair_excel_damage(frame: pl.DataFrame, *, crs: pl.LazyFrame) -> pl.DataFrame:
    errors = {
        name: frame.select(pl.col(name).is_in(_EXCEL_ERRORS).sum()).item()
        for name in _TEXT_COLUMNS
    }
    frame = frame.with_columns(
        pl.when(~pl.col(name).is_in(_EXCEL_ERRORS)).then(pl.col(name))
        for name in _TEXT_COLUMNS
    )
    if any(errors.values()):
        logger.warning("CRDF Excel error cells nulled: %s", errors)

    damaged = pl.col("project_number").str.contains(_DATE_DAMAGED)
    key = ["provider_code", "crs_id", "year"]
    published = (
        crs.filter(pl.col("project_number").is_not_null())
        .select(
            provider_code="donor_code",
            crs_id="crs_id",
            year=pl.col("year").cast(pl.Int32),
            crs_project_number="project_number",
        )
        .unique()
        .filter(pl.len().over(key) == 1)
        .collect()
    )
    frame = frame.with_columns(pl.col("provider_code", "year").cast(pl.Int32)).join(
        published, on=key, how="left", maintain_order="left"
    )
    repaired = frame.select(
        (damaged & pl.col("crs_project_number").is_not_null()).sum()
    )
    nulled = frame.select((damaged & pl.col("crs_project_number").is_null()).sum())
    if repaired.item() or nulled.item():
        logger.warning(
            "CRDF project numbers Excel turned into dates: %d repaired from the CRS, "
            "%d nulled",
            repaired.item(),
            nulled.item(),
        )
    return frame.with_columns(
        project_number=pl.when(damaged)
        .then(pl.col("crs_project_number"))
        .otherwise(pl.col("project_number"))
    ).drop("crs_project_number")


def _bi_multi(frame: pl.DataFrame, *, crs: pl.LazyFrame) -> pl.Series:
    """Core contribution for imputed rows, else the provider's modal CRS Bi_Multi.

    The query views distinguish only the classes {1, 3, 7, 8}, {4}, {6} and 2. On
    the 2015 to 2024 data the derived class equals the linked CRS record's class on
    every row that links by provider, crs_id and year: 89.7% of recipient value and
    96.3% of provider value. Four providers (Japan, Korea, Denmark, Hungary) have
    non-core records in two classes. Each has a few private-class (6) records among
    thousands of bilateral ones and comes out bilateral.
    """
    modal = (
        crs.filter(pl.col("bi_multi") != CORE_CONTRIBUTION)
        .group_by("donor_code", "bi_multi")
        .agg(records=pl.len())
        .sort("donor_code", "records", "bi_multi", descending=[False, True, False])
        .group_by("donor_code", maintain_order=True)
        .first()
        .select(provider_code=pl.col("donor_code"), modal=pl.col("bi_multi"))
        .collect()
    )
    joined = frame.select(
        pl.col("provider_code").cast(modal.schema["provider_code"]),
        imputed=pl.col("climate_objective") == _IMPUTED,
    ).join(modal, on="provider_code", how="left", maintain_order="left")
    if missing := sorted(
        joined.filter(pl.col("modal").is_null())["provider_code"].unique()
    ):
        raise CRDFNormaliseError(
            f"CRDF providers {missing} have no non-core record in the CRS, so their "
            "Bi_Multi is unknown"
        )
    return joined.select(
        pl.when("imputed")
        .then(pl.lit(CORE_CONTRIBUTION))
        .otherwise(pl.col("modal"))
        .cast(pl.Int8)
        .alias("bi_multi")
    ).to_series()


def _marker(column: str) -> pl.Expr:
    return pl.col(column).replace_strict(_MARKERS, return_dtype=pl.Int8)


def _published_share(frame: pl.DataFrame) -> pl.Expr:
    """The climate share as a fraction, null where the row publishes none.

    Live files print a percentage (`climate_share_pct`), archived files a fraction
    (`underlying_commitment_share`).
    """
    shares = [
        pl.col(name) * scale
        for name, scale in (
            ("climate_share_pct", 0.01),
            ("underlying_commitment_share", 1.0),
        )
        if name in frame.columns
    ]
    if not shares:
        return pl.lit(None, pl.Float64)
    share = pl.coalesce(shares)
    return pl.when(share > 0).then(share)


def _is_component() -> pl.Expr:
    return pl.col("rio_adaptation").is_null() & pl.col("rio_mitigation").is_null()


def _mitigation_only(frame: pl.DataFrame) -> pl.DataFrame:
    """`frame` with the climate value of every row before `ADAPTATION_FIRST_YEAR`
    set to its mitigation value, and component rows' climate share recomputed from
    it over their published face value."""
    unmeasured = pl.col("year") < ADAPTATION_FIRST_YEAR
    changed = frame.filter(
        unmeasured, pl.col("climate_usd") != pl.col("mitigation_usd")
    )
    if changed.height:
        logger.warning(
            "%d CRDF rows before %d publish USD %.1fm of climate value "
            "beyond their mitigation value; adaptation is not measured then, so it "
            "counts as 0",
            changed.height,
            ADAPTATION_FIRST_YEAR,
            changed.select(pl.col("climate_usd") - pl.col("mitigation_usd"))
            .sum()
            .item()
            / 1e6,
        )
    return frame.with_columns(
        climate_usd=pl.when(unmeasured)
        .then(pl.col("mitigation_usd"))
        .otherwise(pl.col("climate_usd")),
        climate_share=pl.when(unmeasured & _is_component())
        .then(pl.col("mitigation_usd") / pl.col("face_usd"))
        .otherwise(pl.col("climate_share")),
    )


def _objective() -> pl.Expr:
    adaptation = (pl.col("adaptation_usd") > 0) | (
        pl.col("rio_adaptation").fill_null(0) > 0
    )
    mitigation = (pl.col("mitigation_usd") > 0) | (
        pl.col("rio_mitigation").fill_null(0) > 0
    )
    return (
        pl.when(adaptation & mitigation)
        .then(pl.lit("cross_cutting"))
        .when(adaptation)
        .then(pl.lit("adaptation"))
        .when(mitigation)
        .then(pl.lit("mitigation"))
        .when(pl.col("climate_usd") != 0)
        .then(pl.lit("climate"))
        # Before `ADAPTATION_FIRST_YEAR`, an adaptation mark the CRDF does not
        # measure, on a row with no other part.
        .when(_marker("adaptation_objective") > 0)
        .then(pl.lit("adaptation"))
    )


def _id_scheme() -> pl.Expr:
    crs_id = pl.col("crs_id")
    bulk_prefix = pl.concat_str(
        pl.col("year").cast(pl.String),
        pl.lit(str(EIB_AGENCY)),
        pl.col("project_number"),
    )
    is_eib = (pl.col("provider_code") == EU_INSTITUTIONS) & (
        pl.col("agency_code") == EIB_AGENCY
    )
    return (
        pl.when(~is_eib.fill_null(False))
        .then(pl.lit(None, pl.String))
        .when(crs_id.is_null())
        .then(pl.lit("none"))
        .when(crs_id.str.contains(_EIB_SERIAL))
        .then(pl.lit("eib_serial"))
        .when(crs_id.str.starts_with(bulk_prefix).fill_null(False))
        .then(pl.lit("bulk"))
        .when(crs_id.str.contains(_EIB_SEQUENCE))
        .then(pl.lit("sequence"))
        .otherwise(pl.lit("unclassified"))
    )


def _check(frame: pl.DataFrame) -> None:
    if unplaced := frame.filter(pl.col("objective").is_null()).height:
        raise CRDFNormaliseError(
            f"{unplaced} CRDF rows have no adaptation, mitigation or climate part"
        )
    unknown = frame.filter(pl.col("id_scheme") == "unclassified")
    if unknown.height:
        raise CRDFNormaliseError(
            f"{unknown.height} EIB ids follow no known scheme, for example "
            f"{unknown['crs_id'].head(5).to_list()}"
        )
    missing_share = frame.filter(
        _is_component() & pl.col("published_share").is_null()
    ).height
    if missing_share:
        logger.info(
            "%d CRDF component rows publish no climate share; their face value is "
            "their climate value",
            missing_share,
        )

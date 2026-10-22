"""Key-repair rules: links for disbursements the exact key tiers missed because the
donor keyed one project two ways.

A rule links a disbursement unit to a commitment unit of the same donor, under
another crs id, that no exact tier reached. The disbursement unit may carry exact
links too. Allocation runs its groups in `LINK_METHODS` order, so a rule receives
only what the exact tiers and the earlier rules left of the unit's disbursement.
The rules run in `schema.KEY_REPAIR_METHODS` order, and a commitment unit one rule
links is left out of the later ones:

- `repair_new_crs_id`: the same (donor, agency, project number).
- `repair_pn_variant`: project numbers equal once separators, a `CRSn` suffix and
  leading zeros are dropped (`project_core`).

  In both, a number the commitments place in more than one country names a
  programme or an account, as the US `35_80` (PEPFAR operations) does in 55
  recipients, so it links only within the disbursement's recipient and purpose
  (`_programmes`). Under one such number the US files unrelated activities of one
  recipient, as `76_51689` holds "USAID Pay and Benefits" (91010) and a customs
  project (33120); a link across purposes there has a title that disagrees 22
  times as often, by value, as one within a purpose.
- `repair_wb_parent`: the same World Bank loan or credit number inside project
  numbers of the form `P<project>.<loan>.CRS<n>`, as when additional financing is
  booked under the parent project.
- `repair_afdb_scientific`: a long numeric project number that a spreadsheet
  rounded, as `2.10015E+12` or `2100150000000`, keeps its leading digits and its
  length (`rounded_digits`). It links to a commitment of the same agency and
  recipient whose number starts with those digits and has that length, under a
  similar title, where the disbursements that match it share one title.
- `repair_title_recipient`: the same agency, recipient and title words in any
  order (`title_words`), where those words name one crs id among all the agency's
  commitments in the recipient and one project among the disbursements that match
  them. A project here has at most one project number, and the shared title names
  it where the number is missing. A rounded number counts as the full number it
  can stand for, or else as a number of its own.

The last two guards count projects, not crs ids, since a donor such as the AfDB
files each year's disbursements of one project under a new crs id.

A pair links only when the commitment is placed no later than the record year and
no earlier than the record's floor year.

Allocation pools the disbursement of a group and fills every commitment of the
group that the record's year and floor allow, so a rule keeps or drops a
disbursement unit's pairs within a group together: a unit links into a group only
when no commitment there shares its crs id, and its title agrees with every
commitment there (`TITLE_AGREES`) or the recipient is the same. A unit links within
one group of a rule, to at most `MAX_COMMITMENTS` commitment units, and each
commitment unit falls in one group of a rule by its key.
"""

from collections.abc import Callable
from functools import cache
from importlib.resources import files

import numpy as np
import polars as pl
from rapidfuzz import fuzz
from rapidfuzz.process import cpdist

from climate_finance import schema
from climate_finance.build.link.keys import as_exact_links

# A disbursement reaching more commitments than this sits under a shared key.
MAX_COMMITMENTS = 5
# The token-set ratio at which two titles describe the same project.
TITLE_AGREES = 80
# The title rule reads titles of at least this many characters.
TITLE_MIN_CHARS = 15
# A numeric project number at least this long can be one a spreadsheet rounded.
ROUNDED_MIN_DIGITS = 10
# The shortest project core that still names a project.
CORE_MIN_CHARS = 4

_LONG_NUMBER = rf"^\d{{{ROUNDED_MIN_DIGITS},}}$"
_SCIENTIFIC = r"^(\d)\.(\d+)E\+(\d+)$"
_WB_LOAN = r"^P\d+\.([A-Z]+[A-Z0-9]*\d)\.CRS\d+$"
_PN = ("donor_code", "agency_code", "project_number")
_CORE = ("donor_code", "agency_code", "core")
_LOAN = ("donor_code", "loan")
_PLACE = ("donor_code", "agency_code", "recipient_code")
_TITLE_KEY = (*_PLACE, "title_words")
# What `_settle` reads of a rule's candidate pairs. Commitment columns end in `_c`.
_PAIR = (
    "left_id",
    "right_id",
    "key",
    "recipient_code",
    "recipient_code_c",
    "title",
    "title_c",
)

type Rule = Callable[[pl.DataFrame, pl.DataFrame], pl.DataFrame]


def project_core(project_number: pl.Expr) -> pl.Expr:
    """A project number without separators, a trailing `CRSn` suffix or the leading
    zeros of its digit runs, or null below `CORE_MIN_CHARS` characters."""
    core = (
        project_number.str.to_uppercase()
        .str.replace_all("[^A-Z0-9]", "")
        .str.replace(r"CRS\d+$", "")
        .str.replace_all("(^|[A-Z])0+([0-9])", "${1}${2}")
    )
    return pl.when(core.str.len_chars() >= CORE_MIN_CHARS).then(core)


def wb_loan(project_number: pl.Expr) -> pl.Expr:
    """The loan or credit number in a World Bank `P<project>.<loan>.CRS<n>` project
    number, or null for other numbers."""
    return project_number.str.to_uppercase().str.extract(_WB_LOAN, 1)


def rounded_digits(project_number: pl.Expr) -> pl.Expr:
    """For a project number a spreadsheet rounded, the digits it kept (`prefix`) and
    the length of the number it stands for (`digits`), or null for other numbers."""
    scientific = project_number.str.extract_groups(_SCIENTIFIC)
    lead, rest, exponent = (scientific.struct.field(name) for name in ("1", "2", "3"))
    zeros = project_number.str.contains(_LONG_NUMBER) & project_number.str.ends_with(
        "0000"
    )
    prefix = (
        pl.when(lead.is_not_null())
        .then(lead + rest)
        .when(zeros)
        .then(project_number.str.strip_chars_end("0"))
    )
    digits = (
        pl.when(lead.is_not_null())
        .then(exponent.cast(pl.Int32) + 1)
        .when(zeros)
        .then(project_number.str.len_chars().cast(pl.Int32))
    )
    return pl.when(prefix.is_not_null()).then(pl.struct(prefix=prefix, digits=digits))


def title_tokens(title: pl.Expr) -> pl.Expr:
    """A title lower-cased, with each run of anything but ASCII letters and digits
    as one space, or empty when there is no title."""
    return (
        title.fill_null("")
        .str.to_lowercase()
        .str.replace_all("[^a-z0-9]+", " ")
        .str.strip_chars()
    )


def title_words(tokens: pl.Expr) -> pl.Expr:
    """`title_tokens` output as its distinct words in sorted order."""
    return tokens.str.split(" ").list.unique().list.sort().list.join(" ")


def repair_keys(
    units: pl.DataFrame, *, crs: pl.LazyFrame, links: pl.DataFrame, first_year: int
) -> pl.DataFrame:
    """Key-repair `trace` links as `links` rows, each rule under its own
    `link_method`.

    Args:
        units: the link stage's units.
        crs: the CRS records the units sum, for their titles.
        links: the exact tiers' `trace` links, whose commitments are left to them.
        first_year: the first CRS year the build loads. A disbursement whose floor
            precedes it links to nothing, as in the exact tiers.

    Returns:
        One row per (disbursement unit, commitment unit) pair, `group_id` naming
        the rule and its key.
    """
    facts = _unit_facts(units, crs=crs)
    commitments = facts.filter(pl.col("commitment_usd") > 0)
    unique_titles = _unique_titles(commitments)
    programmes = {key: _programmes(commitments, key=key) for key in (_PN, _CORE)}
    disbursements = facts.filter(
        pl.col("disbursement_usd") > 0,
        pl.col("floor_year").is_null() | (pl.col("floor_year") >= first_year),
    )
    commitments = commitments.join(
        links.select(unit_id="right_id"), on="unit_id", how="anti"
    )
    rules: dict[str, Rule] = {
        "repair_new_crs_id": lambda d, c: _same_number(
            d, c, key=_PN, programmes=programmes[_PN]
        ),
        "repair_pn_variant": lambda d, c: _same_number(
            d, c, key=_CORE, programmes=programmes[_CORE]
        ),
        "repair_wb_parent": lambda d, c: _candidates(d, c, on=_LOAN, key=_LOAN),
        "repair_afdb_scientific": _rounded_numbers,
        "repair_title_recipient": lambda d, c: _same_title(
            d, c.join(unique_titles, on=_TITLE_KEY)
        ),
    }
    if tuple(rules) != schema.KEY_REPAIR_METHODS:
        raise RuntimeError("The rules must follow schema.KEY_REPAIR_METHODS")

    found = []
    for method, rule in rules.items():
        pairs = _settle(rule(disbursements, commitments), method=method)
        found.append(pairs)
        commitments = commitments.join(
            pairs.select(unit_id="right_id"), on="unit_id", how="anti"
        )
    return as_exact_links(pl.concat(found).lazy(), task="trace").collect()


def _unit_facts(units: pl.DataFrame, *, crs: pl.LazyFrame) -> pl.DataFrame:
    """Units with the keys the rules derive, and the title tokens and words of their
    first record, by record id, that has a title."""
    titles = (
        units.lazy()
        .select("unit_id", record_id="record_ids")
        .explode("record_id")
        .join(crs.select("record_id", "title").drop_nulls("title"), on="record_id")
        .group_by("unit_id")
        .agg(pl.col("title").sort_by("record_id").first())
        .collect()
    )
    pn = pl.col("project_number")
    return (
        units.drop("record_ids")
        .join(titles, on="unit_id", how="left")
        .with_columns(title=title_tokens(pl.col("title")))
        .with_columns(
            title_words=title_words(pl.col("title")),
            core=project_core(pn),
            loan=wb_loan(pn),
            rounded=rounded_digits(pn),
        )
    )


def _unique_titles(commitments: pl.DataFrame) -> pl.DataFrame:
    """(donor, agency, recipient, title words) keys, with words long enough for the
    title rule, that name one crs id among all commitment units."""
    return (
        commitments.filter(pl.col("title_words").str.len_chars() >= TITLE_MIN_CHARS)
        .group_by(_TITLE_KEY)
        .agg(ids=pl.col("crs_id").drop_nulls().n_unique())
        .filter(pl.col("ids") == 1)
        .select(_TITLE_KEY)
    )


def _programmes(commitments: pl.DataFrame, *, key: tuple[str, ...]) -> pl.DataFrame:
    """The values of the number `key` whose commitment units name more than one
    country. Regional and unspecified recipients name none."""
    country = ~pl.col("recipient_code").is_in(_regional_codes())
    return (
        commitments.drop_nulls(key)
        .group_by(key)
        .agg(countries=pl.col("recipient_code").filter(country).n_unique())
        .filter(pl.col("countries") > 1)
        .select(key)
    )


@cache
def _regional_codes() -> tuple[int, ...]:
    """The DAC recipient codes of regions and of unspecified recipients."""
    recipients = pl.read_csv(
        (files("climate_finance") / "data" / "recipients.csv").read_bytes(),
        schema_overrides={"code": pl.Int32},
    )
    return tuple(
        recipients.filter(pl.col("name").str.contains("regional|unspecified"))["code"]
    )


def _same_number(
    disbursements: pl.DataFrame,
    commitments: pl.DataFrame,
    *,
    key: tuple[str, ...],
    programmes: pl.DataFrame,
) -> pl.DataFrame:
    """Pairs equal on the number `key`, grouped by it, and for a programme number
    also on the recipient and purpose, grouped by those too.

    A unit falls in one group here, and `_settle` keeps or drops a group whole, so
    a unit whose group holds more than `MAX_COMMITMENTS` eligible commitments
    links to none. Such units are left out before the join (`_within_cap`), since
    a programme or account number pairs thousands of units with each other."""
    within = (*key, "recipient_code", "purpose_code")
    found = []
    for number, on in (
        (commitments.join(programmes, on=key, how="anti"), key),
        (commitments.join(programmes, on=key, how="semi"), within),
    ):
        number = number.select(_columns(on, "placed_year"))
        found.append(
            _candidates(
                _within_cap(
                    disbursements.select(
                        _columns(on, "placed_year", "year", "floor_year")
                    ),
                    number,
                    on=on,
                ),
                number,
                on=on,
                key=on,
            ).select(_PAIR)
        )
    return pl.concat(found)


def _columns(on: tuple[str, ...], *dates: str) -> list[str]:
    """What `_candidates` and `_settle` read of one side of a number rule."""
    return list(
        dict.fromkeys(("unit_id", "crs_id", "recipient_code", "title", *on, *dates))
    )


def _within_cap(
    disbursements: pl.DataFrame, commitments: pl.DataFrame, *, on: tuple[str, ...]
) -> pl.DataFrame:
    """`disbursements` less those that reach more than `MAX_COMMITMENTS`
    commitments equal on `on`, placed no later than the record year and no
    earlier than its floor year. Counted by placement year, without pairing."""
    when = [*on, "year", "floor_year"]
    placed = commitments.drop_nulls(on).group_by(*on, "placed_year").len("placed")
    over = (
        disbursements.drop_nulls(on)
        .select(when)
        .unique()
        .join(placed, on=list(on))
        .filter(
            pl.col("placed_year") <= pl.col("year"),
            pl.col("floor_year").is_null()
            | (pl.col("placed_year") >= pl.col("floor_year")),
        )
        .group_by(when)
        .agg(pl.col("placed").sum())
        .filter(pl.col("placed") > MAX_COMMITMENTS)
    )
    return disbursements.join(over, on=when, how="anti", nulls_equal=True)


def _candidates(
    disbursements: pl.DataFrame,
    commitments: pl.DataFrame,
    *,
    on: tuple[str, ...],
    key: tuple[str, ...],
) -> pl.DataFrame:
    """Pairs equal on `on`, the commitment placed no later than the record year and
    no earlier than its floor year, grouped by the columns `key`, for disbursement
    units no commitment of whose group shares their crs id.

    Disbursement columns keep their names, under `left_id` for the unit. Commitment
    columns end in `_c`, under `right_id` for the unit.
    """
    pairs = (
        disbursements.drop_nulls(on)
        .join(commitments.drop_nulls(on), on=list(on), suffix="_c")
        .filter(
            pl.col("placed_year_c") <= pl.col("year"),
            pl.col("floor_year").is_null()
            | (pl.col("placed_year_c") >= pl.col("floor_year")),
        )
        .with_columns(pl.col(name).alias(f"{name}_c") for name in on)
        .rename({"unit_id": "left_id", "unit_id_c": "right_id"})
        .with_columns(key=pl.concat_str([pl.col(name) for name in key], separator="|"))
    )
    return _whole(pairs, pl.col("crs_id").ne_missing(pl.col("crs_id_c")))


def _rounded_numbers(
    disbursements: pl.DataFrame, commitments: pl.DataFrame
) -> pl.DataFrame:
    """Rounded project numbers to the full numbers they stand for, grouped by the
    place and the full number. A unit keeps a group whose every title agrees with
    its own, and a commitment links when the units that reach it share one title,
    since their rounded numbers cannot tell projects apart."""
    full = pl.col("project_number_c")
    pairs = _candidates(
        disbursements.drop_nulls("rounded").unnest("rounded"),
        commitments.filter(pl.col("project_number").str.contains(_LONG_NUMBER)),
        on=_PLACE,
        key=(*_PLACE, "project_number_c"),
    ).filter(
        full.str.len_chars() == pl.col("digits"),
        full.str.starts_with(pl.col("prefix")),
    )
    return _whole(
        _with_title_ratio(pairs), pl.col("title_ratio") >= TITLE_AGREES
    ).filter(pl.col("title_words").n_unique().over("right_id") == 1)


def _same_title(disbursements: pl.DataFrame, commitments: pl.DataFrame) -> pl.DataFrame:
    """Pairs equal on place and title words, where each commitment is matched by
    one project: at most one project number among the units, which share the
    title. Grouped by the place and the words.

    A rounded number counts apart from the full numbers: as the full number of the
    units that it can stand for, else as its own number, two spellings of one
    rounded number (`2.0002E+12`, `2000200000000`) counting once."""
    pairs = _candidates(disbursements, commitments, on=_TITLE_KEY, key=_TITLE_KEY)
    full = (
        pairs.filter(pl.col("rounded").is_null())
        .select("right_id", "project_number")
        .drop_nulls()
        .unique()
    )
    rounded = (
        pairs.select("right_id", "rounded")
        .drop_nulls()
        .unnest("rounded")
        .with_columns(number=pl.col("prefix").str.pad_end(pl.col("digits"), "0"))
        .unique(["right_id", "number"])
    )
    matched = (
        rounded.join(full, on="right_id")
        .filter(
            pl.col("project_number").str.len_chars() == pl.col("digits"),
            pl.col("project_number").str.starts_with(pl.col("prefix")),
        )
        .select("right_id", "number")
    )
    numbers = pl.concat(
        [
            full.select("right_id", number="project_number"),
            rounded.join(matched, on=["right_id", "number"], how="anti").select(
                "right_id", "number"
            ),
        ]
    )
    ambiguous = (
        numbers.group_by("right_id").len().filter(pl.col("len") > 1).select("right_id")
    )
    return pairs.join(ambiguous, on="right_id", how="anti")


def _with_title_ratio(pairs: pl.DataFrame) -> pl.DataFrame:
    """`pairs` with the token-set ratio of their two titles, 0 where either is
    empty."""
    scores = (
        cpdist(
            pairs["title"].to_list(),
            pairs["title_c"].to_list(),
            scorer=fuzz.token_set_ratio,
            workers=-1,
        )
        if pairs.height
        else []
    )
    return pairs.with_columns(
        title_ratio=pl.Series(np.asarray(scores, dtype=np.float64))
    )


def _whole(pairs: pl.DataFrame, condition: pl.Expr) -> pl.DataFrame:
    """The pairs of each (disbursement unit, group) whose pairs all meet
    `condition`."""
    return pairs.filter(condition.all().over("left_id", "key"))


def _settle(pairs: pl.DataFrame, *, method: str) -> pl.DataFrame:
    """A rule's pairs as (left_id, right_id, group_id, link_method), for disbursement
    units whose title agrees, or recipient matches, with every commitment of their
    group, that sit in one group and reach at most `MAX_COMMITMENTS` commitments."""
    agrees = (pl.col("title_ratio") >= TITLE_AGREES) | (
        pl.col("recipient_code") == pl.col("recipient_code_c")
    )
    return (
        _whole(_with_title_ratio(pairs.select(_PAIR)), agrees)
        .filter(
            pl.col("key").n_unique().over("left_id") == 1,
            pl.col("right_id").n_unique().over("left_id") <= MAX_COMMITMENTS,
        )
        .select(
            "left_id",
            "right_id",
            group_id=pl.concat_str([pl.lit(method), "key"], separator="|"),
            link_method=pl.lit(method),
        )
    )

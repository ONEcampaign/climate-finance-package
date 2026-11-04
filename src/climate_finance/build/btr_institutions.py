"""The institution groups of BTR Table III.2, from `btr_institutions.toml`.

Parties write one institution many ways ("World Bank (IDA)", "IDA/International
Development Association/The World Bank"), so a name is looked up by its key
(`institution_key`), and each key names one group of CRS channel codes.
"""

import re
import tomllib
import unicodedata
from collections import Counter
from collections.abc import Mapping
from functools import cache
from pathlib import Path
from typing import NamedTuple

INSTITUTIONS_FILE = Path(__file__).with_name("btr_institutions.toml")
_SEPARATORS = re.compile(r"[\W_]+")


def institution_key(name: str) -> str:
    """`name` with accents dropped, case folded, and each run of characters other
    than letters and digits made one space, with none at either end."""
    decomposed = unicodedata.normalize("NFKD", name)
    bare = "".join(char for char in decomposed if not unicodedata.combining(char))
    return _SEPARATORS.sub(" ", bare.casefold()).strip()


class InstitutionMap(NamedTuple):
    """Each group's channel codes, and the group each institution key names."""

    groups: Mapping[str, tuple[int, ...]]
    keys: Mapping[str, str]

    def group(self, name: str) -> str | None:
        """The group `name` belongs to, or None for a name the map leaves out."""
        return self.keys.get(institution_key(name))


def read_institution_map(path: Path) -> InstitutionMap:
    """The map in `path`: one table per group, with its `channel_codes` and the
    institution keys in `names`.

    Raises:
        ValueError: for a group without codes or names, a name that is not its own
            key, a name in two groups, or a code in two groups.
    """
    entries = tomllib.loads(path.read_text(encoding="utf-8"))
    groups = {group: tuple(entry["channel_codes"]) for group, entry in entries.items()}
    names = {group: tuple(entry["names"]) for group, entry in entries.items()}
    problems = [
        f"{group}: no channel codes or no names"
        for group in entries
        if not groups[group] or not names[group]
    ]
    problems += [
        f"{group}: {name!r} is not written as its key"
        for group, listed in names.items()
        for name in listed
        if name != institution_key(name)
    ]
    for kind, members in (("name", names), ("channel", groups)):
        counts = Counter(member for listed in members.values() for member in listed)
        problems += [
            f"{kind} {member!r} is in several groups"
            for member, count in counts.items()
            if count > 1
        ]
    if problems:
        raise ValueError(f"{path.name}: {'; '.join(problems)}")
    keys = {name: group for group, listed in names.items() for name in listed}
    return InstitutionMap(groups, keys)


@cache
def institution_map() -> InstitutionMap:
    """The map the build uses, from `INSTITUTIONS_FILE`."""
    return read_institution_map(INSTITUTIONS_FILE)

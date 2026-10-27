"""Greedy one-to-one pairing of ranked candidates."""

import polars as pl


def pair_one_to_one(
    candidates: pl.DataFrame, *, left: list[str], right: list[str]
) -> pl.DataFrame:
    """Keep each candidate whose two sides are both still free, in the given order.

    `candidates` arrive sorted best first, ties broken by the sort.
    """
    taken_left: set[tuple[object, ...]] = set()
    taken_right: set[tuple[object, ...]] = set()
    keep: list[bool] = []
    for a, b in zip(
        candidates.select(left).iter_rows(),
        candidates.select(right).iter_rows(),
        strict=True,
    ):
        free = a not in taken_left and b not in taken_right
        if free:
            taken_left.add(a)
            taken_right.add(b)
        keep.append(free)
    return candidates.filter(pl.Series(keep, dtype=pl.Boolean))

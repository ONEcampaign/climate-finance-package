"""Float sums that give the same bits on every run."""

import polars as pl


def ordered_sum(values: pl.Expr) -> pl.Expr:
    """The sum of `values`, added in ascending order; 0.0 when none are set.

    A polars 2.0 group sum adds partial sums in the order its threads finish, so a
    float total can change in the last bit between two runs. Sorting fixes the
    order, and summing the imploded list keeps the optimiser from dropping the sort.
    An empty list sums to -0.0, which survives `+ 0.0` in a group_by of several
    groups, so a zero total is written as 0.0. `total` takes the first branch so
    the result keeps its name.
    """
    total = values.sort().implode().list.sum()
    return pl.when(total != 0.0).then(total).otherwise(0.0)

"""Float sums that give the same bits on every run."""

import polars as pl


def ordered_sum(values: pl.Expr) -> pl.Expr:
    """The sum of `values`, added in ascending order; 0.0 when none are set.

    A polars 2.0 group sum adds partial sums in the order its threads finish, so a
    float total can change in the last bit between two builds. Sorting fixes the
    order, and summing the imploded list keeps the optimiser from dropping the sort.
    Adding 0.0 turns the -0.0 an empty list sums to into 0.0.
    """
    return values.sort().implode().list.sum() + 0.0

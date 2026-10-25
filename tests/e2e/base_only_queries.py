"""Query a release with sockets blocked and print the answers as JSON.

Run by test_query_archive.py in a venv that holds the base package only:
`python base_only_queries.py <release dir>`.
"""

import json
import socket
import sys
from importlib.util import find_spec


def refuse(*args: object, **kwargs: object) -> None:
    raise OSError("network access while querying a release")


socket.socket.connect = refuse
socket.socket.connect_ex = refuse

import polars as pl  # noqa: E402

import climate_finance as cf  # noqa: E402

# Dependencies of the build extra that no base dependency pulls in (pydeflate brings
# resolvekit and oda-reader, and oda-reader brings openpyxl).
BUILD_ONLY = ("typer", "httpx")


def totals(result: cf.Result) -> dict[str, dict[str, float]]:
    """Per method: value, low and high summed over the result table."""
    table = (
        result.table.to_polars()
        .group_by("method")
        .agg(pl.col("value", "low", "high").sum())
    )
    return {row.pop("method"): row for row in table.iter_rows(named=True)}


release = cf.load(path=sys.argv[1])
years = range(2015, 2025)
results = {
    cf.methods.label(method): release.spending(
        providers="Germany", years=years, method=method
    )
    for method in (cf.methods.ONE, cf.methods.OECD, cf.methods.RANGE)
}
rows = results["ONE"].rows(year=2022)
comparison = cf.compare(results["ONE"], results["OECD"]).table.to_polars()
quality = release.quality(providers="Germany").summary()
profile = release.disbursement_profile().filter(group="all")
repeats = (
    release.table("commitments")
    .filter(pl.col("repeat_of").is_not_null())
    .select("row_id")
    .collect()
)
print(
    json.dumps(
        {
            "build_modules": [name for name in BUILD_ONLY if find_spec(name)],
            "totals": {name: totals(result) for name, result in results.items()},
            "rows_2022": {
                "rows": rows.height,
                "years": rows["year"].unique().to_list(),
                "value": rows["value"].sum(),
                "repeats": rows.filter(
                    pl.col("row_id").is_in(repeats["row_id"].implode())
                ).height,
            },
            "compare": {name: comparison[name].sum() for name in ("ONE", "OECD")},
            "quality": quality.to_dicts(),
            "profile": dict(
                zip(profile["years_since"], profile["cumulative_share"], strict=True)
            ),
        }
    )
)

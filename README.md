[![pypi](https://img.shields.io/pypi/v/climate_finance.svg)](https://pypi.org/project/climate_finance/)
[![python](https://img.shields.io/pypi/pyversions/climate_finance.svg)](https://pypi.org/project/climate_finance/)

# climate-finance

> International public climate finance from OECD and UNFCCC data, with each figure traced to the project records behind it.

The OECD publishes what donors commit to climate action, while the disbursements that reach each project sit in a separate database under ids that donors change from year to year. This package links commitments to disbursements, records the certainty of each link, and returns every figure with a low and a high bound.

A release is a set of versioned tables built from the OECD's Creditor Reporting System (CRS) and Climate-related Development Finance data (CRDF), checked against Biennial Transparency Reports (BTRs) from the UNFCCC. You load a release and query it. The build runs only when someone cuts a new release.

## Install

```bash
pip install climate-finance
```

Requires Python 3.12 or later. To build a release, install the `build` extra with `pip install "climate-finance[build]"`.

## Quickstart

`cf.load()` opens the latest release in `data/releases.json`, the release index shipped with the package. That index ships empty until the first 2.0 release is published, and until then `cf.load()` raises `UnknownNameError`. Pass the directory to open a release you built (`cf.load(path="releases/2026a/assemble")`). The examples in this repository use a small synthetic test release with invented amounts.

```python
import climate_finance as cf

release = cf.load()
result = release.spending(
    providers="Japan",
    recipients="Bangladesh",
    years=range(2021, 2024),
    method=cf.methods.ONE,
)
for row in result.table.iter_rows(named=True):
    print(row["year"], round(row["low"] / 1e6, 1), round(row["value"] / 1e6, 1))
```

```output
2021 0.0 13.3
2022 0.0 18.3
2023 12.0 48.3
```

`result.table` is a polars DataFrame with one row per year, provider, recipient, Bi_Multi code, and method, with adaptation, mitigation, cross-cutting, and total climate `value` in USD. `value` counts every disbursement the build allocated to a climate commitment in the release, from the first CRDF year to the last complete CRS year, and the untraced disbursement: Rio-marked CRS disbursement that reaches no commitment, weighted by the record's own markers. `low` counts only the money on links the build treats as certain by rule, from the CRDF row to its CRS commitment and from the commitment to the disbursement. Most rest on the donor's own ids. The parts of rebuilt aggregate rows, which match their CRS records by amount, count too. In the provider view `low` also counts attributed core contributions, because the bounds measure link certainty and a method's coefficients and agency shares weigh every bound alike. `result.rows()` returns the project rows behind the table, `result.explain()` states the method, flow, and filters in words, and `release.run(result.spec)` reruns the query.

## What you can ask

- Commitments, disbursements, and grant equivalents of climate finance by provider, recipient, and year, in USD, EUR, GBP, or CAD, at current or constant prices.
- Climate value under `ONE` or `OECD` marker weights, under `PROVIDER`, which takes the coefficients each provider applies when it reports to the UNFCCC, or under all three at once with `RANGE`. A release built with Jev's labels also answers `relevance="jev"` queries.
- Core contributions to multilateral agencies attributed to the providers that pay them, so each dollar counts once.
- Link rates, commitment status, and the measured precision of the links with `release.quality()`, and a party's BTR report against the OECD data with `release.reconcile()`.

## When to use something else

The package reports public climate finance as donors file it, taking their Rio markers as given and excluding private-sector providers (Bi_Multi 6). Where a provider files unusable project ids, its rows rest on estimated links, and the gap between `low` and `high` shows how many.

## Documentation

Start with [Trace Japan's climate finance to Bangladesh](docs/tutorial.md). The [documentation index](docs/index.md) lists the how-to guides, the reference, and the explanations. Users of 1.x should read [Migrating from 1.x](docs/migrating-from-1x.md) and the [2.0.0 release notes](docs/release-notes/2.0.0.md), which explain why totals differ.

MIT. See [LICENSE](LICENSE).

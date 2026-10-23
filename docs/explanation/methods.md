# About the methods

As of 2.0. A method turns Rio markers into climate value. [Python API](../reference/python-api.md) lists the fields of `Method`.

## What a Rio marker says

A donor scores each project 0, 1 (significant), or 2 (principal) for adaptation and for mitigation. The score says how central climate is to the project, and the donor reports the project's full value either way. A method decides how much of that value counts as climate finance.

## The named methods

`cf.methods.ONE` counts significant markers at 40% of face value and principal markers at 100%. Each activity counts once, under its higher marker: adaptation if that is higher, mitigation if that is, and cross-cutting if the two are equal. The three objectives are disjoint, so they add up to the total.

`cf.methods.OECD` counts significant and principal markers both at 100%. Adaptation and mitigation each count their own marker, so a project marked for both counts in both, and `cross_cutting` is the overlap, which `value` counts once. OECD also counts EU Institutions as a provider, as the OECD does.

`cf.methods.PROVIDER` is ONE with each provider's own coefficient for significant markers where the release has one. A coefficient is estimated from the ratio of a provider's BTR amounts to its CRS amounts and enters a query only after a reviewer confirms it. Providers without a confirmed coefficient take 0.4, and `result.explain()` names them.

`cf.methods.RANGE` runs the three together. For each row, `low` becomes the lowest of their `low` bounds, which count only disbursements on certain links, and `high` the highest value any of them gives.

Development banks report a climate share (the climate components) for their projects. Rows from these banks use that share under every method and ignore the coefficients.

`Method` takes any other coefficients. Calling `cf.methods.ONE.replace(significant=0.5)` returns a valid method, and `result.explain()` describes it in words.

## Jev labels

Where a release holds Jev's objective labels, `Method(relevance="jev")` reads Jev's adaptation level in place of the adaptation marker for disbursements whose CRS record Jev labelled. Jev labels only records whose commitment is marked adaptation. Mitigation, commitments, and unlabelled records keep their CRS markers. Jev labels a record adaptation when its answer to a lenient adaptation question passes a cut fitted on human-coded projects, at level 2 when it finds principal likelier than significant and at level 1 otherwise. A record below the cut reads level 0. The method weighs these levels at the same coefficients as donor markers.

## Attribution to providers

`view="recipient"`, the default, counts bilateral flows (Bi_Multi 1, 3, 7, and 8) and multilateral outflows (code 4). It leaves out money a donor gives to a multilateral agency (code 2), because the agency's own outflows count when it spends the money.

`view="provider"` counts the same bilateral flows and adds each provider's core contributions times the receiving agency's climate share. Each contribution counts once, in the provider's total. The agency's own outflows stay out of an all-provider total.

Under ONE and PROVIDER the share is the agency's own climate commitments over its total CRS commitments, summed over the year and the year before. Where an agency has too few commitments in a year, the share holds at its last measured value. Where it has none, the OECD's imputed share applies. Each attributed row carries its `share_source`: `computed`, `held`, or `oecd`. Under OECD the share is always the OECD's.

A share with no split by objective, as the OECD gives before 2021, sits in the `unsplit` column, so `value` equals the sum of its parts.

## EU Institutions

EU Institutions is a provider and also a channel for its members' money. `Method(eu="channel")`, the default for ONE and PROVIDER, attributes members' contributions to the EU budget, the European Development Fund, and the EIB to the members, and drops the EU agencies' own rows from the provider view, so the money counts once. `eu="provider"`, the setting of OECD, counts the EU as a provider in its own right and attributes nothing through those three channels. A query for EU Institutions alone returns its own spending under either setting. `snap.quality().eu_left_out` reports the rows an all-provider total leaves out.

## Comparing methods

The same query under each named method shows what the weights change. OECD counts significant markers at 100% where ONE counts them at 40%, so it gives the larger total. PROVIDER differs from ONE only for providers with a confirmed coefficient of their own.

```python
import climate_finance as cf

snap = cf.load()
methods = {
    "ONE": cf.methods.ONE,
    "OECD": cf.methods.OECD,
    "PROVIDER": cf.methods.PROVIDER,
}
for name, method in methods.items():
    result = snap.spending(
        providers=["Germany", "Japan"], years=range(2021, 2024), method=method
    )
    print(name, round(result.table.to_polars()["value"].sum() / 1e6, 1))
```

```output
ONE 126.0
OECD 150.0
PROVIDER 126.8
```

`cf.compare()` joins results on year, provider, recipient, and Bi_Multi code, and names each column after what sets the results apart.

```python
one = snap.spending(providers="Japan", years=[2023], method=cf.methods.ONE)
oecd = snap.spending(providers="Japan", years=[2023], method=cf.methods.OECD)
print(cf.compare(one, oecd).table.to_polars().columns)
```

```output
['year', 'provider_code', 'recipient_code', 'bi_multi', 'ONE', 'OECD']
```

`cf.methods.RANGE` returns the three methods in one table, one row per method. Use it for a figure that has to hold under any of the three readings.

```python
result = snap.spending(providers="Japan", years=[2023], method=cf.methods.RANGE)
print(sorted(result.table.to_polars()["method"].unique()))
```

```output
['OECD', 'ONE', 'PROVIDER']
```

`Method` takes any coefficients, and `replace()` copies a method with changes. `explain()` describes the result in words.

```python
half = cf.methods.ONE.replace(significant=0.5)
result = snap.spending(providers="Japan", years=[2023], method=half)
print(result.explain().splitlines()[1][:46])
```

```output
Method significant 0.5, principal 1, highest m
```

Under PROVIDER, a provider with no confirmed coefficient takes 0.4, and `explain()` names it.

```python
result = snap.spending(
    providers=["Germany", "Japan"], years=[2021], method=cf.methods.PROVIDER
)
for line in result.explain().splitlines():
    if line.startswith("Provider coefficients"):
        print(line)
```

```output
Provider coefficients: providers 701 have no confirmed coefficient and take 0.4.
```

## Next

- [Python API](../reference/python-api.md) lists the fields of `Method` and the arguments of `spending()`.
- [Release tables](../reference/release-tables.md) describes `agency_shares`, `core_contributions`, and `provider_coefficients`.

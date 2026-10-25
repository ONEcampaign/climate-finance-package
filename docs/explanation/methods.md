# About the methods

As of 2.0. A method turns Rio markers into climate value. [Python API](../reference/python-api.md) lists the fields of `Method`.

## What a Rio marker says

A donor scores each project 0, 1 (significant), or 2 (principal) for adaptation and for mitigation. The score says how central climate is to the project, and the donor reports the project's full value either way. A method decides how much of that value counts as climate finance.

## The named methods

`cf.methods.ONE` counts significant markers at 40% of face value and principal markers at 100%. Each activity counts once, under its higher marker: adaptation if that is higher, mitigation if that is, and cross-cutting if the two are equal. The three objectives are disjoint, so they add up to the total.

`cf.methods.OECD` counts significant and principal markers both at 100%. Adaptation and mitigation each count their own marker, so a project marked for both counts in both. `cross_cutting` is the overlap, which `value` counts once. OECD also counts the EU budget and the European Development Fund as a provider and the EIB as a channel, as the OECD does.

`cf.methods.PROVIDER` uses the coefficients each provider applies when it reports to the UNFCCC, as the OECD's survey DCD/DAC/STAT(2024)28/REV1 records them. Ireland's coefficients before 2021 come from its own report, and Luxembourg's practice from its first BTR. Each provider has a significant and a principal coefficient, by agency and year, and Germany also has one for an activity significant on both objectives. Providers that assess each activity case by case, such as the UK, the US, France's AFD, and Luxembourg, take the method's 0.4 and 1.0, as do providers with no row, while Australia takes its stated default of 0.3. `result.explain()` names each provider's source.

Where a provider publishes its climate share of face value, that share caps each marker weight under every method. IFAD publishes its significant rows at 40%, so no method counts them above that. Untraced disbursement reaches no commitment, so no published share caps it: an IFAD record marked significant counts its untraced part at the method's own significant coefficient.

`cf.methods.RANGE` runs the three together. For each row, `low` becomes the lowest of their `low` bounds, which count only disbursements on certain links, and `high` becomes the highest value any of them gives.

Development banks report a climate share (the climate components) for their projects. Rows from these banks use that share under every method and ignore the coefficients.

`Method` takes any other coefficients. Calling `cf.methods.ONE.replace(significant=0.5)` returns a valid method, and `result.explain()` describes it in words.

## Jev labels

Where a release holds Jev's objective labels, `Method(relevance="jev")` reads Jev's adaptation level in place of the adaptation marker for disbursements whose CRS record Jev labelled. Jev labels only records whose commitment its donor marked adaptation and that carry no climate components, and untraced records that their own adaptation marker places among adaptation projects. A label replaces only the adaptation marker a row weighs by: the commitment's for a traced row and the record's own for an untraced one, where that marker is 1 or 2. Mitigation, commitments, and unlabelled records keep their CRS markers. Jev labels a record adaptation when its answer to a lenient adaptation question passes a cut fitted on human-coded projects, at level 2 when it finds principal likelier than significant and at level 1 otherwise. A record below the cut reads level 0. The method weighs these levels at the same coefficients as donor markers.

## Attribution to providers

`view="recipient"`, the default, counts bilateral flows (Bi_Multi 1, 3, 7, and 8) and multilateral outflows (code 4). It leaves out money a donor gives to a multilateral agency (code 2), because the agency's own outflows count when it spends the money.

`view="provider"` counts the same bilateral flows and adds each provider's core contributions times the receiving agency's climate share. Each contribution counts once, in the provider's total. The agency's own outflows stay out of an all-provider total.

Under every method but OECD, the share is the agency's own climate commitments over its total CRS commitments, summed over the year and the year before, weighed with the method's own coefficients. The release holds each agency's commitments by channel, year, and marker level in `agency_spending`, so `ONE.replace(significant=1.0)` weighs an agency's significant commitments at 100%, as it weighs a bilateral row's. Agencies have no confirmed coefficient of their own, so PROVIDER weighs them at 0.4. Where an agency's two-year span holds under USD 100,000 of CRS commitments, the share holds at its last measured value. Channels with no CRS reporter, and years before an agency's CRDF rows begin, take the OECD's imputed share. Each attributed row carries its `share_source`: `computed`, `held`, or `oecd`. Under OECD, the share is always the OECD's.

Contributions to a channel with no share in a year attribute nothing. The Global Fund, Gavi, UNDP, and UNEP carry no share in any year, and the build's log lists every channel and year with contributions and no share, with the amount.

Under `flow="disbursements"` attribution applies the share to disbursed contributions, which reproduces the OECD's provider-perspective CRDF. Under `flow="commitments"` it applies the share to committed contributions, where the OECD imputes on disbursed contributions alone.

Core contributions count on leaf channels, and the aggregate channels the OECD publishes beside them drop out, so each dollar counts once. Where a provider's total on channel 40000 for a year and flow exceeds what its leaf channels itemise, the remainder stays as a 40000 row in `core_contributions`. That row takes no agency share, so it keeps the provider's contributions whole and adds nothing to attributed climate finance.

A share with no split by objective, as the OECD gives before 2021, sits in the `unsplit` column, so `value` equals the sum of its parts.

## EU Institutions

EU Institutions is a provider and also a channel for its members' money. `Method(eu="channel")`, the default for ONE and PROVIDER, attributes members' contributions to the EU budget, the European Development Fund, and the EIB to the members. In each year where an EU agency's channel attributes a member's contribution, the provider view scales that agency's own rows by the share of the channel's contributions from providers outside the query, so the money counts once. A query for EU Institutions alone keeps its own rows whole, a query that also names some of its members keeps the part those members do not fund, and an all-provider query keeps none. `release.quality().eu_left_out` reports the rows an all-provider total leaves out.

`eu="provider"`, the setting of OECD, counts the EU budget and the European Development Fund as a provider in its own right and the EIB as a channel. The EIB's own rows leave the provider view, and members' contributions to the EIB attribute at the method's share. Under either setting an all-provider total leaves the EIB's own lending out of the provider view, USD 8.0bn of disbursements and USD 27.4bn of commitments in 2015 to 2024.

EU Institutions' totals for `share_of_total()` hold the same rows as their amounts, at the same scaling.

## Comparing methods

The examples on this page run on the synthetic test release, so their amounts are invented.

The same query under each named method shows what the weights change. OECD counts significant markers at 100% where ONE counts them at 40%, so it gives the larger total. PROVIDER differs from ONE for providers whose stated coefficients differ from 0.4 and 1.0.

```python
import climate_finance as cf

release = cf.load()
methods = {
    "ONE": cf.methods.ONE,
    "OECD": cf.methods.OECD,
    "PROVIDER": cf.methods.PROVIDER,
}
for name, method in methods.items():
    result = release.spending(
        providers=["Germany", "Japan"], years=range(2021, 2024), method=method
    )
    print(name, round(result.table["value"].sum() / 1e6, 1))
```

```output
ONE 136.5
OECD 160.5
PROVIDER 145.8
```

`cf.compare()` joins results on year, provider, recipient, and Bi_Multi code, and names each column after what sets the results apart.

```python
one = release.spending(providers="Japan", years=[2023], method=cf.methods.ONE)
oecd = release.spending(providers="Japan", years=[2023], method=cf.methods.OECD)
print(cf.compare(one, oecd).table.columns)
```

```output
['year', 'provider_code', 'recipient_code', 'bi_multi', 'ONE', 'OECD']
```

`cf.methods.RANGE` returns the three methods in one table, one row per method. Use it for a figure that has to hold under any of the three readings.

```python
result = release.spending(providers="Japan", years=[2023], method=cf.methods.RANGE)
print(sorted(result.table["method"].unique()))
```

```output
['OECD', 'ONE', 'PROVIDER']
```

`Method` takes any coefficients, and `replace()` copies a method with changes. `explain()` describes the result in words.

```python
half = cf.methods.ONE.replace(significant=0.5)
result = release.spending(providers="Japan", years=[2023], method=half)
print(result.explain().splitlines()[1][:46])
```

```output
Method significant 0.5, principal 1, highest m
```

Under PROVIDER, `explain()` names the source of each provider's coefficients.

```python
result = release.spending(
    providers=["Germany", "Japan"], years=[2021], method=cf.methods.PROVIDER
)
for line in result.explain().splitlines():
    if line.startswith("Coefficients of"):
        print(line)
```

```output
Coefficients of providers 701: Fixture report, footnote 4.
Coefficients of providers 5: Fixture survey, p. 8.
```

## Next

- [Python API](../reference/python-api.md) lists the fields of `Method` and the arguments of `spending()`.
- [Release tables](../reference/release-tables.md) describes `agency_shares`, `core_contributions`, and `provider_coefficients`.

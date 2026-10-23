# Python API

Everything in version 2.0 imports from `climate_finance`, usually as `cf`. The names in `cf.__all__` form the public API. [Migrating from 1.x](../migrating-from-1x.md) covers the deprecated 1.x names.

## `cf.load(release="latest", *, path=None, cache_dir=None, refresh=False)`

Opens a release and returns a `Snapshot`.

Parameters:

- `release` (`str`, default `"latest"`). A release name from the release index, or `"latest"`.
- `path` (`str | Path | None`). A release directory on disk. When given, the function ignores the other arguments and downloads nothing.
- `cache_dir` (`str | Path | None`). Where releases unpack. Defaults to `CLIMATE_FINANCE_CACHE`, then `~/.cache/climate-finance`.
- `refresh` (`bool`, default `False`). When `True`, reads the newest release index online. When `False`, reads the index shipped in the wheel.

A cached release opens offline. For an uncached release, `cf.load()` downloads the pack, checks its sha256 against the index, and unpacks it into the cache. A cached copy that fails its check downloads again.

Raises:

- `ReleaseIntegrityError` when `release.json` or a table is missing, a table's sha256 differs from `release.json`, or a pack's sha256 differs from the index.
- `UnknownNameError` when the index lists no release of that name, with close names as suggestions.
- `SourceUnavailableError` when the index or a pack fails to download.

```python
import climate_finance as cf

snap = cf.load()
print(snap.manifest.release, snap.manifest.package_version)
```

```output
fixture 2.0.0.dev0
```

## `Snapshot`

Represents one release on disk. `snap.path` is its directory and `snap.manifest` is the parsed `release.json`.

### `Snapshot.spending(*, providers, recipients, years, method, flow, view, currency, prices, oda_only, spec)`

Returns a `Result`. Every argument is optional and keyword-only:

- `providers`, `recipients` (`str | int | Sequence[str | int] | None`). Names or DAC codes. `None` takes all. A name resolves by exact match, ignoring case and accents.
- `years` (`int | Iterable[int] | None`). `None` takes all years.
- `method` (`Method | MethodRange`, default `cf.methods.ONE`).
- `flow` (`"commitments" | "disbursements" | "grant_equivalent"`, default `"disbursements"`).
- `view` (`"recipient" | "provider"`, default `"recipient"`).
- `currency` (`"USD" | "EUR" | "GBP" | "CAD"`, default `"USD"`).
- `prices` (`int | "current"`, default `"current"`). A base year for constant prices, or `"current"`.
- `oda_only` (`bool`, default `False`).
- `spec` (`Spec | None`). Replaces every other argument.

Raises `UnknownProviderError` or `UnknownRecipientError` for a name outside the listings.

**Flow.** `flow="disbursements"` counts what the trace allocated to climate commitments. `flow="commitments"` counts CRDF commitments at face value in the commitment year, with `low`, `value`, and `high` equal. `flow="grant_equivalent"` counts the grant equivalent of the disbursements and leaves unvalued records missing. `snap.quality().summary()` reports this coverage as `grant_equiv_coverage`.

```python
for flow in ("commitments", "disbursements", "grant_equivalent"):
    result = snap.spending(providers="Japan", years=[2021, 2022], flow=flow)
    print(flow, round(result.table.to_polars()["value"].sum() / 1e6, 1))
```

```output
commitments 130.0
disbursements 38.0
grant_equivalent 8.0
```

**View.** `view="recipient"` counts what reaches recipients, including the outflows of multilateral agencies. `view="provider"` adds each provider's core contributions to multilateral agencies multiplied by the agency's climate share, and excludes the agencies' own outflows. Attributed rows have no recipient. The `share_source` column of `result.rows(origin="attribution")` shows whether each share is `computed`, `held`, or `oecd`.

```python
result = snap.spending(providers="France", years=[2022], view="provider")
for row in result.table.to_polars().sort("bi_multi").iter_rows(named=True):
    print(row["bi_multi"], round(row["value"] / 1e6, 2))
```

```output
1 0.48
2 12.25
```

Bi_Multi 1 is France's bilateral spending, and 2 is the climate share of its contributions to multilateral agencies.

**Scope.** `oda_only=True` keeps commitments of the CRDF's ODA category and disbursements of the CRS's ODA flow codes 11, 13, and 19.

```python
result = snap.spending(providers="Japan", years=[2021, 2022], oda_only=True)
print(round(result.table.to_polars()["value"].sum() / 1e6, 1))
```

```output
38.0
```

**Currency and prices.** `currency` takes `"USD"`, `"EUR"`, `"GBP"`, or `"CAD"`. `prices` takes `"current"` or a base year. Conversion uses the OECD DAC exchange rates and deflators through pydeflate by provider, falling back to the DAC total for providers without their own series. It downloads those series on the first run, so the test suite skips this example.

```python skip
result = snap.spending(
    providers="France", years=range(2018, 2023), currency="EUR", prices=2022
)
```

`result.explain()` states the currency, the price basis, and the conversion source.

### `Snapshot.quality(*, providers, years, method)`

Returns a `Quality` object.

### `Snapshot.reconcile(*, parties, years, method)`

Compares each party's BTR Table III.1 total with OECD figures for its bilateral finance. Returns a `Reconciliation`:

- `parties` (`str | Sequence[str] | None`). ISO 3 codes as the `btr` table holds them, such as `"NOR"` and `"EUU"`. `None` compares every party.
- `years` (`Iterable[int]`, default `range(2015, 2023)`).
- `method` (`Method | MethodRange`, default `cf.methods.ONE`).

### `Snapshot.disbursement_profile()`

Returns a polars DataFrame of the cumulative disbursed share of traceable climate value by years since commitment, for the groups `all`, `bilateral`, and `multilateral`. `vintages` counts the commitment years observed for that duration.

### `Snapshot.table(name)`

Returns one release table as a polars LazyFrame. Raises `KeyError` for a name outside the release tables.

## `Spec`

A frozen pydantic model of every `spending()` argument. `Spec.methods` returns the tuple of `Method` objects that the `method` field names.

| Field | Type | Default |
|---|---|---|
| `providers` | `tuple[str \| int, ...] \| None` | `None` |
| `recipients` | `tuple[str \| int, ...] \| None` | `None` |
| `years` | `tuple[int, ...] \| None` | `None` |
| `method` | `Method \| MethodRange` | `cf.methods.ONE` |
| `flow` | `Flow` | `"disbursements"` |
| `view` | `View` | `"recipient"` |
| `currency` | `Currency` | `"USD"` |
| `prices` | `int \| "current"` | `"current"` |
| `oda_only` | `bool` | `False` |

With `view="recipient"`, the query counts bilateral flows (Bi_Multi 1, 3, 7, and 8) and multilateral outflows (code 4). With `view="provider"`, it counts the bilateral flows plus each provider's core contributions multiplied by the receiving agency's climate share. Both exclude code 2 rows, so each dollar counts once.

`oda_only=True` keeps commitments of the CRDF's ODA flow category (10) and disbursements of the CRS's ODA flow codes 11, 13, and 19. It keeps bank-ledger and IATI fills whose commitments are in category 10, drops rows without a flow category or code, drops the modelled line, and keeps attributed core contributions.

## `Method` and the named methods

A frozen pydantic model with six fields.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `significant` | `float` | required | Share of face value counted for a significant marker, 0 to 1. |
| `principal` | `float` | required | Share counted for a principal marker, 0 to 1. |
| `highest_marker` | `bool` | required | Count each activity once under its higher marker. |
| `coefficients` | `"fixed" \| "provider"` | `"fixed"` | `"provider"` takes each provider's own `significant` where the release has a confirmed one. |
| `relevance` | `"markers" \| "jev"` | `"markers"` | `"jev"` reads Jev's adaptation level where Jev labelled a record. |
| `eu` | `"channel" \| "provider"` | `"channel"` | How EU Institutions count in the provider view. |

`Method.replace(**changes)` returns a validated copy. `cf.methods.ONE`, `OECD`, and `PROVIDER` are `Method` values, and `cf.methods.RANGE` is a `MethodRange` of the three. `cf.methods.describe(method)` returns the plain-language text that `Result.explain()` prints. [About the methods](../explanation/methods.md) explains the choices.

```python
print(cf.methods.ONE.replace(significant=0.5).significant)
```

```output
0.5
```

## `Result`

The answer to one `Spec`:

- `table` is a `Table` with one row per year, provider, recipient, Bi_Multi code, and method. Its columns are: `year`, `provider_code`, `recipient_code`, `bi_multi`, `method`, `amount`, `adaptation`, `mitigation`, `cross_cutting`, `unsplit`, `value`, `low`, and `high`. `amount` is the unweighted face value. `value` is the weighted climate value, equal to adaptation + mitigation + `cross_cutting` + `unsplit` under the highest-marker rule (where objectives are disjoint), and to adaptation + mitigation − `cross_cutting` + `unsplit` otherwise. `unsplit` holds attributed value whose agency share lacks a split by objective. `Table` converts with `to_polars()`, `to_pandas()`, and `to_arrow()`.
- `modelled` is a `Table` of the "modelled, not reported" range with the columns: `year`, `provider_code`, `recipient_code`, `basis`, `low`, and `high`. It is populated only when the query asks for recipient-view disbursements with `oda_only=False`.
- `spec` is the `Spec`. `snap.spending(spec=result.spec)` reruns the query to an identical table.
- `meta` holds `release`, `package_version`, `source_versions`, and `model_versions`.
- `rows(**filters)` returns the project rows behind the table as a polars DataFrame, filtered by column equality. A list, tuple, set, or range matches any item. Raises `KeyError` for an unknown column. `origin` is `commitment`, `disbursement`, or `attribution`. Alongside the release table columns, a row carries `link_method`, `link_probability`, `allocation_weight`, `objective_label`, `label_source`, `jev_level`, `share_source`, `coefficient_basis`, `method`, and the weighted amounts.
- `explain()` prints the method, view, flow, filters, and units in plain language.
- `share_of_total()` returns a `Result` with every amount divided by all CRS flows for the same flow, year, provider, recipient, and Bi_Multi code. A key missing from `total_flows` gives null.

```python
result = snap.spending(providers="Japan", recipients="Bangladesh", years=[2023])
print(sorted(result.rows(origin="disbursement")["link_method"].unique()))
```

```output
['crs_id', 'pn_recipient', 'programme_number', 'title_recipient']
```

## `Quality`

Returned by `Snapshot.quality()`. Each attribute is a `Table`:

- `table` holds the release's `quality` rows by provider and year.
- `status` holds climate value by provider and commitment status.
- `eu_left_out` holds the value of an EU Institutions agency's own rows that an all-provider provider view excludes under `eu="channel"`, broken down by agency, channel, year, and flow.
- `attribution` holds the value attributed through core contributions by provider, year, and flow, split by share source into `computed_usd`, `held_usd`, and `oecd_usd`.
- `precision` holds one row per provider with the columns: `provider_code`, `estimate`, `lower`, `upper`, `passed`, `waived`, and `detail`. `estimate` is the share of allocated value whose link a blind sample verified, `lower` and `upper` are its interval, `passed` records whether the lower bound reached the release's threshold, and `waived` records whether a failure was waived. The table is empty for an ungated release.
- `summary()` returns a DataFrame with one row per provider containing: `commitments`, `climate_usd`, `repeat_climate_usd`, `link_rate`, `linked_high_probability_share`, `model_labelled_share`, `report_labelled_share`, `grant_equiv_coverage`, and a `<status>_share` for each status.

`link_rate` is the share of climate value that traces to CRS records. `linked_high_probability_share` is the part of that value whose most probable link is at least 0.8. `grant_equiv_coverage` is the share of linked records with a grant equivalent. `repeat_climate_usd` is the climate value of rows that repeat an earlier vintage, which every total excludes.

## `Reconciliation`

Returned by `Snapshot.reconcile()`. `table` has one row per party, year, and method, with the columns: `party`, `provider_code`, `year`, `status`, `method`, `party_weight`, `weight_confirmed`, `btr_face`, `btr_grant_equivalent`, `unfccc_only_usd`, four comparators, and `btr_over_<comparator>` (the BTR face value divided by each comparator). The comparators are `crdf_at_party_weight`, `crdf_at_100`, `traced_climate_value`, and `crs_disbursed_at_party_weight`.

`rows(party, year)` returns the BTR rows of that party and year alongside each linked CRS record, with the record's facts prefixed `crs_`, and `unfccc_only` set to true for unlinked rows.

## `compare(*results)` and `Comparison`

`cf.compare(a, b, ...)` places results side by side. It returns a `Comparison` whose `table` has one row per year, provider, recipient, and Bi_Multi code, and one `value` column per result and method, named by the `Spec` fields that distinguish the results. A key absent from a result is null in its column. Calling `compare()` with no arguments raises `ValueError`.

## Listings

`cf.providers`, `cf.recipients`, `cf.flows`, and `cf.methods` each provide three methods: `.list()`, which returns a polars DataFrame; `.info(name)`, which returns a description by name or, for providers and recipients, by DAC code; and `.search(text)`, which returns the rows whose names contain `text`.

```python
print(cf.providers.info("Japan"))
```

```output
Japan (DAC code 701): DAC member.
```

## Errors

All errors inherit from `ClimateFinanceError`.

| Error | Raised when |
|---|---|
| `UnknownNameError` | a name is absent from its listing. `.suggestions` holds close names. |
| `UnknownProviderError`, `UnknownRecipientError` | a provider or recipient is absent from its listing. |
| `ReleaseIntegrityError` | a release fails its check. |
| `SourceUnavailableError` | a download fails. |
| `SchemaError` | a table fails to match its schema. |

## Next

- [Release tables](release-tables.md) lists the columns behind `Result.rows()`.
- [About the methods](../explanation/methods.md) compares ONE, OECD, and PROVIDER side by side.
- [How to trace a figure](../how-to/trace-a-figure.md) puts `rows()`, `explain()`, and `Spec` to work.

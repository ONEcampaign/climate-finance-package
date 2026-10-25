# Python API

Everything in version 2.0 imports from `climate_finance`, usually as `cf`. The examples run on the synthetic test release, so their amounts are invented. The names in `cf.__all__` form the public API. `Quality` and `Reconciliation`, the return types of `quality()` and `reconcile()`, live in `climate_finance.result`, and the errors live in `climate_finance.errors`. [Migrating from 1.x](../migrating-from-1x.md) covers the deprecated 1.x names.

## `cf.load(release="latest", *, path=None, cache_dir=None, refresh=False)`

Opens a release and returns a `Snapshot`.

Parameters:

- `release` (`str`, default `"latest"`). A release name from the release index, or `"latest"`. The index shipped in the wheel is empty until the first 2.0 release is published, so until then a call without `path` raises `UnknownNameError`.
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

### `Snapshot.spending(*, providers, recipients, years, method, flow, view, currency, prices, oda_only, measure, untraced, spec)`

Returns a `Result`. Every argument is optional and keyword-only:

- `providers`, `recipients` (`str | int | Sequence[str | int] | None`). Names or DAC codes. `None` takes all. A name resolves by exact match, ignoring case and accents.
- `years` (`int | Iterable[int] | None`). `None` takes all years.
- `method` (`Method | MethodRange`, default `cf.methods.ONE`).
- `flow` (`"commitments" | "disbursements" | "grant_equivalent"`, default `"disbursements"`).
- `view` (`"recipient" | "provider"`, default `"recipient"`).
- `currency` (`"USD" | "EUR" | "GBP" | "CAD"`, default `"USD"`).
- `prices` (`int | "current"`, default `"current"`). A base year for constant prices, or `"current"`.
- `oda_only` (`bool`, default `False`).
- `measure` (`"amount" | "share_of_total"`, default `"amount"`). `"share_of_total"` divides every amount by all CRS flows of its key, as `Result.share_of_total()` does.
- `untraced` (`bool`, default `True`). `False` leaves out the untraced rows of disbursements and grant equivalents. Commitments have none.
- `spec` (`Spec | None`). Replaces every other argument.

Raises `UnknownProviderError` or `UnknownRecipientError` for a name outside the listings, and `MissingLabelsError` for a method with `relevance="jev"` on a release that holds no Jev labels.

**Flow.** `flow="disbursements"` counts what the trace allocated to the release's climate commitments, by the year of the disbursement record. A disbursement counts against a commitment the release holds, and the commitments start at the first CRDF year, so the earliest years count less of what was paid. Rio-marked CRS disbursement that reaches no commitment, the excess beyond a full commitment and the records linked to none, counts as untraced, weighted by the record's own markers, in `value` and `high` and not in `low`. Disbursements end at the build's `crs_last_complete`. `flow="commitments"` counts CRDF commitments at face value in the commitment year, with `low`, `value`, and `high` equal. `flow="grant_equivalent"` counts the grant equivalent of the disbursements. A record with no grant equivalent, or a negative one on a positive disbursement, counts nothing, and `snap.quality().summary()` reports the valued share of allocated value as `grant_equiv_coverage`.

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

Returns a `Quality` object. Its amounts are the CRDF's published climate value of the commitments. `method` acts on the provider-view columns, `eu_left_out` and `attribution`, and on the climate value of untraced disbursement in `untraced`.

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
| `measure` | `Measure` | `"amount"` |
| `untraced` | `bool` | `True` |

With `view="recipient"`, the query counts bilateral flows (Bi_Multi 1, 3, 7, and 8) and multilateral outflows (code 4). With `view="provider"`, it counts the bilateral flows plus each provider's core contributions multiplied by the receiving agency's climate share. Both views exclude code 2 rows from the release tables. The provider view adds attributed core contributions, which carry Bi_Multi 2, so each dollar counts once.

`oda_only=True` keeps commitments of the CRDF's ODA flow category (10) and disbursements of the CRS's ODA flow codes 11, 13, and 19. It keeps bank-ledger and IATI fills whose commitments are in category 10, drops rows without a flow category or code, drops the modelled line, and keeps attributed core contributions.

## `Method` and the named methods

A frozen pydantic model with six fields.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `significant` | `float` | required | Share of face value counted for a significant marker, 0 to 1. |
| `principal` | `float` | required | Share counted for a principal marker, 0 to 1. |
| `highest_marker` | `bool` | required | Count each activity once under its higher marker. |
| `coefficients` | `"fixed" \| "provider"` | `"fixed"` | `"provider"` takes each provider's coefficients for the row's year and agency from `provider_coefficients`. |
| `relevance` | `"markers" \| "jev"` | `"markers"` | `"jev"` reads Jev's adaptation level where Jev labelled a record, and needs a release built with `jev_labels` on. |
| `eu` | `"channel" \| "provider"` | `"channel"` | How EU Institutions count in the provider view. |

`Method.replace(**changes)` returns a validated copy. `cf.methods.ONE`, `OECD`, and `PROVIDER` are `Method` values, and `cf.methods.RANGE` is a `MethodRange` of the three. `cf.methods.describe(method)` returns the plain-language text that `Result.explain()` uses for the method. [About the methods](../explanation/methods.md) explains the choices.

```python
print(cf.methods.ONE.replace(significant=0.5).significant)
```

```output
0.5
```

## `Result`

The answer to one `Spec`:

- `table` is a `Table` with one row per year, provider, recipient, Bi_Multi code, and method. Its columns are: `year`, `provider_code`, `recipient_code`, `bi_multi`, `method`, `amount`, `adaptation`, `mitigation`, `cross_cutting`, `unsplit`, `value`, `low`, `high`, and `untraced`. `amount` is the unweighted face value. `value` is the weighted climate value, equal to adaptation + mitigation + `cross_cutting` + `unsplit` under the highest-marker rule (where objectives are disjoint), and to adaptation + mitigation − `cross_cutting` + `unsplit` otherwise. `unsplit` holds attributed value whose agency share lacks a split by objective. `untraced` is the part of `value` from untraced rows, so `value` − `untraced` is the traced value, and it is 0 for commitments. `Table` converts with `to_polars()`, `to_pandas()`, and `to_arrow()`.
- `modelled` is a `Table` of the "modelled, not reported" range with the columns: `year`, `provider_code`, `recipient_code`, `basis`, `low`, and `high`. It is populated only when the query asks for recipient-view disbursements with `oda_only=False`.
- `spec` is the `Spec`. `snap.spending(spec=result.spec)` reruns the query to an identical table.
- `meta` holds `release`, `package_version`, `source_versions`, `model_versions`, `years`, and `conversion`. `years` copies the release's `years`. `conversion` holds the pydeflate version and a SHA-256 of the factors behind a converted result, because pydeflate refreshes its DAC data every 30 days, and is None for USD at current prices.
- `rows(**filters)` returns the project rows behind the table as a polars DataFrame, filtered by column equality. A list, tuple, set, or range matches any item. Raises `KeyError` for an unknown column. `origin` is `commitment`, `disbursement`, `untraced`, or `attribution`. A row has the columns `origin`, `row_id`, `record_id`, `year`, `provider_code`, `agency_code`, `recipient_code`, `channel_code`, `bi_multi`, `purpose_code`, `flow_category`, `flow_code`, `project_number`, `title`, `status`, `rio_adaptation`, `rio_mitigation`, `record_rio_adaptation`, `record_rio_mitigation`, `marker_source`, `reason`, `climate_share`, `share_source`, `link_method`, `link_probability`, `allocation_weight`, `objective_label`, `label_source`, `jev_level`, `amount`, `coefficient_basis`, `coefficient_source`, and `method`, followed by the weighted amounts `adaptation`, `mitigation`, `cross_cutting`, `unsplit`, `value`, `low`, `high`, and `untraced`. An untraced row has no `row_id`, carries the record's own markers with `marker_source` `record`, and has `reason` `excess` for a linked record's disbursement beyond what its commitments take or `unlinked` for a record linked to no commitment. `coefficient_basis` is `provider`, `default`, or `case_by_case`, and null where the release has no coefficient row for the provider.
- `explain()` returns the method, view, flow, filters, and units in plain language, as a string.
- `share_of_total()` returns the same result under `measure="share_of_total"`, with every amount divided by all CRS flows for the same flow, year, provider, recipient, and Bi_Multi code, so its spec reruns to the same shares. A key missing from `total_flows` gives null. In the provider view, EU Institutions' total holds the same rows as their amounts under each method, at the same scaling.

```python
result = snap.spending(providers="Japan", recipients="Bangladesh", years=[2023])
print(sorted(result.rows(origin="disbursement")["link_method"].unique()))
```

```output
['crosswalk_amount', 'crs_id', 'pn_recipient', 'title_recipient']
```

The synthetic release behind these examples carries `crosswalk_amount` and `title_recipient` as disbursement links. A build writes both only as routes from a CRDF row to its CRS commitment. [How linking works](../explanation/how-linking-works.md) lists the methods it writes.

## `Quality`

Returned by `Snapshot.quality()`. Each attribute is a `Table`:

- `table` holds the release's `quality` rows by provider and year.
- `status` holds climate value by provider and commitment status.
- `eu_left_out` holds the value of an EU Institutions agency's own rows that an all-provider provider view excludes under `eu="channel"`, broken down by agency, channel, year, and flow.
- `attribution` holds the value attributed through core contributions by provider, year, and flow, split by share source into `computed_usd`, `held_usd`, and `oecd_usd`.
- `untraced` holds, by provider and disbursement year, the Rio-marked CRS disbursement outside `disbursements` at face value, from the release's `unallocated` table: `excess_usd`, `unlinked_marked_usd`, `outside_release_usd`, and `fund_unlinked_usd`. `excess_climate_usd` and `unlinked_climate_usd` are the climate value of the first two under `method` in the recipient view, which disbursement totals count as untraced. No total counts the other two.
- `precision` holds one row per provider with the columns: `provider_code`, `estimate`, `lower`, `upper`, `passed`, `waived`, and `detail`. `estimate` is the share of allocated value whose link a blind sample verified, `lower` and `upper` are its interval, `passed` records whether the lower bound reached the release's threshold, and `waived` records whether a failure was waived. The table is empty for an ungated release.
- `summary()` returns a DataFrame with one row per provider containing: `commitments`, `climate_usd`, `repeat_climate_usd`, `link_rate`, `certain_route_share`, `model_labelled_share`, `report_labelled_share`, `grant_equiv_coverage`, and a `<status>_share` for each status.

`link_rate` is the share of climate value whose CRDF rows match a CRS commitment, whether or not anything disbursed against it, so a provider can read a `link_rate` of 1.0 beside a `not_traceable_share` of 1.0. `certain_route_share` is the part of the climate value whose CRDF rows all reach their CRS commitment through certain routes. `grant_equiv_coverage` is the part of the linked commitments' allocated value whose record has a grant equivalent. `repeat_climate_usd` is the climate value of rows that repeat an earlier vintage, which every total excludes.

## `Reconciliation`

Returned by `Snapshot.reconcile()`. `table` has one row per party, year, status, and method, and rows with no status sit on their own row. The columns, in order:

- `party`, `provider_code`, `year`, `status`, `method`, `party_weight`, `party_principal`, `weight_source`, `coefficient_basis`. The last four come from the provider's `provider_coefficients` row for every agency without a row of its own. `coefficient_basis` is `provider`, `default`, or `case_by_case`, as in `Result.rows()`, and null where the release holds no such row.
- `crdf_method_share` and `crs_method_share`: the share of `crdf_at_party_weight`, and of `crs_disbursed_at_party_weight`, that rests on `PROVIDER`'s coefficients in place of the party's own, because the row's coefficients are case by case with no value for its marker or the provider has none in the release. The grant-equivalent comparator weighs the same records and shares `crs_method_share`. Each is null where its comparator is zero or null.
- `btr_face`: the BTR total at the DAC rate. `btr_face_reported` is the total at the party's own USD conversion, `dac_rate` the DAC rate, and `party_rate` the rate the party's USD figure implies (`dac_rate` × `btr_face` ÷ `btr_face_reported`).
- `btr_grant_equivalent`: the BTR grant equivalent at the DAC rate, counting rows with no status and rows with zero face value.
- `unfccc_only_usd`: BTR rows that no CRS record links to, split by funding source into `unfccc_only_oda_usd`, `unfccc_only_oof_usd`, and `unfccc_only_other_usd`.
- `oecd_only_usd`: the status's CRS flow on bilateral climate-marked records that no BTR row links to. Null on a row with no status, and on a year the release's CRS does not cover.
- The comparators `crdf_at_party_weight`, `crdf_at_100`, `traced_climate_value`, `crs_disbursed_at_party_weight`, and `crs_grant_equivalent_at_party_weight`. The three weighted ones weigh each commitment or CRS record as `PROVIDER` does: by its agency's coefficients, else its provider's, at the both-significant coefficient for one significant on both objectives where the provider states one, and at `PROVIDER`'s 0.4 and 1.0 where those coefficients are case by case with no default, at the agency level as for France's AFD or the provider level as for the UK. A provider with no `provider_coefficients` row weighs every activity at `PROVIDER`'s coefficients, as `spending()` does, and reads 1.0 in both shares. `reconcile` raises `ReleaseIntegrityError` when two `provider_coefficients` rows of one provider and agency cover the same year.
- `comparator`: the comparator of the row's flow, `crdf_at_party_weight` for committed rows and `crs_disbursed_at_party_weight` for disbursed rows, null on a row with no status. `btr_over_comparator` is the BTR total over it, and `thin_comparator` is true where the comparator is below USD 10m (`query.THIN_COMPARATOR_USD`).
- `btr_over_<comparator>` for each of the four face comparators, null for the other flow's comparators, and `btr_grant_equivalent_over_crs_grant_equivalent`. Every ratio is null where its comparator is zero or null.

`rows(party, year)` returns the BTR rows of that party and year alongside each linked CRS record, with the record's facts prefixed `crs_`, and `unfccc_only` set to true for unlinked rows. On a published release, which leaves out `btr` and `btr_links`, it raises `WithheldTableError`.

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

All errors inherit from `ClimateFinanceError`. The three name errors import from `climate_finance`, and every error imports from `climate_finance.errors`.

| Error | Raised when |
|---|---|
| `UnknownNameError` | a name is absent from its listing. `.suggestions` holds close names. |
| `UnknownProviderError`, `UnknownRecipientError` | a provider or recipient is absent from its listing. |
| `ReleaseIntegrityError` | a release fails its check. |
| `SourceUnavailableError` | a download fails. |
| `MissingLabelsError` | a method with `relevance="jev"` queries a release with no Jev labels. The message names the `jev_labels` setting. |
| `WithheldTableError` | `Snapshot.table()` asks for a table the published release leaves out, `btr` or `btr_links`. `release.json` names each under `withheld_tables` with a note on how to build it. |
| `SchemaError` | a table fails to match its schema. |

## Next

- [Release tables](release-tables.md) lists the columns behind `Result.rows()`.
- [About the methods](../explanation/methods.md) compares ONE, OECD, and PROVIDER side by side.
- [How to trace a figure](../how-to/trace-a-figure.md) puts `rows()`, `explain()`, and `Spec` to work.

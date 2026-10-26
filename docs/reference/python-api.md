# Python API

Everything in version 2.0 imports from `climate_finance`, usually as `cf`. The examples run on the synthetic test release, so their amounts are invented. The names in `cf.__all__` form the public API, including `Quality` and `Reconciliation`, the return types of `quality()` and `reconcile()`, and `errors`, the module of the package's [error classes](#errors). [Migrating from 1.x](../migrating-from-1x.md) covers the deprecated 1.x names.

## `cf.load(release="latest", *, path=None, cache_dir=None, refresh=False)`

Opens a release and returns a `Release`.

Parameters:

- `release` (`str`, default `"latest"`). A release name from the release index, or `"latest"`. The index shipped in the wheel is empty until the first 2.0 release is published, so until then a call without `path` raises `UnknownNameError`, whose message names `cf.load(path=...)` and the package's GitHub releases page, where releases built with `cf-build release` are published.
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

release = cf.load()
print(release.manifest.release, release.manifest.package_version)
```

```output
fixture 2.0.0.dev0
```

## `Release`

Represents one release on disk. `release.path` is its directory and `release.manifest` is the parsed `release.json`.

A release prints its name, the years its commitments and disbursements cover, and its table count, naming any table it withholds. The synthetic test release carries the reserved name `fixture` and prints `synthetic figures` beside it. `cf-build release` refuses to cut or publish a release of that name.

```python
print(release)
```

```output
Release('fixture', synthetic figures): commitments 2021 to 2022, disbursements 2021 to 2024; 19 tables
```

### `Release.spending(*, providers, recipients, years, method, flow, view, currency, prices, oda_only, measure, untraced)`

Returns a `Result`. Every argument is optional and keyword-only, and each is a `Spec` field that `Spec(...)` takes in the same forms:

- `providers` (`str | int | Agency | Iterable[str | int | Agency] | None`). Provider names, DAC codes, or agencies, one or many. `None` takes all. A name can name one agency, such as `"EIB"`, and the query then counts that agency's rows only.
- `recipients` (`str | int | Iterable[str | int] | None`). Recipient names or DAC codes. `None` takes all.
- `years` (`int | Iterable[int] | None`). One year or many, such as `2022` or `range(2015, 2023)`. `None` takes all years.
- `method` (`Method | MethodRange | str | Iterable[Method | str]`, default `cf.methods.ONE`). A method, a range, a name from `cf.methods.list()` ignoring case, such as `"OECD"`, or a list of methods and names, which becomes a range.
- `flow` (`"commitments" | "disbursements" | "grant_equivalent" | "net_disbursements" | "net_transfers"`, default `"disbursements"`).
- `view` (`"recipient" | "provider"`, default `"recipient"`).
- `currency` (`"USD" | "EUR" | "GBP" | "CAD"`, default `"USD"`).
- `prices` (`int | "current"`, default `"current"`). A base year for constant prices, or `"current"`.
- `oda_only` (`bool`, default `False`).
- `measure` (`"amount" | "share_of_total"`, default `"amount"`). `"share_of_total"` divides every amount by all CRS flows of its key, as `Result.share_of_total()` does. A net flow refuses it.
- `untraced` (`bool`, default `True`). `False` leaves out the untraced rows of disbursements, grant equivalents, and net flows, and the repayments of the loans those rows disbursed. Commitments have none.

Raises `UnknownProviderError` or `UnknownRecipientError` for a name, code, or agency outside the listings, `UnknownNameError` for a method name outside `cf.methods.list()`, `InvalidInputError` for an argument in another form (see [Errors](#errors)), such as `years="2022"`, `flow="commitment"`, `spec=`, which `release.run(spec)` takes, or `measure="share_of_total"` or a method with `relevance="jev"` on a net flow, and `MissingLabelsError` for a method with `relevance="jev"` on a release that holds no Jev labels.

**Flow.** `flow="disbursements"` counts what the trace allocated to the release's climate commitments, by the year of the disbursement record. A disbursement counts against a commitment the release holds, and the commitments start at the first CRDF year, so the earliest years count less of what was paid. Rio-marked CRS disbursement that reaches no commitment, the excess beyond a full commitment and the records linked to none, counts as untraced, weighted by the record's own markers, in `value` and `high` and not in `low`. Disbursements end at the build's `crs_last_complete`. `flow="commitments"` counts CRDF commitments at face value in the commitment year, with `low`, `value`, and `high` equal. `flow="grant_equivalent"` counts the grant equivalent of the disbursements. A record with no grant equivalent, or a negative one on a positive disbursement, counts nothing, and `release.quality().summary()` reports the valued share of allocated value as `grant_equiv_coverage`.

`flow="net_disbursements"` counts the disbursements of `flow="disbursements"` less the principal repaid on the loans they disbursed, and `flow="net_transfers"` less the interest paid too. A repayment subtracts at the share its loan was counted under, so a loan counted at 40% climate repays at 40%. Each subtraction is a row of `result.rows()` with a negative amount and origin `repayment` or `interest`. Only repayments tied to a disbursement the release counts subtract, and `release.quality().repayments` reports the rest. Repayments end at `crs_last_complete`, and a repayment counts in the year it is made, so a loan disbursed in one year reduces the totals of the years that repay it. A year of heavy repayment can leave a provider's net flow below zero. The development banks report no interest after 2018, so their net transfers equal their net disbursements from 2019. `low` subtracts every attributed repayment and `high` only those whose link to the loan is certain, so `high` sits above `value`, as [Low and high bounds](../explanation/low-and-high-bounds.md) explains. Under `view="provider"` the bilateral repayments subtract, and core contributions attribute as disbursements, since a core contribution is not repaid. `flow="grant_equivalent"` subtracts nothing. [About the methods](../explanation/methods.md#net-flows) describes the attribution.

```python
for flow in ("commitments", "disbursements", "grant_equivalent"):
    result = release.spending(providers="Japan", years=[2021, 2022], flow=flow)
    print(flow, round(result.table["value"].sum() / 1e6, 1))
```

```output
commitments 130.0
disbursements 38.0
grant_equivalent 8.0
```

```python
for flow in ("disbursements", "net_disbursements", "net_transfers"):
    result = release.spending(providers="Germany", years=range(2021, 2025), flow=flow)
    print(flow, round(result.table["value"].sum() / 1e6, 1))
```

```output
disbursements 45.5
net_disbursements 41.5
net_transfers 41.2
```

The subtractions are rows. Germany's one repayment, in 2022, takes USD 4.0m of principal and USD 0.3m of interest off the net transfers.

```python
rows = result.rows(origin=["repayment", "interest"])
for row in rows.iter_rows(named=True):
    print(row["origin"], row["year"], row["row_id"], row["value"])
```

```output
interest 2022 DEU-1 -300000.0
repayment 2022 DEU-1 -4000000.0
```

**View.** `view="recipient"` counts what reaches recipients, including the outflows of multilateral agencies. `view="provider"` adds each provider's core contributions to multilateral agencies multiplied by the agency's climate share, and excludes the agencies' own outflows. Attributed rows have no recipient. The `share_source` column of `result.rows(origin="attribution")` shows whether each share is `computed`, `held`, or `oecd`.

```python
result = release.spending(providers="France", years=[2022], view="provider")
for row in result.table.sort("bi_multi").iter_rows(named=True):
    print(row["bi_multi"], round(row["value"] / 1e6, 2))
```

```output
1 0.48
2 12.25
```

Bi_Multi 1 is France's bilateral spending, and 2 is the climate share of its contributions to multilateral agencies.

**Scope.** `oda_only=True` keeps commitments of the CRDF's ODA category and disbursements of the CRS's ODA flow codes 11, 13, and 19.

```python
result = release.spending(providers="Japan", years=[2021, 2022], oda_only=True)
print(round(result.table["value"].sum() / 1e6, 1))
```

```output
38.0
```

**Currency and prices.** `currency` takes `"USD"`, `"EUR"`, `"GBP"`, or `"CAD"`. `prices` takes `"current"` or a base year. Conversion uses the OECD DAC exchange rates and deflators through pydeflate by provider, falling back to the DAC total for providers without their own series. It downloads those series on the first run, so the test suite skips this example.

```python skip
result = release.spending(
    providers="France", years=range(2018, 2023), currency="EUR", prices=2022
)
```

`result.explain()` states the currency, the price basis, and the conversion source.

### `Release.run(spec)`

Runs a saved `Spec`, such as `result.spec` or one read back with `cf.Spec.model_validate_json`, and returns its `Result`. A mapping, such as a saved spec read back with `json.load`, reads as `cf.Spec.model_validate` reads it, and any other value raises `InvalidInputError`. `spec.replace(...)` changes a field and takes the forms `spending()` takes. `cf.Spec.model_validate_json` raises `InvalidInputError` for malformed JSON.

### `Release.quality(*, providers, years, method)`

Returns a `Quality` object. Its amounts are the CRDF's published climate value of the commitments. `method` acts on the provider-view columns, `eu_left_out` and `attribution`, and on the climate value of untraced disbursement in `untraced`. `providers` and `years` take the forms `spending()` takes, and `method` one method or its name. A range raises `InvalidInputError`. An agency raises `InvalidInputError` naming its provider, since the tables count providers, and so does an unknown keyword, naming a close one. Years the `quality` table lacks give a `CoverageWarning`.

### `Release.reconcile(*, providers, years, method)`

Compares each party's BTR Table III.1 total with OECD figures for its bilateral finance. An unknown keyword raises `InvalidInputError` naming a close one, so `parties=` names `providers`. Returns a `Reconciliation`:

- `providers` (`str | int | Iterable[str | int] | None`). The parties, named as `spending()` names providers, so `"Germany"`, `"DEU"`, and `5` name one party. `None` compares every party. An agency raises `InvalidInputError` naming its provider, since the BTR reports providers.
- `years` (`int | Iterable[int] | None`). `None` takes every year the BTR tables hold.
- `method` (`Method | MethodRange | str`, default `cf.methods.ONE`), in the forms `spending()` takes.

### `Release.jev_flags(*, provider, year, flagged, experimental)`

Returns a polars DataFrame of Jev's answers about the CRS records the release flags, one row per record, with the record's provider, agency, recipient, year, title, purpose, flow, commitment and disbursement, both Rio markers and CRS id. The table is empty unless the release was built with `jev_flags` on. [About the methods](../explanation/methods.md#jev-flags) describes the four questions and the cuts.

- `provider` (`str | int | Iterable[str | int] | None`). Providers named as `quality()` names `providers`. An agency raises `InvalidInputError` naming its provider. `None` takes all.
- `year` (`int | Iterable[int] | None`). The records' CRS years. `None` takes all.
- `flagged` (`"no_aim" | "surprising" | None`). Keeps the records where that flag holds. `None` keeps every record, flagged or not.
- `experimental` (`bool`, default `False`). Adds `mitigation_aim_p`, which no external check backs.

Each row holds `no_aim_p`, `adaptation_aim_p`, `surprising_p` and `counterproductive_p`, the flags `no_aim` and `surprising`, and `short_text`, true for a text too short to ask, whose answers and flags are null. `counterproductive_p` ranks records and has no flag. `in_release` is true for a record that `disbursements` or `untraced` holds, or whose provider holds a `commitments` row with its CRS id, and false for a record the release's totals do not draw on. The flags change no total.

Raises `MissingLabelsError` naming `jev_flags` for a release built with it off, and `InvalidInputError` for a `flagged` or `experimental` outside the values above, an empty list of providers or years, years in another form, or an unknown keyword, naming a close one.

### `Release.disbursement_profile()`

Returns a polars DataFrame of the cumulative disbursed share of traceable climate value by years since commitment, for the groups `all`, `bilateral`, and `multilateral`. `vintages` counts the commitment years observed for that duration.

### `Release.table(name)`

Returns one release table as a polars LazyFrame. Raises `UnknownNameError`, with close names as suggestions, for a name outside the release tables.

## `Spec`

A frozen pydantic model of every `spending()` argument. `Spec.methods` returns the tuple of `Method` objects that the `method` field names.

| Field        | Type                                | Default           |
| ------------ | ----------------------------------- | ----------------- |
| `providers`  | `tuple[int \| Agency, ...] \| None` | `None`            |
| `recipients` | `tuple[int, ...] \| None`           | `None`            |
| `years`      | `tuple[int, ...] \| None`           | `None`            |
| `method`     | `Method \| MethodRange`             | `cf.methods.ONE`  |
| `flow`       | `Flow`                              | `"disbursements"` |
| `view`       | `View`                              | `"recipient"`     |
| `currency`   | `Currency`                          | `"USD"`           |
| `prices`     | `int \| "current"`                  | `"current"`       |
| `oda_only`   | `bool`                              | `False`           |
| `measure`    | `Measure`                           | `"amount"`        |
| `untraced`   | `bool`                              | `True`            |

The table gives the types a spec holds. `Spec(...)` takes each field in the forms `spending()` lists, and `cf.SpecInputs` types them for a type checker. `years` holds the years sorted and without duplicates, and `method` holds the `Method` or `MethodRange` a name names, so JSON keeps its coefficients.

`providers` and `recipients` read names, codes, and agencies through the listings and hold the DAC codes and `Agency` values they name, sorted and without duplicates. So `providers="Japan"` and `providers=701` give equal specs, and a provider named beside one of its agencies covers it. In JSON an agency is `{"provider": 918, "agency": 3}`.

```python
print(release.spending(providers=["EIB", "Japan", 701]).spec.providers)
```

```output
(701, Agency(provider=918, agency=3))
```

A spec prints only the fields away from their defaults, and a named method as the expression that returns it.

```python
print(repr(cf.Spec(providers="Japan", method="OECD")))
```

```output
Spec(providers=(701,), method=cf.methods.OECD)
```

An agency selects the rows whose `provider_code` and `agency_code` match it, and the result table of a spec that names one gains `agency` and `agency_code`, as `Result` describes. Attributed core contributions carry no agency, so a query for an agency returns none of them and no modelled line. Under a method with `eu="provider"`, such as `cf.methods.OECD`, the provider view leaves out the EIB's own rows, so `providers="EIB"` returns no rows there.

With `view="recipient"`, the query counts bilateral flows (Bi_Multi 1, 3, 7, and 8) and multilateral outflows (code 4). With `view="provider"`, it counts the bilateral flows plus each provider's core contributions multiplied by the receiving agency's climate share. Both views exclude code 2 rows from the release tables. The provider view adds attributed core contributions, which carry Bi_Multi 2, so each dollar counts once.

`oda_only=True` keeps commitments of the CRDF's ODA flow category (10) and disbursements of the CRS's ODA flow codes 11, 13, and 19. It keeps bank-ledger and IATI fills whose commitments are in category 10, drops rows without a flow category or code, drops the modelled line, and keeps attributed core contributions.

## `Method` and the named methods

A frozen pydantic model with six fields.

| Field            | Type                      | Default     | Meaning                                                                                                           |
| ---------------- | ------------------------- | ----------- | ----------------------------------------------------------------------------------------------------------------- |
| `significant`    | `float`                   | required    | Share of face value counted for a significant marker, 0 to 1.                                                     |
| `principal`      | `float`                   | required    | Share counted for a principal marker, 0 to 1.                                                                     |
| `highest_marker` | `bool`                    | required    | Count each activity once under its higher marker.                                                                 |
| `coefficients`   | `"fixed" \| "provider"`   | `"fixed"`   | `"provider"` takes each provider's coefficients for the row's year and agency from `provider_coefficients`.       |
| `relevance`      | `"markers" \| "jev"`      | `"markers"` | `"jev"` reads Jev's adaptation level where Jev labelled a record, and needs a release built with `jev_labels` on. |
| `eu`             | `"channel" \| "provider"` | `"channel"` | How EU Institutions count in the provider view.                                                                   |

`Method.replace(**changes)` returns a validated copy. `cf.methods.ONE`, `OECD`, and `PROVIDER` are `Method` values, and `cf.methods.RANGE` is a `MethodRange` of the three. `cf.methods.describe(method)` returns the plain-language text that `Result.explain()` uses for the method. [About the methods](../explanation/methods.md) explains the choices.

```python
print(cf.methods.ONE.replace(significant=0.5).significant)
```

```output
0.5
```

## `Result`

The answer to one `Spec`:

- `table` is a polars DataFrame with one row per year, provider, recipient, Bi_Multi code, and method. Its columns, in order, are: `year`, `provider`, `recipient`, `method`, `provider_code`, `recipient_code`, `bi_multi`, `amount`, `adaptation`, `mitigation`, `cross_cutting`, `unsplit`, `value`, `low`, `high`, and `untraced`, so polars' truncated print shows the year, the names, the method and the last four. When the spec names an agency, the table also has `agency` after `provider` and `agency_code` after `provider_code`, and its rows are keyed by agency too: `agency_code` holds the agency of a provider the spec names through its agencies, and is null for the other providers. `provider` and `recipient` are the listing names of the codes beside them, null for a code the listings lack, as for the attributed rows' missing recipient. `amount` is the unweighted face value. `value` is the weighted climate value, equal to adaptation + mitigation + `cross_cutting` + `unsplit` under the highest-marker rule (where objectives are disjoint), and to adaptation + mitigation − `cross_cutting` + `unsplit` otherwise. `unsplit` holds attributed value whose agency share lacks a split by objective. `untraced` is the part of `value` from untraced rows, so `value` − `untraced` is the traced value, and it is 0 for commitments. It is computed on first access and kept, so reading it twice runs the query once. `pa.table(result.table)` reads it as an Arrow table, and `result.table.to_pandas()` returns a pandas frame when pandas is installed. `low` and `high` bound only `value`, the total over all objectives. [Step 6 of the tutorial](../tutorial.md#step-6-ask-for-one-objective) sums one objective.
- `modelled` is a polars DataFrame of the "modelled, not reported" range with the columns: `year`, `provider`, `provider_code`, `recipient`, `recipient_code`, `basis`, `low`, and `high`. It is populated only when the query asks for recipient-view disbursements with `oda_only=False`.
- `spec` is the `Spec`. `release.run(result.spec)` reruns the query to an identical table.
- `meta` holds `release`, `package_version`, `source_versions`, `model_versions`, `years`, and `conversion`. `years` copies the release's `years`. `conversion` holds the pydeflate version and a SHA-256 of the factors behind a converted result, because pydeflate refreshes its DAC data every 30 days, and is None for USD at current prices.
- `rows(*, provider=None, recipient=None, **filters)` returns the project rows behind the table as a polars DataFrame, filtered by column equality. A list, tuple, set, or range matches any item. `provider` and `recipient` take names or DAC codes, and `provider` agencies too, as `spending()` does, so `rows(recipient="kenya")` and `rows(recipient="KEN")` return Kenya's rows. Each filter item must be of its column's type, and agencies filter through `provider`, as `rows(provider="EIB")`. `origin`, `link_method`, `share_source`, `status`, `label_source`, and `reason` take only the values their columns hold, and `method` takes a method of the result in the forms `spending()` takes, so `rows(method="one")` returns the ONE rows. Raises `UnknownNameError`, with close names as suggestions, for an unknown column, a value outside its column's values, or a method the result lacks, `InvalidInputError` for an item of another type or an `agency` filter, so `rows(provider_code="Japan")` names `provider="Japan"`, and `UnknownProviderError` or `UnknownRecipientError`, with close names as suggestions, for a name the listings lack. `origin` is `commitment`, `disbursement`, `untraced`, `attribution`, `repayment`, or `interest`. A row has the columns `origin`, `year`, `record_id`, `link_method`, `provider`, `agency`, `recipient`, `method`, `row_id`, `provider_code`, `agency_code`, `recipient_code`, `channel_code`, `bi_multi`, `purpose_code`, `flow_category`, `flow_code`, `project_number`, `title`, `status`, `rio_adaptation`, `rio_mitigation`, `record_rio_adaptation`, `record_rio_mitigation`, `marker_source`, `reason`, `climate_share`, `share_source`, `link_probability`, `allocation_weight`, `objective_label`, `label_source`, `jev_level`, `coefficient_basis`, `coefficient_source`, and `amount`, followed by the weighted amounts `adaptation`, `mitigation`, `cross_cutting`, `unsplit`, `value`, `low`, `high`, and `untraced`. `agency` is the agency's name in the listings, null where they lack it. An untraced row has no `row_id`, carries the record's own markers with `marker_source` `record`, and has `reason` `excess` for a linked record's disbursement beyond what its commitments take or `unlinked` for a record linked to no commitment. `coefficient_basis` is `provider`, `default`, or `case_by_case`, and null where the release has no coefficient row for the provider.
- `explain()` returns the method, view, flow, filters, and units in plain language, as a string.
- `share_of_total()` returns the same result under `measure="share_of_total"`, with every amount divided by all CRS flows for the same flow, year, provider, recipient, and Bi_Multi code, so its spec reruns to the same shares. A key missing from `total_flows` gives null. In the provider view, EU Institutions' total holds the same rows as their amounts under each method, at the same scaling.

```python
result = release.spending(providers="Japan", recipients="Bangladesh", years=[2023])
print(sorted(result.rows(origin="disbursement")["link_method"].unique()))
```

```output
['crosswalk_amount', 'crs_id', 'pn_recipient', 'title_recipient']
```

The synthetic release behind these examples carries `crosswalk_amount` and `title_recipient` as disbursement links. A build writes both only as routes from a CRDF row to its CRS commitment. [How linking works](../explanation/how-linking-works.md) lists the methods it writes.

DuckDB queries the table by the name of the variable that holds it, here `frame`:

```python
import duckdb

frame = result.table
print(
    duckdb.sql(
        "select year, round(sum(value) / 1e6, 1) as value_musd from frame group by year order by year"
    )
)
```

```output
┌───────┬────────────┐
│ year  │ value_musd │
│ int16 │   double   │
├───────┼────────────┤
│  2023 │       48.3 │
└───────┴────────────┘
```

## `Quality`

Returned by `Release.quality()`. Each attribute is a polars DataFrame, and each table keyed by provider holds the provider's name, `provider`, before `provider_code`:

- `table` holds the release's `quality` rows by provider and year.
- `status` holds climate value by provider and commitment status.
- `eu_left_out` holds the value of an EU Institutions agency's own rows that an all-provider provider view excludes under `eu="channel"`, broken down by agency, channel, year, and flow, with the agency's name, `agency`, before `agency_code`.
- `attribution` holds the value attributed through core contributions by provider, year, and flow, split by share source into `computed_usd`, `held_usd`, and `oecd_usd`.
- `untraced` holds, by provider and disbursement year, the Rio-marked CRS disbursement outside `disbursements` at face value, from the release's `unallocated` table: `excess_usd`, `unlinked_marked_usd`, `outside_release_usd`, and `fund_unlinked_usd`. `excess_climate_usd` and `unlinked_climate_usd` are the climate value of the first two under `method` in the recipient view, which disbursement totals count as untraced. No total counts the other two.
- `repayments` holds, by provider and repayment year, the CRS principal and interest that a net flow reads: `received_usd` is the principal at face value, and one `<basis>_usd` column per basis of the release's [`reflows`](release-tables.md#reflows) table splits it by where it goes. `repaid_climate_usd` is the climate value of the principal a net flow subtracts under `method` in the recipient view, `interest_usd` the interest paid, and `measured_share` the share of principal tied to a disbursement the release counts, in the `commitment` and `untraced` bases. It is null where a provider and year repay no principal. A low `measured_share` marks a provider whose repayments the net flows leave out, such as a donor whose loans predate the CRS window.
- `precision` holds one row per provider with the columns: `provider`, `provider_code`, `estimate`, `lower`, `upper`, `passed`, `waived`, and `detail`. `estimate` is the share of allocated value whose link a blind sample verified, `lower` and `upper` are its interval, `passed` records whether the lower bound reached the release's threshold, and `waived` records whether a failure was waived. The table is empty for an ungated release.
- `summary()` returns a DataFrame with one row per provider containing: `provider`, `provider_code`, `commitments`, `climate_usd`, `repeat_climate_usd`, `link_rate`, `certain_route_share`, `model_labelled_share`, `report_labelled_share`, `grant_equiv_coverage`, and a `<status>_share` for each status.

A `Quality` prints a title line, with the number of providers, the years, and its other tables, above `summary()`.

`link_rate` is the share of climate value whose CRDF rows match a CRS commitment, whether or not anything disbursed against it, so a provider can read a `link_rate` of 1.0 beside a `not_traceable_share` of 1.0. `certain_route_share` is the part of the climate value whose CRDF rows all reach their CRS commitment through certain routes. `grant_equiv_coverage` is the part of the linked commitments' allocated value whose record has a grant equivalent. `repeat_climate_usd` is the climate value of rows that repeat an earlier vintage, which every total excludes.

## `Reconciliation`

Returned by `Release.reconcile()`. It prints a title line, naming the providers, years, and methods, above `table`. `table` has one row per party, year, status, and method, and rows with no status sit on their own row. The columns, in order:

- `party`, `provider`, `provider_code`, `year`, `status`, `method`, `party_weight`, `party_principal`, `weight_source`, `coefficient_basis`. The last four come from the provider's `provider_coefficients` row for every agency without a row of its own. `coefficient_basis` is `provider`, `default`, or `case_by_case`, as in `Result.rows()`, and null where the release holds no such row.
- `crdf_method_share`, `crs_method_share`, and `crs_grant_equivalent_method_share`: the share of `crdf_at_party_weight`, of `crs_disbursed_at_party_weight`, and of `crs_grant_equivalent_at_party_weight` that rests on `PROVIDER`'s coefficients in place of the party's own, because the row's coefficients are case by case with no value for its marker or the provider has none in the release. The grant-equivalent comparator weighs the same records as the disbursed one, but a loan's grant equivalent is a fraction of its disbursement, so its share differs where loans concentrate in agencies that assess case by case, as at France's AFD. Each is null where its comparator is zero or null.
- `btr_face`: the BTR total at the DAC rate. `btr_face_reported` is the total at the party's own USD conversion, `dac_rate` the DAC rate, and `party_rate` the rate the party's USD figure implies (`dac_rate` × `btr_face` ÷ `btr_face_reported`).
- `btr_grant_equivalent`: the BTR grant equivalent at the DAC rate, counting rows with no status and rows with zero face value.
- `unfccc_only_usd`: BTR rows that no CRS record links to, split by funding source into `unfccc_only_oda_usd`, `unfccc_only_oof_usd`, and `unfccc_only_other_usd`.
- `oecd_only_usd`: the status's CRS flow on bilateral climate-marked records that no BTR row links to. Null on a row with no status, and on a year the release's CRS does not cover.
- The comparators `crdf_at_party_weight`, `crdf_at_100`, `traced_climate_value`, `crs_disbursed_at_party_weight`, and `crs_grant_equivalent_at_party_weight`. The three weighted ones weigh each commitment or CRS record as `PROVIDER` does: by its agency's coefficients, else its provider's, at the both-significant coefficient for one significant on both objectives where the provider states one, and at `PROVIDER`'s 0.4 and 1.0 where those coefficients are case by case with no default, at the agency level as for France's AFD or the provider level as for the UK. A provider with no `provider_coefficients` row weighs every activity at `PROVIDER`'s coefficients, as `spending()` does, and reads 1.0 in all three shares. `reconcile` raises `ReleaseIntegrityError` when two `provider_coefficients` rows of one provider and agency cover the same year.
- `comparator`: the comparator of the row's flow, `crdf_at_party_weight` for committed rows and `crs_disbursed_at_party_weight` for disbursed rows, null on a row with no status. `btr_over_comparator` is the BTR total over it, and `thin_comparator` is true where the comparator is below USD 10m (`query.THIN_COMPARATOR_USD`).
- `btr_over_<comparator>` for each of the four face comparators, null for the other flow's comparators, and `btr_grant_equivalent_over_crs_grant_equivalent`. Every ratio is null where its comparator is zero or null.

`rows(provider=..., year=...)` returns the BTR rows of one party and year with each CRS record linked to them, with the record's facts prefixed `crs_`, and `unfccc_only` set to true for a BTR row that no CRS record links to. `provider` takes a party as `providers` does. The rows hold the provider's name, `provider`, before `provider_code`; `recipient` is the BTR's own recipient text. It needs a local build that holds `btr` and `btr_links`. On a published release, which leaves them out, it raises `WithheldTableError`.

## `compare(*results)` and `Comparison`

`cf.compare(a, b, ...)` places results side by side. It returns a `Comparison` whose `table` has one row per year, provider, recipient, and Bi_Multi code, with the key columns `year`, `provider`, `recipient`, `provider_code`, `recipient_code`, and `bi_multi`, and one `value` column per result and method. When every result names an agency, the keys also hold `agency` and `agency_code`. When only some do, their agencies' amounts, and under `measure="share_of_total"` the totals that divide them, are summed to each provider's key, so `cf.compare(giz, germany)` puts GIZ's value and Germany's on one row per year and recipient, and a column's name says which agencies its spec named. A column is named by what distinguishes its result: the release (`2025a`), then each `Spec` field that differs across the results (`ONE`, `years 2021`), so `cf.compare(r2025, r2026)` gives columns `2025a` and `2026a`, and results that differ in both give `2025a, years 2021`. Fields all the results share stay out of the name. Results that nothing sets apart are numbered, as `result 1: ONE`. The bounds `low` and `high` stay on each `Result`. A key absent from a result is null in its column. Calling `compare()` with no arguments, or with a list such as `cf.compare([a, b])`, raises `InvalidInputError`. `cf.compare(*results)` compares a list.

## Listings

`cf.providers`, `cf.recipients`, `cf.flows`, and `cf.methods` each provide three methods: `.list()`, which returns a polars DataFrame; `.info(name)`, which returns a description by name or, for providers and recipients, by DAC code; and `.search(text)`, which returns the entries whose names contain `text`. For providers and recipients, `search` reads every name the resolver reads, ignoring case and accents, so `cf.providers.search("kfw")` finds KfW, and returns one row per entry: the listing rows first, then each agency it matches, with `type` naming its provider, as `Agency 2 of Germany`. `alias` holds the shortest name that matched, folded to lower case without accents, where the entry's own name does not contain `text`, so `cf.providers.search("Allemagne")` returns Germany with `alias` `allemagne`.

```python
print(cf.providers.info("Japan"))
```

```output
Japan (DAC code 701): DAC member.
```

A provider or recipient name can take any common form: the listing name, the French provider name, an ISO 3 or ISO 2 code, or another spelling resolvekit knows, matched ignoring case, accents, and surrounding spaces. `"DEU"`, `"Allemagne"`, and `" germany "` all name Germany. A provider name can also name one agency, such as `"EIB"` for the European Investment Bank, agency 3 of the EU Institutions. `cf.providers.code(name)` and `cf.recipients.code(name)` return the DAC code, or for an agency a `cf.Agency(provider=918, agency=3)`, which serialises as `{"provider": 918, "agency": 3}`. A DAC code, or a string of digits, passes through when its listing holds it. An `Agency` passes through when `data/agencies.csv` lists it, or when it is agency 0 or 1 of a listed provider, which the OECD files use for the provider itself.

```python
print(cf.providers.code("Allemagne"))
print(cf.providers.info("EIB"))
```

```output
5
European Investment Bank (agency 3 of EU Institutions, DAC code 918).
```

A name of several entries raises `UnknownProviderError` or `UnknownRecipientError` with every entry it names in `.suggestions`, an unknown name raises one with close names, and an unknown code one with the closest listed codes. The resolver applies none of the suggestions.

```python
try:
    cf.providers.code("Japn")
except cf.UnknownProviderError as error:
    print(error)
```

```output
Unknown provider 'Japn'. Did you mean: Japan?
```

## Errors

All errors inherit from `ClimateFinanceError`, and `climate_finance.errors` holds every class. `UnknownNameError`, `UnknownProviderError`, and `UnknownRecipientError` also import from `climate_finance`.

| Error                                           | Raised when                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| ----------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `InvalidInputError`                             | an argument has a form or value the call does not take: an unknown keyword, a value outside a field's choices, years, a method, or prices in another form, an empty list of providers, recipients, or years, an agency where a call reads providers, a `rows()` filter of another type than its column, or a `Spec`, `Method`, or `Agency` that pydantic refuses, malformed JSON included. The message names the form the call takes. It is also a `TypeError`. |
| `UnknownNameError`                              | a name is absent from its listing, or names several entries: a provider, recipient, method, release, release table, `rows()` column, or `rows()` filter value outside its column's values. `.suggestions` holds close names, or every entry the name names.                                                                                                                                                                                                     |
| `UnknownProviderError`, `UnknownRecipientError` | a provider, agency, or recipient name or code is absent from its listing.                                                                                                                                                                                                                                                                                                                                                                                       |
| `ReleaseIntegrityError`                         | a release fails its check.                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `SourceUnavailableError`                        | a download fails.                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `MissingLabelsError`                            | a method with `relevance="jev"` queries a release with no Jev labels, or `jev_flags()` queries a release with no Jev flags. The message names the `jev_labels` or `jev_flags` setting.                                                                                                                                                                                                                                                                                                                                               |
| `WithheldTableError`                            | `Release.table()` asks for a table the published release leaves out, `btr` or `btr_links`. `release.json` names each under `withheld_tables` with a note on how to build it.                                                                                                                                                                                                                                                                                    |
| `SchemaError`                                   | a table fails to match its schema.                                                                                                                                                                                                                                                                                                                                                                                                                              |

Catch the base class for any package error. `InvalidInputError` is also a `TypeError`, and `UnknownNameError` and `WithheldTableError` are also `LookupError`s, so the built-in classes catch them as well.

```python
for call in (
    lambda: release.spending(years="2022"),
    lambda: release.spending(providers="Japn"),
):
    try:
        call()
    except cf.errors.ClimateFinanceError as error:
        print(type(error).__name__, "-", error)
```

```output
InvalidInputError - years takes an int or ints, such as 2022 or range(2015, 2023); got '2022'
UnknownProviderError - Unknown provider 'Japn'. Did you mean: Japan?
```

`spending()`, `run()`, `quality()`, and `reconcile()` give one `CoverageWarning`, a `UserWarning` in `climate_finance.errors`, when `years` asks for years the release holds no rows for, naming the years it holds and the years that return no rows. `spending()` reads the years of the flow's tables: `commitments` for commitments, `disbursements` and, with `untraced`, `untraced` for the other flows, and in the provider view `core_contributions` too. `quality()` reads the `quality` table and `reconcile()` the `btr_totals` table. Silence it by class:

```python
import warnings

import climate_finance as cf

warnings.filterwarnings("ignore", category=cf.errors.CoverageWarning)
```

## Next

- [Release tables](release-tables.md) lists the columns behind `Result.rows()`.
- [About the methods](../explanation/methods.md) compares ONE, OECD, and PROVIDER side by side.
- [How to trace a figure](../how-to/trace-a-figure.md) puts `rows()`, `explain()`, and `Spec` to work.

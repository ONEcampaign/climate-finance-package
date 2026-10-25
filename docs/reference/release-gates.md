# Release gates

`cf-build release` (from 2.0) runs seven gates on a build before packing the release, and `cf-build gate` runs them on their own. A failed gate stops the release with `GateFailedError`. The results go into `release.json` under `gates`, and `release.quality().precision` reads the precision rows back.

Each gate returns one or more rows with `gate`, `subject`, `value`, `lower`, `upper`, `threshold`, `passed`, `waived`, `detail`, `code`, and `strata`. On precision and human-audit rows, `code` names why a row could not estimate or why its estimate cannot be read, and is null on a row that estimated. Rows of the other gates carry no `code`. `strata` is null on every gate but precision. A row passes when its value is at or below its threshold, with three exceptions. The `raw_links` share must stay under its threshold. Precision's lower bound must reach its threshold, and so must the human-audit share. The thresholds live in `src/climate_finance/build/gates.yaml`.

A gate with nothing to measure fails. Precision fails with no allocated links, the human audit with no blind sample or an empty one, conservation with no linked record that disbursed, the `raw_links` row with no linked commitment unit, `oecd_reconciliation` when neither the release nor the published CRDF holds a climate commitment, and `withheld_rows` with an empty `disbursements` table. Drift fails without a previous release to compare.

## `precision`

Returns one row per provider with allocated links. The row states the share of allocated value whose link a blind sample found right (`value`) and the interval around it (`lower`, `upper`). The gate passes or fails on the provider's estimate, which blends every stratum. `strata` lists each stratum beside it, largest allocated value first, with `stratum` (`ledger`, `key`, `model`, or `reviewed`), `allocated_usd`, `labels` (the draws the look read), `estimate`, `lower`, and `upper`, so the estimated links carry a figure of their own. The strata are reported and not gated. Their bounds are Wilson bounds at the provider's look: exact where a census stratum has every pair decided, and null where no draw was read. `release.quality().precision` carries the provider's figures, and `release.json` holds the strata under `gates.precision`.

The sample is drawn per provider from the allocated links across four strata: ledger-confirmed links, key-tier links without a ledger confirmation, every other method, and links that people or agents accepted in the review queue. A stratum with at most 50 links (`min_draws`) is a census: each of its links is drawn once and weighs the value it carries. Every other stratum takes at least 50 of the provider's 500 draws, and these strata share the draws the census strata leave by value. Within such a stratum, a draw picks a link with probability proportional to the value it carries, and weighs its stratum's value over the stratum's draws, so the estimate counts every stratum at its share of the provider's allocated value however many draws it took. People or agents label draws in batches without seeing the link method, a score, or the stratum.

The interval is a Wilson interval on the effective sample size. At an estimate of exactly 0 or 1, the effective size is the number of draws read. A provider passes when the lower bound reaches 0.90. It fails when the upper bound falls below 0.90, when the lower bound would miss 0.90 at the cap of 500 draws even with every later draw right, or at the cap. At the cap each stratum's draws carry its whole value, so the best estimate the cap can give is one less the value of the wrong draws already read, as a share of the provider's allocated value. A provider with fewer than 60 allocated links, or with every stratum a census, is labelled in full and decides once every link is decided. Looks happen every 50 labelled draws, at 2.5 standard errors before the cap and 1.96 at it. For a provider whose true precision is 0.90, the rule passes about 2.6% of the time.

Without a blind sample, every provider fails with code `no_sample`. A build with no allocated links fails one row, with no subject, coded `no_allocated_pairs`. A sampled provider whose links are gone fails as `stale_sample`. A provider whose estimate or bound is NaN or infinite fails as `non_finite_estimate`, and reading a design whose draw weights are not finite positive amounts raises `SampleDesignError`. The sample records a fingerprint of each provider's allocated links: every link's units, method, stratum, and allocated value. A provider fails without an estimate when this build's fingerprint differs, because a link was added or dropped or changed method, stratum, or value; its code is `stale_sample`, which also covers a design file with no `population` column, and a new sample must be drawn. Each sample id carries the fingerprint, and a decision counts for a draw only when its link is the draw's link, so a new link drawn under a reused id starts undecided. `cf-build release --waive-precision REASON` packs a release anyway, marks the failed precision and human-audit rows `waived`, sets `precision_gate_waived` in `release.json`, and records `REASON` there as `precision_waiver`.

## `human_audit`

Returns the share of the blind sample's draws, over every provider, whose standing decision is a person's. With no sample the row fails with code `no_sample`, and with an empty one with `empty_sample`. The threshold is 0.10, which the share must reach, so people decide at least one draw in ten and agents may decide the rest. `--waive-precision` also waives this gate. `release.upload_release` and `release.add_to_index` publish a pack only when its `release.json` records `precision` and `human_audit` rows and every recorded gate row passed or was waived. Only precision and human-audit rows can be waived, and only with a recorded `precision_waiver`. Otherwise it raises `GateFailedError`.

## `conservation`

Returns the largest gap between a linked CRS record's disbursement and its allocations, excess included. The threshold is USD 0.000001.

## `over_allocation`

Returns two rows. The first is the largest amount by which a commitment row is disbursed above its face value, with a tolerance of USD 0.000001. The second is the share of the release's linked commitment units, those a `commitments` row reaches, whose links bring more than 1.1 times their face value before allocation caps them. The share must stay under the threshold of 0.10.

## `oecd_reconciliation`

Returns the largest relative gap, by provider and year, between the release's climate commitments (repeats of an earlier vintage included) and the climate commitments in the published CRDF. The threshold is 0.01.

## `drift`

Returns the largest relative move of a provider-year's climate commitments or disbursed climate value from the previous release, unless a cause recorded in `gates.yaml` names the release, the provider, and the year. The threshold is 0.05. A provider-year that disappears counts as a move of 1, and so does one that only this release holds in a year the previous release covered. A provider-year after the previous release's last year is named in the detail and skips the gate. Without a previous release, or with one whose `quality` table is empty, the gate fails unless `--first-release` is passed.

## `withheld_rows`

Returns the number of rows, in every release table that carries a `record_id`, `disbursements` and `untraced` among them, whose record comes from a withheld source, and its detail names each table and record. The threshold is 0. [How linking works](../explanation/how-linking-works.md) lists the withheld sources.

## Next

- [How to cut a release](../how-to/cut-a-release.md) runs the gates in order.
- [How to review links](../how-to/review-links.md) produces the decisions the precision gate reads.

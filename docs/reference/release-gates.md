# Release gates

`cf-build release` (from 2.0) runs nine gates on a build before packing the release, and `cf-build gate` runs them on their own. A failed gate stops the release with `GateFailedError`. The results go into `release.json` under `gates`, and `release.quality().precision` reads the precision rows back.

Each gate returns one or more rows with `gate`, `subject`, `value`, `lower`, `upper`, `threshold`, `passed`, `waived`, `detail`, `code`, and `strata`. On precision and human-audit rows, `code` names why a row could not estimate or why its estimate cannot be read, and is null on a row that estimated. A `reflow_conservation` row that could not read an input carries the code `missing_input`. Rows of the other gates carry no `code`. `strata` is null on every gate but precision. A row passes when its value is at or below its threshold, with three exceptions. The `raw_links` share must stay under its threshold. Precision's lower bound must reach its threshold, and so must the human-audit share. The thresholds live in `src/climate_finance/build/gates.yaml`.

A gate with nothing to measure fails. Precision fails with no allocated links, the human audit with no blind sample or an empty one, conservation with no linked record that disbursed, `reflow_conservation` with no repayment in the raw CRS, the `raw_links` row with no linked commitment unit, `oecd_reconciliation` when neither the release nor the published CRDF holds a climate commitment, `withheld_rows` with an empty `disbursements` table, and `listed_codes` when no release table holds a provider code. Drift fails without a previous release to compare.

## `precision`

Returns one row per provider with allocated links. The row states the share of allocated value whose link is right (`value`) and the interval around it (`lower`, `upper`). A link that joins a unit to itself, whose records both committed and disbursed, cannot be wrong: the gate counts its value right and leaves it out of the blind sample. The rest of the value counts at the blind sample's estimate, so `value` is the self-linked share plus the remaining share times the sample's estimate, and `lower` and `upper` combine the same way. The detail gives the self-linked share of the provider's value. A provider whose every link joins a unit to itself passes at 1 without a sample. The gate passes or fails on the provider's estimate, which blends every stratum. `strata` lists each sampled stratum beside it, largest allocated value first, with `stratum` (`ledger`, `key`, `model`, or `reviewed`), `allocated_usd`, `labels` (the draws the look read), `estimate`, `lower`, and `upper`, so the estimated links carry a figure of their own. The strata are reported and not gated. Their bounds are Wilson bounds at the provider's look: exact where a census stratum has every pair decided, and null where no draw was read. `release.quality().precision` carries the provider's figures, and `release.json` holds the strata under `gates.precision`.

The sample is drawn per provider from the allocated links that join two different units, across four strata: ledger-confirmed links, key-tier links without a ledger confirmation, every other method, and links that people or agents accepted in the review queue. A stratum with at most 50 links (`min_draws`) is a census: each of its links is drawn once and weighs the value it carries. Every other stratum takes at least 50 of the provider's 500 draws, and these strata share the draws the census strata leave by value. Within such a stratum, a draw picks a link with probability proportional to the value it carries, and weighs its stratum's value over the stratum's draws, so the estimate counts every stratum at its share of the provider's allocated value however many draws it took. People or agents label draws in batches without seeing the link method, a score, or the stratum.

The sample's interval is a Wilson interval on the effective sample size. At an estimate of exactly 0 or 1, the effective size is the number of draws read. A provider passes when the lower bound reaches 0.90. It fails when the upper bound falls below 0.90, when the lower bound would miss 0.90 at the cap of 500 draws even with every later draw right, or at the cap. At the cap each stratum's draws carry its whole value, so the best estimate the cap can give is one less the value of the wrong draws already read, as a share of the provider's allocated value. A provider with fewer than 60 allocated links, or with every stratum a census, is labelled in full and decides once every link is decided. Looks happen every 50 labelled draws, at 2.5 standard errors before the cap and 1.96 at it. For a provider with no self-linked value whose true precision is 0.90, the rule passes about 2.6% of the time.

Without a blind sample, every provider fails with code `no_sample`. A build with no allocated links fails one row, with no subject, coded `no_allocated_pairs`. A sampled provider whose links are gone fails as `stale_sample`. A provider whose estimate or bound is NaN or infinite fails as `non_finite_estimate`, and reading a design whose draw weights are not finite positive amounts raises `SampleDesignError`. The sample records a fingerprint of each provider's sampled links: every link's units, method, stratum, and allocated value, after a tag naming which links the sample leaves out. A provider fails without an estimate when this build's fingerprint differs, because a sampled link was added or dropped or changed method, stratum, or value, or because the sample was drawn when it still held links that join a unit to itself; its code is `stale_sample`, which also covers a design file with no `population` column, and a new sample must be drawn. Each sample id carries the fingerprint, and a decision counts for a draw only when its link is the draw's link, so a new link drawn under a reused id starts undecided. `cf-build release --waive-precision REASON` packs a release anyway, marks the failed precision and human-audit rows `waived`, sets `precision_gate_waived` in `release.json`, and records `REASON` there as `precision_waiver`.

## `human_audit`

Returns the share of the blind sample's draws, over every provider, whose standing decision is a person's. The sample holds no link that joins a unit to itself, so the share counts only links a person could find wrong. With no sample the row fails with code `no_sample`, and with an empty one with `empty_sample`. The threshold is 0.10, which the share must reach, so people decide at least one draw in ten and agents may decide the rest. `--waive-precision` also waives this gate. `cf-build publish` publishes a pack only when its `release.json` records a row for every gate and every recorded row passed or was waived. Only precision and human-audit rows can be waived, and only with a recorded `precision_waiver`. Otherwise it raises `GateFailedError`.

## `conservation`

Returns the largest gap between a linked CRS record's disbursement and its allocations, excess included. The threshold is USD 0.000001.

## `reflow_conservation`

Returns two rows, `received` and `interest`. Each is the largest gap, by provider and year, between three sums of the same repayment: the bulk CRS's `usd_received` (or `usd_interest`) times one million, the normalise `reflows` table, and the release `reflows` table summed over every basis. The sums cover the CRS years from the first the build read to `years.crs_last_complete`. The threshold is USD 0.01, which leaves room for float rounding in the largest provider-years and sits far below any repayment. A repayment that the normalise stage drops, or that attribution loses or counts twice, shows as a gap.

The gate fails with code `missing_input`, and a detail naming each input, when the raw CRS file, the normalise `reflows` table, the release `reflows` table, or the CRS years in `release.json` cannot be read. It also fails when no repayment of the row's kind is in any of the three. `cf-build gate --report` returns the rows without failing.

## `over_allocation`

Returns two rows. The first is the largest amount by which a commitment row is disbursed above its face value, with a tolerance of USD 0.000001. The second is the share of the release's linked commitment units, those a `commitments` row reaches, whose links bring more than 1.1 times their face value before allocation caps them. The share must stay under the threshold of 0.10.

## `oecd_reconciliation`

Returns the largest relative gap, by provider and year, between the release's climate commitments (repeats of an earlier vintage included) and the climate commitments in the published CRDF. The threshold is 0.01.

## `drift`

Returns the largest relative move of a provider-year's climate commitments or disbursed climate value from the previous release, unless a `[[drift_causes]]` entry in `build.toml` names the release, the provider, and the year. The threshold is 0.05. A provider-year that disappears counts as a move of 1, and so does one that only this release holds in a year the previous release covered. A provider-year after the previous release's last year is named in the detail and skips the gate. Without a previous release, or with one whose `quality` table is empty, the gate fails unless `--first-release` is passed. `release.json` records the previous release's name as `previous_release` and the sha256 of the pack it came from as `previous_pack_sha256`. `cf-build publish` refuses a release compared with any release but the latest published one, with a copy from another pack, or with none once a release is published, so `--first-release` serves only the first release.

## `withheld_rows`

Returns the number of rows, in every release table that carries a `record_id`, `disbursements` and `untraced` among them, whose record comes from a withheld source, and its detail names each table and record. The threshold is 0. [How linking works](../explanation/how-linking-works.md) lists the withheld sources.

## `listed_codes`

Returns the number of provider, agency, and recipient codes in the release tables that the package's listings lack, and its detail names each one, as `provider 1058`, `agency 3/5`, or `recipient 434`. The listings are `oecd_classification.csv`, `agencies.csv`, and `recipients.csv` in `src/climate_finance/data`. Agency codes 0 and 1 and a null agency stand for the provider itself, so only other agency codes need a row in `agencies.csv`. The threshold is 0. A failure means the OECD files carry a code the listings lack. Run `cf-build listings` on the build's raw directory, then `cf-build names`, and commit both.

## Next

- [How to cut a release](../how-to/cut-a-release.md) runs the gates in order.
- [How to review links](../how-to/review-links.md) produces the decisions the precision gate reads.

# Release gates

As of 2.0, `cf-build release` runs seven gates on a build before packing the release, and `cf-build gate` runs them on their own. A failed gate stops the release with `GateFailedError`. The results go into `release.json` under `gates`, and `snap.quality().precision` reads the precision rows back.

Each gate returns one or more rows with `gate`, `subject`, `value`, `lower`, `upper`, `threshold`, `passed`, `waived`, and `detail`. A row passes when its value is at or below its threshold, with three exceptions: the `raw_links` share must stay under its threshold, precision's lower bound must reach its threshold, and so must the human-audit share. The thresholds live in `src/climate_finance/build/gates.yaml`.

A gate with nothing to measure fails. Precision fails with no allocated links, the human audit with no blind sample or an empty one, conservation with no linked record that disbursed, the `raw_links` row with no linked commitment unit, `oecd_reconciliation` when neither the release nor the published CRDF holds a climate commitment, and `withheld_rows` with an empty `disbursements` table. Drift fails without a previous release to compare.

## `precision`

Returns one row per provider with allocated links. The row states the share of allocated value whose link a blind sample found right (`value`) and the interval around it (`lower`, `upper`).

The sample is drawn per provider from the allocated links across four strata: ledger-confirmed links, key-tier links without a ledger confirmation, every other method, and links that people or agents accepted in the review queue. Within a stratum, a draw picks a link with probability proportional to the value it carries. People or agents label draws in batches without seeing the link method, a score, or the stratum.

The interval is a Wilson interval on the effective sample size. A provider passes when the lower bound reaches 0.90. It fails when the upper bound falls below 0.90, when the lower bound could not reach 0.90 by the cap of 500 draws even if every later draw were right, or at the cap. A provider with fewer than 60 allocated links is labelled in full. Looks happen every 50 labelled draws, at 2.5 standard errors before the cap and 1.96 at it. For a provider whose true precision is 0.90, the rule passes about 2.6% of the time.

Without a blind sample, every provider fails. So does a provider with a sampled link that this build leaves unallocated or allocates by another method or stratum, since its labels describe another build. `cf-build release --waive-precision` packs a release anyway, marks the failed precision and human-audit rows `waived`, and sets `precision_gate_waived` in `release.json`.

## `human_audit`

Returns the share of the blind sample's draws, over every provider, whose standing decision is a person's. The threshold is 0.10, which the share must reach, so people decide at least one draw in ten and agents may decide the rest. `--waive-precision` also waives this gate.

## `conservation`

Returns the largest gap between a linked CRS record's disbursement and its allocations, excess included. The threshold is USD 0.000001.

## `over_allocation`

Returns two rows. The first is the largest amount by which a commitment row is disbursed above its face value, with a tolerance of USD 0.000001. The second is the share of linked commitment units whose links bring more than 1.1 times their face value before allocation caps them, with a threshold of 0.03, which the share must stay under.

## `oecd_reconciliation`

Returns the largest relative gap, by provider and year, between the release's climate commitments (repeats of an earlier vintage included) and the climate commitments in the published CRDF. The threshold is 0.01.

## `drift`

Returns the largest relative move of a provider-year's climate commitments or disbursed climate value from the previous release, unless a cause recorded in `gates.yaml` names the release, the provider, and the year. The threshold is 0.05. A provider-year that disappears counts as a move of 1, and so does one that only this release holds in a year the previous release covered. A provider-year after the previous release's last year is named in the detail and not gated. Without a previous release, or with one whose `quality` table is empty, the gate fails unless `--first-release` is passed.

## `withheld_rows`

Returns the number of `disbursements` rows whose record comes from a withheld source. The threshold is 0. [How linking works](../explanation/how-linking-works.md) lists the withheld sources.

## Next

- [How to cut a release](../how-to/cut-a-release.md) runs the gates in order.
- [How to review links](../how-to/review-links.md) produces the decisions the precision gate reads.

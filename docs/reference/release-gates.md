# Release gates

As of 2.0, `cf-build release` runs six gates on a build before packing the release, and `cf-build gate` runs them on their own. A failed gate stops the release with `GateFailedError`. The results go into `release.json` under `gates`, and `snap.quality().precision` reads the precision rows back.

Each gate returns one or more rows with `gate`, `subject`, `value`, `lower`, `upper`, `threshold`, `passed`, `waived`, and `detail`. A row passes when its value is at or below its threshold, except for precision, where the lower bound must reach the threshold. The thresholds live in `src/climate_finance/build/gates.yaml`.

## `precision`

Returns one row per provider with allocated links. The row states the share of allocated value whose link a blind sample found right (`value`) and the interval around it (`lower`, `upper`).

The sample is drawn per provider from the allocated links across four strata: ledger-confirmed links, key-tier links without a ledger confirmation, every other method, and links that people or agents accepted in the review queue. Within a stratum, a draw picks a link with probability proportional to the value it carries. People or agents label draws in batches without seeing the link method, a score, or the stratum.

The interval is a Wilson interval on the effective sample size. A provider passes when the lower bound reaches 0.90. It fails when the upper bound falls below 0.90, when the lower bound could not reach 0.90 by the cap of 500 draws even if every later draw were right, or at the cap. A provider with fewer than 60 allocated links is labelled in full. Looks happen every 50 labelled draws, at 2.5 standard errors before the cap and 1.96 at it. For a provider whose true precision is 0.90, the rule passes about 2.6% of the time.

Without a blind sample, every provider fails. `cf-build release --waive-precision` packs a release anyway, marks the failed rows `waived`, and sets `precision_gate_waived` in `release.json`.

## `conservation`

Returns the largest gap between a linked CRS record's disbursement and its allocations, excess included. The threshold is USD 0.000001.

## `over_allocation`

Returns two rows. The first is the largest amount by which a commitment row is disbursed above its face value, with a tolerance of USD 0.000001. The second is the share of linked commitment units whose links bring more than 1.1 times their face value before allocation caps them, with a threshold of 0.03.

## `oecd_reconciliation`

Returns the largest relative gap, by provider and year, between the release's climate commitments (repeats of an earlier vintage included) and the climate commitments in the published CRDF. The threshold is 0.01.

## `drift`

Returns the largest relative move of a provider-year's climate commitments or disbursed climate value from the previous release, unless a cause recorded in `gates.yaml` names the release, the provider, and the year. The threshold is 0.05. A provider-year that disappears counts as a move of 1. Without a previous release, the gate fails unless `--first-release` is passed.

## `withheld_rows`

Returns the number of `disbursements` rows whose record comes from a withheld source. The threshold is 0. [How linking works](../explanation/how-linking-works.md) lists the withheld sources.

## Next

- [How to cut a release](../how-to/cut-a-release.md) runs the gates in order.
- [How to review links](../how-to/review-links.md) produces the decisions the precision gate reads.

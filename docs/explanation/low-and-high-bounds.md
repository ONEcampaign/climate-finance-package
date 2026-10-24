# Low and high bounds

As of 2.0, every row of a `spending()` table carries `low`, `value`, and `high`. `low` counts the disbursements whose whole path to a CRDF commitment is certain. `value` counts every disbursement the build allocated, estimated links included. `high` equals `value` for a single method. When a query asks for several methods, `low` is the lowest of their `low` bounds and `high` the highest of their values.

## Where the bounds come from

Each row of the release's `disbursements` table is one CRS record's allocation to one commitment. It holds `allocated_usd`, `allocated_low_usd`, and `allocated_high_usd`, with `0 <= allocated_low_usd <= allocated_usd <= allocated_high_usd`. The query scales an allocation's climate value by the ratio of each bound to `allocated_usd`.

- An allocation's low bound is its full amount when a certain link carried it to a commitment whose CRDF rows reached it by certain routes, and zero otherwise. A link is certain when its method is `wb_loan`, `crs_id_purpose`, `crs_id`, or `crs_id_parent` and its records agree on recipient and project number (`schema.CERTAIN_METHODS`), or when one CRS record commits and disburses and the link joins it to its own commitment. The certain routes are `within_year`, `crosswalk`, and `aggregate_partition` (`schema.CERTAIN_ROUTES`), so money that reaches an EIB commitment matched by `eib_list` or `title_recipient` counts in `value` alone.
- Bank-ledger and IATI fills count in full in the low bound, because they match a commitment on the publisher's own id.
- The high bound is the allocated amount.

A commitment's `disbursed_low_usd`, `disbursed_usd`, and `disbursed_high_usd` sum its allocations' bounds. The release holds them to `0 <= disbursed_low_usd <= disbursed_usd <= disbursed_high_usd <= face_usd`.

!!! note
    With `flow="commitments"`, `low`, `value`, and `high` are equal.

## Under several methods

`cf.methods.RANGE` computes ONE, OECD, and PROVIDER together. For each year, provider, recipient, and Bi_Multi code, every method's row carries the same `low` and `high`. `low` is the lowest of the three methods' `low` bounds, each counting only what certain links carried. `high` is the highest value any of the three gives, because each method's `high` equals its `value`. The range covers marker weights as well as link certainty.

## How to read a wide gap

A wide gap between `low` and `value` says that much of the money rests on estimated links. To judge accuracy, check `snap.quality().precision`, which states how often a provider's links were right in a blind sample, and `snap.quality().summary()`, which gives each provider's `link_rate`. [How to check link quality](../how-to/check-link-quality.md) shows both.

A donor whose ids persist from year to year shows a narrow gap. A donor whose CRS ids change, and whose records the build links by project number, title, or model, shows a wide one.

## The modelled line

Providers that publish no project disbursements (IFC, IDB Invest, CAF, and the NDB), and the EIB commitments that no route traces, have no records to link. The release multiplies their climate commitments by a range of disbursement ratios, taken from the CRS series the provider does publish and from its own annual reports where it gives a ratio.

`result.modelled` holds this "modelled, not reported" line as its own range, separate from `value`, `low`, and `high`, with one row per provider, year, and recipient, and a `basis` column naming the source of the ratios.

## Next

- [About the methods](methods.md) explains ONE, OECD, PROVIDER, and RANGE.
- [Release tables](../reference/release-tables.md) lists every column named here.

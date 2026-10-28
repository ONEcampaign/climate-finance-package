# Low and high bounds

Since 2.0, every row of a `spending()` table carries `low`, `value`, and `high`. `low` counts the disbursements whose whole path to a CRDF commitment is certain. `value` counts every disbursement the build allocated, estimated links included, and the untraced disbursement, Rio-marked CRS disbursement that reaches no commitment. `high` equals `value` for a single method of a gross flow. Net flows differ, as [Net flows](#net-flows) explains. When a query asks for several methods, each method's row keeps its own `low` and `high`, and `range_low` and `range_high` hold the lowest `low` and the highest `high` across the methods.

The bounds measure link certainty alone. A method's parameters, its coefficients and the agency shares it applies, weigh `low`, `value`, and `high` alike, so comparing methods shows what the parameters change.

## Where the bounds come from

Each row of the release's `disbursements` table is one CRS record's allocation to one commitment. The row holds `allocated_usd` and `allocated_low_usd`, with `0 <= allocated_low_usd <= allocated_usd`. The query scales an allocation's climate value by the ratio of its low bound to `allocated_usd` for `low`, and takes `allocated_usd` itself for `value` and `high`.

- An allocation's low bound is its full amount when a certain link carried it to a commitment whose CRDF rows reached it by certain routes, and zero otherwise. A link is certain when its method is `wb_loan`, `crs_id_purpose`, `crs_id`, or `crs_id_parent` and its records agree on recipient and project number (`schema.CERTAIN_METHODS`), or when one CRS record commits and disburses and the link joins that record to its own commitment. The certain routes are `within_year`, `crosswalk`, and `aggregate_partition` (`schema.CERTAIN_ROUTES`), so money that reaches an EIB commitment matched by `eib_list`, `crosswalk_amount`, `title_recipient`, `eib_text_amount`, or `eib_amount` counts in `value` alone.
- The certain routes include the parts of rebuilt aggregate rows. A part takes the crs id of the CRS record it stands for, and the build matches parts to records by amount, within 0.1% to 1%. Those parts carry USD 4.3bn of `low` in the 2015 to 2024 disbursements.
- Bank-ledger and IATI fills count in full in the low bound, because they match a commitment on the publisher's own id.
- In the provider view, attributed core contributions count in full in the low bound. A contribution is the donor's own reported payment, and the agency's climate share applied to it is a method parameter.
- The high bound is the allocated amount.
- Untraced disbursement counts in `value` and `high` and at zero in `low`. It has no link and no commitment, so no route to it is certain. Counting it in `low` would let a lost link raise `low`: a record whose only link is estimated adds nothing to `low`, and without the link it would become untraced in full. The result's `untraced` column holds this part of `value`, and `untraced=False` leaves it out.
- Under `flow="grant_equivalent"`, a disbursement whose CRS record has no grant equivalent, such as a loan or a disbursement filled from a source that reports no finance type, counts 0 in `low`, `value`, and `high`. Its grant equivalent is unknown, so no bound can hold it. The result's `no_grant_equivalent` column holds its climate value at face, beside every total.

A commitment's `disbursed_low_usd` and `disbursed_usd` sum its allocations' bounds. The release holds them to `0 <= disbursed_low_usd <= disbursed_usd <= face_usd`.

!!! note
With `flow="commitments"`, `low`, `value`, and `high` are equal.

## Net flows

`flow="net_disbursements"` and `flow="net_transfers"` subtract repayments from gross disbursements, and the bounds pair each with the gross bound that makes the interval widest:

- `low` is gross `low` less every attributed repayment.
- `value` is gross `value` less every attributed repayment.
- `high` is gross `high` less the repayments whose link to the loan is certain.

A repayment is certain when the tier that tied it to its loan is `wb_loan`, `crs_id_purpose`, `crs_id`, or `crs_id_parent` and its records agree on recipient and project number. A certain repayment subtracts from `high` in full on an untraced part, and on a `commitment` row for the share that certain links allocated to the row. A repayment tied by a project-number tier subtracts nothing from `high`. The `reflows` table holds each part's certain amount as `received_low_usd` and `interest_low_usd`.

The pairing keeps `low <= value <= high` on every row. A repayment whose link is estimated might belong to another loan, so the highest net figure leaves it out, and the lowest net figure counts it against the smallest gross. Because `high` subtracts less than `value` does, `high` is above `value` for a single method. A net figure can be negative. When a provider repays more than it disburses in the years queried, `low` can fall below zero, and so can `value`.

The interval is wider than a gross one. Gross `low` and `high` differ by link certainty alone, and a net flow adds the certainty of the repayments' own links. `release.quality().repayments` shows how much principal reaches a total at all.

## Under several methods

`cf.methods.RANGE` computes ONE, OECD, and PROVIDER together. Every method carries a row for every year, provider, recipient, and Bi_Multi code that any of them holds, at zero where it has no money. Each method's row keeps that method's own `low` and `high`, so `result.rows(method="ONE")` sums to the ONE row's bounds. `range_low` and `range_high` span the methods, the same on every method's row of a key. `range_low` is the lowest of the three methods' `low` bounds, each counting only what certain links carried. `range_high` is the highest value any of the three gives for a gross flow, because each method's `high` equals its `value`. For a net flow it is the highest `high` any of them gives. The span covers marker weights and link certainty.

## How to read a wide gap

A wide gap between `low` and `value` says that much of the money rests on estimated links. To judge accuracy, check `release.quality().precision`, which states how often a provider's links were right in a blind sample, and `release.quality().summary()`, which gives each provider's `link_rate`. [How to check link quality](../how-to/check-link-quality.md) shows both.

A provider's precision covers all its allocated value, certain links and estimated links together. Certain links are rarely wrong, so a provider with mostly certain links can pass on them while its estimated links do worse. The per-stratum estimates that `release.json` records beside each provider's, under `gates.precision`, give the estimated links a figure of their own.

A donor whose ids persist from year to year shows a narrow gap. A donor whose CRS ids change, and whose records the build links by project number, title, or model, shows a wide one.

## What a narrow gap hides

Allocation caps each commitment at its face value, so a commitment whose links bring more money than its face reads `disbursed` and the surplus becomes excess. 7.9% of the commitment units behind the release's commitments draw more than 1.1 times their face value before the cap, and they hold 4.5% of `value`. Nearly all of that over-linking comes on certain key-tier links, and the cap hides it in both bounds.

## The modelled line

Providers that publish no project disbursements (IFC, IDB Invest, CAF, and the NDB), and the EIB commitments that no route traces, have no records to link. The release multiplies their climate commitments by a range of disbursement ratios, taken from the CRS series the provider publishes and from its own annual reports where it gives a ratio.

`result.modelled` holds this modelled line as its own range, separate from `value`, `low`, and `high`, with one row per provider, year, and recipient, and a `basis` column naming the source of the ratios.

## Next

- [About the methods](methods.md) explains ONE, OECD, PROVIDER, and RANGE.
- [Release tables](../reference/release-tables.md) lists every column named here.

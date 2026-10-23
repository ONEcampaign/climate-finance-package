# How linking works

As of 2.0. A disbursement in the CRS counts against a climate commitment in the CRDF through a four-step linking build, which also drops restricted source rows. [Low and high bounds](low-and-high-bounds.md) covers what the links mean for a figure's range.

## The question

The CRDF lists what a donor committed to climate action, at face value, in the year of the commitment. The CRS lists every record of money a donor committed or disbursed, with Rio markers. The two databases share no key, and a disbursement can arrive years after its commitment, under an id the donor has changed. To say how much of a climate commitment reached a recipient, the package has to decide which CRS records belong to which commitment.

## The short answer

A build links records in two steps, spreads each disbursement over the commitments it was linked to, and measures how often the links are right. Every link carries the method that made it. Methods that rest on an id the donor reported are certain, and everything else is an estimate. A figure's `low` bound counts the certain links, and its `value` counts all of them.

## Step 1 from CRDF rows to CRS commitments

A CRDF row joins the CRS commitment that shares its provider, crs id, purpose, and year (`within_year`). Where the CRDF reports an aggregate row, as development banks do, the build rebuilds it from the CRS records it pools and counts the parts. Rows that the join misses go to three recovery routes: the EIB's project list, its IATI activity ids, and a title match (`eib_list`, `crosswalk`, `title_recipient`). A CRDF row that repeats an earlier vintage counts once.

## Step 2 from disbursements to commitments

The build sums CRS records into units that share every key field, then links each disbursement unit to the commitment units it may fill. Methods run in order, and a later method sees only what an earlier one left.

Certain links, which count in `low`:

- `wb_loan`, `crs_id_purpose`, `crs_id`, and `crs_id_parent` match where the donor's own ids name the commitment, whether the World Bank loan number, the crs id with its purpose, the crs id alone, or the parent project's id. A link drops out of the certain set when its records disagree on recipient or project number, until a person or an agent accepts it in the review queue.
- A unit that commits and disburses in one record links to itself.
- Bank-ledger and IATI fills match on the publisher's own id and count in `low` as well as `value`.

Estimated links, which count in `value` alone:

- `pn_recipient_date`, `pn_recipient_purpose`, and `pn_recipient` match on the project number, narrowed by recipient, commitment date, or purpose.
- The key-repair rules (`repair_new_crs_id`, `repair_pn_variant`, `repair_afdb_scientific`, `repair_title_recipient`, `repair_stray_country`) link a project the donor filed under two ids.
- `classifier` and `weak_classifier` use a gradient-boosted classifier trained on the agencies whose ids persist and calibrated on held-out pairs. A pair whose calibrated probability falls in the Jev band, 0.3 up to 0.9, goes to Jev, TypeSafe's judgement model, which reads both records and answers whether they describe the same activity. A pair Jev decides links as `classifier_jev`. The Jev version is pinned in `release.json` under `model_versions`.
- `splink_jev` uses one Splink model per provider, calibrated on labelled pairs, to link a disbursement to its best commitment at a probability of 0.7 or more for providers without reliable ids.
- `title_recipient`, `eib_list`, `crosswalk`, `programme_number`, `aggregate_partition`, and `iati` handle rows that carry no usable shared id, such as EIB projects and GEF activities.

A person or an agent can accept or reject any queued link. [How to review links](../how-to/review-links.md) describes the queue.

## Step 3 allocation

A disbursement unit pools its records' disbursements and fills the commitments it reached, oldest first, in proportion to headroom within a year, capped at each commitment's face value. Certain groups fill before estimated groups of any year, so certain links take all available headroom first. What no commitment can take is excess and appears in no figure. Amounts stay at face value. The query applies the climate share when it sums.

## Step 4 status

A commitment's status reads what was allocated against the face of the CRS commitment it matches:

- `disbursed` means at least 90% allocated.
- `partly_disbursed` means more than zero and less than 90%.
- `no_disbursement_record` means the commitment is traceable and nothing was allocated.
- `not_traceable` means the commitment reaches no CRS commitment, or its agency files neither a crs id nor a project number that persists from year to year. A missing disbursement then says nothing about payment.
- `pending_review` means a queued link would change the status if rejected.

## How sure the build is

A blind sample measures precision. For each provider the build draws allocated links, weighted by the value they carry, and people or agents label them without seeing the link method or any score. The share of allocated value whose link is right, with a Wilson interval, is the provider's precision. [Release gates](../reference/release-gates.md) sets the threshold a release has to meet.

## What the release leaves out

The Green Climate Fund's projects API and readiness list come under terms that bar redistribution and derivative works without written consent. The release drops the disbursement rows built from them (the `ledger:gcf:` and `ledger:gcf_readiness:` fill records). Each commitment they reached states their amount in `withheld_usd`, so the commitment's `disbursed_usd` and the `quality` table include it. `spending()` sums the `disbursements` table, so its figures leave the withheld amount out.

The Green Climate Fund's IATI files are CC0 on the IATI Registry. Their fill rows (`iati:gcf:`) ship in `disbursements`. The `withheld_rows` gate fails a release that carries a row from a withheld source.

[Release tables](../reference/release-tables.md) lists the terms each source comes under.

## Why two figures

A figure built from exact ids alone leaves out the money of every donor whose ids change. A figure built from every link the models reach gives no sign of which part to trust. `low` and `value` report both.

## Next

- [Low and high bounds](low-and-high-bounds.md) shows how the bounds are computed and how they behave under several methods.
- [How to trace a figure](../how-to/trace-a-figure.md) lists the commands that expose each link.

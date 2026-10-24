# Release tables

As of 2.0. A release holds twelve parquet tables, each sorted by its key so that two builds from the same inputs hash the same. Amounts are float64 USD at current prices and face value, DAC codes int32, years int16, and `bi_multi` and Rio markers int8. `snap.table(name)` scans one table. [Python API](python-api.md) describes how queries read them.

## Files in a release

A release is a folder, `climate-finance-<name>/`, packed as one `.tar.zst`. It holds the twelve tables below, `release.json`, and `sources.json`:

- `release.json` has each table's sha256 and row count, the source and model versions, the gate results, and the `precision_gate_waived` flag. `cf.load()` checks every table against it and raises `ReleaseIntegrityError` on a mismatch. A pack that fails its sha256 in the release index is refused before it unpacks.
- `sources.json` lists each source with its version, URLs, licence, and any note on rows the release leaves out.

The release holds only processed tables and metadata. The Green Climate Fund's ledger rows are withheld. [How linking works](../explanation/how-linking-works.md) explains why, and `commitments.withheld_usd` states their amount.

## `commitments`

Key `row_id`. One row per recipient-perspective CRDF commitment, which is the population the trace follows. Aggregate rows rebuilt from their parts keep the parts and omit the parent. Rows that repeat an earlier vintage stay, with their amounts and a `repeat_of`:

- `row_id`, `vintage`, `year`: the CRDF row, the vintage of the CRDF file it came from, and the commitment year.
- `provider_code`, `agency_code`, `recipient_code`, `channel_code`, `purpose_code`: DAC codes. `bi_multi` is the Bi_Multi code. `flow_category` is the CRDF's flow category: 10 ODA, 21 other official flows, 22 and 30 private grants, and 60 private market flows.
- `crs_id`, `project_number`: the donor's ids.
- `rio_adaptation`, `rio_mitigation`: the Rio markers, 0, 1, or 2. `climate_share` is set for development-bank rows that report climate components, and null for marker rows.
- `face_usd`, `climate_usd`, `adaptation_usd`, `mitigation_usd`, `overlap_usd`: the commitment and its climate part. Adaptation and mitigation each include the overlap, so `climate_usd` is `adaptation_usd` + `mitigation_usd` − `overlap_usd`. A negative `face_usd` is a published downward revision with zero disbursed.
- `status`: `disbursed`, `partly_disbursed`, `no_disbursement_record`, `not_traceable`, or `pending_review`.
- `disbursed_usd`, `disbursed_low_usd`, `disbursed_high_usd`: the sums of the commitment's allocations in `disbursements` plus `withheld_usd`, with `0 <= disbursed_low_usd <= disbursed_usd <= disbursed_high_usd <= face_usd`.
- `withheld_usd`: the part of `disbursed_usd` that comes from fill records the release leaves out (the Green Climate Fund's ledger). It is excluded from `disbursements`.
- `project_disbursing`: true when the commitment has no record of its own and its id disbursed against an earlier commitment.
- `ledger_undisbursed`: true where the bank's ledger confirms the commitment disbursed nothing, and null where no ledger covers it or the ledger shows money moving. Only the IBRD, IDA, and IDB ledgers can set it, since they match the CRS closely enough to confirm a non-disbursement. A row whose `status` says disbursed while `ledger_undisbursed` is true points at a link to check.
- `repeat_of`: the first row of a commitment republished in a later vintage. `parent_row_id` names the published aggregate row a rebuilt child came from.

## `disbursements`

Key `row_id`, `record_id`, `year`. Allocations of CRS records, and of bank-ledger and IATI fills, to climate commitments, one row per allocation. Every row names a commitment. The table omits any unallocated excess on a record:

- `row_id`, `record_id`, `year`: the commitment, the CRS record (or fill record), and the record's year.
- `provider_code`, `agency_code`, `recipient_code`, `channel_code`, `bi_multi`, `purpose_code`, `flow_code`, `project_number`, `title`: the record's own fields. `flow_code` is the CRS flow: 11 ODA grants, 13 ODA loans, and 19 equity.
- `allocated_usd`, `allocated_low_usd`, `allocated_high_usd`: the part of the record's disbursement given to the commitment, with `0 <= allocated_low_usd <= allocated_usd <= allocated_high_usd`.
- `record_disbursement_usd`, `record_grant_equiv_usd`: the whole record's disbursement and grant equivalent, repeated on each row of the record. A query takes an allocation's grant equivalent as `record_grant_equiv_usd` × `allocated_usd` ÷ `record_disbursement_usd`.
- `inherited_rio_adaptation`, `inherited_rio_mitigation`: the markers of the commitment, which weight the allocation. `record_rio_adaptation` and `record_rio_mitigation` are the record's own. `marker_source` says how they relate: `components`, `commitment`, or `record_differs`.
- `climate_share`: the commitment's share for rows weighted by climate components.
- `link_method`, `link_probability`: the method that linked the record to the commitment and its probability. Null for fills.
- `rule`: the allocation rule, which is `fifo`, `dated`, `window`, `excess`, `bank_ledger`, or `iati`.
- `disbursement_source`: `crs`, `bank_ledger`, `iati`, or `none`.
- `ledger_check`: `confirmed`, `contradicted`, `none`, or `confirmed_undisbursed`, from comparing the link with a bank's ledger.

## `objectives`

Key `record_id`, `objective`, `label_source`. One row per CRS record, objective, and label source:

- `record_id`, `objective` (`adaptation`, `mitigation`, or `cross_cutting`).
- `level`: 0, 1, or 2.
- `label_source`: `crdf` for a development bank's reported components, `marker` for the donor's Rio markers, and `jev` for Jev's label.
- `probability`: the label's probability, where it has one.

## `agency_shares`

Key `channel_code`, `year`, `objective`, `source`. The share of a multilateral channel's spending that is climate finance, which `view="provider"` applies to core contributions:

- `channel_code`, `year`.
- `objective`: `adaptation`, `mitigation`, `cross_cutting`, or `climate` for a single unsplit total. A channel, year, and source holds either the split rows or one `climate` row.
- `share`: the share. Adaptation and mitigation each include the cross-cutting share.
- `source`: `oecd` for the OECD's imputed share, or `one` for the share measured on the agency's own commitments.
- `share_source`: `computed`, `held`, or `oecd`.

## `modelled`

Key `provider_code`, `year`, `recipient_code`. The modelled range for providers that omit project disbursements:

- `provider_code`, `year`, `recipient_code`.
- `low_usd`, `high_usd`: the range.
- `basis`: where the disbursement ratios come from.

## `core_contributions`

Key `provider_code`, `channel_code`, `year`, `flow`. Core contributions of a provider to a multilateral channel. Rows sit on leaf channels, and the aggregate channels the OECD publishes beside them drop out. Where a provider's channel 40000 total for a year and flow exceeds its leaves, the remainder stays as a 40000 row. That row takes no agency share, so it keeps the provider's total whole and attributes nothing:

- `provider_code`, `channel_code`, `year`.
- `flow`: `commitments`, `disbursements`, or `grant_equivalent`.
- `contribution_usd`: the amount.

## `btr`

Key `row_id`. Parsed rows of Biennial Transparency Reports, Table III.1:

- `row_id`, `party` (ISO 3), `provider_code` (null for a party absent from CRS), `year`, `source_file`.
- `recipient`, `recipient_code`, `title`, `channel`, `funding_source`, `instrument`, `support_type`, `sector`, `additional_information`: the row's text and codes as reported.
- `amount_domestic`, `currency`: the reported amount.
- `face_usd`, `grant_equiv_usd`: the amount in USD.
- `status`: `committed` or `disbursed`.
- `is_aggregate`: true for a row that sums several activities.

## `btr_links`

Key `row_id`, `record_id`. Links from BTR rows to same-year CRS records of the party, with the record's facts:

- `row_id`, `record_id`, `link_method` (`btr_id` or `btr_splink`), `probability`. A `btr_splink` probability comes from the party's own calibration map when its id-linked rows give at least 30 right and 30 wrong labels, and from a map pooled over every party otherwise, which needs as many. A row links at a probability of 0.5 or more, and a party with neither map links by ids alone.
- `provider_code`, `year`, `recipient_code`, `project_number`, `title`: the record's fields.
- `commitment_usd`, `disbursement_usd`: the record's amounts.
- `rio_marker`: the higher of the record's two Rio markers.

## `disbursement_profile`

Key `group`, `years_since`. The cumulative disbursed share of traceable climate value by years since commitment:

- `group`: `all`, `bilateral`, or `multilateral`.
- `years_since`, `cumulative_share`.
- `vintages`: the commitment years observed that long.

## `quality`

Key `provider_code`, `year`. Link and coverage statistics. Every amount is climate value of counted commitments:

- `provider_code`, `year`, `commitments` (the count).
- `climate_usd`: all counted commitments. `linked_usd` is the part that traces to at least one CRS record, and `linked_high_probability_usd` the part whose most probable link is at least 0.8.
- `disbursed_climate_usd`, `certain_disbursed_climate_usd`: the climate value of the commitments' disbursements in the high and low bounds.
- `model_labelled_usd`, `report_labelled_usd`: the part labelled by a model or by the donor's report.
- `repeat_climate_usd`: rows that repeat an earlier vintage, which the other amounts leave out.
- `grant_equiv_coverage`: the share of linked records with a grant equivalent.

## `total_flows`

Key `provider_code`, `recipient_code`, `year`, `bi_multi`, `flow`, `rio_marker`. Every CRS flow, across climate and non-climate records. It is the denominator of `Result.share_of_total()`:

- `provider_code`, `recipient_code`, `year`, `bi_multi`, `flow`.
- `rio_marker`: the higher of a record's two Rio markers, 0 for none.
- `amount_usd`.

## `provider_coefficients`

Key `provider_code`. A provider's own coefficient for significant markers:

- `provider_code`, `significant`: the coefficient.
- `basis`: how it was estimated, `btr_crs_ratio` for the ratio of BTR to CRS amounts.
- `confirmed`: whether a reviewer confirmed it. `cf.methods.PROVIDER` uses only confirmed coefficients.

## Terms by source

`sources.json` is the authority for each release, and lists these terms:

- OECD datasets (CRS, CRDF, Multisystem, imputed shares, DAC exchange rates): the OECD Terms and Conditions and the dataset terms in each dataset's metadata.
- World Bank Finances One: the World Bank Data Catalog licence, CC BY 4.0 by default and set per dataset.
- IMF: derivative works with attribution. Commercial use and automated bulk download need permission.
- IATI files of banks and funds: the publisher's licence in the IATI Registry.
- UNFCCC Biennial Transparency Reports: the UNFCCC terms of use, public domain with the source acknowledged.
- Green Climate Fund projects API and readiness list: commercial redistribution and derivative works require written consent. Its rows are withheld. Its IATI files are CC0 on the IATI Registry, and their rows ship.
- Human-coded adaptation labels of Weikmans et al.: they fit the map behind Jev's adaptation levels. The release omits the rows.

Credit the sources in `sources.json` when you publish a figure. `result.meta.source_versions` carries the version of each.

## Next

- [Release gates](release-gates.md) lists the checks that run on these tables before a release ships.
- [Low and high bounds](../explanation/low-and-high-bounds.md) explains the bound columns.

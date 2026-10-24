# Release tables

As of 2.0. A build writes the parquet tables below, each sorted by its key so that two builds from the same inputs hash the same. Amounts are float64 USD at current prices and face value, DAC codes int32, years int16, and `bi_multi` and Rio markers int8. `snap.table(name)` scans one table. [Python API](python-api.md) describes how queries read them.

## Files in a release

A release is a folder, `climate-finance-<name>/`, packed as one `.tar.zst`. It holds the tables below except `btr` and `btr_links`, `release.json`, and `sources.json`, and none of the build's stage records:

- `release.json` has each table's sha256 and row count, the source and model versions, every build switch and year range under `settings`, the years each source holds under `years`, the gate results, the `precision_gate_waived` flag and the waiver's reason as `precision_waiver`, and under `withheld_tables` each table the pack leaves out, with a note on how to build it. `cf.load()` checks every other table against it and raises `ReleaseIntegrityError` on a mismatch. A pack that fails its sha256 in the release index is refused before it unpacks.
- `sources.json` lists each source with its version, URLs, licence, and any note on rows the release leaves out, and, under `provider_reports`, the annual reports behind the modelled line's disbursement ratios, cited by title, year, and page. The BTR archive's entry, `unfccc_btr1`, carries the note from `withheld_tables` and a `workbooks` list, one entry per BTR workbook read, with its file, party, the sheets read, the rows kept, the rows dropped as blank, as a notation key, or as a total, and any draft marker in the file name (`started` or `awaiting_submission`).

The release holds only processed tables and metadata. The Green Climate Fund's ledger rows are withheld. [How linking works](../explanation/how-linking-works.md) explains why, and `commitments.withheld_usd` states their amount.

The published release also leaves out the row-level BTR tables `btr` and `btr_links` until the UNFCCC confirms its terms cover them, and `snap.table("btr")` on it raises `WithheldTableError`. A build from BTR workbooks downloaded from the UNFCCC writes both. `btr_totals` and `btr_crs_unlinked` hold the reconciliation totals in either.

## `commitments`

Key `row_id`. One row per recipient-perspective CRDF commitment, which is the population the trace follows. Aggregate rows rebuilt from their parts keep the parts and omit the parent. Rows that repeat an earlier vintage stay, with their amounts and a `repeat_of`:

- `row_id`, `vintage`, `year`: the CRDF row, the vintage of the CRDF file it came from, and the commitment year. The release's commitment years start with the first year the recipient-perspective CRDF holds, 2000.
- `provider_code`, `agency_code`, `recipient_code`, `channel_code`, `purpose_code`: DAC codes. `bi_multi` is the Bi_Multi code. `flow_category` is the CRDF's flow category, where 10 is ODA, 21 is other official flows, 22 and 30 are private grants, and 60 is private market flows.
- `crs_id`, `project_number`: the donor's ids.
- `rio_adaptation`, `rio_mitigation`: the Rio markers, 0, 1, or 2. `climate_share` is set for development-bank rows that report climate components, and null for marker rows.
- `face_usd`, `climate_usd`, `adaptation_usd`, `mitigation_usd`, `overlap_usd`: the commitment and its climate part. Adaptation and mitigation each include the overlap, so `climate_usd` is `adaptation_usd` + `mitigation_usd` − `overlap_usd`. Before 2010 the CRDF measures no adaptation, so `rio_adaptation`, `adaptation_usd`, and `overlap_usd` are null and `climate_usd` is the mitigation value. A negative `face_usd` is a published downward revision with zero disbursed.
- `status`: `disbursed`, `partly_disbursed`, `no_disbursement_record`, `not_traceable`, or `pending_review`. Cancellations feed no status, and the CRS carries no cancellations of the development banks' loans.
- `disbursed_usd`, `disbursed_low_usd`, `disbursed_high_usd`: the sums of the commitment's allocations in `disbursements` plus `withheld_usd`, with `0 <= disbursed_low_usd <= disbursed_usd <= disbursed_high_usd <= face_usd`.
- `withheld_usd`: the part of `disbursed_usd` that comes from fill records the release leaves out (the Green Climate Fund's ledger). It is excluded from `disbursements`.
- `project_disbursing`: true when the commitment has no record of its own and its id disbursed against an earlier commitment.
- `ledger_undisbursed`: true where the bank's ledger confirms the commitment disbursed nothing, and null where no ledger covers it or the ledger shows money moving. Only the IBRD, IDA, and IDB ledgers can set it, since they match the CRS closely enough to confirm a non-disbursement. The archived IDB extract covers 3 of the 74 IDB projects with no disbursement record, so most IDB rows stay null. A row whose `status` says disbursed while `ledger_undisbursed` is true points at a link to check.
- `repeat_of`: the first row of a commitment the build finds repeated in a later year's file. The pairing is inferred: a row repeats an earlier one when it carries the same crs id and the CRS holds no commitment of its own for it, when its project number or specific title matches within 10% of the amount, or, for the EIB, when the bulk match pairs it within 3%. `parent_row_id` names the published aggregate row a rebuilt child came from.

## `disbursements`

Key `row_id`, `record_id`, `year`. Allocations of CRS records, and of bank-ledger and IATI fills, to climate commitments, one row per allocation. Every row names a commitment. Records run to `crs_last_complete`. The table omits what the trace could not allocate. `untraced` holds the part of it that totals count, and `unallocated` sums all of it:

- `row_id`, `record_id`, `year`: the commitment, the CRS record (or fill record), and the record's year.
- `provider_code`, `agency_code`, `recipient_code`, `channel_code`, `bi_multi`, `purpose_code`, `flow_code`, `project_number`, `title`: the record's own fields. `flow_code` is the CRS flow, where 11 is ODA grants, 13 is ODA loans, and 19 is equity.
- `allocated_usd`, `allocated_low_usd`, `allocated_high_usd`: the part of the record's disbursement given to the commitment, with `0 <= allocated_low_usd <= allocated_usd <= allocated_high_usd`.
- `record_disbursement_usd`, `record_grant_equiv_usd`: the whole record's disbursement and grant equivalent, repeated on each row of the record. A query takes an allocation's grant equivalent as `record_grant_equiv_usd` × `allocated_usd` ÷ `record_disbursement_usd`. `record_grant_equiv_usd` is null where the CRS gives no grant equivalent, and where it gives a negative one on a positive disbursement.
- `inherited_rio_adaptation`, `inherited_rio_mitigation`: the markers of the commitment, which weight the allocation. `record_rio_adaptation` and `record_rio_mitigation` are the record's own. `marker_source` says how they relate, taking `components`, `commitment`, or `record_differs`.
- `climate_share`: the commitment's share for rows weighted by climate components.
- `link_method`, `link_probability`: the method that linked the record to the commitment and its probability. The probability is 1.0 on a certain link, the model's score on a `classifier`, `classifier_jev`, or `splink_jev` link, and null on any other rule link. Both are null for fills.
- `rule`: the allocation rule, which is `fifo`, `dated`, `window`, `bank_ledger`, or `iati`.
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
- `source`: `oecd` for the OECD's imputed share. A `one` row copies the OECD's share for a channel and year that `agency_spending` does not measure, so methods other than OECD read it there.

## `agency_spending`

Key `channel_code`, `year`, `rio_adaptation`, `rio_mitigation`, `published`. Each agency's CRDF commitments over a share year's span, the year and the year before, as fractions of its CRS commitments over the span. The query weighs these rows with the method's own coefficients to give the agency's climate share:

- `channel_code`, `year`: the channel and the share year.
- `rio_adaptation`, `rio_mitigation`: the markers of a marker row. Both are null on the row that holds the commitments reporting climate components.
- `published`: the climate share of face value the provider published, which caps each marker weight, as IFAD's significant rows at 0.4 are capped. Null where the provider publishes none.
- `face_share`: the face value at those markers over the span's CRS commitments.
- `adaptation_share`, `mitigation_share`, `overlap_share`: the climate components over the span's CRS commitments, on the components row. Adaptation and mitigation each include the overlap.
- `adaptation_measured`: false where the span reaches a year before 2010, when the CRDF measures no adaptation. The agency's adaptation and cross-cutting shares are then null.
- `share_source`: `computed` for a share year measured on at least USD 100,000 of CRS commitments, and `held` for a later year that rests on less, which repeats the rows of the last computed year.

A measured share year whose span holds no CRDF commitment holds one row at markers 0 and 0, so its share is 0.

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

Key `row_id`. Parsed rows of Biennial Transparency Reports, Table III.1. A build writes `btr` and `btr_links` when its configuration names a BTR archive, and a published release leaves both out. [How to reproduce the BTR tables](../how-to/reproduce-btr-tables.md) builds them locally:

- `row_id`, `party` (ISO 3), `provider_code` (null for a party absent from CRS), `year`, `source_file`.
- `recipient`, `recipient_code`, `title`, `channel`, `funding_source`, `instrument`, `support_type`, `sector`, `additional_information`: the row's text and codes as reported.
- `amount_domestic`, `grant_equiv_domestic`, `currency`: the reported amount and grant equivalent in the party's currency.
- `face_usd`, `grant_equiv_usd`: the amount and grant equivalent in USD at the party's own conversion.
- `dac_rate`: the DAC annual rate of `currency` in the row's year, in `currency` per USD, the rate CRS amounts use. Null where the DAC has none.
- `face_usd_at_dac`, `grant_equiv_usd_at_dac`: the domestic amounts at `dac_rate`, or the party's own USD where the domestic amount or the rate is missing.
- `status`: `committed` or `disbursed`.
- `is_aggregate`: true for a row that sums several activities.

## `btr_links`

Key `row_id`, `record_id`. Links from BTR rows to same-year CRS records of the party, with the record's facts:

- `row_id`, `record_id`, `link_method` (`btr_id` or `btr_splink`), `probability`. A `btr_splink` probability comes from the party's own calibration map when its id-linked rows give at least 30 right and 30 wrong labels, and from a map pooled over every party otherwise, which needs as many. A row links at a probability of 0.5 or more, and a party with neither map links by ids alone.
- `provider_code`, `year`, `recipient_code`, `project_number`, `title`: the record's fields.
- `commitment_usd`, `disbursement_usd`: the record's amounts.
- `rio_marker`: the higher of the record's two Rio markers.

## `btr_totals`

Key `party`, `year`, `status`, `funding`, `currency`, `linked`. The `btr` rows summed, which is what the reconciliation reads where the release leaves out `btr` and `btr_links`:

- `party`, `provider_code`, `year`, `status`, `currency`: as in `btr`.
- `funding`: `oda` or `oof` where the rows' funding source names only that flow, such as "ODA (ETS)", and `other` otherwise, such as "ODA/OOF" or none.
- `linked`: true for rows that `btr_links` links to a CRS record.
- `rows`: the number of rows.
- `face_usd`, `grant_equiv_usd`, `amount_domestic`: the rows' sums, null where every row lacks the amount.
- `dac_rate`, `face_usd_at_dac`, `grant_equiv_usd_at_dac`: the DAC rate of the year in domestic currency per USD, one per party and year, and the face and grant-equivalent sums restated at it.

## `btr_crs_unlinked`

Key `provider_code`, `year`, `flow`. The OECD side of the reconciliation that no BTR row reaches, for each BTR party's DAC provider and each year its BTR rows cover:

- `provider_code`, `year`.
- `flow`: `commitments` or `disbursements`.
- `amount_usd`: the flow's sum over the provider's bilateral CRS records of the year with a Rio marker of 1 or 2 that no `btr_crs` link reaches, zero where every such record is linked, and null where the build's CRS holds no record of the provider and year, as for a year past the last complete CRS year.

## `disbursement_profile`

Key `group`, `years_since`. The cumulative disbursed share of traceable climate value by years since commitment:

- `group`: `all`, `bilateral`, or `multilateral`.
- `years_since`, `cumulative_share`.
- `vintages`: the commitment years observed that long.

## `quality`

Key `provider_code`, `year`. Link and coverage statistics. Every amount is climate value of counted commitments:

- `provider_code`, `year`, `commitments` (the count).
- `climate_usd`: all counted commitments. `linked_usd` is the part that traces to at least one CRS record.
- `certain_route_usd`: the part of `linked_usd` whose CRDF rows reach their CRS commitment through certain routes alone.
- `disbursed_climate_usd`, `certain_disbursed_climate_usd`: the climate value of the commitments' disbursements in the high and low bounds.
- `model_labelled_usd`, `report_labelled_usd`: the part labelled by a model or by the donor's report.
- `repeat_climate_usd`: rows that repeat an earlier vintage, which the other amounts leave out.
- `grant_equiv_coverage`: the part of the linked commitments' allocated value whose record has a grant equivalent, null where nothing is allocated.

## `untraced`

Key `record_id`. Rio-marked CRS disbursement that reaches no commitment in the release, one row per CRS record, at face value. Disbursement and grant-equivalent totals count it in `value` and `high` and not in `low`, weighted by the record's own markers. A record enters when its Bi_Multi code is in the recipient view, it disburses a positive amount in a year up to `crs_last_complete`, and it carries a Rio marker above 0:

- `record_id`, `year`: the CRS record and its disbursement year.
- `provider_code`, `agency_code`, `recipient_code`, `channel_code`, `bi_multi`, `purpose_code`, `flow_code`, `project_number`, `title`: the record's own fields, as on `disbursements`.
- `rio_adaptation`, `rio_mitigation`: the record's own markers. Before 2010 `rio_adaptation` is null, as the CRDF's is, and the record counts mitigation alone.
- `untraced_usd`: the face value that reaches no commitment.
- `reason`: `excess` for a linked record's disbursement beyond what its commitments take, and `unlinked` for the whole disbursement of a record linked to no commitment.
- `record_disbursement_usd`, `record_grant_equiv_usd`: as on `disbursements`. A query takes an untraced row's grant equivalent as `record_grant_equiv_usd` × `untraced_usd` ÷ `record_disbursement_usd`.

A record's `disbursements` rows and its `untraced` row never share money: its allocations plus its `untraced_usd` are at most its disbursement. With `gap_fill` on, the funds whose commitments the build routes through their IATI files, the GEF and the GCF, keep their unlinked records out of `untraced`, since a fill may count the same money, and `unallocated.fund_unlinked_usd` holds them. Their excess stays in.

## `unallocated`

Key `provider_code`, `year`. Rio-marked CRS disbursements that reach no commitment in the release, by provider and disbursement year, at face value. With the `disbursements` rows of Rio-marked CRS records, they sum to every Rio-marked CRS disbursement:

- `provider_code`, `year`.
- `excess_usd`: a linked record's disbursement beyond what its commitments take.
- `outside_release_usd`: disbursements linked to CRS commitments with no CRDF row in the release.
- `unlinked_marked_usd`: records linked to no commitment.
- `fund_unlinked_usd`: the unlinked records of the funds the build routes through their IATI files, 0 with `gap_fill` off.

`excess_usd` and `unlinked_marked_usd` equal the `untraced` sums by provider, year, and reason, and count in totals through it. No total counts `outside_release_usd` or `fund_unlinked_usd`.

## `total_flows`

Key `provider_code`, `agency_code`, `recipient_code`, `year`, `bi_multi`, `flow`, `rio_marker`. Every CRS flow, across climate and non-climate records, up to `crs_last_complete`. It is the denominator of `Result.share_of_total()`, and the BTR reconciliation weights its climate disbursements by `rio_marker`:

- `provider_code`, `recipient_code`, `year`, `bi_multi`, `flow`.
- `agency_code`: the CRS agency, so a query can take out the EIB's flows where the EIB counts as a channel. Null on core contributions.
- `rio_marker`: the higher of the record's two Rio markers, 0 for none.
- `amount_usd`.

## `provider_coefficients`

Key `provider_code`, `agency_code`, `first_year`. The coefficients each provider applies to Rio-marked activities when it reports to the UNFCCC, as its source states them. `cf.methods.PROVIDER` reads them:

- `provider_code`.
- `agency_code`: the agency a row covers. Null covers every agency of the provider without a row of its own.
- `first_year`, `last_year`: the years a row covers, null for no bound.
- `significant`, `principal`: the coefficients for a significant and a principal marker.
- `both_significant`: the coefficient for an activity significant for adaptation and mitigation alike, null where it equals `significant`.
- `case_by_case`: true for a provider that assesses each activity's climate share. Its coefficients are null, or the default its source gives for an activity whose share is unknown.
- `source`, `locator`: the document, and the page, note, or paragraph that states the coefficients.

## Terms by source

`sources.json` is the authority for each release, and lists these terms:

- OECD datasets (CRS, CRDF, Multisystem, imputed shares, DAC exchange rates): the OECD Terms and Conditions and the dataset terms in each dataset's metadata. A release is an adaptation of OECD data and carries no endorsement by the OECD, and a figure published from it should say so.
- World Bank Finances One: the World Bank Data Catalog licence, CC BY 4.0 by default and set per dataset.
- IMF: derivative works with attribution. Commercial use and automated bulk download need permission.
- IATI files of banks and funds: the publisher's licence in the IATI Registry.
- UNFCCC Biennial Transparency Reports: the UNFCCC terms of use, public domain where no change to the content is introduced and the source is acknowledged. Parsed rows change the content, so the published release ships the totals in `btr_totals` and `btr_crs_unlinked` and leaves out `btr` and `btr_links` until the UNFCCC confirms its terms cover them.
- Green Climate Fund projects API and readiness list: the Fund's website terms allow use for private study, research, and teaching, and bar commercial derivative works without written consent. The API states no terms of its own. Their rows are withheld. Its IATI files are CC0 on the IATI Registry, and their rows ship.
- Human-coded adaptation labels of Weikmans et al.: they fit the map behind Jev's adaptation levels. The release omits the rows.

Credit the sources in `sources.json` when you publish a figure. `result.meta.source_versions` carries the version of each.

## Next

- [Release gates](release-gates.md) lists the checks that run on these tables before a release ships.
- [Low and high bounds](../explanation/low-and-high-bounds.md) explains the bound columns.

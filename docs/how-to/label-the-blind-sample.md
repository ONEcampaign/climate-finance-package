# How to label the blind sample

`cf-build label` opens a page in your browser where you decide your share of the blind sample, one pair at a time. Each answer is saved as you give it, in the form `cf-build import-labels` records. You need the `build` extra, a build directory, and the sample directory that `cf-build sample` wrote.

## Your share

The sample holds no link that joins a unit to itself, since such a link cannot be wrong and the precision gate counts it right. The `human_audit` gate requires a person to decide at least a tenth of the sample's draws. `cf-build label` queues two kinds of pair.

- Judgement calls are the pairs whose link did not come from a certain rule (`wb_loan`, `crs_id_purpose`, `crs_id` or `crs_id_parent`). The queue orders them by a hash of their sample ids, which gives the same order on every run, and takes pairs until the draws they back reach `human_audit.min_share` of all the sample's draws, the share the gate counts. A pair can back several draws, so the queue holds fewer pairs than a tenth of the draws.
- Spot checks are 50 pairs a certain rule linked, taken in the same hash order, one provider after another a round at a time so they spread over providers. They sit among the judgement calls at even spacing, about one in 20, and check that the rule holds.

On the 2026a-rc sample, 22,814 draws fall on 11,694 pairs, and 6,628 of those pairs (11,007 draws) come from certain rules. The queue holds 964 judgement calls backing 2,327 draws (10.2% of all draws) and 50 spot checks, 1,014 pairs in all. Decisions already in the decisions file stay valid and count toward the gate. A pair that anyone has decided stays in the queue, counted as done and skipped, so stopping and starting again resumes where you left off. The progress line names each pair a judgement call or a spot check and nothing more. Judge each pair on its two records alone.

## Steps

1. Start the page. `--reviewer` names you in the decisions.

   ```bash
   cf-build label releases/2026a sample --reviewer analyst-1 --config build.toml
   ```

   The command serves the page on `127.0.0.1` only, at a free port unless you pass `--port`, and opens it in your browser. `--no-browser` prints the address and leaves the browser alone.

2. Read the page. The question at the top is "Is A money spent under commitment B?", and the two columns under it say what counts as a yes and what counts as a no. The line below shows your place, the provider by name, and the time left at your recent pace. "By provider" lists every provider's progress.

    The bar above the two cards names the donor, agency and recipient once. A recipient, donor or agency that differs between the records shows in red with both values, because a different recipient is the commonest reason for a no. Card A is the later spending record and card B the earlier commitment record. Fields that differ carry a yellow background, and fields that agree collapse into the line "Same on both", which lists them and opens to show the values. The last section opens to show the CRS rows each record sums and the count of other commitments to the same recipient with similar titles, which helps to check that B is the right commitment. A long description shows three lines and expands with "more".

3. Answer. A key press saves and shows the next pair at once.

    | Key | Action |
    | --- | --- |
    | `y` | Yes, A is money spent under B |
    | `n` | No |
    | `Shift`+`y`, `Shift`+`n` | Yes or No at low confidence (0.6 or 0.4) |
    | `u` | Can't tell: set the pair aside |
    | `r` | Open the reason box. Type, press `Enter` to leave the box, then press `y` or `n` to save with the reason |
    | Left arrow or `Backspace` | Go back to an earlier pair |
    | Right arrow | Go forward without saving |

    The same three actions are buttons at the bottom of the page. After a save, a note at the bottom says what was saved, and its Undo link reopens that pair with your answer in place, so you can change it. A pair you reopen shows "You answered", and a new answer replaces the old one. Confidence is optional; an answer without Shift saves with none.

    A pair you can't tell records nothing, since no label is true of it, and it comes back after the other pairs. A pair left undecided at the end counts against the `human_audit` share unless an agent or another person decides it.

4. Stop with `Ctrl-C` in the terminal. Nothing is pending.

## What a save writes

Each save records that one answer in the review decisions that `sources.review_decisions` names, and leaves your other answers as they were. It then rewrites your labels CSV beside the pair's packet, `sample/<provider>/batch-<NN>.<reviewer>.labels.csv`. The CSV has the columns `pair,label,confidence,reason` and lists the pairs you have answered in that packet. A changed answer replaces your earlier one, so the decisions hold one row per person and pair. The CSV is also valid input for `cf-build import-labels --partial` with a `--config` that names another decisions file.

Run one `cf-build label` at a time against a decisions file, and do not run `cf-build import-labels` on it meanwhile: two writers can overwrite each other's updates.

## Verify it worked

Run the gate. The `human_audit` row counts the draws a person has decided.

```bash
cf-build gate releases/2026a --config build.toml --sample sample --report
```

## Next

- [How to cut a release](cut-a-release.md) runs the gates once the sample is labelled.
- [cf-build](../reference/cf-build.md) lists every option of `label`.

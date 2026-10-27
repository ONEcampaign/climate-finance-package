# How to label the blind sample

`cf-build label` opens a page in your browser where you decide your share of the blind sample, one pair at a time. Each answer is saved as you give it, in the form `cf-build import-labels` records. You need the `build` extra, a built release directory, and the sample directory that `cf-build sample` wrote.

## Your share

The `human_audit` gate requires a person to decide at least a tenth of the sample's draws. `cf-build label` queues that tenth for each provider. It orders a provider's pairs by a hash of their sample ids, which gives the same order on every run, and takes pairs until the draws they back reach `human_audit.min_share` of the provider's draws. A pair can back several draws, so the queue can hold fewer pairs than a tenth of the draws. A pair that anyone has decided is skipped, so stopping and starting again resumes where you left off.

## Steps

1. Start the page. `--reviewer` names you in the decisions.

   ```bash
   cf-build label releases/2026a sample --reviewer analyst-1 --config build.toml
   ```

   The command serves the page on `127.0.0.1` only, at a free port unless you pass `--port`, and opens it in your browser. `--no-browser` prints the address and leaves the browser alone.

2. Read the pair. Record A is the later disbursement and record B the earlier commitment. Fields that differ between the two carry a yellow background. The matching rule sits under "Matching rule", and the CRS rows each record sums sit under the table. The page shows the fields the packet shows, so the score and link method stay hidden.

3. Answer with the keyboard.

   | Key                       | Action                                                                |
   | ------------------------- | --------------------------------------------------------------------- |
   | `y`                       | A matches B                                                           |
   | `n`                       | A does not match B                                                    |
   | `u`                       | Unsure (confidence 0.5)                                               |
   | `1`, `2`, `3`             | Low, medium or high confidence (0.6, 0.8 or 0.95 on the label's side) |
   | `r`                       | Type a reason. `Escape` leaves the box                                |
   | `Enter`                   | Save and show the next undecided pair                                 |
   | `Backspace` or left arrow | Go back to an earlier pair                                            |
   | Right arrow               | Go forward without saving                                             |

   Confidence is optional. A pair you reopen shows your saved answer, and saving again replaces it.

4. Stop with `Ctrl-C` in the terminal. Nothing is pending.

## What a save writes

Each save records that one answer in the review decisions that `sources.review_decisions` names, and leaves your other answers as they were. It then rewrites your labels CSV beside the pair's packet, `sample/<provider>/batch-<NN>.<reviewer>.labels.csv`. The CSV has the columns `pair,label,confidence,reason` and lists the pairs you have answered in that packet. A changed answer replaces your earlier one, so the decisions hold one row per person and pair. The CSV is also valid input for `cf-build import-labels --partial` on another decisions file.

Run one `cf-build label` at a time against a decisions file, and do not run `cf-build import-labels` on it meanwhile: two writers can overwrite each other's updates.

## Verify it worked

Run the gate. The `human_audit` row counts the draws a person has decided.

```bash
cf-build gate releases/2026a --config build.toml --sample sample/design.parquet --report
```

## Next

- [How to cut a release](cut-a-release.md) runs the gates once the sample is labelled.
- [cf-build](../reference/cf-build.md) lists every option of `label`.

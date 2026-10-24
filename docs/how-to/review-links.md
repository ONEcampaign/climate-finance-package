# How to review links

Record people's and agents' decisions on the links a build queued for review, then rebuild so the decisions take effect. You need the `build` extra and a built release directory.

## Before you start

The review stage runs when `build.toml` sets `review = true`. When `review = false`, the build passes links and commitments through unchanged and writes no queue. A queued link keeps its allocation until someone rejects it, so money stays in totals while it waits.

A link joins the queue with the first `review_reason` that applies:

- `ledger_contradicted`, when a bank's ledger contradicts it.
- `rule_disagreement`, when its records disagree on recipient or project number.
- `band`, when the classifier gave it a probability in the Jev band set by `jev.band` in `build.toml`.
- `jev_band`, when Jev gave a Splink link a probability between 0.5 and 0.9.
- `calibration`, when it belongs to a Splink model's calibration sample, which sets the model's probabilities.

The queue also holds every commitment row allocated more than 1.1 times its face value (`over_face`). A decided pair stays out of later queues. An over-face row returns in every build while its allocation exceeds 1.1 times its face value.

## Steps

1. Write the queue as a Markdown labelling packet. Each numbered section shows the later disbursement unit (A) and the earlier commitment unit (B) side by side, with every CRS row each unit sums. The command writes the pairs file `queue.pairs.parquet` beside it.

    ```bash
    cf-build queue releases/2026a --out queue.md
    ```

    Add `--no-ids` to hide crs ids and project numbers and blank them in the records' text, which lets you measure how well labellers do without ids.

2. Decide each pair. A matches B when A's money is spending under the commitment B records. That includes a commitment the donor reported on several lines for the same recipient, renumbered, or left partly undisbursed and drawn down later. It excludes a different recipient country, a sibling project, a later phase with its own new commitment, a separate yearly grant spent in its own year, and an unrelated activity. The packet opens with this definition in full.

3. Write a labels CSV with one row per pair. Give the packet's pair number, `match` or `no_match`, a confidence from 0 to 1 that the pair matches, and a reason. A `match` requires a confidence of 0.5 or more, and a `no_match` requires 0.5 or less.

    ```csv
    pair,label,confidence,reason
    1,match,0.95,same crs id and recipient; the later record is a drawdown
    2,no_match,0.1,different recipient country
    ```

4. Import the labels and name the reviewer. A person's decisions take no model. An agent's decisions require `--model` and may name the `--round`: `label` for a first label, `second` for a second label on an unsure pair, and `adjudicate` for a decision made with ids visible.

    ```bash
    cf-build import-labels queue.md labels.csv --source human --reviewer analyst-1
    cf-build import-labels queue.md agent-labels.csv --source agent --reviewer run-3 --model claude-opus-5-5 --round label
    ```

    The CSV must answer each pair in the packet once, and the packet must match its pairs file. Otherwise the import raises `ReviewDecisionsError` and records nothing. Decisions append to `data/review_decisions.parquet`, or to the file `--decisions` names. The build, `sample`, and `gate` read decisions from `sources.review_decisions` in `build.toml`, so point both at the same file.

5. Rebuild. The link stage reads the decisions, and `--offline` reuses the sources the first build fetched. One decision stands for each pair. A person's decision stands over any agent's, whatever the import order, and among people the latest stands. Among agents, the latest round stands (`label`, then `second`, then `adjudicate`), then the latest import. An accepted link takes probability 1 and clears any record disagreement, so a flagged exact-key link becomes certain. A rejected link leaves `links`, and its money moves to the next link or to excess. Decisions on Splink calibration pairs set those models' probabilities.

    ```bash
    cf-build build 2026a --config build.toml --offline
    ```

## Verify it worked

The rebuilt queue omits the decided pairs. `cf-build queue` writes a smaller packet, and `snap.quality().summary()` shows the change in `pending_review_share`.

## Blind-sample decisions are separate

The precision gate draws its own sample with `cf-build sample` and reads decisions that carry a `sample_id`. Those decisions measure precision and leave models and links untouched, so they stay separate from the queue's decisions. See [Release gates](../reference/release-gates.md) for the sample rules.

## Next

- [How to cut a release](cut-a-release.md) runs the gates once the queue is clear.
- [cf-build](../reference/cf-build.md) lists every option of the commands used here.

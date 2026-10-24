# climate-finance documentation

climate-finance 2.0 returns international public climate finance from OECD and UNFCCC data. Each figure traces to the project records behind it and carries a low and a high bound. Install the package with `pip install climate-finance`.

## Learn

- [Trace Japan's climate finance to Bangladesh](tutorial.md) loads a release, asks for a figure, follows it to the records behind it, and checks how much traces.

## Do a task

- [How to trace a figure](how-to/trace-a-figure.md) lists the projects and links behind a number, and saves the query to rerun.
- [How to check link quality](how-to/check-link-quality.md) covers link rates, status, certain share, and measured precision.
- [How to review links](how-to/review-links.md) explains how a person or an agent decides the pairs a build queued. For release managers.
- [How to reconcile a party's BTR report with OECD data](how-to/reconcile-with-btr.md)
- [How to cut a release](how-to/cut-a-release.md) covers how to build, sample, gate, and pack. For release managers.
- [Migrating from 1.x](migrating-from-1x.md)

## Look something up

- [Python API](reference/python-api.md) documents `load`, `Snapshot`, `Spec`, `Method`, `Result`, `Quality`, `Reconciliation`, and the flow, view, currency, and scope arguments.
- [Release tables](reference/release-tables.md) lists the files in a release, every table and column, and the terms for each source.
- [cf-build](reference/cf-build.md) lists the build and release commands.
- [Release gates](reference/release-gates.md) details the seven checks a release passes.

## Understand

- [How linking works](explanation/how-linking-works.md) explains how a disbursement counts against a commitment, and which rows a release leaves out.
- [Low and high bounds](explanation/low-and-high-bounds.md) explains which links count where.
- [About the methods](explanation/methods.md) covers marker weights, comparing methods, attribution to providers, and EU Institutions.

## Release notes

- [2.0.0](release-notes/2.0.0.md) covers what changed and why totals differ from 1.x.

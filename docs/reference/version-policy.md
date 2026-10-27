# Version policy

climate-finance numbers its releases `major.minor.patch`. A patch release fixes defects, a minor release adds to the API, and a major release may break it. The package version and the data release are separate. A new data release ships without a new package version, and its figures differ from the last release's. [Release tables](release-tables.md) describes the data format number. The number rises when a release table, a column of one, or a `release.json` field is added, removed, or changes meaning, because a newer package's queries may read the new column from an older release.

## What breaks

A change breaks users, and waits for a major release, when it:

- removes or renames a name in `cf.__all__` or `cf.methods.__all__`, or a parameter or option of one
- moves a parameter, or makes an optional one required
- changes a default, a return type, or the type of a field
- changes an error class
- changes an ordering that the docs promise
- changes the column names, column order, dtypes, or missing-value representation of a returned table
- raises the floor of a runtime dependency past a major version

Dropping a Python version that SPEC 0 no longer lists is allowed in a minor release, provided `requires-python` changes in the same release. Installers then keep users on the last release that supports their Python.

CI checks the rules on names, parameters, and defaults. A test pins the exact contents of `cf.__all__` and `cf.methods.__all__`, so adding or removing a name changes the test in the same pull request. A `griffe check` job compares the source with the latest `v2` tag and fails on a removed name, a moved parameter, or a changed default.

## What the package leaves unpromised

- The prose of an error or warning message.
- The text of a `repr`.
- Any name with a leading underscore.
- Anything marked experimental, such as `Release.jev_flags(experimental=True)` and the `mitigation_aim_p` column it adds.

## How a deprecation runs

The package ships the new form beside the old one in a minor release, and the old form warns with a `DeprecationWarning` that names its replacement and the major version that removes it. The old form stays for at least one more minor release and goes in the next major. `ClimateData` follows this rule: it runs with a warning in 2.x and goes in 3.0.

## Next

- [Python API](python-api.md) documents the public names.
- [Migrating from 1.x](../migrating-from-1x.md) lists what changed from 1.2.3 to 2.0.

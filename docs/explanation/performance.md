# Performance

A query reads the release's Parquet tables through Polars and applies its method at query time, so it needs neither the build stack nor a network connection. A build runs once per release and holds the heavy work: cleaning the sources, linking, allocation and Jev adjudication.

The figures below come from the 2026a release candidate, built with every switch at its shipped setting (review with the bank ledgers, key repair, recoveries, Jev links and Jev flags), on an Apple M5 Pro with 18 cores and 48 GB of memory, macOS 27, Python 3.13. Memory is the process's peak memory footprint, which on macOS counts what the process holds better than its resident set size.

| Task                                                                       | Time       | Peak memory |
| -------------------------------------------------------------------------- | ---------- | ----------- |
| Unpack the 117 MB archive (112 MB of tables)                               | 0.5 s      |             |
| Open the release, checking every table's sha256                            | 0.06 s     | 56 MB       |
| One provider and one year, `spending(providers=["Germany"], years=[2023])` | 0.3 s      | 449 MB      |
| DAC bilateral gross and net, 2015 to 2024                                  | 0.6 s      | 738 MB      |
| Every provider, 2015 to 2024, `method=RANGE` (87,840 rows)                 | 1.5 s      | 1.1 GB      |
| Build, normalise to assemble, with every Jev answer cached                 | 8 min 56 s | 12.6 GB     |

The query times exclude starting Python and importing the package. The build time excludes the fetch, which depends on the sources' servers: the OECD bulk files, the CRDF workbooks, the Green Climate Fund API and the bank ledgers. A first build with an empty Jev cache also waits on Jev's API.

A build needs about 13 GB of memory; two at once on one 48 GB machine leave little room for anything else.

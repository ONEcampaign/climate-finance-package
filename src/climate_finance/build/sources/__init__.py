"""Downloads of the raw files the readers parse. Only the fetch stage calls them.

Each `download_*` function takes the raw directory and `offline`, writes under
`<raw_dir>/ledgers/` (the GCF projects file under `<raw_dir>/external/gcf/`) and
returns one `RawFile` per file, the fields `fetch` records in `raw_manifest`. With
`offline=True` it makes no request and raises `SourceUnavailableError`, unless the
ledger downloads are given an `archive`, a local copy to import.
"""

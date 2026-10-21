"""Downloads of the raw files the readers parse. Only the fetch stage calls them.

Each source module has one `download_*` function taking the raw directory and
`offline`. It writes under `<raw_dir>/ledgers/` and returns one `RawFile` per file,
the fields `fetch` records in `raw_manifest`. With `offline=True` it makes no request
and raises `SourceUnavailableError`, unless `archive` names a local copy to import.
"""

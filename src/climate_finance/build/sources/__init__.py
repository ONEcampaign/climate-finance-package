"""Downloads of the raw files the readers parse. Only the fetch stage calls them.

Each source module has one `download_*` function taking the raw directory. It
writes under `<raw_dir>/`: the ledgers and the IMF's SDR rate under `ledgers/`, and
the GCF projects file under `external/gcf/`. It returns one `RawFile` per file, the
fields `fetch` records in `raw_manifest`. Where `archive` names a local copy, it
places that and makes no request. An offline build never calls them.
"""

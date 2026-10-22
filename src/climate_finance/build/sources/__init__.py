"""Downloads of the raw files the readers parse. Only the fetch stage calls them.

Each source module has one `download_*` function taking the raw directory and
`offline`. It writes under `<raw_dir>/`: the ledgers and the IMF's SDR rate under
`ledgers/`, the GCF projects file under `external/gcf/`, the ForeignAssistance.gov
crosswalk under `external/foreignassistance/`. It returns one `RawFile` per file,
the fields `fetch` records in `raw_manifest`. With `offline=True` it makes no
request and raises `SourceUnavailableError`, unless `archive` names a local copy to
import.
"""

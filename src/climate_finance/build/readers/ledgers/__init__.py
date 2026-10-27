"""Bank and bilateral disbursement ledgers used to check and fill CRS links.

Readers take local paths and return the frame described in `frame.py`. Each reader
module names where its files sit in the raw layout, under `LEDGER_DIR`; the downloads
that put them there live in `climate_finance.build.sources`.
"""

LEDGER_DIR = "ledgers"

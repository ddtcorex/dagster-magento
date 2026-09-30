# Compatibility records

`compat/results/<version>-<date>.json` holds one live run of the whole
`pytest -m live` suite against a fresh Magento sandbox of that version. The
README compatibility table is generated from the newest record per version.

Produce or refresh them with `scripts/compat-matrix.sh` (see its header for
the flags); each record states the exact Magento patch, PHP, database, search
engine and bridge module version the run used. Do not edit a record by hand.

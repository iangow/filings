#!/usr/bin/env bash
pg_dump --format custom --no-tablespaces --file ~/Dropbox/pg_backup/filings_filings.backup \
    --table 'filings.filings'

# pg_restore --host localhost --username "igow" --dbname "crsp" --verbose --no-tablespaces \
#   --clean ~/Dropbox/pg_backup/filings_filings.backup

psql -c "GRANT SELECT ON filings.filings TO crsp_basic"
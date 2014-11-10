# Code to get data on or from filings on EDGAR

- Get filing index files: `get_filings.R`. The data contain meta-data on every filing on EDGAR: `company_name, CIK, date_filed, form_type, file_name`.
The `file_name` allows on to construct a URL from which the filing can be obtained.
The resulting data are stored in a PostgreSQL table `filings.filings`.

## Code to get CUSIP-CIK mappings from 13D and 13G filings

Filings by institutional investors on form 13D and 13G provide data on 
mappings from CUSIPs to CIKs. 
The code in the files below collects and extract these data.

- Get 13D filings: `get_13D_filings.R`.
- Extract CUSIP data from filings: `extract_cusips_perl.pl`
- Import extracted CUSIP data: `import_cusip_cik.pl`

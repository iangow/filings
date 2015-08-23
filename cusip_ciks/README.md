# Extracting a CUSIP-CIK mapping from 13D and 13G filings

## 1. Get the filings

Run the code in `get_13D_filings.R`.

## 2. Scrape data from the downloaded filings

Run the code in `extract_cusips.R` (this calls `extract_cusips.pl` repeatedly).
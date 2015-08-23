# Extracting a CUSIP-CIK mapping from 13D and 13G filings

[ ] Work out how to handle filings that are not available to the public 
(e.g., [edgar/data/909465/0000909465-95-000005.txt](www.sec.gov/Archives/edgar/data/909465/0000909465-95-000005.txt)).

## 1. Get the filings

Run the code in `get_13D_filings.R`.

## 2. Scrape data from the downloaded filings

Run the code in `extract_cusips.R` (this calls `extract_cusips.pl` repeatedly).
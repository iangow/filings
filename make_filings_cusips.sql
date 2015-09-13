SET work_mem='15GB';

DROP TABLE IF EXISTS filings.cusips;

CREATE TABLE filings.cusips AS 
WITH 
sec AS (
  SELECT DISTINCT company_name, cik
  FROM filings.filings
  WHERE form_type IN ('10-K')),
ww AS (
  SELECT DISTINCT stock_record_name, cusip_number
  FROM whalewisdom.filing_stock_records)
SELECT DISTINCT sec.company_name, sec.cik, ww.cusip_number
FROM sec
LEFT JOIN ww
ON sec.company_name=ww.stock_record_name

SET work_mem='3GB';

CREATE TABLE filings.sec_names AS
SELECT cik::integer, array_agg(DISTINCT company_name) AS sec_names
FROM filings.filings
GROUP BY 1;

GRANT SELECT ON filings.sec_names TO crsp_basic;

CREATE INDEX ON filings.sec_names (cik);

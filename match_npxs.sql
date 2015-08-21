CREATE TABLE filings.npxs AS
SELECT * 
FROM issvoting.npx_id AS a
INNER JOIN filings.filings AS b
ON a.npx_file_id=regexp_replace(b.file_name, 'edgar/data/\d+/(.*).txt', '\1')

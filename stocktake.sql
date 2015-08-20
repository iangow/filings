SET work_mem='1GB';

WITH raw_data AS (
    SELECT extract(quarter FROM date_filed) AS quarter, 
        extract(year FROM date_filed) AS year,
        file_name
    FROM filings.filings)

SELECT year, quarter, count(*)
FROM raw_data
GROUP BY year, quarter
ORDER BY year, quarter;

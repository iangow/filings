#!/usr/bin/env Rscript

# Get a list of files that need to be processed ----

library("RPostgreSQL")
pg <- dbConnect(PostgreSQL())

if (!dbExistsTable(pg, c("filings", "cusip_cik"))) {
    dbGetQuery(pg, "
        CREATE TABLE filings.cusip_cik
            (
              file_name text,
              cusip text,
              cik integer,
              company_name text,
              format text
            )

        GRANT SELECT ON TABLE filings.cusip_cik TO crsp_basic;

        CREATE INDEX ON filings.cusip_cik (cusip);
        CREATE INDEX ON filings.cusip_cik (cik);")
}

dbDisconnect(pg)

library(dplyr)
pg <- src_postgres()
filings <- tbl(pg, sql("SELECT * FROM filings.filings"))
cusip_cik <- tbl(pg, sql("SELECT * FROM filings.cusip_cik"))

file_list <-
    filings %>%
    filter(form_type %in% c('SC 13G', 'SC 13G/A', 'SC 13D', 'SC 13D/A')) %>%
    select(file_name) %>%
    anti_join(cusip_cik) %>%
    collect()

# Create function to parse a SC 13D or SC 13F filing ----
parseFile <- function(file_name) {

    # Parse the indicated file using a Perl script
    system(paste("cusip_ciks/extract_cusips.pl", file_name),
           intern = TRUE)
}

# Apply parsing function to files ----
library(parallel)
system.time({
    res <- unlist(mclapply(file_list$file_name, parseFile, mc.cores=12))
})

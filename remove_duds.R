# Get a list of 13D and 13G filings with no entries in the CUSIP-CIK table ----
library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())

file.list <- dbGetQuery(pg, "
    SET work_mem='2GB';

    SELECT *
    FROM filings.filings
    WHERE form_type IN ('SC 13G', 'SC 13G/A', 'SC 13D', 'SC 13D/A')
    AND file_name NOT IN (
        SELECT file_name 
        FROM filings.cusip_cik)
")

dbDisconnect(pg)
# Identify and delete "duds" ----
# Duds are files not downloaded due to SEC traffic limits.
dud_file <- function(file_name) {
    file_path <- file.path(Sys.getenv("EDGAR_DIR"), file_name)
    if (file.exists(file_path)) {
        temp <- system(paste("grep -l \"Traffic Limit\" ", file_path),
                       intern=TRUE, ignore.stderr = FALSE)
        return(temp)
    }
}
library(parallel)
duds <- unlist(mclapply(file.list$file_name, dud_file, mc.cores=10))
lapply(duds, unlink)

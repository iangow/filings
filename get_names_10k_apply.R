# Get a list of 10-K filings ----
library(RPostgreSQL)

pg <- dbConnect(PostgreSQL())

# dbGetQuery(pg, "DELETE
# FROM filings.original_names
# WHERE original_name ~ '</[^T]';")

ars <- dbGetQuery(pg, "
    SET work_mem='3GB';

    WITH extracted AS (
        SELECT file_name
        FROM filings.extracted)
    SELECT *
    FROM filings.filings
    WHERE form_type='10-K' AND
        file_name NOT IN (SELECT file_name FROM filings.original_names)")
rs <- dbDisconnect(pg)

# Get text filings ----
if (FALSE) {
    source("filings/download_filing_functions.R")

    ars$have_text_file <- unlist(lapply(ars$file_name, get_text_file))
    ars$extracted <- unlist(lapply(ars$file_name, extract.filings))
}

# Get names from filings using regexes ----
source("filings/get_names_10k.R")

filing_nums <- 1:(length(ars$file_name))
to_batches <- function(vector, batch_size) {
    vector_nums <- 1:length(vector)
    split(vector, floor(vector_nums/batch_size))
}

batches <- to_batches(ars$file_name, 100)

process_batch <- function(batch) {
    original_name <- unlist(lapply(batch, check_name_regex))
    temp <- data.frame(file_name=batch, original_name=original_name,
                       stringsAsFactors = FALSE)

    pg <- dbConnect(PostgreSQL())
    append = dbExistsTable(pg, c("filings", "original_names"))
    dbWriteTable(pg, c("filings", "original_names"),
                 temp, append=append, row.names=FALSE)
    rs <- dbDisconnect(pg)
}
library(parallel)
processed <- mclapply(batches, process_batch, mc.cores=4)
processed[error_batches] <- mclapply(batches[error_batches], process_batch, mc.cores=4)


trim_dashes <- function(names) {
    new_names <- gsub("</?TABLE>", "", names)
    new_names <- gsub("(?:\302\240)", " ", new_names)
    new_names <- gsub("\\.{2,}", "", new_names)
    new_names <- gsub("^[-\\.=\\s ]+", "", new_names)
    new_names <- gsub("[-=\\s ]+$", "", new_names)

    # Scrub names with HTML tags in them
    new_names[grepl("</", new_names)] <- NA
    trimws(new_names)
}

nuke_duds <- function(name) {
    new_name <- gsub("(?i)Securities and Exchange Commission", NA, name)
    new_name <- gsub("(?i)Transition Report", NA, new_name)
}
# to.fix <- grep("^Error", ars$original_name)
library(dplyr)
pg <- src_postgres()


filings <- tbl(pg, sql("
    SELECT file_name, cik
    FROM filings.filings"))

original_names <- tbl(pg, sql("
    SELECT *
    FROM filings.original_names")) %>%
    inner_join(filings)

need_extraction <-
    original_names %>%
    filter(original_name %~% "</[^T]") %>%
    select(original_name) %>%
    collect()

original_names_edited <-
    original_names %>%
    # filter(cik=="1000015") %>%
    collect() %>%
    mutate(original_name_edited = nuke_duds(trim_dashes(original_name))) %>%
    # select(original_name, original_name_edited) %>%
    # print(n=30)
    as.data.frame()

# Push data to PostgreSQL ----
library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())
dbGetQuery(pg, "DROP VIEW IF EXISTS filings.company_names")
rs <- dbWriteTable(pg, c("filings", "original_names_edited"),
             original_names_edited, overwrite=TRUE, row.names=FALSE)
rs <- dbGetQuery(pg, "
    CREATE VIEW filings.company_names AS
    SELECT DISTINCT cik, upper(original_name_edited) AS company_name
    FROM filings.original_names_edited
    WHERE original_name_edited IS NOT NULL
    UNION
    SELECT DISTINCT cik, upper(company_name) AS company_name
    FROM filings.filings
    WHERE form_type='10-K'
    ORDER BY cik")
dbDisconnect(pg)

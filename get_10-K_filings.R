library(RPostgreSQL)
drv <- dbDriver("PostgreSQL")
pg <- dbConnect(drv, dbname="crsp")

# The name of the local directory where filings are stored.
raw_directory <- "/Volumes/2TB/data/"

# Pull together a list of all N-PX filings on EDGAR
file.list <- dbGetQuery(pg, "
    SELECT * FROM filings.filings AS b
    WHERE form_type IN ('10-K', '10-KT')")

# Function to download header (SGML) files associated with a filing.
# Most of the work is in parsing the name of the text filing and transforming 
# that into the URL of the SGML file.
get_sgml_file <- function(path) {
  directory <- raw_directory
  
  if (is.na(path)) return(NA)
  
  # The remote SGML file to be downloaded. Note that SGML files used to be 
  # found in the directory for the firm, but now go in a sub-directory.
  # The code below looks in both places.
  sgml_basename <- basename(gsub(".txt$", ".hdr.sgml", path, perl=TRUE))
  sgml_path <- file.path(dirname(path), 
                         gsub("(-|\\.hdr\\.sgml$)", "", 
                              sgml_basename, perl=TRUE))
  sgml_path_old <- file.path(dirname(path), sgml_basename)
  ftp <- file.path("http://www.sec.gov/Archives", sgml_path, sgml_basename)
  
  ftp_old <- file.path("http://www.sec.gov/Archives", sgml_path_old,
                       sgml_basename)
  
  # The local filename for the SGML file
  
  local_filename <- file.path(directory, sgml_path, sgml_basename)
  local_filename_old <- file.path(directory, sgml_path_old, sgml_basename)    
  
  # Skip if we already have the file in the "new" location
  if (file.exists(local_filename)) {
    return(file.path(sgml_path, sgml_basename))
  } else if (class(con <- try(url(ftp, open="rb")))[1]=="try-error") {
    # If there's no file on the SEC site in the "new" location,
    # try the "old" location
    dir.create(dirname(local_filename_old), showWarnings=FALSE, recursive=TRUE)
    if (!file.exists(local_filename_old)) {
      old <- try(download.file(url=ftp_old, destfile=local_filename_old))
      if (old==0) {
        return(file.path(sgml_path_old, sgml_basename))
      } else {
        return(NA)
      }
    } else { 
      return(file.path(sgml_path_old, sgml_basename))
    }
  } else {
    # Download the file from the "new" location
    dir.create(dirname(local_filename), showWarnings=FALSE, recursive=TRUE)
    new <- try(download.file(url=ftp, destfile=local_filename))
    if (new==0) {
      return(file.path(sgml_path, sgml_basename))
    }
    close(con)
    return(NA)
  } 
}

# Now, pull SGMLs for each filing
file.list$sgml_file <- NA
to.get <- 1:length(file.list$sgml_file) #

library(parallel)
# Get the file
system.time({
  file.list$sgml_file[to.get] <- 
    unlist(mclapply(file.list$file_name[to.get], get_sgml_file,
                    mc.preschedule=FALSE, mc.cores=6))
})

parseSGMLfile <- function(sgml_file, field="<PERIOD>") {
  
  con <- file(file.path(raw_directory, sgml_file), "r", blocking = FALSE)
  
  text <- readLines(con)
  
  value <- text[grep(paste("^", field, sep=""), text, perl=TRUE)]
  if(length(value)==0) {
    close(con)
    return(NA)
  }
  value <- gsub(paste("^", field, sep=""), "", value, perl=TRUE)
  close(con)
  return(value[[1]])
}

file.list$period <- NA
file.list$period <- 
    unlist(lapply(file.list$sgml_file, parseSGMLfile, field="<PERIOD>"))
file.list$period <- as.Date(file.list$period, format="%Y%m%d")

file.list$conformed_name <- NA
file.list$conformed_name <- 
  unlist(mclapply(file.list$sgml_file, parseSGMLfile, field="<CONFORMED-NAME>", mc.cores=12))

fyear <- function(date) {
  date <- as.Date(date)
  month <- as.integer(format(date, "%m"))
  year <- as.integer(format(date, "%Y"))
  fyear <- year - (month <= 5)
  return(fyear)
}

file.list$fyear <- fyear(file.list$period)

rs <- dbWriteTable(pg, c("filings", "filing_10k"), file.list, overwrite=TRUE, row.names=FALSE)

rm(file.list)
                           
matched <- dbGetQuery(pg, "
  SET work_mem='10GB';
  
  WITH compustat AS (
    SELECT gvkey, cik, conm, datadate
    FROM comp.funda
    INNER JOIN (SELECT DISTINCT gvkey, datadate FROM comp.secm) AS secm
    USING (gvkey, datadate)
    WHERE indfmt='INDL' AND datafmt='STD' AND popsrc='D' AND consol='C' 
      AND cik IS NOT NULL AND sale IS NOT NULL 
      AND datadate > '1999-12-31'
      AND fic='USA')
  SELECT *
    FROM compustat AS a
  LEFT JOIN filings.filing_10k AS b
  ON a.cik::integer=b.cik AND b.period 
    BETWEEN a.datadate AND a.datadate + interval '2 months'")


table(is.na(matched$date_filed), fyear(matched$datadate))

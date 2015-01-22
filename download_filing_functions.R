# The name of the local directory where filings are stored.
# Set this as an environment variable. In R:
# > Sys.setenv(EDGAR_DIR="/Volumes/2TB/data")
raw_directory <- Sys.getenv("EDGAR_DIR")

get_text_file <- function(path) {
  
  local_filename <- file.path(raw_directory, path)
  # Only download the file if we don't already have a local copy
  download.text <- function(path) {
    
    ftp <- file.path("http://www.sec.gov/Archives", path) 
    cat(dirname(local_filename), "\n")
    dir.create(dirname(local_filename), showWarnings=FALSE)        
    if (!file.exists(local_filename)) {
      try(download.file(url=ftp, destfile=local_filename))
    }
  }                      
  
  #     print(path[!file.exists(local_filename) & !is.na(path)])
  lapply(path[!file.exists(local_filename) & !is.na(path)],
         download.text)    
  
  # Return the local filename if the file exists
  return(file.exists(local_filename))
}

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
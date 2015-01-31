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

extract.filings <- function(file_path) {
## A function to extract filings from complete submission text files submitted
## to the SEC into the component files contained within them.
    require(XML)
    
    new_location <- Sys.getenv("EDGAR_DIR")
     
    # Parse the file as an XML file containing multiple documents
    webpage <- readLines(file_path)
     
    # Extract a list of file names from the complete text submission
    file.name <- gsub("<FILENAME>","", 
                      grep("<FILENAME>.*$", webpage,  perl=TRUE, value=TRUE))
    print(file.name)
    
    # If there are no file names, then the full text submission is simply a text file.
    # Rather than copying this to the new location, I just symlink it (this saves space).
    if (length(file.name)==0) { 
        return(file.name)
    } 
     
    # If got here, we have a full-text submission that isn't simply a text file
    # We need to make the parent directory for the component files that are 
    # embedded in the submission
    file.dir <- gsub("-(\\d{2})-(\\d{6})\\.txt$", "\\1\\2", file_path, perl=TRUE)
    print(file.dir)
    dir.create(file.dir, showWarnings=FALSE, recursive=TRUE)
     
    # Get a list of file names, and their start and end locations within the
    # text file. (I use unique file names, as sometimes--albeit rarely--the
    # filename is repeated).
    file.name <- unique(file.path(file.dir, file.name))
    start.line <- grep("<DOCUMENT>.*$", webpage,  perl=TRUE) 
    end.line <- grep("</DOCUMENT>.*$", webpage,  perl=TRUE)     
    print(file.name)
     
    for (i in 1:length(file.name)) {
        # Skip the file if it already exists and the extracted file was extracted 
        # recently.
        if(file.exists(file.name[i]) && 
            as.Date(file.info(file.name[i])$ctime) > "2012-02-15") {
            next
        }
         
        # Get the extension of the file to be extracted
        file.ext <- gsub(".*\\.(.*?)$", "\\1", file.name[i])
         
        # Extract binary files
        if (file.ext %in% c("zip", "xls", "jpg", "gif")) {
            temp <- webpage[start.line[i]:end.line[i]]
            pdf.start <- grep("^begin", temp,  perl=TRUE)
            pdf.end <- grep("^end", temp,  perl=TRUE)  
            t <- tempfile()
            writeLines(temp[pdf.start:pdf.end], con=t)
            print(paste("uudecode -o", file.name[i], t))
            system(paste("uudecode -o", file.name[i], t))
            unlink(t)
        }
         
        # Extract simple text files
        if (file.ext=="txt") {
            temp <- webpage[start.line[i]:end.line[i]]
            writeLines(temp, con=file.name[i])
        }
         
        # Extract text-based formatted file types
        if (file.ext %in% c("htm", "js", "css", "paper", "xsd")) {
            temp <- webpage[start.line[i]:end.line[i]]
            pdf.start <- grep("^<TEXT>", temp,  perl=TRUE) +1
            pdf.end <- grep("^</TEXT>", temp,  perl=TRUE) -1  
            t <- tempfile()
            writeLines(temp[pdf.start:pdf.end], con=file.name[i])
            unlink(t)
        }
         
        # Extract PDFs
        if (file.ext=="pdf") {
            temp <- webpage[start.line[i]:end.line[i]]
            pdf.start <- grep("^<PDF>", temp,  perl=TRUE) +1
            pdf.end <- grep("^</PDF>", temp,  perl=TRUE) -1  
            t <- tempfile()
            writeLines(temp[pdf.start:pdf.end], con=t)
            print(paste("uudecode -o", file.name[i], t))
            system(paste("uudecode -o", file.name[i], t))
            unlink(t)
        }
 
    }
    return(file.name)
}

html2txt <- function(file) {
    xpathApply(htmlParse(file, encoding="UTF-8"), "//body", xmlValue)[[1]] 
}

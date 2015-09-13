CREATE OR REPLACE FUNCTION get_edgar_dir_listing (file_name text) 
RETURNS text[] AS 
$$
    
    # Use FTP to get a list of documents associated with a filing.
    
    library("RCurl")
    # Convert URL to that of parent directory of filing documents
    url <- gsub("(\\d{10})-(\\d{2})-(\\d{6})\\.txt", "\\1\\2\\3", file_name) 
    
    # Use FTP to get a list of files
    ftp_url <- paste0("ftp.sec.gov/", url, "/")
    file.list <- unlist(strsplit(getURL(ftp_url, ftplistonly=TRUE, async=FALSE),
                                 "\n"))
    
    # Exclude complete submission text file from list of files for download
    text.file <- gsub("^.*\\/", "", file_name)
    file.path(url, setdiff(file.list, text.file))

$$ LANGUAGE plr;

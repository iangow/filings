# Utility functions ----
html2txt <- function(file) {
    library(XML)
    xpathApply(htmlParse(file, encoding="UTF-8"), "//body", xmlValue)[[1]]
}

apply_name_regex <- function(file_path, ignore.case=FALSE) {

    if (grepl(".html?$", file_path)) {
        # If HTML
        text <- html2txt(file_path)
    } else {
        # If text
        text <- paste(readLines(file_path), collapse="\n")
    }
    text <- substr(text, 1, 5000)

    post_phrases <-
        c("Exact name of registrant as specified in its charter",
          "Name of Registrant in Its Charter",
          "Name of Issuer in its charter",
          "(Exact )?name of (?:the )?registrant (?:issuer )?as specified in its charter")

    post_phrases <- gsub("\\s+", "\\\\s+", post_phrases)

    pre_regex <- "(?i)Commission\\s+(file\\s+)?(?:number|[Nn]o\\.):?\\s\\s*\\d(?:-|\\d){3,}(?-i)"
    post_regex <-
        paste0("(?i)\\((?:", paste(post_phrases, collapse="|"), ")\\)(?-i)")

    pre_match <- regexpr(pre_regex, text, perl=TRUE)
    post_match <- regexpr(post_regex, text, perl=TRUE)

    if (pre_match !=-1 && post_match !=-1) {
        # Extract text between pre- and post text
        start <- pre_match[1] + attr(pre_match, "match.length")
        end <- post_match[1] - 1
        match <- substr(text, start, end)

        # Clean up match
        match <- gsub("(\\n|_)+", " ", match)
        match <- gsub("\\s{2,}", " ", match)
        match <- gsub("^\\s+", "", match)
        match <- gsub("\\s+$", "", match)
        return(match)
    } else {
        return(NA)
    }
}

get_file_list <- function(file_path) {

    # Function to get a list of the files associated with a filing

    # Files associated with a filing go in a directory with a related name
    root.path <- gsub("(\\d{10})-(\\d{2})-(\\d{6})\\.txt", "\\1\\2\\3", file_path)

    if (file.exists(root.path)) {
        # Currently, the code only looks for HTML and text files
        files <- list.files(path = root.path, pattern="(txt|htm|html)$",
                            full.names = TRUE)
    } else {
        # If there is no directory at root.path, there is just the single
        # complete text submission file
        files <- file_path
    }
    if (length(files)==0) return(file_path)

    return(files)
}

check_name_regex <- function(file_name) {
    path <- file.path(Sys.getenv("EDGAR_DIR"), file_name)

    files <- get_file_list(path)
    if (length(files)==0) return(NA)

    res <- mapply(apply_name_regex, files)
    res <- res[!is.na(res)]
    if (length(res)>=1) {
        return(as.character(res[1]))
    } else {
        return(NA)
    }
}

browseFiling <- function(file_name) {
    path <- gsub("(\\d{10})-(\\d{2})-(\\d{6})\\.txt", "\\1\\2\\3", file_name)
    url <- paste0("http://www.sec.gov/Archives/", path)
    browseURL(url)
}

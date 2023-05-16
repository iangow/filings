# Some functions to download comment letters ----
readPDF <- function(url) {

    # Download PDF
    library(curl)
    t <- tempfile()
    curl::curl_download(url, t)

    # Extract text from PDF (requires installation of a free program) and
    # remove pagebreaks from text
    text <- paste(system(paste("pdftotext", t, " - "), intern=TRUE), collapse="\n")
    gsub("\f", "\n", text)
}

readtxt <- function(url) {

    # Get text from URL
    # remove pagebreaks from text
    lines <- readLines(url)
    start_line <-  grep("<TEXT>" , lines) + 1
    end_line <-  grep("</TEXT>" , lines) - 1
    text <- paste(lines[start_line:end_line], collapse="\n")
    gsub("\f", "\n", text)
}

getCommentLetter <- function(file_name) {
    pdf_url <- file.path("http://www.sec.gov/Archives",
                     gsub("(\\d{10})-(\\d{2})-(\\d{6})\\.txt", "\\1\\2\\3", file_name),
                     "filename1.pdf")

    text_url <- file.path("http://www.sec.gov/Archives", file_name)

    # First try the URL for a PDF, then try for text
    tryCatch(text <- readPDF(pdf_url),
             error = function(cond) {
                 text <- readtxt(text_url)
             })
    return(text)
}

# Get a list of court orders filed by SEC ----
library(dplyr)

pg <- src_postgres()
filings <- tbl(pg, sql("SELECT * FROM filings.filings"))

com_ltrs <-
    filings %>%
    filter(form_type=="UPLOAD") %>%
    collect(n=100)

temp <- com_ltrs %>% collect() %>% .[1:10, ] %>% as.data.frame()
# Now, get the text of comment letters and save as an R file ----
com_ltrs$order_text <- unlist(lapply(com_ltrs$file_name, getCommentLetter))

com_ltrs <- com_ltrs %>% mutate(text = getCommentLetter(file_name))


# Put text data into my database ----
library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())

dbWriteTable(pg, c("filings", "comment_letters"), com_ltrs,
             overwrite=TRUE, row.names=FALSE)

rs <- dbDisconnect(pg)

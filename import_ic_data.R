ic_data <- read.csv("http://www.sec.gov/open/datasets/investment_company_series_class.csv", as.is=TRUE)

library(RPostgreSQL)
drv <- dbDriver("PostgreSQL")
pg <- dbConnect(drv, dbname = "crsp")
names(ic_data) <- tolower(names(ic_data))
rs <- dbWriteTable(pg, c("filings", "ic_data"), ic_data, overwrite=TRUE, row.names=FALSE)

rs <- dbDisconnect(pg)


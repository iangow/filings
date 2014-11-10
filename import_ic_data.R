# Get data from SEC website
ic_data <- read.csv("http://www.sec.gov/open/datasets/investment_company_series_class.csv",
                    as.is=TRUE)

# Drop empty variables
for (var in names(ic_data)) {
    if (sum(is.na(ic_data[, var]))==length(ic_data[, var])) {
        ic_data[, var] <- NULL
    }
}

# Drop extra variable
ic_data$X.1 <- NULL

# Push data to PostgreSQL databas
library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())
names(ic_data) <- tolower(names(ic_data))
rs <- dbWriteTable(pg, c("filings", "ic_data"), ic_data, overwrite=TRUE, row.names=FALSE)

rs <- dbDisconnect(pg)

# Clean up
rm(ic_data, var, rs, pg)
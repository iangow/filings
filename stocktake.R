sql <- paste(readLines("filings/stocktake.sql"), collapse="\n")

# Get data for local database
Sys.setenv(PGHOST="localhost", PGDATABASE="crsp")
pg <- dbConnect(PostgreSQL())
local <- dbGetQuery(pg, sql)
rs <- dbDisconnect(pg)

# Get data for database on iangow.me
Sys.setenv(PGHOST="iangow.me", PGDATABASE="crsp")
pg <- dbConnect(PostgreSQL())
remote <- dbGetQuery(pg, sql)
rs <- dbDisconnect(pg)

# Compare two versions of filings data
stocktake <- merge(local, remote, 
                   by=c("year", "quarter"), 
                   suffixes = c("_local", "_remote"))
subset(stocktake, count_local != count_remote)
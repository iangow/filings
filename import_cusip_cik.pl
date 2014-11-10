#!/usr/bin/env perl
use DBI;
use POSIX qw(strftime);

$dbname = "crsp";
my $dbh = DBI->connect("dbi:Pg:dbname=$dbname", 'igow')	
	or die "Cannot connect: " . $DBI::errstr;

$sql = "
  -- CREATE SCHEMA filings;

  DROP TABLE IF EXISTS filings.cusip_cik;
  CREATE TABLE filings.cusip_cik
(
  file_name text,
  cusip text,
  cik integer, 
  company_name text,
  format text);
";

# Run SQL to create the table
$dbh->do($sql);

# Use PostgreSQL's COPY function to get data into the database
for ($i=1; $i<=6; $i++) {
  $time = localtime; 
  $now_string = strftime "%a %b %e %H:%M:%S %Y", localtime;
  $filename = "/Users/igow/Dropbox/data/filings/cusip_cik_" . $i . ".csv.gz";
  printf "Beginning import of $filename at $now_string\n";  

  $cmd  = "gunzip -c \"$filename\""; # | sed 's/\\\"//g'  ";
  $cmd .=  "| psql -d $dbname -c \"COPY filings.cusip_cik FROM STDIN CSV HEADER \";";
  print "$cmd\n";
  $result = system($cmd);
  print "Result of system command: $result\n";

  $now_string = strftime "%a %b %e %H:%M:%S %Y", localtime;
  printf "Completed import of $filename at $now_string\n"; 
}
# Fix permissions and set up indexes


$sql = "
    SET maintenance_work_mem='10GB';
    CREATE INDEX ON filings.cusip_cik (cusip);";
$dbh->do($sql);

$dbh->disconnect();

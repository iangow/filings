#!/usr/bin/env perl
use DBI;
use HTML::Strip;
use File::stat;

$path_to_edgar = "/Volumes/2TB/data/";
$dbname = "crsp";

# connect
my $dbh = DBI->connect("dbi:Pg:dbname=$dbname", 'igow')	
	or die "Cannot connect: " . $DBI::errstr;
#  
# Get the list of filings
my $sth = $dbh->prepare("
  SET work_mem='10GB';
  
  SELECT file_name
  FROM filings.filings 
  WHERE form_type IN ('SC 13G', 'SC 13G/A', 'SC 13D', 'SC 13D/A')
  EXCEPT 
  SELECT file_name 
  FROM filings.cusip_cik
  ORDER BY file_name
");

$sth->execute();

# Output header row
print "file_name,cusip,cik,co_name,format\n";

# iterate through resultset
# Open each file and extract the contents into a string $lines
while(my $ref = $sth->fetchrow_hashref()) {

  # Reset the variables
  $cusip=""; $cik=""; $co_name=""; $format="";
  
  # Get the file name
  $file = $ref->{'file_name'};
	$full_path = $path_to_edgar . '/' .$file;

  # Skip if there is no file or if the file is over 1MB
  unless (-e $full_path) {
    next;
  } 
  my $filesize = stat($full_path)->size;
  if ($filesize > 1000000) {
    next;
  }

  # Open the SEC text filing
  open(my $fh, "<", $full_path) or die "$0: can't open $full_path: $!";
  my $lines = join '', <$fh>;
  
  # Strip out HTML tags
  my $hs = HTML::Strip->new();
  my $lines = $hs->parse( $lines );
  $hs->eof;

  # Regular expressions
  $cusip_hdr = 'CUSIP\s+(?:No\.|#|Number):?';
  $cusip_fmt = '[0-9A-Z]{1,3}[\s-]?[0-9A-Z]{3}[\s-]?[0-9A-Z]{2}[\s-]?\d{1}';

  if ($lines =~ /$cusip_hdr\s+($cusip_fmt)/si) {
    # Format A:
    # CUSIP No. (or CUSIP #) followed by seven- to nine-character CUSIP
    $format= "A";
    $cusip = $1;
    $cusip =~ s/[\s-]//g;

  } elsif ($lines =~ /($cusip_fmt)\s+(?:[_-]{9,})?\s*\(CUSIP Number\)/si) {
    # Format D:
    # CUSIP followed by "CUSIP Number" perhaps with a row of underscores
    # between.
    #                    808513-10-5
    #                   (CUSIP Number) 
    $format = "D";
    $cusip = $1;
    $cusip =~ s/[)(\.\s-]//g;
  }

  # Close the full-text filing
  close($fh);

  # Now get data from the SGML header file.
  $sgml_file = $full_path;

  # Use a regular expression to locate the SGML header file
  $sgml_file =~ s/(\d{10})-(\d{2})-(\d{6})\.txt/$1$2$3\/$1-$2-$3.hdr.sgml/g;

  # Skip if there is no file
  unless (-e $sgml_file) {
    next;
  } 
  
  # Open the SGML header file and join its text
  open(my $fh, "<", $sgml_file) or die "$0: can't open $sgml_file: $!";
  my $lines = join '', <$fh>;
  
  # Get the portion related the SUBJECT COMPANY
  if ($lines =~ /<SUBJECT-COMPANY>(.*)<\/SUBJECT-COMPANY>/s) {
    $sub_co_text = $1;
    
    # Get the name ...
    if ($sub_co_text =~ /<CONFORMED-NAME>(.*)/) {
      $co_name = $1;
    }

    # ... and CIK
    if ($sub_co_text =~ /<CIK>(.*)/) {
      $cik = $1;
    } 
 
  }   

  # Close the SGML header file 
  close($fh);

  # Output the result
  print "$file,$cusip,$cik,\"$co_name\",$format\n";
        
}

# clean up
$dbh->disconnect();



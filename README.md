# S3 Bucket Analysis with Python

## Usage

***Warning this does output files to your /tmp directory***

This script assumes that you have aws credentials in an aws credentials file per AWS' documentation
http://docs.aws.amazon.com/cli/latest/userguide/cli-config-files.html

* ``` sh python s3_analysis.py --creds ~/.aws/credentials --exclude-bucket fmds captures cloudtrail logs ttdiag atlas --files 10 --sort oldest```

### Optional Arguments:
* -o, --stdout
  * Option to have logging go to stdout
* -f
  * Desired name of log of the execution
* --log-dir
  * Desire directory for placement of log file
* --account-profile
  * Optional name of account profile to use
* --creds
  * Location of an AWS credentials file
* --exclude-bucket
  * Space separated list of buckets to exclude
* --files
  * INT for number of files to display default to 20
* --include-bucket
  * Space separated list of buckets to include
* --size-format
  * Provide the the format that you would like the size information displayed bytes, KB, MB, GB or TB
* --sort
  * Sorting order either "newest" or "oldest"
* --output
  * desired output format screen or csv/tab for redirect.  No JSON :(
* --workers
  * An attempt at multithreading that failed miserably when I went to using awscli for getting the object data
* --slower
  * A flag to revert back to using object pagination w/ multithreading to and ALL THE SWAPPING

#!/usr/bin/env python2.7
import os
import sys
import datetime
import boto3
import argparse
import logging
from bucket import Bucket
from my_thread import ThreadPool

class report(object):
    def __init__(self, logger, creds_file='~/.aws/credentials', exclude_b=None, include_b=None, file_count=None, sort_order="newest", workers=10, acct_profile=None):
        self.logger = logger
        self.account_profiles = []
        self.creds_file = creds_file
        self.acct_profile  = acct_profile
        self.include = include_b
        self.exclude = exclude_b
        self.sort_order = sort_order
        self.workers = workers
        self.bucket_list = []

        if self.acct_profile:
            self.logger.info("Verifying that {} profile can be found in {}".format(self.acct_profile, creds_file))
            self.verify_account_profile()
            self.get_bucket_list()
        else:
            self.logger.info("Finding all account profiles in {}.".format(creds_file))
            self.get_bucket_list(self.get_account_profiles())

    def get_account_profiles(self):
        if '~/' in self.creds_file:
            full_path = os.path.expanduser(self.creds_file)
        else:
            full_path = os.path.abspath(self.creds_file)

        self.logger.info("Full path for credentials file is {}".format(full_path))

        if os.path.exists(full_path):
            os.environ['AWS_SHARED_CREDENTIALS_FILE'] = full_path
            with open(full_path, "r") as f:
                contents = f.read()
            for line in contents.split('\n'):
                if line.strip().startswith('[') and line.strip().endswith(']'):
                    tmp = line.replace('[', '').replace(']', '').strip()
                    logger.info("From {} extracted account profile named {}".format(line, tmp))
                    self.account_profiles.append(tmp)
        else:
            sys.exit("Unable to locate the provided creds file. Please verify that it exists and that you have permissions to read.")

        return self.account_profiles

    def get_buckets(self, name, creation, session, sort_order):
        self.bucket_list.append(Bucket(name, creation, session, sort_order, self.logger))

    def get_bucket_list(self, account_profiles):

        pool = ThreadPool(self.workers)
        for profile in account_profiles:
            buckets = []
            session = boto3.Session(profile_name=profile)
            s3_client = session.client('s3')
            bucket_list = s3_client.list_buckets()['Buckets']

            if self.exclude != None:
                for e in self.exclude:
                    logger.info("Excluding buckets named {}".format(e))
                    for bd in bucket_list:
                        if e in bd['Name']:
                            logger.info("Found and excluding bucket {}.".format(bd['Name']))
                            bucket_list.remove(bd)

            if self.include != None:
                for i in self.include:
                    self.logger.info("Including any buckets matching {} from account {}.".format(i,profile))
                    for bd in bucket_list:
                        if i in bd['Name']:
                            self.logger.info("Found and including bucket {}".format(bd['Name']))
                            buckets.append({
                                    'Name': bd['Name'],
                                    'CreationDate': bd['CreationDate']
                                    },)
                for bucket in buckets:
                    pool.add_task(self.get_buckets(bucket['Name'],bucket['CreationDate'],session,self.sort_order))
            else:
                for bd in bucket_list:
                    self.logger.info("Including bucket {}".format(bd['Name']))
                    pool.add_task(self.get_buckets(bd['Name'], bd['CreationDate'], session, self.sort_order))
        pool.wait_completion()

    def verify_account_profile(self):
        found_match = False
        if '~/' in self.creds_file:
            full_path = os.path.expanduser(self.creds_file)
        else:
            full_path = os.path.abspath(self.creds_file)

        self.logger.info("Full path for credentials file is {}".format(full_path))

        if os.path.exists(full_path):
            os.environ['AWS_SHARED_CREDENTIALS_FILE'] = full_path
            with open(full_path, "r") as f:
                contents = f.read()
            for line in contents.split('\n'):
                if self.acct_profile in line:
                    found_match = True
        if found_match:
            return True
        else:
            sys.exit("Unable to find profile {} in {}".format(self.acct_profile, self.creds_file))

def analysis_log(log_file):
    logger = logging.getLogger('s3_analysis')
    logFileHandler = logging.FileHandler(log_file)
    logFormatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
    logFileHandler.setFormatter(logFormatter)
    logger.addHandler(logFileHandler)


def analysis_logs_out():
    logger = logging.getLogger('s3_analysis')
    logFormatter = logging.Formatter('%(levelname)s: %(message)s')
    logStreamHandler = logging.StreamHandler()
    logStreamHandler.setFormatter(logFormatter)
    logger.addHandler(logStreamHandler)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="This is used to get that bucket info",
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    # General arguments
    parser.add_argument("-o", "--stdout", action="store_true", default=False,
                        help="Verbose output")
    parser.add_argument("-f", "--filename", action="store", default="s3_analysis.log",
                        help="The log output will go to /tmp/s3_analysis.log")
    parser.add_argument('--log-dir', action='store', default='/tmp/',
                        help=("Directory for log to be created"))

    # Script actions
    parser.add_argument("--acct-profile", action="store", default = None,
                        help="If you have multiple accounts in your creds file and only want to get ")
    parser.add_argument("--creds", action="store", default="~/.aws/credentials",
                        help="Input the location of your AWS credentials file.  See http://docs.aws.amazon.com/cli/latest/userguide/cli-config-files.html for file formatting information.")
    parser.add_argument("--exclude-bucket", action="store", default=None, nargs='+',
                        help="Space seperated list of buckets to exclude")
    parser.add_argument("--files", action="store", default=None,
                        help="Number of files to list")
    parser.add_argument("--include-bucket", action="store", default=None, nargs='+',
                        help="Space seperated list of buckets to include")
    parser.add_argument("--size-format", action="store", default='MB',
                        help="Provide the the format that you would like the size information displayed")
    parser.add_argument("--sort", action="store", default='newest',
                        help="Sorting order either \"newest\" or \"oldest\"")
    parser.add_argument("--output", action="store", default='screen',
                        help="Your desired output type either \"screen\", \"csv\", or \"tab\"")
    parser.add_argument("--workers", action="store", default=10,
                        help="Your desired amount of workers to speed up the process")

    args = parser.parse_args()
    analysis_log(args.log_dir + args.filename)
    logger = logging.getLogger('s3_analysis')
    logger.setLevel(logging.INFO)

    if args.stdout:
        analysis_logs_out()
    if args.files:
        try:
            num_of_files = int(args.files)
        except:
            sys.exit("I can't cast your number of files to an int, please review your input")
    if args.output == 'tab':
        delim = '\t'
    elif args.output == 'csv':
        delim = ','
    else:
        delim = '\t\t'

    if args.sort != 'oldest' and args.sort != 'newest':
        sys.exit("I don't think I can sort by {} please enter \"oldest\" or \"newest\"".format(args.sort))

    if args.include_bucket and args.exclude_bucket:
        if args.include_bucket == args.exclude_bucket:
            sys.exit("You're including and excluding the same buckets. Nothing to do here.")
        for i in args.include_bucket:
            if i in args.exclude_bucket:
                sys.exit("You're including and excluding some of the same buckets. Maybe give your lists another look.")

    if args.size_format == 'bytes':
        div = 1.0
    elif args.size_format == 'KB':
        div = 1000.0
    elif args.size_format == 'MB':
        div = 1000000.0
    elif args.size_format == 'GB':
        div = 1000000000.0
    elif args.size_format == 'TB':
        div = 1000000000000.0
    else:
        sys.exit("Sorry I can't support that size format.  Please check your input either that or you just need to cool it with the data storage, homie.")

    if args.workers:
        try:
           workers = int(args.workers)
        except:
            sys.exit("I can't cast your number of files to an int, please review your input")

    s3_report = report(
        logger,
        args.creds,
        args.exclude_bucket,
        args.include_bucket,
        args.size_format,
        args.sort,
        workers,
        args.acct_profile)

    get_last_modified = lambda obj: obj.creation_date
    if args.sort == 'oldest':
        sorted_buckets = sorted(s3_report.bucket_list, key=get_last_modified, reverse=True)
    else:
        sorted_buckets = sorted(s3_report.bucket_list, key=get_last_modified)

    for bucket in sorted_buckets:
        print "------------------------------------------------------------------------------------------------------------------------------------"
        print bucket.bucket_name," ",bucket.total_file_size/div,args.size_format," ",datetime.datetime.strftime(bucket.last_modified, '%m-%d-%Y %H:%M:%S')," ",bucket.message
        print "------------------------------------------------------------------------------------------------------------------------------------"
        if args.files:
            for index, key in enumerate(bucket.sorted_keys):
                print bucket.bucket_name,delim,key.key,delim,datetime.datetime.strftime(key.last_modified, '%m-%d-%Y %H:%M:%S'),delim,key.size/div,args.size_format
                if index == num_of_files-1:
                    break
        else:
            for key in bucket.sorted_keys:
                print bucket.bucket_name,delim,key.key,delim,datetime.datetime.strftime(key.last_modified, '%m-%d-%Y %H:%M:%S'),delim,key.size/div,args.size_format

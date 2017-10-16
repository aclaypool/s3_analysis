#!/usr/bin/env python2.7
import os
import sys
import datetime
import boto3
import argparse
import logging


class report(object):
    def __init__(self, logger, creds_file='~/.aws/credentials', exclude_b=None, include_b=None, file_count=None, sort_order="newest"):
        self.include = include_b
        self.exclude = exclude_b
        self.sort_order = sort_order
        self.bucket_list = self.get_bucket_list(self.get_account_profiles(creds_file))


    class bucket(object):
        def __init__(self, bucket_name, create_date, session_obj, sort_order):
            self.session = session_obj
            self.bucket_name = bucket_name
            self.creation_date = create_date
            try:
                self.bucket_region = session_obj.client('s3').get_bucket_location(Bucket=bucket_name)['LocationConstraint']
            except Exception,e:
                logger.error("Unable to get bucket location from bucket {} with Error: {}".format(self.bucket_name,e))
                self.bucket_region = 'Unknown'
                self.last_modified = self.creation_date
                self.total_file_size = 0
                self.file_count = 0
                self.sorted_keys = []
                self.message = "Unknown you may be unable to list contents of this bucket."
            self.sort_order = sort_order
            self.all_keys = self.get_all_keys()
            if self.all_keys == None:
                self.last_modified = self.creation_date
                self.total_file_size = 0
                self.file_count = 0
                self.sorted_keys = []
                self.message = "Unknown you may be unable to list contents of this bucket."
            else:
                self.last_modified, self.total_file_size, self.file_count, self.sorted_keys = self.get_details(self.all_keys)
                self.message = ""
            if not self.last_modified:
                self.last_modified = self.creation_date
            logger.info("Bucket {} was created on {} and was last modified on {}.\nIt has {} files totaling {} bytes.".format(
                    self.bucket_name,
                    datetime.datetime.strftime(self.creation_date, '%m-%d-%Y'),
                    datetime.datetime.strftime(self.last_modified, '%m-%d-%Y'),
                    self.file_count,self.total_file_size))


        def get_all_keys(self):
            keys = []
            try:
                paginator = self.session.client('s3',region_name=self.bucket_region).get_paginator('list_objects')
                page_iterator = paginator.paginate(Bucket=self.bucket_name)
                for page in page_iterator:
                    contents = page.get('Contents',None)
                    if contents:
                        for pk in contents:
                            keys.append({
                                'Key': pk['Key'],
                                'LastModified': pk['LastModified'],
                                'Size': pk['Size']
                                },)
                    else:
                        logger.info("{} is an empty Bucket and will be Excluded".format(self.bucket_name))
            except Exception,e:
                logger.error("Unable to list keys from bucket {} with Error: {}".format(self.bucket_name,e))
                return None
            return keys

        def get_details(self, all_keys):
            total_size = 0
            file_count = len(all_keys)
            get_last_modified = lambda obj: obj['LastModified']

            oldest = sorted(all_keys, key=get_last_modified)
            newest = sorted(all_keys, key=get_last_modified, reverse=True)

            if self.sort_order == 'oldest':
                sorted_keys = oldest
            else:
                sorted_keys = newest

            if file_count > 0:
                key_lm = newest[0]['LastModified']
                for key in all_keys:
                    total_size += key['Size']
            else:
                key_lm = None

            return key_lm, total_size, file_count, sorted_keys



    def get_account_profiles(self, creds_file):
        account_profiles = []
        if '~/' in creds_file:
            full_path = os.path.expanduser(creds_file)
        else:
            full_path = os.path.abspath(creds_file)

        logger.info("Full path for credentials file is {}".format(full_path))

        if os.path.exists(full_path):
            os.environ['AWS_SHARED_CREDENTIALS_FILE'] = full_path
            with open(full_path, "r") as f:
                contents = f.read()
            for line in contents.split('\n'):
                if '[' in line:
                    tmp_profile = line.replace('[', '').replace(']', '')
                    logger.info("From {} extracted account profile named {}".format(line, tmp_profile))
                    account_profiles.append(tmp_profile)
        else:
            sys.exit("Unable to locate the provided creds file. Please verify that it exists and that you have permissions to read.")

        return account_profiles

    def get_bucket_list(self, account_profiles):
        buckets = []
        for profile in account_profiles:
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
                    logger.info("Including buckets named {}".format(i))
                    for bd in bucket_list:
                        if i in bd['Name']:
                            logger.info("Found and including bucket {}".format(bd['Name']))
                            create_date = bd['CreationDate']
                            buckets.append(self.bucket(bd['Name'], create_date, session, self.sort_order))
            else:
                for bd in bucket_list:
                    logger.info("Including bucket {}".format(bd['Name']))
                    create_date = bd['CreationDate']
                    buckets.append(self.bucket(bd['Name'], create_date, session, self.sort_order))

        return buckets

        # def get_


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

    s3_report = report(
        logger,
        args.creds,
        args.exclude_bucket,
        args.include_bucket,
        args.size_format,
        args.sort)

    get_last_modified = lambda obj: obj.creation_date
    if args.sort == 'oldest':
        sorted_buckets = sorted(s3_report.bucket_list, key=get_last_modified, reverse=True)
    else:
        sorted_buckets = sorted(s3_report.bucket_list, key=get_last_modified)

    for bucket in sorted_buckets:
        print "------------------------------------------------------------------------------------------------------------------------------------"
        print bucket.bucket_name," ",bucket.total_file_size/div," ",datetime.datetime.strftime(bucket.last_modified, '%m-%d-%Y %H:%M:%S')," ",bucket.message
        print "------------------------------------------------------------------------------------------------------------------------------------"
        if args.files:
            for index, key in enumerate(bucket.sorted_keys):
                print bucket.bucket_name,delim,key['Key'],delim,datetime.datetime.strftime(key['LastModified'], '%m-%d-%Y %H:%M:%S'),delim,key['Size']/div
                if index == num_of_files-1:
                    break
        else:
            for key in bucket.sorted_keys:
                print bucket.bucket_name,delim,key['Key'],delim,datetime.datetime.strftime(key['LastModified'], '%m-%d-%Y %H:%M:%S'),delim,key['Size']/div

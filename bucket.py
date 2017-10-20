#!/usr/bin/env python2.7
import datetime
import os
import json


###  Class Bucket for storing information about each bucket ###
class Bucket(object):
    # Init with account profile from ~/.aws/credentials, bucket_name, create date, slow down flag, s3 session object, and logging
    def __init__(self, profile, bucket_name, create_date, slower, session_obj, sort_order,logger):
        self.logger = logger
        self.account_profile = profile
        self.slower = slower
        self.session = session_obj
        self.bucket_name = bucket_name
        self.creation_date = create_date
        self.sort_order = sort_order
        self.bucket_region = self.get_bucket_region()
        self.all_keys, self.file_count, self.already_sorted = self.get_all_keys()

        # Handling for possibility of empty buckets or access denied buckets
        if self.all_keys == None:
            self.last_modified = self.creation_date
            self.total_file_size = 0
            self.file_count = 0
            self.sorted_keys = []
            self.message = "Unknown you may not have permissions to the contents of this bucket."
        else:
            self.last_modified, self.total_file_size, self.sorted_keys = self.get_details()
            self.message = ""

        # If we can't get the last modified file setting last modified to creation date
        if not self.last_modified:
            self.last_modified = self.creation_date

        # The out to file and loading from json destroys the datetime object so recreating
        if isinstance(self.last_modified,unicode):
            self.last_modified = datetime.datetime.strptime(self.last_modified,'%Y-%m-%dT%H:%M:%S.%fZ')

        self.logger.info("Bucket {} was created on {} and was last modified on {}.\nIt has {} files totaling {} bytes.".format(
                self.bucket_name,
                datetime.datetime.strftime(self.creation_date, '%m-%d-%Y'),
                datetime.datetime.strftime(self.last_modified, '%m-%d-%Y'),
                self.file_count,self.total_file_size))

    # Get all keys used to get all keys in the bucket for sorting Pagination was slow and awscli s3api was at least half the time.
    def get_all_keys(self):
        keys = []
        count = 0
        already_sorted = False

        # Built this in if the awscli was not installed and I couldn't install it
        if self.slower == True:
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
                    self.logger.info("{} is an empty Bucket and will be Excluded".format(self.bucket_name))
        else:
            # Building commands to use awscli for speed and system memory concerns
            already_sorted = True
            if self.sort_order == 'newest':
                cmd = 'aws s3api list-objects-v2 --bucket \''+self.bucket_name+'\' --query "reverse(sort_by(Contents, &LastModified))[*].{Key:Key,Size:Size,LastModified:LastModified}" --profile '+self.account_profile+' > /tmp/'+self.bucket_name+'.out'
            else:
                cmd = 'aws s3api list-objects-v2 --bucket \''+self.bucket_name+'\' --query "sort_by(Contents, &LastModified)[*].{Key:Key,Size:Size,LastModified:LastModified}" --profile '+self.account_profile+' > /tmp/'+self.bucket_name+'.out'
            self.logger.info(cmd)
            try:
                # Exception needed in the event of empty or permission denied buckets
                os.popen(cmd,'r')

                if os.path.exists('/tmp/{}.out'.format(self.bucket_name)):
                    with open('/tmp/{}.out'.format(self.bucket_name),'r') as f:
                        keys = f.read()
                        keys = json.loads(keys)
                count = len(keys)
            except Exception, e:
                self.logger.info(e)

        return keys, count, already_sorted

    # Return bucket region.  Exception needed in event of denied permissions
    def get_bucket_region(self):
        try:
            bucket_region = self.session.client('s3').get_bucket_location(Bucket=self.bucket_name)['LocationConstraint']
        except Exception,e:
            self.logger.error("Unable to get bucket location from bucket {} with Error: {}".format(self.bucket_name,e))
            bucket_region = None
        return bucket_region

    # Returning the details about the keys themselves to the Class
    def get_details(self):
        total_size = 0
        get_last_modified = lambda obj: obj['LastModified']
        lm_key = 0

        if self.already_sorted == False:
            self.logger.info("Time Before Sort: {}".format(datetime.datetime.strftime(datetime.datetime.now(), '%H:%M:%S')))

            if self.sort_order == 'oldest':
                sorted_keys = sorted(self.all_keys, key=get_last_modified)
                lm_key = len(self.all_keys)-1
            else:
                sorted_keys = sorted(self.all_keys, key=get_last_modified, reverse=True)

                self.logger.info("Time After Sort Before Size Calc: {}".format(datetime.datetime.strftime(datetime.datetime.now(), '%H:%M:%S')))

            if len(self.all_keys) > 0:
                key_lm = sorted_keys[lm_key]['LastModified']
                for key in self.all_keys:
                    total_size += key['Size']
            else:
                key_lm = None
            self.logger.info("Time After Size Calc: {}".format(datetime.datetime.strftime(datetime.datetime.now(), '%H:%M:%S')))
            return key_lm, total_size, sorted_keys
        else:
            if len(self.all_keys) > 0:
                key_lm = self.all_keys[lm_key]['LastModified']
                for key in self.all_keys:
                        total_size += key['Size']
            else:
                key_lm = None
            return key_lm, total_size, self.all_keys

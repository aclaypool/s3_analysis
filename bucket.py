#!/usr/bin/env python2.7
import datetime
import os
from my_thread import ThreadPool

class Bucket(object):
    def __init__(self, bucket_name, create_date, session_obj, sort_order,logger):
        self.logger = logger
        self.session = session_obj
        self.bucket_name = bucket_name
        self.creation_date = create_date
        self.sort_order = sort_order
        self.bucket_region = self.get_bucket_region()
        self.all_keys, self.file_count = self.get_all_keys()

        if self.all_keys == None:
            self.last_modified = self.creation_date
            self.total_file_size = 0
            self.file_count = 0
            self.sorted_keys = []
            self.message = "Unknown you may not have permissions to the contents of this bucket."
        else:
            self.last_modified, self.total_file_size, self.sorted_keys = self.get_details()
            self.message = ""

        if not self.last_modified:
            self.last_modified = self.creation_date

        self.logger.info("Bucket {} was created on {} and was last modified on {}.\nIt has {} files totaling {} bytes.".format(
                self.bucket_name,
                datetime.datetime.strftime(self.creation_date, '%m-%d-%Y'),
                datetime.datetime.strftime(self.last_modified, '%m-%d-%Y'),
                self.file_count,self.total_file_size))

    def get_all_keys(self):
        keys = []
        count = 0
        self.logger.info("Time Before Keys: {}".format(datetime.datetime.strftime(datetime.datetime.now(), '%H:%M:%S')))
        try:
            keys = list(self.session.resource('s3',region_name=self.bucket_region).Bucket(self.bucket_name).objects.all())
            count = len(keys)
            self.logger.info("Time After Keys: {}".format(datetime.datetime.strftime(datetime.datetime.now(), '%H:%M:%S')))

        except Exception,e:
            self.logger.error("Unable to list keys from bucket {} with Error: {}".format(self.bucket_name,e))
            return None, 0
        return keys, count

    def get_bucket_region(self):
        try:
            bucket_region = self.session.client('s3').get_bucket_location(Bucket=self.bucket_name)['LocationConstraint']
        except Exception,e:
            self.logger.error("Unable to get bucket location from bucket {} with Error: {}".format(self.bucket_name,e))
            bucket_region = None
        return bucket_region

    def get_details(self):
        total_size = 0
        get_last_modified = lambda obj: obj.last_modified
        lm_key = 0

        self.logger.info("Time Before Sort: {}".format(datetime.datetime.strftime(datetime.datetime.now(), '%H:%M:%S')))

        if self.sort_order == 'oldest':
            sorted_keys = sorted(self.all_keys, key=get_last_modified)
            lm_key = len(self.all_keys)-1
        else:
            sorted_keys = sorted(self.all_keys, key=get_last_modified, reverse=True)

        self.logger.info("Time After Sort Before Size Calc: {}".format(datetime.datetime.strftime(datetime.datetime.now(), '%H:%M:%S')))

        if len(self.all_keys) > 0:
            key_lm = sorted_keys[lm_key].last_modified
            for key in self.all_keys:
                total_size += key.size

            self.logger.info("Time After Size Calc: {}".format(datetime.datetime.strftime(datetime.datetime.now(), '%H:%M:%S')))
        else:
            key_lm = None

        return key_lm, total_size, sorted_keys

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
        self.bucket_region = self.get_bucket_region()

    def get_bucket_region(self):
        try:
            bucket_region = self.session.client('s3').get_bucket_location(Bucket=self.bucket_name)['LocationConstraint']
        except Exception,e:
            self.logger.error("Unable to get bucket location from bucket {} with Error: {}".format(self.bucket_name,e))
            bucket_region = None
        return bucket_region

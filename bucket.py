#!/usr/bin/env python3
import datetime
import asyncio

class Bucket(object):
    def __init__(self):
        self.session
        self.bucket_name
        self.creation_date
        self.sort_order
        self.bucket_region
        self.all_keys
        self.last_modified
        self.total_file_size = 0
        self.file_count = 0
        self.sorted_keys = []
        self.message

    def async_create_bucket(coroutine):
        try:
            coroutine.send(None)
        except Exception as e:
            logger.error("Coroutine failed with erro {}".format(e))

    async def create_bucket(self, bucket_name, create_date, session_obj, sort_order,logger):
        self.session = session_obj
        self.bucket_name = bucket_name
        self.creation_date = create_date
        self.sort_order = sort_order
        self.bucket_region = self.get_bucket_region(self.bucket_name,logger)
        self.all_keys = self.get_all_keys(logger)

        if self.all_keys == None:
            self.last_modified = self.creation_date
            self.total_file_size = 0
            self.file_count = 0
            self.sorted_keys = []
            self.message = "Unknown you may not have permissions to the contents of this bucket."
        else:
            self.last_modified, self.total_file_size, self.file_count, self.sorted_keys = self.get_details(self.all_keys, logger)
            self.message = ""

        if not self.last_modified:
            self.last_modified = self.creation_date

        logger.info("Bucket {} was created on {} and was last modified on {}.\nIt has {} files totaling {} bytes.".format(
                self.bucket_name,
                datetime.datetime.strftime(self.creation_date, '%m-%d-%Y'),
                datetime.datetime.strftime(self.last_modified, '%m-%d-%Y'),
                self.file_count,self.total_file_size))

    def get_all_keys(self, logger):
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
        except Exception as e:
            logger.error("Unable to list keys from bucket {} with Error: {}".format(self.bucket_name,e))
            return None
        return keys

    def get_bucket_region(self,bucket_name,logger):
        try:
            bucket_region = self.session.client('s3').get_bucket_location(Bucket=bucket_name)['LocationConstraint']
        except Exception as e:
            logger.error("Unable to get bucket location from bucket {} with Error: {}".format(self.bucket_name,e))
            bucket_region = None
        return bucket_region

    def get_details(self, all_keys, logger):
        total_size = 0
        file_count = len(all_keys)
        get_last_modified = lambda obj: obj['LastModified']
        lm_key = 0

        logger.info("Time Before Sort: {}".format(datetime.datetime.strftime(datetime.datetime.now(), '%H:%M:%S')))

        if self.sort_order == 'oldest':
            sorted_keys = sorted(all_keys, key=get_last_modified)
            lm_key = len(all_keys)-1
        else:
            sorted_keys = sorted(all_keys, key=get_last_modified, reverse=True)

        logger.info("Time After Sort Before Size Calc: {}".format(datetime.datetime.strftime(datetime.datetime.now(), '%H:%M:%S')))

        if len(all_keys) > 0:
            key_lm = sorted_keys[lm_key]['LastModified']
            for key in all_keys:
                total_size += key['Size']

            logger.info("Time After Size Calc: {}".format(datetime.datetime.strftime(datetime.datetime.now(), '%H:%M:%S')))
        else:
            key_lm = None

        return key_lm, total_size, file_count, sorted_keys

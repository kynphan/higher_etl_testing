#!/usr/bin/env python
#
#  Intra daily scheduled jobs for EDU Direct
#
from __future__ import print_function

import unittest
import datetime
import logmatic
import logging
import boto3
import json

from utilities import get_json_file, get_job_object, get_job_state, check_file_s3, get_date_folders, run_jobs

import os
import sys
sys.path.append('..')


class test_nightly_events(unittest.TestCase):

    def setUp(self):

        # access glue service
        self.glue = boto3.client(
            service_name='glue',
            region_name='us-east-1',
            endpoint_url='https://glue.us-east-1.amazonaws.com'
            )

        # Create CloudWatch client
        self.cloudwatch = boto3.client('cloudwatch')

        # access s3 storage
        self.s3 = boto3.resource('s3')

        # get json file for this test suite 
        self.json_results = {}

        # define the jobs list, including initial params
        self.job_list = {
            'PlatformEvents_cap_info_to_parquet': {
                'args': {
                    '--TYPE': 'current_day'
                },
                'bucket': 'highereducation-dw-transformed-data',
                'initial_folders': ['PlatformEvents-current'],
                'tables': ['cap_info_csv'],
                'file_extension': 'parquet'
            },
            'PlatformEvents_to_parquet': {
                'args': {
                     '--MONTHS': '3',
                },
                'bucket': 'highereducation-dw-transformed-data',
                'initial_folders': ['PlatformEvents'],
                'tables': ['platform_events_public_normalized_events'],
                'date_partition': True,
                'file_extension': 'parquet'
            },
            'PlatformEvents_to_staging': {
                'args': {
                    '--TYPE': 'current_day',
                    '--ENVIRONMENT': 'dev'
                },
                'bucket': 'highereducation-dw-staging-data',
                'initial_folders': ['PlatformEvents', 'tmp'],
                'tables': ['fact_table_dev_current_v1'],
                'date_partition': True,
                'file_extension': 'parquet'
            },
            'PlatformEvents_prices': {
                'args': {
                    '--TYPE': 'current_day',
                    '--ENVIRONMENT': 'dev'
                },
                'bucket': 'highereducation-dw-staging-data',
                'initial_folders': ['PlatformEvents', 'dev'],
                'tables': [
                    'normalized_events_fact_table_dev_current'
                ],  
                'date_partition': True,
                'file_extension': 'parquet'
            },
        }

        # initialize logger
        self.logger = logging.getLogger()

        handler = logging.StreamHandler()
        handler.setFormatter(logmatic.JsonFormatter())

        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    def test_job_list(self):
        # get logger
        logger = logging.getLogger("test")
        self.json_results = run_jobs(self.glue, self.s3, self.job_list, self.json_results, logger)

        with open('results/night_events.json', 'w+') as outfile:
            json.dump(self.json_results, outfile)
        self.assertTrue(len(self.json_results) == len(self.job_list))


if __name__ == '__main__':
    print(unittest.main())

        
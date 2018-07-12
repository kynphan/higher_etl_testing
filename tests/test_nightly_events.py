#!/usr/bin/env python
#
#  Intra daily scheduled jobs for EDU Direct
#
from __future__ import print_function

import unittest
import time
import logging
import boto3
import json

from utilities import  get_json_file, get_job_object, get_job_state

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
        self.json_results = []

        # define the jobs list, including initial params
        self.job_list = {
            'PlatformEvents_cap_info_to_parquet': {
                'args': {
                    '--TYPE': 'current_day'
                }
            },
            'PlatformEvents_to_parquet': {
                'args': {}
            },
            'PlatformEvents_to_staging': {
                'args': {
                    '--TYPE': 'current_day',
                    '--ENVIRONMENT': 'dev'
                }
            },
            'PlatformEvents_prices': {
                'args': {
                    '--TYPE': 'current_day',
                    '--ENVIRONMENT': 'dev'
                }
            },
        }
        
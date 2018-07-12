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

class test_daily_edu(unittest.TestCase):

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
            'EDUDirect_to_parquet_current_day': {
                'args': {}
            },
            'EDUDirect_user_agent': {
                'args': {
                    '--TYPE': 'current_day'
                }
            },
            'EDUDirect_to_staging': {
                'args': {
                    '--TYPE': 'current_day',
                    '--ENVIRONMENT': 'dev',
                    '--START_DATE': '000',
                    '--END_DATE': '000',
                }
            },
            'EDUDirect_related_subject': {
                'args': {
                    '--TYPE': 'current_day',
                    '--ENVIRONMENT': 'dev',
                    '--START_DATE': '000',
                    '--END_DATE': '000',
                }
            },
        }
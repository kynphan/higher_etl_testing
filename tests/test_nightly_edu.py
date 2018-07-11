#!/usr/bin/env python
#
#  Nightly scheduled jobs for EDU Direct
#

import unittest
import boto3

import os
import sys
sys.path.append('..')  

class test_nightly_edu(unittest.TestCase):

    def setUp(self):

        # access glue service
        glue = boto3.client(
            service_name='glue',
            region_name='us-east-1',
            endpoint_url='https://glue.us-east-1.amazonaws.com'
            )

        # Create CloudWatch client
        cloudwatch = boto3.client('cloudwatch')

    def test_EDUDirect_to_parquet_last_N_months(self):
        job = glue.start_job_run( JobName=myJob['PlatformEvents_prices'] )

    unittest.main()

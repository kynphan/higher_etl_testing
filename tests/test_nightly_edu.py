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
        self.glue = boto3.client(
            service_name='glue',
            region_name='us-east-1',
            endpoint_url='https://glue.us-east-1.amazonaws.com'
            )

        # Create CloudWatch client
        cloudwatch = boto3.client('cloudwatch')

    def test_EDUDirect_to_parquet_last_N_months(self):
        job = self.glue.start_job_run( 
            JobName='PlatformEvents_prices',
            Arguments={
                '--ENVIRONMENT': 'dev'
                }
        )
        status = self.glue.get_job_run( JobName='PlatformEvents_prices' )

    unittest.main()

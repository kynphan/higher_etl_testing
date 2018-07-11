#!/usr/bin/env python
#
#  Nightly scheduled jobs for EDU Direct
#
from __future__ import print_function

import unittest
import logging
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
        self.cloudwatch = boto3.client('cloudwatch')

        # access s3 storage
        s3 = boto3.resource('s3')

    def test_EDUDirect_to_parquet_replace(self):
        job = self.glue.start_job_run( 
            JobName='EDUDirect_to_parquet_replace',
        )
        status = self.glue.get_job_run( JobName='EDUDirect_to_parquet_last_N_months' )

    # https://github.com/aws-samples/aws-etl-orchestrator/blob/master/lambda/gluerunner/gluerunner.py
    def test_EDUDirect_to_parquet_last_N_months(self):
        # argument options
        envs = ['dev', 'staging', 'prod']

        response = self.glue.start_job_run( 
            JobName='EDUDirect_to_parquet_last_N_months',
            Arguments={
                '--ENVIRONMENT': 'dev'
            }
        )
        print(response)
        job_run_id =  response['JobRunId']
        job_run_state = response['JobRun']['JobRunState']
        job_run_error_message = response['JobRun'].get('ErrorMessage', '')

        status = self.glue.get_job_run( JobName='EDUDirect_to_parquet_last_N_months' )
        print(job_run_state, job_run_error_message)

        if job_run_state in ['SUCCEEDED']:
            print('success')


if __name__ == '__main__':
    print(unittest.main())

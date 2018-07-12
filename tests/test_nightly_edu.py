#!/usr/bin/env python
#
#  Nightly scheduled jobs for EDU Direct
#
from __future__ import print_function

import unittest
import logging
import boto3
import json

from utilities import  get_job_object, get_job_state

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
        self.s3 = boto3.resource('s3')

    def test_EDUDirect_to_parquet_replace(self):
        job_name = 'EDUDirect_to_parquet_replace'
        job = get_job_object(self.glue, job_name)
        status = get_job_state(self.glue, job_name, job)
        job_run_state = status['JobRunState']
        print(status, job_run_state)

    # https://github.com/aws-samples/aws-etl-orchestrator/blob/master/lambda/gluerunner/gluerunner.py
    def test_EDUDirect_to_parquet_last_N_months(self):
        # argument options
        args = {
            '--ENVIRONMENT': 'dev'
        }

        job_name = 'EDUDirect_to_parquet_last_N_months'
        job = get_job_object(self.glue, job_name , args)
        status = get_job_state(self.glue, job_name, job)
        job_run_state = status['JobRunState']
        print(status, job_run_state)

        if job_run_state in ['SUCCEEDED']:
            print('success')


if __name__ == '__main__':
    print(unittest.main())

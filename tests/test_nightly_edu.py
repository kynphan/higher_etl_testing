#!/usr/bin/env python
#
#  Nightly scheduled jobs for EDU Direct
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

        # get json file for this test suite 
        self.json_results = []

        # define the jobs list, including initial params
        self.job_list = {
            'EDUDirect_to_parquet_last_N_months': {
                'args': {
                    '--MONTHS': '3',
                    '--ALL_TABLES': 'False'
                }
            },
            'EDUDirect_to_parquet_replace': {
                'args': {}
            },
            'EDUDirect_to_parquet_new_snapshot': {
                'args': {}
            },
            'EDUDirect_to_parquet_current_dimensions': {
                'args': {}
            },
        }


    def test_job_list(self):
        pending_jobs = self.job_list
        while(len(pending_jobs)> 0):

            for job_name, job in pending_jobs.items():
                job_object = get_job_object(self.glue, job_name , job['args'])
                if job_object and job_object['JobRunId']:
                    status = get_job_state(self.glue, job_name, job_object)
                    job_status = status['JobRun']['JobRunState']
                    if job_status not in ['STARTING', 'RUNNING', 'STARTING', 'STOPPING']:
                        pending_jobs.pop(job_name, None)

                        #add info to the json
                        self.json_results[job_name] = {
                            'status': status['JobRunState'],
                            'execution_time': status['ExecutionTime']
                        }

            # wait 10 seconds before try to run jobs again
            time.sleep(10)

        print(self.json_results)


if __name__ == '__main__':
    print(unittest.main())

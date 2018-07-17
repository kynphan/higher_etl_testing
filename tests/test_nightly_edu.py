#!/usr/bin/env python
#
#  Nightly scheduled jobs for EDU Direct
#
from __future__ import print_function

import unittest
import time
import logmatic
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
        self.json_results = {}

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
            'EDUDirect_user_agent': {
                 'args': {
                     '--TYPE': 'historical',
                 }
            },
            'EDUDirect_nightly_dummy_user_agent': {
                 'args': {}
            },
            'EDUDirect_to_staging': {
                 'args': {
                     '--TYPE': 'historical',
                     '--ENVIRONMENT': 'dev',
                     '--START_DATE': '000',
                     '--END_DATE': '000',
                 }
            },
            'EDUDirect_related_subject': {
                 'args': {
                     '--TYPE': 'historical',
                     '--ENVIRONMENT': 'dev',
                     '--START_DATE': '000',
                     '--END_DATE': '000',
                 }
            },
        }

        # initialize logger
        self.logger = logging.getLogger()

        handler = logging.StreamHandler()
        handler.setFormatter(logmatic.JsonFormatter())

        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    def test_job_list(self):
        pending_jobs = self.job_list.copy()
        pending_jobs_to_start = self.job_list.copy()

        # get logger
        logger = logging.getLogger("test")
        while(len(pending_jobs_to_start)> 0):
            for job_name, job in pending_jobs_to_start.items():
                # have we already started this job?
                job_run_id = None
                if 'job_run_id' in job:
                    job_run_id = job['JobRunId']
                    pending_jobs[job_name]['JobRunId'] = job_run_id
                    del pending_jobs_to_start[job_name]
                else:
                    args = job['args'] if 'args' in job else {}
                    job_object = get_job_object(self.glue, job_name , args)
                    if job_object and 'JobRunId' in job_object:    
                        job_run_id = job_object['JobRunId']
                        pending_jobs[job_name]['JobRunId'] = job_run_id
                        del pending_jobs_to_start[job_name]

        
        while(len(pending_jobs)> 0):

            for job_name, job in pending_jobs.items():
                job_run_id = job['JobRunId']
                status = get_job_state(self.glue, job_name, job_run_id)
                job_status = status['JobRun']['JobRunState']
                if job_status not in ['STARTING', 'RUNNING', 'STARTING', 'STOPPING']:
                    # remove job from pending list
                    del pending_jobs[job_name]

                    #add info to the json
                    item = {
                        'status': status['JobRun']['JobRunState'],
                        'execution_time': status['JobRun']['ExecutionTime']
                    }
                    # save into json file
                    self.json_results[job_name] = item

                    # save results into logger
                    logger.info(job_name, extra=item)

            print(self.json_results)
            # wait 20 seconds before try to run jobs again
            time.sleep(20)

        self.assertTrue(len(self.json_results) == len(self.job_list))


if __name__ == '__main__':
    print(unittest.main())

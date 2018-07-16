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
        self.json_results = {}

        # define the jobs list, including initial params
        self.job_list = {
            'EDUDirect_to_parquet_last_N_months': {
                'args': {
                     '--MONTHS': '3',
                     '--ALL_TABLES': 'False'
                },
                'bucket' : 'highereducation-dw-transformed-data',
                'files': [
                    'platform_events_public_normalized_events'
                ]
            },
            'EDUDirect_to_parquet_replace': {
                'args': {},
                'bucket' : 'highereducation-dw-transformed-data',
                'files': [
                    'cddirect_production_lead_cap',
                    'cddirect_production_migration_versions',
                    'cddirect_production_school_campus_program',
                    'cddirect_production_school_criteria',
                    'cddirect_production_school_criteria_affiliate',
                    'cddirect_production_school_multilead_segment',
                    'cddirect_production_visitor_tag',
                    'cddirect_production_zip_state'
                ]
            },
             'EDUDirect_to_parquet_new_snapshot': {
                 'args': {},
                 'bucket': 'highereducation-dw-transformed-data',
                 'files': [
                    'cddirect_production_affiliate',
                    'cddirect_production_country',
                    'cddirect_production_education_level',
                    'cddirect_production_publisher',
                    'cddirect_production_school',
                    'cddirect_production_school_alias',
                    'cddirect_production_school_campus',
                    'cddirect_production_school_eligible_country',
                    'cddirect_production_school_eligible_state',
                    'cddirect_production_school_program',
                    'cddirect_production_school_program_ineligible_state',
                    'cddirect_production_school_provider',
                    'cddirect_production_school_provider_campus',
                    'cddirect_production_school_provider_cap',
                    'cddirect_production_school_provider_cap_program',
                    'cddirect_production_school_provider_cap_publisher',
                    'cddirect_production_school_provider_cap_state',
                    'cddirect_production_school_provider_category',
                    'cddirect_production_school_provider_education_level',
                    'cddirect_production_school_provider_leadid_flag',
                    'cddirect_production_school_provider_program',
                    'cddirect_production_school_publisher',
                    'cddirect_production_school_targus_score',
                    'cddirect_production_state',
                    'cddirect_production_tag',
                    'cddirect_production_targus_score',
                    'cddirect_production_user',
                    'cddirect_production_widget_category',
                    'cddirect_production_widget_degree',
                    'cddirect_production_widget_degree_recommendation',
                    'cddirect_production_widget_subject',
                    'cddirect_production_widget_subject_alias',
                    'cddirect_production_widget_subject_recommendation',
                    'form_position_csv'
                 ]
            },
            'EDUDirect_to_parquet_current_dimensions': {
                 'args': {},
                 'bucket': 'highereducation-dw-transformed-data',
                 'files': [
                    'cddirect_production_affiliate',
                    'cddirect_production_country',
                    'cddirect_production_lead_cap',
                    'cddirect_production_publisher',
                    'cddirect_production_school',
                    'cddirect_production_school_program',
                    'cddirect_production_school_provider',
                    'cddirect_production_school_provider_cap',
                    'cddirect_production_school_provider_category',
                    'cddirect_production_school_provider_program',
                    'cddirect_production_school_provider_education_level',
                    'cddirect_production_state',
                    'cddirect_production_user',
                    'cddirect_production_widget_category',
                    'cddirect_production_widget_degree',
                    'cddirect_production_widget_subject',
                    'cddirect_production_widget_degree_recommendation',
                    'cddirect_production_widget_subject_recommendation',
                    'form_position_csv',
                 ]
            },
            'EDUDirect_user_agent': {
                'args': {
                     '--TYPE': 'historical',
                },
                'bucket': 'highereducation-dw-transformed-data',
                'files': ['user_agent']
            },
            'EDUDirect_to_staging': {
                 'args': {
                     '--TYPE': 'historical',
                     '--ENVIRONMENT': 'dev',
                     '--START_DATE': '000',
                     '--END_DATE': '000',
                },
                'bucket': 'highereducation-dw-staging-data',
                'files': ['lead_fact_table_dev_v1']
            },
            'EDUDirect_related_subject': {
                'args': {
                     '--TYPE': 'historical',
                     '--ENVIRONMENT': 'dev',
                     '--START_DATE': '000',
                     '--END_DATE': '000',
                },
                'bucket': 'highereducation-dw-staging-data',
                'files': ['lead_fact_table_dev']
            },
        }


    def test_job_list(self):
        pending_jobs = self.job_list.copy()
        pending_jobs_to_start = self.job_list.copy()

        while(len(pending_jobs_to_start)> 0):
            for job_name, job in pending_jobs_to_start.items():
                # have we already started this job?
                job_run_id = None
                if 'job_run_id' in job:
                    job_run_id = job['JobRunId']
                    pending_jobs[job_name]['JobRunId'] = job_run_id
                    del pending_jobs_to_start[job_name]
                else:
                    job_object = get_job_object(self.glue, job_name , job['args'])
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
                    self.json_results[job_name] = item

            # wait 20 seconds before try to run jobs again
            time.sleep(20)

        print((self.json_results)
        # save json containing all the results
        with open('results/test_nightly.json', 'w') as outfile:
            json.dump(self.json_results, outfile)

        self.assertTrue(len(self.json_results) == len(self.job_list))


if __name__ == '__main__':
    print(unittest.main())

    

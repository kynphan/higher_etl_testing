#!/usr/bin/env python
#
#  Nightly scheduled jobs for EDU Direct
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
                'bucket': 'highereducation-dw-transformed-data',
                'date_partition': True,
                'initial_folders': [
                    'EDUDirectDB'
                ],
                'tables': [
                    'cddirect_production_lead',
                    'cddirect_production_visitor'
                ],
            },
            'EDUDirect_to_parquet_replace': {
                'bucket': 'highereducation-dw-transformed-data',
                'initial_folders': [
                   'EDUDirectDB'
                ],
                'tables': [
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
            #  'EDUDirect_to_parquet_new_snapshot': {
            #      'bucket': 'highereducation-dw-transformed-data',
            #      'tables': [
            #         'cddirect_production_affiliate',
            #         'cddirect_production_country',
            #         'cddirect_production_education_level',
            #         'cddirect_production_publisher',
            #         'cddirect_production_school',
            #         'cddirect_production_school_alias',
            #         'cddirect_production_school_campus',
            #         'cddirect_production_school_eligible_country',
            #         'cddirect_production_school_eligible_state',
            #         'cddirect_production_school_program',
            #         'cddirect_production_school_program_ineligible_state',
            #         'cddirect_production_school_provider',
            #         'cddirect_production_school_provider_campus',
            #         'cddirect_production_school_provider_cap',
            #         'cddirect_production_school_provider_cap_program',
            #         'cddirect_production_school_provider_cap_publisher',
            #         'cddirect_production_school_provider_cap_state',
            #         'cddirect_production_school_provider_category',
            #         'cddirect_production_school_provider_education_level',
            #         'cddirect_production_school_provider_leadid_flag',
            #         'cddirect_production_school_provider_program',
            #         'cddirect_production_school_publisher',
            #         'cddirect_production_school_targus_score',
            #         'cddirect_production_state',
            #         'cddirect_production_tag',
            #         'cddirect_production_targus_score',
            #         'cddirect_production_user',
            #         'cddirect_production_widget_category',
            #         'cddirect_production_widget_degree',
            #         'cddirect_production_widget_degree_recommendation',
            #         'cddirect_production_widget_subject',
            #         'cddirect_production_widget_subject_alias',
            #         'cddirect_production_widget_subject_recommendation',
            #         'form_position_csv'
            #      ],
            #      'date_partition': True
            # },
            # 'EDUDirect_to_parquet_current_dimensions': {
            #      'bucket': 'highereducation-dw-transformed-data',
            #      'initial_folders': ['EDUDirectDB-current'],
            #      'files': [
            #         'cddirect_production_affiliate',
            #         'cddirect_production_country',
            #         'cddirect_production_lead_cap',
            #         'cddirect_production_publisher',
            #         'cddirect_production_school',
            #         'cddirect_production_school_program',
            #         'cddirect_production_school_provider',
            #         'cddirect_production_school_provider_cap',
            #         'cddirect_production_school_provider_category',
            #         'cddirect_production_school_provider_program',
            #         'cddirect_production_school_provider_education_level',
            #         'cddirect_production_state',
            #         'cddirect_production_user',
            #         'cddirect_production_widget_category',
            #         'cddirect_production_widget_degree',
            #         'cddirect_production_widget_subject',
            #         'cddirect_production_widget_degree_recommendation',
            #         'cddirect_production_widget_subject_recommendation',
            #         'form_position_csv',
            #      ]
            # },
            # 'EDUDirect_user_agent': {
            #     'args': {
            #          '--TYPE': 'historical',
            #     },
            #     'bucket': 'highereducation-dw-transformed-data',
            #     'files': ['user_agent']
            # },
            # 'EDUDirect_to_staging': {
            #      'args': {
            #          '--TYPE': 'historical',
            #          '--ENVIRONMENT': 'dev',
            #          '--START_DATE': '000',
            #          '--END_DATE': '000',
            #     },
            #     'bucket': 'highereducation-dw-staging-data',
            #     'initial_folders': ['EDUDirectDB', 'tmp'],
            #     'files': ['lead_fact_table_dev_v1']
            # },
            # 'EDUDirect_related_subject': {
            #     'args': {
            #          '--TYPE': 'historical',
            #          '--ENVIRONMENT': 'dev',
            #          '--START_DATE': '000',
            #          '--END_DATE': '000',
            #     },
            #     'bucket': 'highereducation-dw-staging-data',
            #     'files': ['lead_fact_table_dev']
            # },
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
        self.json_results = run_jobs(self.glue, self.job_list, self.json_results, self.logger)

        print(self.json_results)
        with open('results/night_edu.json', 'w') as outfile:
            json.dump(self.json_results, outfile)
        self.assertTrue(len(self.json_results) == len(self.job_list))


if __name__ == '__main__':
    print(unittest.main())
    

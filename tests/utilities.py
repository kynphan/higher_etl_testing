#!/usr/bin/env python
from __future__ import print_function

import os
import sys
import datetime
import json
import boto3
from botocore.errorfactory import ClientError
import logging, logging.config

def load_log_config():
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    return root


def get_json_file(test_name, json_items):
    now = datetime.datetime.now()
    time_format = now.isoformat()
    current_path = os.path.dirname(os.path.abspath(__file__))
    full_file_name = os.path.join(current_path , 'results' , test_name + '_' + str(time_format) + '.json')
    with open(full_file_name, 'w+') as json_file:
        for item in json_items:
            json.dump(item, json_file)


def check_file_s3(s3, bucket_name, file_name):
    try:
        s3.head_object(Bucket=bucket_name, Key=file_name)
        return True
    except ClientError:
        return False



def get_job_object(glue_service, job_name, args={}):
    try:
        return glue_service.start_job_run( 
                JobName=job_name,
                Arguments=args
        )
    except:
        return None


# https://github.com/aws-samples/aws-etl-orchestrator/blob/master/lambda/gluerunner/gluerunner.py
def get_job_state(glue_service, job_name, job):
    job_run_id = job['JobRunId']
    status = glue_service.get_job_run(
        JobName=job_name,
        RunId=job_run_id
    )
    return status

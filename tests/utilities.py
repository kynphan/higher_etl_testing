#!/usr/bin/env python
from __future__ import print_function

import boto3
from botocore.errorfactory import ClientError
import logging, logging.config

def load_log_config():
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    return root


def check_file_s3(s3, bucket_name, file_name):
    try:
        s3.head_object(Bucket=bucket_name, Key=file_name)
        return True
    except ClientError:
        return False


def get_job_object(glue_service, job_name, args={}):
    return glue_service.start_job_run( 
            JobName=job_name,
            Arguments=args
        )


def get_job_state(glue_service, job_name, job):
    job_run_id = job['JobRunId']
    status = glue_service.get_job_run(
        JobName=job_name,
        RunId=job_run_id
    )
    return status['JobRunState']

#!/usr/bin/env python

import boto3
from botocore.errorfactory import ClientError
import logging, logging.config
from __future__ import print_function

def load_log_config():
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    return root


def check_file_s3(bucket_name, file_name):
    s3 = boto3.resource('s3')
    try:
        s3.head_object(Bucket=bucket_name, Key=file_name)
        return True
    except ClientError:
        return False
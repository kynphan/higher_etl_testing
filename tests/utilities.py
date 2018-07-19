#!/usr/bin/env python
from __future__ import print_function

import os
import sys
from datetime import date, datetime
import time

import json
import boto3
import logging, logging.config


def load_log_config():
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    return root


def get_json_file(test_name, json_items):
    now = datetime.now()
    time_format = now.isoformat()
    current_path = os.path.dirname(os.path.abspath(__file__))
    full_file_name = os.path.join(current_path, 'results' , test_name + '_' + str(time_format) + '.json')
    with open(full_file_name, 'w+') as json_file:
        for item in json_items:
            json.dump(item, json_file)


# from he glue liblibs stored in s3
def first_day_next_month(input_date):
    next_month = (input_date.month + 1) % 12
    next_month = 12 if next_month == 0 else next_month
    next_year = input_date.year + 1 if next_month == 1 else input_date.year
    return date(next_year, next_month, 1)


def first_day_months_ago(months_ago, start_date):
    month_delta = start_date.month - months_ago
    month_ago = month_delta % 12
    month_ago = 12 if month_ago == 0 else month_ago

    year_delta = months_ago / 12
    year_delta = year_delta + 1 if month_ago > start_date.month else year_delta
    year_ago = start_date.year - year_delta
    return start_date.replace(day=1, month=month_ago, year=year_ago)


def get_month_boundaries(number_months=1, start_date=date.today()):
    tuples = []
    first_date = first_day_months_ago(number_months - 1, start_date)
    for x in xrange(number_months):
        second_date = first_day_next_month(first_date)
        tuples.append((first_date, second_date))
        first_date = second_date
    return tuples


def get_date_folders(tables, date_partition=False, months=0, initial_folders = []):
    folder_names = []
    folders = []
    for f in initial_folders:
        folders.append(f)

    for table in tables:

        date_folders = []
        if(date_partition):
            if(months > 0):
                for window in get_month_boundaries(months):
                    date_folders = []
                    init_date = window[0]

                    # add date format
                    date_folders.append('year={}'.format(init_date.year))
                    date_folders.append('month={}'.format(init_date.month))
                    date_folders.append('day={}'.format(init_date.day))

                    full_path = folders + [table] + date_folders
                    folder_names.append(os.path.join(*full_path))

            else:
                today = date.today()
                date_folders.append('year={}'.format(today.year))
                date_folders = [].append('month={}'.format(today.month))
                date_folders = [].append('day={}'.format(today.day))

                full_path = folders + [table] + date_folders
                folder_names.append(os.path.join(*full_path))

        else:
            full_path = folders + [table]
            folder_names.append(os.path.join(*full_path))

    return folder_names


def check_file_s3(s3, bucket_name, file_name):
    bucket = s3.Bucket(bucket_name)
    objs = list(bucket.objects.filter(Prefix=file_name))
    return len(objs) > 0


def get_job_object(glue_service, job_name, args={}):
    try:
        return glue_service.start_job_run( 
                JobName=job_name,
                Arguments=args
        )
    except:
        return None


def launch_crawler(glue_service, crawler_name):
    glue_service.start_crawler(Name=crawler_name)


# https://github.com/aws-samples/aws-etl-orchestrator/blob/master/lambda/gluerunner/gluerunner.py
def get_job_state(glue_service, job_name, job_run_id):
    status = glue_service.get_job_run(
        JobName=job_name,
        RunId=job_run_id
    )
    return status


def run_jobs(glue, s3, job_list, json_results, logger):
    pending_jobs = job_list.copy()
    pending_jobs_to_start = job_list.copy()

    while(len(pending_jobs_to_start) > 0):
            for job_name, job in pending_jobs_to_start.items():
                args = job['args'] if 'args' in job else {}
                job_object = get_job_object(glue, job_name, args)
                if job_object and 'JobRunId' in job_object:    
                    job['JobRunId'] = job_object['JobRunId']
                    job['execution_start'] = datetime.now()
                    pending_jobs[job_name] = job
                    del pending_jobs_to_start[job_name]

    while(len(pending_jobs) > 0):

        for job_name, job in pending_jobs.items():
            job_run_id = job['JobRunId']
            job_started = job['execution_start']
            status = get_job_state(glue, job_name, job_run_id)
            job_status = status['JobRun']['JobRunState']
            status_list = ['STARTING', 'RUNNING', 'STARTING', 'STOPPING']
            if job_status not in status_list:
                # remove job from pending list
                del pending_jobs[job_name]

                # add info to the json
                files_created = {}
                if job_status in ['SUCCEEDED']:
                    bucket = job['bucket'] if 'bucket' in job else ''
                    if len(bucket) > 0:
                        files = job['files'] if 'files' in job else []
                        tables = job['tables'] if 'tables' in job else []
                        initial_folders = job['initial_folders'] if 'initial_folders' in job else []
                        date_partition = job['date_partition'] if 'date_partition' in job else False
                        file_extension = job['file_extension'] if 'file_extension' in job else ''

                        # test folder creation
                        if(len(tables) > 0):
                            folder_names = get_date_folders(tables, date_partition, 3, initial_folders)
                            for f in folder_names:
                                file_status = check_file_s3(s3, bucket, f)
                                files_created[f] = file_status

                            # check the files contained in each one of the folders
                            for prefix, folder_status in files_created.items():
                                if folder_status:
                                    print('prefix:' + prefix)
                                    validated_contents = check_bucket_content(s3, bucket, prefix, file_extension, job_started)
                                    files_created[prefix] = validated_contents

                        elif(len(files) > 0):
                            for f in files:
                                file_status = check_file_s3(s3, bucket, f)
                                files_created[f] = file_status
                item = {
                    'status': job_status,
                    'execution_time': status['JobRun']['ExecutionTime'],
                    'files_results_status': files_created
                }
                # save into json file
                json_results[job_name] = item

                # save results into logger
                logger.info(job_name, extra=item)

            # wait 20 seconds before try to run jobs againzzz
            time.sleep(20)

    return json_results


def get_bucket_content(bucket_name, file_extension=''):
    s3 = boto3.client('s3')
    objs = s3.list_objects_v2(Bucket=bucket_name)['Contents']
    for obj in objs:
        key = obj['Key']
        if(file_extension):
            if not key.endswith(file_extension):
                print('to remove')
                print(key)
                objs.remove(obj)

    return objs


def check_bucket_content(s3, bucket_name, folder_prefix='', file_extension='', creation_date=''):
    objs = get_bucket_content(bucket_name, folder_prefix)
    print(objs)
    print(folder_prefix)
    objs_validated = {}
    for obj in objs:
        key = obj['Key']
        folder_flag = True
        if(folder_prefix):
            print('folder validation')
            print(key, folder_prefix, key.startswith(folder_prefix))
            if not key.startswith(folder_prefix):
                folder_flag = False

        date_flag = True
        if(creation_date):
            print(obj['LastModified'].replace(tzinfo=None) > creation_date.replace(tzinfo=None))
            if not obj['LastModified'].replace(tzinfo=None) > creation_date.replace(tzinfo=None):
                date_flag = False

        objs_validated[key] = folder_flag and date_flag
        sys.exit(0)

    return objs_validated
       
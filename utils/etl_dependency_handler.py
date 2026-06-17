import json
import logging
import sys
import time

import boto3
from utils.ETLDependencyHelper import ETLDependencyHelper
import argparse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)


# get RDS secret
def get_secrets(secret_name):
    # session = boto3.session.Session()
    region = session.region_name
    client = session.client(
        service_name='secretsmanager',
        region_name=region
    )
    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
    secret_arn = get_secret_value_response['ARN']
    secret = get_secret_value_response['SecretString']
    secret_json = json.loads(secret)
    return secret_json


# parse the input args
def parse_args():
    parser = argparse.ArgumentParser(description="ETL Dependency Checker")
    parser.add_argument("--mode", required=True, help="check or update uow, possible value is check or update")
    parser.add_argument("--tgt_uow_id", required=True, help="Target UOW ID")
    parser.add_argument("--tgt_uow_value", required=False, help="Target UOW timestamp")
    parser.add_argument("--src_offset_steps_in_mins", required=False, help="Minutes to add for source UOW ID timestamp")
    return parser.parse_args()


# check source data with max retry configured in config table
def check_src_ready_with_retry(tgt_uow_id, tgt_uow_value, src_offset_steps_in_mins):
    src_ready_flag = 0
    retry_times = etl_dependency_helper.check_max_retry(tgt_uow_id)
    if retry_times is None:
        logging.error(
            f'{retry_times} max retry check times for {tgt_uow_id}'
        )

    # if no input uow value, then default to the config table
    if tgt_uow_value is None:
        tgt_uow_value = etl_dependency_helper.get_uow_value(tgt_uow_id)
        print(f'tgt_uow_value: {tgt_uow_value}')

    for i in range(0, retry_times):
        src_ready_flag, df = etl_dependency_helper.check_src_ready(tgt_uow_id, src_offset_steps_in_mins)
        etl_dependency_helper.update_check_log(df)

        if src_ready_flag:
            print("Source data is ready, finished checking successfully")
            break

        print(f'Source data is not ready, will retry {i + 1} time after {sleep_seconds} seconds ...')
        time.sleep(sleep_seconds)

    if not src_ready_flag:
        print(f'Source data is not ready after {retry_times} times retry, abort the source data ready checking!')
        raise Exception(f'Source data is not ready after {retry_times} times retry')

    return src_ready_flag


if __name__ == '__main__':
    session = boto3.Session(profile_name="DataEngineer-859004686855")
    args = parse_args()
    mode = args.mode
    tgt_uow_id = args.tgt_uow_id
    tgt_uow_value = args.tgt_uow_value
    src_offset_steps_in_mins = args.src_offset_steps_in_mins

    print(f'mode: {mode}')
    print(f'tgt_uow_id: {tgt_uow_id}')
    print(f'tgt_uow_value: {tgt_uow_value}')
    print(f'src_offset_steps_in_mins: {src_offset_steps_in_mins}')

    secret_name = 'data_center_etl_dependency_db_test'
    secret_json = get_secrets(secret_name)

    host = secret_json['host']
    username = secret_json['username']
    password = secret_json['password']
    port = secret_json['port']
    db = secret_json['dbname']
    sleep_seconds = 1

    # build a helper
    etl_dependency_helper = ETLDependencyHelper(host, username, password, port, db)

    if mode == 'check':
        check_src_ready_with_retry(tgt_uow_id, tgt_uow_value, src_offset_steps_in_mins)
    elif mode == 'update':
        etl_dependency_helper.update_uow_value(tgt_uow_id, tgt_uow_value, src_offset_steps_in_mins)
        tgt_uow_value_updated = etl_dependency_helper.get_uow_value(tgt_uow_id)
        print(f'Updated UOW ID for: {tgt_uow_id} to {tgt_uow_value_updated}')

    etl_dependency_helper.close()


import boto3
import numpy as np
import pandas as pd
import pymysql
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine, text
import json


def get_rds_secrets(rds_secret_name):
    secret_name = rds_secret_name
    session = boto3.session.Session()
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


def get_rds_con(secret_json, db):
    connection = pymysql.connect(
        host=secret_json['host'],
        user=secret_json['username'],
        password=secret_json['password'],
        port=int(secret_json['port']),
        database=db,
        cursorclass=pymysql.cursors.Cursor,
        read_timeout=780
    )
    return connection


def get_crm_con(secret_json, db):
    connection = pymysql.connect(
        host='jms-ot.lifebyte.io',
        user='xxx',
        password='xxx',
        port=33061,
        database=db,
        cursorclass=pymysql.cursors.Cursor,
        read_timeout=780
    )
    return connection


def get_dw_secrets():
    secret_name = "data_dw_sysuser"
    session = boto3.session.Session()
    region = session.region_name
    # region = os.environ['REGION_NAME']
    # print(region)
    client = session.client(
        service_name='secretsmanager',
        region_name=region
    )
    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
    # secret_arn = get_secret_value_response['ARN']
    secret = get_secret_value_response['SecretString']
    secret_json = json.loads(secret)
    return secret_json


def get_snowflake_df_con(secret_json, schema, tenant):
    engine = create_engine(URL(
        account=secret_json['snowflakeAccount'],
        user=secret_json['snowflakeUser'],
        password=secret_json['snowflakePassword'],
        database=f'{tenant}_datawarehouse',
        schema=schema,
        warehouse='DEV_COMPUTE_WH',
        role='data_dw_sysrole',
    ))
    return engine, engine.connect()


def get_rds_df(rds_secret_name, db):
    try:
        check = -1
        print('cal starts for database')

        query = f'''
                 select
                    id
                    ,ticket
                    ,"no"
                    ,user_id
                    ,external_id
                    ,processed_at
                    ,completed_at
                    ,created_at
                    ,updated_at
                from {db}.transactions
                where id in (
                     49656010
                     ,49656011
                     ,49656012
                     ,49656013
                     ,49656014
                     ,49656015
                     ,49656016
                     ,49656017
                     ,49656018
                     ,49656019
                     ,49656020
                     ,49656021
                     ,49656022
                     ,49656023
                     ,49656024
                     ,49656025
                     ,49656026
                     ,49656027
                     ,49656028
                     ,49656029
                     ,49656030
                     ,49656031
                     ,49656032
                     ,49656033
                     ,49656034
                 )
            '''

        secret_rds_json = get_rds_secrets(rds_secret_name)
        mysql_conn = get_crm_con(secret_rds_json, db)
        check = 0

        result_df = pd.read_sql_query(query, mysql_conn)
        mysql_conn.close()
        crm_df = result_df.replace(np.nan, '\\N')
        check = 1
        return result_df
    except Exception as e:
        if check == 0:
            mysql_conn.close()
        print('Unable to sync data in crm...')
        print(e)
        raise e


def save_df_to_snowflake(df, schema, target_table):
    print('saving data into Snowflake')
    try:
        df.to_csv(f'/tmp/{target_table}.csv', index=False, header=False)
        secret_json = get_dw_secrets()
        snowflake_eng, snowflake_con = get_snowflake_df_con(secret_json, schema, tenant)
        snowflake_con.execute(text(f'DROP TABLE IF EXISTS {target_table}'))

        snf_sql = f"""
                CREATE TABLE IF NOT EXISTS {target_table} (
                    id integer,
                    ticket integer,
                    "no" string,
                    user_id integer,
                    external_id string,
                    processed_at datetime,
                    completed_at datetime,
                    created_at datetime,
                    updated_at datetime
                )
                STAGE_FILE_FORMAT = ( TYPE = 'csv' FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '"' EMPTY_FIELD_AS_NULL = FALSE);
            """
        snowflake_con.execute(text(snf_sql))

        snowflake_con.execute(text(f"PUT file:///tmp/{target_table}.csv @%{target_table}"))
        snowflake_con.execute(text(f"COPY INTO {target_table} from @%{target_table}"))
        snowflake_con.execute(text(f'commit'))

        snf_sql = f"""
                SELECT
                *
                FROM {schema}.{target_table}
                limit 10
                ;
                """
        # print(snf_sql)
        snf_df = pd.read_sql_query(snf_sql, snowflake_con)
        snowflake_con.close()
        snowflake_eng.dispose()
        return snf_df
    except Exception as e:
        print('Failed to save data to snowflake ...')
        print(e)
        raise e


if __name__ == '__main__':
    schema = 'tmgm_dev_iad_2940_lnd'
    tenant = 'TMGM'
    target_table = 'crm_transaction_adj_new_20250812'
    rds_secret_name = 'AWS_tm-prod-mysql-db01-secret-read-replica'
    db = 'tm_portal'
    result_df = get_rds_df(rds_secret_name, db)
    print('Source data ...')
    print(result_df.head(10))

    # snf_df = save_df_to_snowflake(result_df, schema, target_table)
    # print('Target data ...')
    # print(snf_df)

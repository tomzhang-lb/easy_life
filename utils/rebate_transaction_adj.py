import re
from pathlib import Path

import boto3
import numpy as np
import pandas as pd
import pymysql
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine, text
import json


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


def get_rds_df(rds_secret_name, db, query, crm_flag=1):
    try:
        check = -1
        print('cal starts for database')

        secret_rds_json = get_rds_secrets(rds_secret_name)
        if crm_flag == 1:
            mysql_conn = get_crm_con(secret_rds_json, db)
        else:
            mysql_conn = get_rds_con(secret_rds_json, db)
        check = 0

        result_df = pd.read_sql_query(query, mysql_conn)
        mysql_conn.close()
        result_df = result_df.replace(np.nan, '\\N')
        check = 1
        return result_df
    except Exception as e:
        if check == 0:
            mysql_conn.close()
        print('Unable to sync data in crm...')
        print(e)
        raise e


def save_df_to_snowflake(df, schema, target_table, tenant, table_ddl):
    print('saving data into Snowflake')
    try:
        df.to_csv(f'/tmp/{target_table}.csv', index=False, header=False)
        secret_json = get_dw_secrets()
        snowflake_eng, snowflake_con = get_snowflake_df_con(secret_json, schema, tenant)
        snowflake_con.execute(text(f'DROP TABLE IF EXISTS {schema}.{target_table}'))
        snowflake_con.execute(text(table_ddl))
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
        snf_df = pd.read_sql_query(snf_sql, snowflake_con)
        snowflake_con.close()
        snowflake_eng.dispose()
        return snf_df
    except Exception as e:
        print('Failed to save data to snowflake ...')
        print(e)
        raise e


def read_adj_trx(adj_file):
    if Path(adj_file).exists():
        source_df = pd.read_excel(adj_file)
        return source_df
    else:
        print(f'File {adj_file} does not exist')
        raise FileNotFoundError


def gen_adj_ddl(adj_file, schema, target_table):
    source_df = read_adj_trx(adj_file)
    adj_table_ddl = f'CREATE TABLE IF NOT EXISTS {schema}.{target_table} ( '
    adj_table_columns = source_df.columns
    for column in adj_table_columns:
        adj_table_ddl = adj_table_ddl + f'{column} string,'
    adj_table_ddl = re.sub(r',$', '', adj_table_ddl)
    adj_table_ddl = adj_table_ddl + ''') STAGE_FILE_FORMAT = ( TYPE = 'csv' FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '"' EMPTY_FIELD_AS_NULL = FALSE)'''
    return adj_table_ddl


def query_snowflake(schema, tenant, snf_sql):
    try:
        secret_json = get_dw_secrets()
        snowflake_eng, snowflake_con = get_snowflake_df_con(secret_json, schema, tenant)
        snf_df = pd.read_sql_query(snf_sql, snowflake_con)
        snowflake_con.close()
        snowflake_eng.dispose()
        return snf_df
    except Exception as e:
        print('Failed to query to snowflake ...')
        print(e)
        raise e


if __name__ == '__main__':
    adj_file = '/Users/tom.zhang/Downloads/adj_rebate_transaction.xlsx'
    schema = 'tmgm_dev_iad_tom_lnd'
    tenant = 'tmgm'
    adj_table = 'adj_transaction_20250812'
    rs_secret_name = 'AWS_tm-prod-mysql-db01-secret-read-replica'
    rs_db = 'mt5_tmgm_live01'
    rs_source_table_prefix = 'mt5_deals_'
    rs_target_table = 'rs_transaction_adj_20250812'
    crm_target_table = 'crm_transaction_adj_20250812'
    crm_db = 'tm_portal'

    # 1.) save adj file to Snowflake
    # adj_df = read_adj_trx(adj_file)
    # adj_table_ddl = gen_adj_ddl(adj_file, schema, adj_table)
    # tmp_df = save_df_to_snowflake(adj_df, schema, adj_table, tenant, adj_table_ddl)
    # print(tmp_df.head())
    #
    #
    # # 2.) query CRM data and save to Snowflake based on adj file
    # snf_sql = f"""
    #             SELECT
    #                 trx.id
    #             FROM {schema}.{adj_table} adj
    #             INNER JOIN lnd.crm_transactions trx
    #             ON adj.trading_server_transaction_no = trx.ticket
    #             AND CAST(adj.trading_server_login AS VARCHAR) = trx.external_id
    #             """
    # snf_df = query_snowflake(schema, tenant, snf_sql)
    # # print(snf_df)
    #
    # crm_sql = f'''
    #          select
    #             id
    #             ,ticket
    #             ,"no"
    #             ,user_id
    #             ,external_id
    #             ,processed_at
    #             ,completed_at
    #             ,created_at
    #             ,updated_at
    #         from {crm_db}.transactions
    #         where id in ('''
    # crm_sql = crm_sql + ','.join(map(str, snf_df['id'].tolist())) + ')'
    # # print(crm_sql)
    #
    # crm_df = get_rds_df(rs_secret_name, crm_db, crm_sql)
    # print(crm_df)
    # crm_target_table_ddl = f'''
    #                         CREATE TABLE IF NOT EXISTS {schema}.{crm_target_table} (
    #                         ID NUMBER(38,0),
    #                         TICKET NUMBER(38,0),
    #                         "no" VARCHAR(16777216),
    #                         USER_ID NUMBER(38,0),
    #                         EXTERNAL_ID VARCHAR(16777216),
    #                         PROCESSED_AT TIMESTAMP_NTZ(9),
    #                         COMPLETED_AT TIMESTAMP_NTZ(9),
    #                         CREATED_AT TIMESTAMP_NTZ(9),
    #                         UPDATED_AT TIMESTAMP_NTZ(9)
    #                         )
    #                         '''
    # tmp_df = save_df_to_snowflake(crm_df, schema, crm_target_table, tenant, crm_target_table_ddl)
    # print(tmp_df.head())

    # 3.) query report server data and save to Snowflake based on adj file
    snf_sql = f"""
                SELECT
                    adj.trading_server_transaction_no
                    ,adj.trading_server_login
                    ,trx.server
                    ,EXTRACT('year', crm_trx.created_at) AS year_num
                FROM {schema}.{adj_table} adj
                INNER JOIN lnd.report_server_trades_transactions trx
                ON adj.trading_server_transaction_no = trx.ticket
                AND adj.trading_server_login = trx.login
                INNER JOIN {schema}.{crm_target_table} crm_trx
                ON adj.trading_server_transaction_no = crm_trx.ticket
                AND CAST(adj.trading_server_login AS VARCHAR) = crm_trx.external_id
                """
    snf_df = query_snowflake(schema, tenant, snf_sql)
    # snf_df['server'] = np.where(snf_df["trading_server_transaction_no"] == '68541921', 'mt5_tmgm_demo', snf_df["server"])
    # snf_df['year_num'] = np.where(snf_df["trading_server_transaction_no"] == '68541918', '2025', snf_df["year_num"])
    # print(snf_df)
    # query the deals data from different server and different year
    rs_combine_df = pd.DataFrame()
    for server_year, group_df in snf_df.groupby(['server', 'year_num']):
        server = server_year[0]
        year = server_year[1]
        rs_sql = f'''
                    select
                        deal
                        ,time
                        ,timeMsc
                        ,login
                        ,'{server}' as server
                    from {server}.{rs_source_table_prefix}{year}
                    where
                    ('''
        group_df['deal_login'] = '(deal =' + group_df['trading_server_transaction_no'].astype(str) + ' and login =' + group_df['trading_server_login'].astype(str) + ')'
        rs_sql = rs_sql + ' or '.join(map(str, group_df['deal_login'].tolist())) + ')'
        print(rs_sql)

        rs_df = get_rds_df(rs_secret_name, rs_db, rs_sql, crm_flag=0)
        rs_combine_df = pd.concat([rs_combine_df, rs_df], ignore_index=True)

    print(rs_combine_df)

    rs_target_table_ddl = f'''
                             CREATE TABLE IF NOT EXISTS {schema}.{rs_target_table} (
                                DEAL NUMBER(38,0),
                                OPEN_TIME TIMESTAMP_NTZ(9),
                                TIMEMSC TIMESTAMP_NTZ(9),
                                LOGIN NUMBER(38,0),
                                SERVER VARCHAR(16777216)
                            )
                            '''
    tmp_df = save_df_to_snowflake(rs_combine_df, schema, rs_target_table, tenant, rs_target_table_ddl)
    print(tmp_df.head())

    print('ALL DONE!')

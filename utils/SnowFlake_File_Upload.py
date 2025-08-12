import boto3
import pandas as pd
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



def upload_file_to_snowflake(file, target_table, schema, tenant):
    df = pd.read_csv(f'{file}', sep=',', header=0, index_col=False)
    df.to_csv(f'/tmp/{target_table}.csv', index=False, header=False)
    secret_json = get_dw_secrets()
    snowflake_eng, snowflake_con = get_snowflake_df_con(secret_json, schema, tenant)
    snowflake_con.execute(text(f'DROP TABLE IF EXISTS {target_table}'))
    snf_sql = f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                trading_server_transaction_no integer,
                trading_server_login integer,
                comment string,
                fund_amount decimal(38,6)
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
    print(snf_sql)
    snf_df = pd.read_sql_query(snf_sql, snowflake_con)
    snowflake_con.close()
    snowflake_eng.dispose()
    return snf_df


if __name__ == '__main__':
    file = '/Users/tom.zhang/Downloads/snowflake_upload.csv'
    schema = 'tmgm_dev_iad_2940_lnd'
    tenant = 'TMGM'
    target_table = 'crm_transaction_adj_20250812'
    df = upload_file_to_snowflake(file, target_table, schema, tenant)
    print(df.head(10))
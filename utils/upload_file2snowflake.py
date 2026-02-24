import boto3
import pandas as pd
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine, text
import json




# s3 = session.client("s3")


def get_dw_secrets(session):
    secret_name = "data_dw_sysuser"
    # session = boto3.session.Session()
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


def upload_file_to_snowflake(session, file, schema, tenant):
    df = pd.read_csv(f'{file}', sep=',', header=0, index_col=False)
    # take the file name as the target table name , and first column as the column name with data type as string
    target_table = file.split('/')[-1].split('.')[0]
    table_columns_type = ' string, '.join(df.columns.values) + ' string'
    snf_sql = f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                {table_columns_type}
            )
            STAGE_FILE_FORMAT = ( TYPE = 'csv' FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '"' EMPTY_FIELD_AS_NULL = FALSE);
        """
    print(snf_sql)
    # save data and upload to snowflake
    df.to_csv(f'/tmp/{target_table}.csv', index=False, header=False)
    secret_json = get_dw_secrets(session)
    snowflake_eng, snowflake_con = get_snowflake_df_con(secret_json, schema, tenant)
    snowflake_con.execute(text(f'DROP TABLE IF EXISTS {target_table}'))
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
    file = '/Users/tom.zhang/Downloads/email_verify.csv'
    schema = 'tmgm_dev_iad_tom_lnd'
    tenant = 'TMGM'
    session = boto3.Session(profile_name="DataEngineer-859004686855")
    # sts = session.client("sts")
    # sts.get_caller_identity()
    result_df = upload_file_to_snowflake(session, file, schema, tenant)
    print(result_df.head())

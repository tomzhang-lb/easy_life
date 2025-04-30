import boto3
import pandas as pd
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
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
    snowflake_con.execute(f'DROP TABLE IF EXISTS {target_table}')
    snf_sql = f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                report_id integer,
                report_name string,
                report_week_beg_dt string,
                report_dt string,
                user_id integer,
                l5d_withdrawal_usd_amt string,
                p5d_balance_usd_amt string,
                turn_to_negative_balance_usd_amt string,
                stop_out_hours_cnt string,
                stop_out_trade_cnt string,
                elder_has_trades_flag string,
                claimed_annual_income_usd_amd  string,
                claimed_saving_usd_amt string,
                ytd_net_deposit_usd_amt string,
                lifetime_net_deposit_usd_amt string,
                l3_month_inactive_flag string,
                l6_month_inactive_flag string
            )
            STAGE_FILE_FORMAT = ( TYPE = 'csv' FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '"' EMPTY_FIELD_AS_NULL = FALSE);
        """
    snowflake_con.execute(snf_sql)
    snowflake_con.execute(f"PUT file:///tmp/{target_table}.csv @%{target_table}")
    snowflake_con.execute(f"COPY INTO {target_table} from @%{target_table}")
    snf_sql = f"""
            SELECT
            *
            FROM {schema}.{target_table}
            limit 10
            ;
            """
    snf_df = pd.read_sql_query(snf_sql, snowflake_con)
    return snf_df


if __name__ == '__main__':
    file = '/Users/tom.zhang/Desktop/asic_compliance.csv'
    schema = 'tmgm_dev_iad_2159_tst'
    tenant = 'TMGM'
    target_table = 'compliance_triggers_delta'
    df = upload_file_to_snowflake(file, target_table, schema, tenant)
    print(df.head(10))
# procedure
import json
import boto3
from sqlalchemy import create_engine, text
from snowflake.sqlalchemy import URL


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


def kill_long_running_queries(schema, tenant):
    queries = []
    kill_long_queries = f'''
        CREATE OR REPLACE PROCEDURE {tenant}_datawarehouse.{schema}.kill_long_running_queries()
        RETURNS STRING
        LANGUAGE SQL
        EXECUTE AS CALLER
        AS
        $$
        DECLARE
            query_id STRING;
            query_ids VARCHAR DEFAULT '';
            c1 CURSOR FOR (
                SELECT
                    query_id
                FROM table(information_schema.query_history())
                WHERE execution_status = 'RUNNING'
                AND start_time < dateadd('HOUR', -1, current_timestamp())
                AND user_name != 'SNOWFLAKE'  -- optional: avoid system queries
            );
        BEGIN
          FOR rec IN c1
          DO
            BEGIN
              query_id := rec.query_id;
              query_ids := query_id ||', '|| query_ids;
              CALL SYSTEM$CANCEL_QUERY(query_id);
            END;
          END FOR;
          RETURN 'Long-running queries cancelled: ' || query_ids;
        END;
        $$
        ;
    '''

    kill_long_queries_task = f'''
        CREATE OR REPLACE TASK {tenant}_datawarehouse.{schema}.kill_long_running_queries_task
          WAREHOUSE = DEV_COMPUTE_WH
          SCHEDULE = 'USING CRON 0 */1 * * * UTC'  -- runs every hour
        AS
        CALL {tenant}_datawarehouse.{schema}.kill_long_queries()
        ;
        '''

    enable_task = f'''
        ALTER TASK {tenant}_datawarehouse.{schema}.kill_long_running_queries_task RESUME
        ;
        '''
    queries.append(kill_long_queries)
    queries.append(kill_long_queries_task)
    queries.append(enable_task)

    return queries

if __name__ == '__main__':
    schema = 'tmgm_dev_iad_2905_tst'
    tenant = 'tmgm'
    secret_json = get_dw_secrets()
    snowflake_eng, snowflake_con = get_snowflake_df_con(secret_json, schema, tenant)
    queries = kill_long_running_queries(schema, tenant)
    for query in queries:
        print(query)
        try:
            snowflake_con.execute(text(query))
        except Exception as e:
            print(e)

    snowflake_con.close()
    snowflake_eng.dispose()


import pandas as pd
import json
import operator as op
import boto3
import snowflake.connector
from datetime import datetime, timezone
from decimal import Decimal


def _json_serializer(obj):
    if isinstance(obj, Decimal):
        return int(obj) if obj == obj.to_integral_value() else float(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


class AlarmException(Exception):
    pass


OPERATORS = {
    '>':  op.gt,
    '>=': op.ge,
    '<':  op.lt,
    '<=': op.le,
    '==': op.eq,
    '!=': op.ne,
}


def get_snowflake_credentials(snf_secret_name):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=session.region_name)
    response = client.get_secret_value(SecretId=snf_secret_name)
    return json.loads(response['SecretString'])


def get_snowflake_cursor(event, snf_secret_name, schema, tenant):
    secret = get_snowflake_credentials(snf_secret_name)
    wh = tenant.lower() + '_collection_wh' if event['env'].lower() == 'prod' else 'dev_collection_wh'
    conn = snowflake.connector.connect(
        account=secret['snowflakeAccount'],
        user=secret['snowflakeUser'],
        password=secret['snowflakePassword'],
        database='{0}_DATAWAREHOUSE'.format(tenant),
        schema=schema,
        warehouse=wh,
        role='data_dw_sysrole',
    )
    return conn.cursor()


def write_history(cursor, snf_secret_name, event, tenant, history_schema,
                  executed_at, env, client, object_name, query, query_id,
                  operator_str, expected_value, alarm_triggered, row_count,
                  results, error_message, priority):
    try:
        if cursor is None:
            secret = get_snowflake_credentials(snf_secret_name)
            wh = tenant.lower() + '_collection_wh' if event['env'].lower() == 'prod' else 'dev_collection_wh'
            conn = snowflake.connector.connect(
                account=secret['snowflakeAccount'],
                user=secret['snowflakeUser'],
                password=secret['snowflakePassword'],
                database='ALARM_DATABASE',
                schema=history_schema,
                warehouse=wh,
                role='data_dw_sysrole',
            )
            cursor = conn.cursor()

        cursor.execute(
            f"""
            INSERT INTO ALARM_DATABASE.{history_schema}.ALARM_SQL_EXECUTION_HISTORY (
                executed_at, env, client, object_name, query, query_id,
                expected_operator, expected_value, alarm_triggered, result_row_count,
                results, error_message, priority
            ) SELECT
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                parse_json(%s), %s, %s
            """,
            (executed_at, env, client, object_name, query, query_id,
             operator_str, expected_value, alarm_triggered, row_count,
             results, error_message, priority)
        )
    except Exception as e:
        print(f"[write_history] failed to record history: {e}")


def alarm_sql_execute(cursor, query, operator_str, expected_value):
    cursor.execute(query)
    query_id = cursor.sfqid
    df = pd.DataFrame.from_records(cursor.fetchall(), columns=[d[0] for d in cursor.description])
    df.columns = df.columns.str.lower()
    row_count = len(df)
    results = json.dumps(df.to_dict('records'), default=_json_serializer)
    # alarm fires when the expected normal condition is NOT met
    alarm_triggered = not OPERATORS[operator_str](row_count, expected_value)
    return query_id, row_count, results, alarm_triggered, df


def lambda_handler(event, context):
    executed_at = datetime.now(timezone.utc)
    env = event['env']
    client = event['client']
    object_name = event.get('object_name', '')
    snf_secret_name = event['snf_secret_name']
    query = event['query']
    operator_str = event['expected_operator']
    expected_value = float(event['expected_value'])
    priority = event.get('priority', 'MEDIUM').upper()
    escalation_column = event.get('escalation_column', None)
    escalation_aggregation = event.get('escalation_aggregation', None)
    escalation_operator = event.get('escalation_operator', None)
    escalation_threshold = event.get('escalation_threshold', None)

    if operator_str not in OPERATORS:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f"Unsupported expected_operator: '{operator_str}'", 'client': client, 'env': env})
        }

    if env.lower() == 'prod':
        alarm_schema = 'LND'
        history_schema = 'PROD'
    else:
        alarm_schema = object_name.replace('-', '_').upper() + '_LND'
        history_schema = 'DEV'

    cursor = None
    query_id = None
    row_count = None
    alarm_triggered = None
    results = None
    error_message = None

    try:
        cursor = get_snowflake_cursor(event, snf_secret_name, alarm_schema, client)
        query_id, row_count, results, alarm_triggered, df = alarm_sql_execute(
            cursor, query, operator_str, expected_value
        )
        max_delay_hours = str(getattr(df[escalation_column], escalation_aggregation)()) if escalation_column is not None else None

        if (
            alarm_triggered and not df.empty
            and escalation_column and escalation_threshold is not None
            and escalation_column in df.columns
            and escalation_operator in OPERATORS
            and OPERATORS[escalation_operator](
                getattr(df[escalation_column], escalation_aggregation)(),
                escalation_threshold
            )
        ):
            priority = 'HIGH'
        print(json.dumps({
            'status': 'OK',
            'priority': priority,
            'query_id': query_id,
            'query': query,
            'expected_scenario': f"row_count {operator_str} {expected_value}",
            'row_count': row_count,
            'max_delay_hours': max_delay_hours,
            'client': client,
            'env': env,
        }))
        if alarm_triggered:
            raise AlarmException(f"row_count ({row_count}) does not satisfy expected: {operator_str} {expected_value}")
        return {
            'statusCode': 200,
            'body': json.dumps({'alarm_triggered': False, 'client': client, 'env': env})
        }

    except AlarmException as e:
            # 2. DRY (Don't Repeat Yourself): Create the payload dictionary once
            error_details = {
                'status': 'AQA_Failed',
                'priority': priority,
                'query_id': query_id,
                'query': query,
                'expected_scenario': f"row_count {operator_str} {expected_value}",
                'row_count': row_count,
                'client': client,
                'env': env,
                'message': str(e),
            }

            # 3. Log structured JSON to CloudWatch
            print(json.dumps(error_details))

            # 4. Format a human-readable message for the SNS Email
            error_message = (
                f"Snowflake Data Quality Alert [{env.upper()}]\n"
                f"----------------------------------------\n"
                f"Client: {client}\n"
                f"Priority: {priority}\n"
                f"Query ID: {query_id}\n"
                f"Expected: {error_details['expected_scenario']}\n"
                f"Actual Row Count: {row_count}\n"
                f"Max Delay Hours: {max_delay_hours}\n"
                f"Error: {str(e)}\n\n"
                f"Query:\n{query}"
            )

            # 5. Publish to SNS with basic error handling
            try:
                sns_client = boto3.client('sns')
                SNS_TOPIC_ARN = 'arn:aws:sns:ap-southeast-2:859004686855:DataTeamMailTopic'
                sns_client.publish(
                    TopicArn=SNS_TOPIC_ARN,
                    Subject=f'[{priority}] Snowflake DQ Alert: {client} ({env})',
                    Message=error_message
                )
            except Exception as sns_e:
                # Prevent SNS failure from masking the original Snowflake failure
                print(f"CRITICAL: Failed to publish to SNS. Error: {str(sns_e)}")

            # 6. Return a clean, nested response
            return {
                'statusCode': 300,
                'body': json.dumps({
                    'alarm_triggered': True,
                    'details': error_details
                })
            }


    except Exception as e:
        print(json.dumps({
            'status': 'ERROR',
            'priority': priority,
            'query_id': query_id,
            'query': query,
            'expected_scenario': f"row_count {operator_str} {expected_value}",
            'client': client,
            'env': env,
            'message': str(e),
        }))
        error_message = str(e)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_message, 'client': client, 'env': env})
        }
    finally:
        write_history(
            cursor, snf_secret_name, event, client, history_schema,
            executed_at, env, client, object_name, query, query_id,
            operator_str, expected_value, alarm_triggered, row_count,
            results, error_message, priority
        )

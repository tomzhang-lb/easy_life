import boto3
import json
import snowflake.connector

session = boto3.Session(profile_name="DataEngineer-859004686855")


def get_dw_secrets():
    secret_name = "data_dw_sysuser"
    region = session.region_name
    client = session.client(
        service_name='secretsmanager',
        region_name=region
    )
    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
    secret = get_secret_value_response['SecretString']
    secret_json = json.loads(secret)
    return secret_json


def get_snowflake_con(secret_json, schema, tenant):
    ctx = snowflake.connector.connect(
                user=secret_json['snowflakeUser'],
                password=secret_json['snowflakePassword'],
                account=secret_json['snowflakeAccount'],
                warehouse='dev_compute_wh',
                database='{0}_DATAWAREHOUSE'.format(tenant),
                schema=schema,
                role='data_dw_sysrole'
    )
    return ctx


def create_procedure(tenant, schema, snowflake_cur):
    try:
        ddl = f"""
                CREATE OR REPLACE PROCEDURE {tenant}_DATAWAREHOUSE.{schema}.DROP_OLD_SCHEMAS
                (
                    days_threshold FLOAT,
                    keyword ARRAY,
                    dry_run BOOLEAN DEFAULT TRUE
                )
                RETURNS VARCHAR
                LANGUAGE JAVASCRIPT
                EXECUTE AS CALLER 
                AS
                $$
                    var days = Math.floor(DAYS_THRESHOLD);
                    var keyword = KEYWORD;
                    var isDryRun = DRY_RUN;
                    
                    var results = [];           
                    var droppedCount = 0; 
                    var errors = [];
                    var dropQuery = '';
                    var schema_exclusion = ['%1477%', '%6915%', '%6917%', '%1650%', 'fw_'].map(x => `'${{x}}'`).join(", ");
                    var schema_keywords = keyword.map(x => `lower('%${{x}}%')`).join(", ");
                
                    /* 
                     * get all the schema needs to be drop, 
                     * condition needs to be further refined as it may delete some UAT environment 
                     * a whitelist schema register table might be good option 
                     */
                    var findQuery = `
                        SELECT
                            catalog_name,
                            schema_name,
                            created,
                            DATEDIFF('day', created, CURRENT_TIMESTAMP()) AS days_old
                        FROM information_schema.schemata
                        WHERE LOWER(schema_name) LIKE ANY (${{schema_keywords}})
                            AND REGEXP_LIKE(schema_name, '\\\\\\\\w+\\\\\\\\d[0-9]{{3,}}\\\\\\\\w+')
                            AND NOT (schema_name like any (${{schema_exclusion}}))
                            AND DATEDIFF('day', created, CURRENT_TIMESTAMP()) > ${{days}}
                        ORDER BY schema_name
                    `;
                    
                    var stmt = snowflake.createStatement({{sqlText: findQuery}});
                    var resultSet = stmt.execute();
                    
                    while (resultSet.next()) {{
                        var DbName = resultSet.getColumnValue(1);
                        var schemaName = resultSet.getColumnValue(2);
                        var createdDate = resultSet.getColumnValue(3);
                        var daysOld = resultSet.getColumnValue(4);

                        var info = {{
                            db: DbName,
                            schema: schemaName,
                            created: createdDate,
                            days_old: daysOld,
                            status: ''
                        }};

                        if (isDryRun) {{
                            info.status = 'DRY RUN - Would be dropped';
                        }} else {{
                            try {{
                                dropQuery = `DROP SCHEMA IF EXISTS ${{DbName}}.${{schemaName}} CASCADE;`;
                                droppedCount++;
                            }} catch (err) {{
                                info.status = 'ERROR: ' + err.message;
                                errors.push(schemaName + ': ' + err.message);
                            }}
                        }}

                        results.push(dropQuery);
                    }}

                    var summary = {{
                        dry_run: isDryRun,
                        threshold_days: days,
                        schemas_found: results.length,
                        schemas_dropped: droppedCount,
                        errors: errors,
                        details: results
                    }};

                    return JSON.stringify(summary, null, 2);
                $$
                ;
        """
        # print(ddl)
        snowflake_cur.execute(ddl)
        query_id = snowflake_cur.sfqid
        print(f'Procedure creation query_id: ' + query_id)
    except Exception as e:
        print(e)


def call_procedure(tenant, schema, snowflake_cur, retention_days=365, schema_keyword=['IAD_DEV',], dry_run='FALSE' ) -> list:
    call_procedure = f"""
        call {tenant}_DATAWAREHOUSE.{schema}.DROP_OLD_SCHEMAS({retention_days}, {schema_keyword}, {dry_run})
        ;
    """
    # print(call_procedure)
    snowflake_cur.execute(call_procedure)
    call_result = snowflake_cur.fetchone()
    print(f'call_result: ' + call_result[0])
    call_result_dict = json.loads(call_result[0])
    return call_result_dict['details']


def lambda_handler(event, context):
    tenant = event['client']
    schema = event['schema']
    retention_days = event['retention_days']
    schema_keyword = event['schema_keyword']
    dry_run = event['dry_run']
    secret_json = get_dw_secrets()
    snowflake_ctx = get_snowflake_con(secret_json, schema, tenant)
    snowflake_cur = snowflake_ctx.cursor()
    # build procedure
    create_procedure(tenant, schema, snowflake_cur)
    # call procedure
    drop_schema_statements = call_procedure(tenant, schema, snowflake_cur, retention_days=retention_days, schema_keyword=schema_keyword, dry_run=dry_run )

    for drop_schema_cmd in drop_schema_statements:
        schema = drop_schema_cmd.split(' ')[4]
        drop_yn = input('Do you want to (y/n):  ' + schema + '\n')
        if drop_yn.lower() == 'y':
            try:
                # snowflake_cur.execute(drop_schema_cmd)
                print(f'Successfully dropped: {schema} ')
            except Exception as e:
                print(e)
        else:
            print(f'DO NOT dropped: {schema}')

    snowflake_ctx.close()


if __name__ == '__main__':
    event = {
        "client": "TMGM",
        "schema": "DQRC",
        "retention_days": 90,
        "schema_keyword": ["DEV_IAD", "UAT_IAD"],
        "dry_run": "False"
    }
    lambda_handler(event, None)

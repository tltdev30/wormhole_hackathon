import json
import pandas as pd
import numpy as np
from io import StringIO
import snowflake.connector
from snowflake.connector import DictCursor
import io
import datetime
import string
import boto3


def lambda_handler(event, context):

    # Parse the event received from the previous Lambda function to get list of tables having changes
    df = pd.json_normalize(event)
    changed_tables = df.TableName.unique()
    
    config_df = read_file_from_s3('wormholeltd-bucket', 'config/config-snowflake.csv')
    config = config_df.to_dict(orient='records')

    for table in changed_tables:
        df_t = df[df['TableName']==table]
        schema = ' '.join(df['TableSchema'].unique())
        alter_statements = ''
        need_approval = False
        changes = ''
        
        # For each change, we generate a corresponding DDL query and classify whether it requires approval or can be automatically deployed
        for index, row in df_t.iterrows():
            if row['ChangeType'] == 'CHANGED DATA TYPE':
                need_approval = True
                change = f'{row["ORIGINAL_COLUMN_NAME"]} is changed datatype from {row["OLD_DATA_TYPE"]}({row["OLD_DATA_LENGTH"]}) to {row["NEW_DATA_TYPE"]}({row["NEW_DATA_LENGTH"]})'
                alter_str = f'alter table {row["Database"]}.{row["TableSchema"]}.{row["TableName"]} alter column {row["ORIGINAL_COLUMN_NAME"]} {row["NEW_DATA_TYPE"]}({row["NEW_DATA_LENGTH"]})'
            elif row['ChangeType'] == 'ADD NEW COLUMN':
                change = f'{row["NEW_COLUMN_NAME"]} is added'
                alter_str = f'alter table {row["Database"]}.{row["TableSchema"]}.{row["TableName"]} add column {row["NEW_COLUMN_NAME"]} {row["NEW_DATA_TYPE"]}'
            elif row['ChangeType'] == 'RENAME COLUMN':
                change = f'{row["ORIGINAL_COLUMN_NAME"]} is renamed to {row["NEW_COLUMN_NAME"]}'
                need_approval = True
                alter_str = f'alter table {row["Database"]}.{row["TableSchema"]}.{row["TableName"]} rename column {row["ORIGINAL_COLUMN_NAME"]} to {row["NEW_COLUMN_NAME"]}'
            elif row['ChangeType'] == 'REMOVE COLUMN':
                change = f'{row["ORIGINAL_COLUMN_NAME"]} is removed'
                need_approval = True
                alter_str = f'alter table {row["Database"]}.{row["TableSchema"]}.{row["TableName"]} drop column {row["ORIGINAL_COLUMN_NAME"]}'            
            alter_statements += alter_str + ';\n'
            changes += change + ',\n'
        
        # Insert DDL query into DDL_HISTORY table
        if need_approval:
            insert_ddl_history(schema, table, 'requesting approval', alter_statements, changes, config)
        else:
            insert_ddl_history(schema, table, 'pending deployment', alter_statements, changes, config)
    
    # invoke lambda noti-and-deploy-for-auto-deploy-cases
    invoke_lambda('arn:aws:lambda:ap-southeast-2:316920261407:function:noti-and-deploy-for-auto-deploy-cases')

    return {
        'statusCode': 200,
        'body': json.dumps('generate and insert DDL into DDL_HISTORY table successfully')
    }


def read_file_from_s3(bucket_name, file_key) -> None:
    try:
        s3_resource = boto3.resource('s3')
        s3_client = s3_resource.meta.client
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        csv_data = io.BytesIO(response['Body'].read())
        df = pd.read_csv(csv_data)
        return df
    except Exception as e:
        print("Error when get file from S3")
        raise e

def snowflake_credential(config):
    try:
        snowflake_secret = {
            "user": config[0]['user'] ,
            "password": config[0]['password'],
            "account": config[0]['account'],
            "schema": config[0]['schema'],
            "warehouse": config[0]['warehouse'],
            "database": config[0]['database'],
            "role": config[0]['role'],
        }
        return snowflake_secret

    except Exception as e:
        print('Error')
        raise e


def run_query(query, config):
    try:
        snowflake_secret = snowflake_credential(config)
        with snowflake.connector.connect(**snowflake_secret) as con:
            print("connect successfully!")
            print(f"SQL query: {query}")
            cur = con.cursor(DictCursor).execute(query)
            return cur
            
    except Exception as e:
        print("Error")
        raise e

    
def insert_ddl_history(schema, table_name, status, ddl_statement, changes, config):
    query_insert = f'''
                    insert into WORMHOLE.SCHEMA_MANAGEMENT.DDL_HISTORY 
                    (database_name, schema_name, table_name, status, changes, ddl_statement, created_at, updated_at)
                    values
                    ('WORMHOLE', '{schema}','{table_name}', '{status}', '{changes}', '{ddl_statement}', sysdate(), sysdate())
                '''
    run_query(query_insert, config)

    return query_insert

    
def invoke_lambda(FunctionName):
    client = boto3.client('lambda')

    res = client.invoke(
        	FunctionName = FunctionName,
        	InvocationType = 'RequestResponse',
        )
    return None
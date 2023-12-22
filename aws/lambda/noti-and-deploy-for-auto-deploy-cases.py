import json
import pandas as pd
import numpy as np
from io import StringIO
import snowflake.connector
from snowflake.connector import DictCursor
import io
from datetime import datetime
import string
import boto3
from urllib.request import Request, urlopen
from urllib.parse import unquote
import base64
from time import time
 
def lambda_handler(event, context):
    try:
        #get tables that need request approval from Snowflake
        config_df = read_file_from_s3('wormholeltd-bucket', 'config/config-snowflake.csv')
        config = config_df.to_dict(orient='records')
        
        query_table_auto_deploy = "SELECT * FROM WORMHOLE.SCHEMA_MANAGEMENT.DDL_HISTORY WHERE STATUS = 'pending deployment';" 
        request_auto_deploy = run_query(query_table_auto_deploy, config)
        
        #perform deployment for auto-deploy cases
        if request_auto_deploy.rowcount != 0:
            status = 'done deployment'
            for i in request_auto_deploy:
                id_change = i['ID']
                database_name = i['DATABASE_NAME']
                schema_name = i['SCHEMA_NAME']
                table_name = i['TABLE_NAME']
                ddl = i['DDL_STATEMENT']
                
                #deploy ddl to production
                for x in ddl.split(";"):
                    run_query(x, config)
                
                #execute ingestion pipeline
                QUERY = f"""call INGESTION.COPY_SP('{schema_name}.{table_name}');"""
                run_query(QUERY, config)
                
                current_timestamp = int(time() * 1000)

                # Send noti to slack for completed deploymnet
                slack_message = {
                    "attachments": 
                            [
                                {
                                    "color": "#36a64f",
                                    "pretext":"Having a detected schema change for `{}.{}.{}`".format(database_name,schema_name,table_name),
                                    "text": "_Status_: `{}`\n_DDL Statement_:\n```{}```".format(status, ddl),
                                    "footer": ':successful:   Auto deployment',
                                    "ts": current_timestamp
                              
                                }
                            ]
                        }
                        
                sent_to_slack(slack_message)
                    
                #update status in snowflake database after deploy
                query_update_status = "UPDATE WORMHOLE.SCHEMA_MANAGEMENT.DDL_HISTORY SET STATUS = '{}' WHERE ID = {};".format(status, id_change)
                request_approval_tbl = run_query(query_update_status, config)
        
        #Invoke request approval lambda to resolve for needed-approval cases
        invoke_lambda('arn:aws:lambda:ap-southeast-2:316920261407:function:request-approval')
        
        return  None
    except Exception as e:
        print('Error')
        raise e




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
            "role": config[0]['role']
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
        
def sent_to_slack(slack_message):
    try: 
        config_df = read_file_from_s3('wormholeltd-bucket', 'config/config-snowflake.csv')
        config = config_df.to_dict(orient='records')
        slack_webhook_url = config[0]['slack_webhook_url']
        req = Request(slack_webhook_url, json.dumps(slack_message).encode('utf-8'))
        response = urlopen(req)
        response.read()
        return None
    except Exception as e:
        print('Error')
        raise e
        
def invoke_lambda(FunctionName):
    client = boto3.client('lambda')

    res = client.invoke(
        	FunctionName = FunctionName,
        	InvocationType = 'RequestResponse',
        )
    return None
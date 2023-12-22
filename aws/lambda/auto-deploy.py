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
from urllib.request import Request, urlopen
from urllib.parse import unquote
import base64

def lambda_handler(event, context):
    try:
        #build connection to snowflake
        config_df = read_file_from_s3('wormholeltd-bucket', 'config/config-snowflake.csv')
        config = config_df.to_dict(orient='records')
        
        #get response infomation from slack
        body_event = unquote(base64.b64decode(event['body']).decode('utf8'))
        payload =  json.loads(body_event.split("payload=",1)[1])
        res = payload['actions'][0]['value'].replace("+", " ")
        res_content = json.loads(res)
        is_approve = res_content["approve"]
        id_change = res_content['id_change']
        database_name = res_content['database_name']
        schema_name = res_content['schema_name']
        table_name = res_content['table_name']
        ddl = res_content['ddl']
        action_ts = payload['action_ts']
        response_by = payload['user']['name']
        
        #Check response
        if is_approve: 
            action = ':successful:   Approved'
            status = 'done deployment'
            
            #deploy ddl to production
            for i in ddl.split(";"):
                run_query(i, config)
            
            #execute ingestion pipeline
            QUERY = f"""call INGESTION.COPY_SP('{schema_name}.{table_name}');"""
            run_query(QUERY, config)
                
        else: 
            action = ":error-deny:   Denied"
            status = 'deny deployment'
        
       
        #update status on snowflake database
        query_update_status = "UPDATE WORMHOLE.SCHEMA_MANAGEMENT.DDL_HISTORY SET STATUS = '{}' WHERE ID = {};".format(status, id_change)
        request_approval_tbl = run_query(query_update_status, config)
        
        #Send noti to slack
        slack_message = {
            "attachments": 
                    [
                        {
        
                            "color": "#36a64f",
                            "pretext":"Having a detected schema change for `{}.{}.{}`".format(database_name,schema_name,table_name),
                            "text": "_Status_: `{}`\n_DDL Statement_:\n```{}```".format(status, ddl),
                            "footer": "{} by {}".format(action, response_by),
                            "ts": action_ts
                        }
                    ]
                }        
        sent_to_slack(slack_message)
        return None 
        
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
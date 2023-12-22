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


def lambda_handler(event, context):
    try:
        #get tables that need request approval from Snowflake
        config_df = read_file_from_s3('wormholeltd-bucket', 'config/config-snowflake.csv')
        config = config_df.to_dict(orient='records')
        
        query_table_request_approval = "SELECT * FROM WORMHOLE.SCHEMA_MANAGEMENT.DDL_HISTORY WHERE STATUS = 'requesting approval';" 
        request_approval_tbl = run_query(query_table_request_approval, config)
        
        if request_approval_tbl.rowcount != 0:
            # sent request to slack 
            for i in request_approval_tbl:
                id_change = i['ID']
                database_name = i['DATABASE_NAME']
                schema_name = i['SCHEMA_NAME']
                table_name = i['TABLE_NAME']
                ddl = i['DDL_STATEMENT']
                
                slack_message = {
                    "text": "Having a detected schema change for `{}.{}.{}`\nProposed DDL:```{}```".format(database_name, schema_name, table_name, ddl),
                    "attachments": [
                        {
                            "pretext": "Would you like to deploy the proposed DDL to production?",
                            "fallback": "You are unable to promote a build",
                            "callback_id": "get_res",
                            "color": "#36a64f",
                            "attachment_type": "default",
                            "response_type": "in_channel",
                            "replace_original": False,
                            "delete_original": False,
                            "actions": [
                                {
                                    "name": "deployment",
                                    "text": "Yes",
                                    "style": "danger",
                                    "type": "button",
                                    "value": json.dumps({"approve": True, "id_change":id_change, "database_name": database_name, "schema_name": schema_name, "table_name": table_name, "ddl": ddl, "replace_original": False,
              "delete_original": False}),
                                    "confirm": {
                                        "title": "Are you sure?",
                                        "text": "This will deploy the build to production",
                                        "ok_text": "Yes",
                                        "dismiss_text": "No"
                                    }
                                },
                                {
                                    "name": "deployment",
                                    "text": "No",
                                    "type": "button",
                                    "value": json.dumps({"approve": False, "id_change":id_change, "database_name": database_name, "schema_name": schema_name, "table_name": table_name, "ddl": ddl, "replace_original": False,
              "delete_original": False})
                                }  
                            ]
                        }
                    ]
                }
                sent_to_slack(slack_message)
                
                
            #update after sending request
            query_update_status = "UPDATE WORMHOLE.SCHEMA_MANAGEMENT.DDL_HISTORY SET STATUS = 'pending approval' WHERE STATUS = 'requesting approval';"
            request_approval_tbl = run_query(query_update_status, config)
            return "requested successfully"
            
        return "No tables need approval"
        
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
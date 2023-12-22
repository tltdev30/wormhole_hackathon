import boto3
import pandas as pd
from io import StringIO
import snowflake.connector
from snowflake.connector import DictCursor
import io
import datetime
import string
import json
from urllib.parse import unquote_plus

def lambda_handler(event, context):
    if event:
        # Get event from S3 when a new file is added to the S3 bucket
        bucket_name = str(event["Records"][0]["s3"]["bucket"]["name"])
        file_key = unquote_plus(str(event["Records"][0]["s3"]["object"]["key"]))
        print("Filename: ", file_key)

        config_df = read_file_from_s3('wormholeltd-bucket', 'config/config-snowflake.csv')
        config = config_df.to_dict(orient='records')
        
        # List the tables needing schema change monitoring
        table_schema = 'INGESTION'
        table_list = ['EMPLOYEES']
        log_change_union = pd.DataFrame()

        for table_name in table_list:
            if table_name in file_key:
                QUERY = f"""select COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH from information_schema.columns where table_catalog = 'WORMHOLE' and table_schema = '{table_schema}' and table_name = '{table_name}' ;"""
                df1 = pd.DataFrame.from_records(iter(run_query(QUERY, config)), columns=[x[0] for x in run_query(QUERY, config).description])
                df2 = get_schema_from_s3(bucket_name, file_key)
                log_change = compare(df1, df2, table_schema, table_name)
                if not log_change.empty:
                    # insert the change into CHANGES_HISTORY table
                    log_change.apply(update_table, args=[config], axis=1)
                    log_change_union = pd.concat([log_change_union, log_change])
                else:
                    # If there are no schema changes, then call the procedure to load data into the table
                    QUERY = f"""call INGESTION.COPY_SP('{table_schema}.{table_name}');"""
                    run_query(QUERY, config)

        # If any schema changes occur, invoke lambda generate-ddl to generate a DDL query for the change.
        if not log_change_union.empty:
            invoke_lambda('arn:aws:lambda:ap-southeast-2:316920261407:function:generate-ddl', log_change_union.to_json(orient='records'))
            return {
                            'statusCode': 200,
                            'body': log_change_union.to_json(orient='records')
                        }
        else:
            return {
                            'statusCode': 200,
                            'body': json.dumps('no changes')
                        }
       
                        
def invoke_lambda(FunctionName, Payload):
    client = boto3.client('lambda')

    res = client.invoke(
        	FunctionName = FunctionName,
        	InvocationType = 'RequestResponse',
        	Payload = Payload
        )
    return Payload

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

def get_schema_from_s3(bucket_name, file_key) -> None:
    df = read_file_from_s3(bucket_name, file_key)
    schema_type = {column: str(df[column].dtype) for column in df.columns}
    schema_type_df = pd.DataFrame({'S3_COLUMN_NAME' : schema_type.keys() , 'S3_DATA_TYPE' : schema_type.values() })
    schema_length = {column: df[column].astype(str).str.len().max() for column in df}
    schema_length_df = pd.DataFrame({'S3_COLUMN_NAME' : schema_length.keys() , 'S3_DATA_LENGTH' : schema_length.values() })
    schema_df = schema_type_df.merge(schema_length_df, left_on='S3_COLUMN_NAME', right_on='S3_COLUMN_NAME')
    return schema_df

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

def classify_change_type(row):
    if pd.notna(row['S3_COLUMN_NAME']) and row['S3_COLUMN_NAME_TRIM'] != row['S3_COLUMN_NAME']:
        return 'RENAME COLUMN'
    if pd.notna(row['S3_COLUMN_NAME_TRIM']) and pd.isna(row['COLUMN_NAME']):
        return 'ADD NEW COLUMN'
    if pd.notna(row['COLUMN_NAME']) and pd.isna(row['S3_COLUMN_NAME_TRIM']):
        return 'REMOVE COLUMN'
    else:
        return None

def classify_change_data_type(row):
    if (row['DATA_TYPE'] == 'TEXT') & (row['CHARACTER_MAXIMUM_LENGTH'] < row['S3_DATA_LENGTH']):
        return 'CHANGED DATA TYPE'
    if (row['DATA_TYPE'] != row['S3_DATA_TYPE'] != 'int64'):
        return 'CHANGED DATA TYPE'
    else:
        return None

def compare(df1, df2, table_schema, table_name):

        # Apply cleaning rules to standardize column names, such as removing prefixes or suffixes.
        trim(df2)
        current_time = datetime.datetime.now()

        # Compare the current schema in S3 against Snowflake versions to detect changes
        # 1. Detect renaming, deletion, or addition of columns

        merged_df_col = pd.merge(df1, df2, left_on='COLUMN_NAME', right_on='S3_COLUMN_NAME_TRIM', how='outer')
        merged_df_col['Is_changed'] = (merged_df_col['COLUMN_NAME'] != merged_df_col['S3_COLUMN_NAME'])
        print(merged_df_col)
        filtered_df_col = merged_df_col.loc[(merged_df_col['Is_changed'] == True)]
        if not filtered_df_col.empty:
            filtered_df_col['ChangeType'] = filtered_df_col.apply(classify_change_type, axis=1)
            filtered_df_col['Database'] = 'WORMHOLE'
            filtered_df_col['TableSchema'] = table_schema
            filtered_df_col['TableName'] = table_name
            filtered_df_col['Created_at'] = current_time
        filtered_df_col = filtered_df_col.rename(columns={'COLUMN_NAME': 'ORIGINAL_COLUMN_NAME', 'S3_COLUMN_NAME': 'NEW_COLUMN_NAME', 'DATA_TYPE': 'OLD_DATA_TYPE', 'CHARACTER_MAXIMUM_LENGTH':'OLD_DATA_LENGTH', 'S3_DATA_TYPE': 'NEW_DATA_TYPE', 'S3_DATA_LENGTH': 'NEW_DATA_LENGTH'})
        
        
        # 2. Detect datatype change
        merged_df_data_type = pd.merge(df1, df2, left_on='COLUMN_NAME', right_on='S3_COLUMN_NAME_TRIM', how='outer')
        merged_df_data_type['Is_changed'] = ((merged_df_data_type['DATA_TYPE'] == 'TEXT') & (merged_df_data_type['CHARACTER_MAXIMUM_LENGTH'] < merged_df_data_type['S3_DATA_LENGTH'])) | ((merged_df_data_type['DATA_TYPE'] == 'NUMBER') & (merged_df_data_type['S3_DATA_TYPE'] != 'int64')) | ((merged_df_data_type['DATA_TYPE'] == 'FLOAT') & (merged_df_data_type['S3_DATA_TYPE'] != 'float64'))
        filtered_df_data_type = merged_df_data_type.loc[(merged_df_data_type['Is_changed'] == True)]
        if not filtered_df_data_type.empty:
            filtered_df_data_type['ChangeType'] = filtered_df_data_type.apply(classify_change_data_type, axis=1)
            filtered_df_data_type['Database'] = 'WORMHOLE'
            filtered_df_data_type['TableSchema'] = table_schema
            filtered_df_data_type['TableName'] = table_name
            filtered_df_data_type['Created_at'] = current_time
        
        filtered_df_data_type = filtered_df_data_type.rename(columns={'COLUMN_NAME': 'ORIGINAL_COLUMN_NAME', 'S3_COLUMN_NAME': 'NEW_COLUMN_NAME', 'DATA_TYPE': 'OLD_DATA_TYPE', 'CHARACTER_MAXIMUM_LENGTH':'OLD_DATA_LENGTH', 'S3_DATA_TYPE': 'NEW_DATA_TYPE', 'S3_DATA_LENGTH': 'NEW_DATA_LENGTH'})
    
        
        filtered_df = pd.concat([filtered_df_col,filtered_df_data_type])
        
        if not filtered_df.empty:
            preprocess(filtered_df)
            filtered_df = filtered_df.sort_values(by=['ChangeType'], ascending=True)

        return filtered_df

def trim(df):
    trans = str.maketrans('', '', string.punctuation)
    df['S3_COLUMN_NAME_TRIM'] = df['S3_COLUMN_NAME'].str.translate(trans)

def preprocess(df):
    df['NEW_DATA_LENGTH'].fillna(0, inplace = True)
    df['OLD_DATA_LENGTH'].fillna(0, inplace = True)
    df['NEW_DATA_TYPE'] = df['NEW_DATA_TYPE'].str.replace('object','TEXT')
    df['NEW_DATA_TYPE'] = df['NEW_DATA_TYPE'].str.replace('int64','NUMBER')
    df['NEW_DATA_TYPE'] = df['NEW_DATA_TYPE'].str.replace('float64','FLOAT')
    df['NEW_DATA_LENGTH'] = df['NEW_DATA_LENGTH'].astype('int')
    df['OLD_DATA_LENGTH'] = df['OLD_DATA_LENGTH'].astype('int')

def update_table(row, config):
    db_query = f"""insert into SCHEMA_MANAGEMENT.CHANGE_HISTORY (database_name, schema_name, table_name, old_column_name, old_data_type, old_data_length, new_column_name, new_data_type, new_data_length, change_type, created_at, updated_at ) values('{row['Database']}', '{row['TableSchema']}', '{row['TableName']}', '{row['ORIGINAL_COLUMN_NAME']}', '{row['OLD_DATA_TYPE']}', '{row['OLD_DATA_LENGTH']}', '{row['NEW_COLUMN_NAME']}',  '{row['NEW_DATA_TYPE']}', '{row['NEW_DATA_LENGTH']}', '{row['ChangeType']}', '{row['Created_at']}', '{row['Created_at']}');"""
    run_query(db_query, config)
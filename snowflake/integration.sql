-- Create integration between s3 and Snowflake

CREATE STORAGE INTEGRATION integration_s3
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::316920261407:role/snowflake-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://wormholeltd-bucket/data/')
;

DESC INTEGRATION integration_s3;


-- Create file format
CREATE OR REPLACE FILE FORMAT WORMHOLE.INGESTION.CSV_FORMAT
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null')
  EMPTY_FIELD_AS_NULL = true;


-- Create external stage
CREATE or replace STAGE WORMHOLE.INGESTION.S3_STAGE
  STORAGE_INTEGRATION = integration_s3
  URL = 's3://wormholeltd-bucket/data/'
  file_format = CSV_FORMAT;
;

LIST @WORMHOLE.INGESTION.S3_STAGE;
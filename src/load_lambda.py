import boto3
from botocore.exceptions import ClientError, InterfaceError
import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO
import logging
import json
from src.extract_lambda import retrieve_secrets
from sqlalchemy import create_engine


logger = logging.getLogger(__name__)

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.DEBUG,
)

logging.getLogger("botocore").setLevel(logging.INFO)


def lambda_handler(event, context):
    try:
        uploaded_tables = upload_dfs_to_database()
        if not uploaded_tables:
            return {
                "statusCode": 200,
                "body": json.dumps("No dataframes were uploaded."),
            }
        return {
            "statusCode": 200,
            "body": json.dumps(
                f"""The following dataframes were uploaded successfully: 
                {', '.join(uploaded_tables)} ."""
            ),
        }
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return {"statusCode": 500, "body": json.dumps("Internal server error.")}

# connect to database, slightly different way of doing it, to allow manipulation through pandas
def connect_to_db_and_return_engine():
    try:
        secrets = json.loads(retrieve_secrets("bentley-RDS-credentials"))  #need to amend retrieve secrets function
        host = secrets["host"]
        port = secrets["port"]
        user = secrets["user"]
        password = secrets["password"]
        database = secrets["database"]
        conn_str = f'postgresql+pg8000://{user}:{password}@{host}:{port}/{database}'
        engine = create_engine(conn_str) #interface between python (pandas) and SQL
        return engine
    except Exception as e:
        logger.error(f"Interface error: {e}")
        raise RuntimeError("Failed to create database engine")
    

# get transform bucket
def get_transform_bucket(client=None):
    if client is None:
        client = boto3.client("s3")
    try:
        response = client.list_buckets()
    except ClientError as e:
        logger.error(f"Error listing S3 buckets: {e}")
        raise RuntimeError("Error listing S3 buckets")
    
    transform_bucket_filter = [
        bucket["Name"] for bucket in response["Buckets"] if "transform" in bucket["Name"]
    ]

    if not transform_bucket_filter:
        logger.error("No transform bucket found")
        raise ValueError("No transform bucket found")

    return transform_bucket_filter[0]

# list and then retrieve parquet files from S3 bucket
# convert parquet files into dataframes
# return a dictionary of dataframes with name as key, and dataframe object as value 
def convert_parquet_files_to_dfs(bucket_name=None, client=None):
    try:
        if client is None:
            client = boto3.client("s3")
        if bucket_name is None:
            bucket_name = get_transform_bucket()
        files = client.list_objects_v2(Bucket=bucket_name)

        dfs = {}
        if "Contents" in files:
            for file in files["Contents"]:
                file_key = file['Key']
                try:
                    file_obj = client.get_object(Bucket=bucket_name, Key=file_key)
                    parquet_file = pq.ParquetFile(BytesIO(file_obj['Body'].read()))
                    df = parquet_file.read().to_pandas()
                    dfs[file_key] = df
                except ClientError as e:
                    logger.error(f"Unable to retrieve S3 object {file_key}: {e}")
                except Exception as e:
                    logger.error(f"Unable to process file {file_key}: {e}")
        else:
            logger.error(f"No files found in {bucket_name}.")
            return {}
    except ValueError as value_error:
        logger.error(f"Unable to list objects: {value_error}")
        raise
    except ClientError as client_error:
        logger.error(f"Unable to list objects: {client_error}")
        raise

    return dfs

def upload_dfs_to_database():
    uploaded = []
    dict_of_dfs = convert_parquet_files_to_dfs()
    db_engine = connect_to_db_and_return_engine()
    try:
        for table_name, df in dict_of_dfs.items():
            df.to_sql(table_name, con=db_engine, if_exists="replace", index=False)
            uploaded.append(table_name)
    except Exception as e:
        logger.error(f"Error uploading dataframes: {e}")
        raise
    db_engine.dispose()
    return uploaded

    # aiming to return a list of uploaded tables
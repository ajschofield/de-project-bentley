import boto3
from botocore.exceptions import ClientError
import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO
import logging
import json
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
        if not uploaded_tables["uploaded"]:
            return {
                "statusCode": 200,
                "body": json.dumps("No dataframes were uploaded."),
            }
        return {
            "statusCode": 200,
            "body": json.dumps(
                f"""The following dataframes were uploaded successfully: 
                {uploaded_tables["uploaded"]} ."""
            ),
        }
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return {"statusCode": 500, "body": json.dumps("Internal server error.")}


def retrieve_secrets():
    secret_name = "bentley-RDS-credentials"
    region_name = "eu-west-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        logger.error(f"Failed to retrieve secret {secret_name}: {str(e)}")
        raise e
    except KeyError:
        logger.error(f"Secret {secret_name} does not contain a SecretString")
        raise ValueError(f"Secret {secret_name} does not contain a SecretString")

    return get_secret_value_response["SecretString"]


# connect to database, slightly different way of doing it, to allow manipulation through pandas


def connect_to_db_and_return_engine():
    try:
        secrets = json.loads(retrieve_secrets())
        host = secrets["host"]
        port = secrets["port"]
        user = secrets["user"]
        password = secrets["password"]
        database = secrets["database"]
        conn_str = f"postgresql+pg8000://{user}:{password}@{host}:{port}/{database}"
        # interface between python (pandas) and SQL
        engine = create_engine(conn_str)
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
        bucket["Name"]
        for bucket in response["Buckets"]
        if "transform" in bucket["Name"]
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
                file_key = file["Key"]
                try:
                    file_obj = client.get_object(Bucket=bucket_name, Key=file_key)
                    parquet_file = pq.ParquetFile(BytesIO(file_obj["Body"].read()))
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
    upload_status = {"uploaded": [], "not_uploaded": []}
    dict_of_dfs = convert_parquet_files_to_dfs()
    db_engine = connect_to_db_and_return_engine()
    immutable_df_dict = [
        "dim_counterparty.parquet",
        "dim_date.parquet",  # this needs to be mutable
        "dim_location.parquet",
        "dim_staff.parquet",
        "dim_design.parquet",
    ]
    mutable_df_dict = [
        "fact_sales_order",
        "fact_purchase_order",
        "fact_payment",
        "dim_currency",
    ]

    for file_name, df in dict_of_dfs.items():
        if file_name in immutable_df_dict:
            table_name = file_name.split(".")[0]
            try:
                df.to_sql(
                    table_name,
                    con=db_engine,
                    schema="project_team_2",
                    if_exists="append",
                    index=False,
                )
                upload_status["uploaded"].append(table_name)
            except Exception as e:
                logger.error(f"Error uploading dataframe {file_name} to database: {e}")
                raise
        elif file_name.rsplit("_", 1)[0] in mutable_df_dict:
            table_name = file_name.rsplit("_", 1)[0]
            try:
                df.to_sql(
                    table_name,
                    con=db_engine,
                    schema="project_team_2",
                    if_exists="append",
                    index=False,
                )
                upload_status["uploaded"].append(table_name)
            except Exception as e:
                logger.error(f"Error uploading dataframe {file_name} to database: {e}")
                raise
        else:
            upload_status["not_uploaded"].append(file_name)
            logger.error(f"{file_name} does not correspond with table in database")
    db_engine.dispose()
    return upload_status

if __name__ == "__main__":
    lambda_handler(None, None)

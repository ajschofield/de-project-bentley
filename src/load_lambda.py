import boto3
from botocore.exceptions import ClientError
import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO
import logging
import json
import traceback
from sqlalchemy import create_engine
from datetime import datetime as dt
import re

logger = logging.getLogger(__name__)

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO,
)
# logging.getLogger("botocore").setLevel(logging.INFO)
# logging.getLogger('sqlalchemy.engine').setLevel(logging.DEBUG)


def lambda_handler(event, context):
    try:
        uploaded_tables = upload_dfs_to_database()
        if uploaded_tables["not_uploaded"]:
            return {
                "statusCode": 200,
                "body": json.dumps("No dataframes were uploaded."),
            }
        elif uploaded_tables["uploaded"]:
            return {
                "statusCode": 200,
                "body": json.dumps(
                    f"""The following dataframes were uploaded successfully: 
                    {uploaded_tables["uploaded"]} ."""
                ),
            }
        else:
            logger.error(f"error", exc_info=True)
            return {"error"}
    except Exception as e:
        logger.error({e}, exc_info=True)
        return {"statusCode": 500, "body": {e}}


def retrieve_secrets(client=None, secret_name=None):
    session = boto3.session.Session()
    region_name = "eu-west-2"

    if secret_name == None:
        secret_name = "bentley-RDS-credentials"
    if client == None:
        client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        logger.error(
            f"Failed to retrieve secret {secret_name}: {str(e)}", exc_info=True
        )
        raise e
    except KeyError:
        logger.error(
            f"Secret {secret_name} does not contain a SecretString", exc_info=True
        )
        raise ValueError(f"Secret {secret_name} does not contain a SecretString")

    return get_secret_value_response["SecretString"]


# connect to database, slightly different way of doing it, to allow manipulation through pandas


def connect_to_db_and_return_engine(sm_secret=None):
    if sm_secret is None:
        sm_secret = json.loads(retrieve_secrets())

    try:
        secrets = sm_secret
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
        logger.error(f"Interface error: {e}", exc_info=True)
        raise RuntimeError("Failed to create database engine")


# get transform bucket
def get_transform_bucket(client=None):
    if client is None:
        client = boto3.client("s3")
    try:
        response = client.list_buckets()
    except ClientError as e:
        logger.error(f"Error listing S3 buckets: {e}", exc_info=True)
        raise RuntimeError("Error listing S3 buckets")

    transform_bucket_filter = [
        bucket["Name"]
        for bucket in response["Buckets"]
        if "transform" in bucket["Name"]
    ]

    if not transform_bucket_filter:
        logger.error("No transform bucket found", exc_info=True)
        raise ValueError("No transform bucket found")

    return transform_bucket_filter[0]


# list and then retrieve parquet files from S3 bucket
# convert parquet files into dataframes
# return a dictionary of dataframes with name as key, and dataframe object as value


def get_latest_timestamp(existing_files):
    if existing_files:
        all_datetimes = []
        for file_name in existing_files:
            match = re.search(r"\/(.+/).+_(.+)\.parquet", file_name)
            if match:
                datetime_str = "".join(match.group(1, 2))
                all_datetimes.append(dt.strptime(datetime_str, "%Y/%m/%d/%H:%M:%S"))
        return max(all_datetimes) if all_datetimes else dt.min
    return existing_files


def convert_parquet_files_to_dfs(bucket_name=None, client=None):
    mutable_df_dict = [
        "dim_currency",
        "fact_sales_order",
        "fact_purchase_order",
        "fact_payment",
    ]

    try:
        if client is None:
            client = boto3.client("s3")
        if bucket_name is None:
            bucket_name = get_transform_bucket()
        files = client.list_objects_v2(Bucket=bucket_name)

        dfs = {}
        if "Contents" in files:
            s3_key_list = [file["Key"] for file in files["Contents"]]
            immutables_l = []
            mutables_d = {prefix: [] for prefix in mutable_df_dict}
            for tab, s3_key in mutables_d.items():
                for file in s3_key_list:
                    if tab in file:
                        s3_key.append(file)
                    elif "2024" not in file:
                        immutables_l.append(file)
                    else:
                        continue
            immutables_l = list(set(immutables_l))
            latest_s3_keys = []
            for k, v in mutables_d.items():
                latest_s3_keys.append(
                    dt.strftime(
                        get_latest_timestamp(v), f"{k}/%Y/%m/%d/{k}_%H:%M:%S.parquet"
                    )
                )
            for file_key in immutables_l + latest_s3_keys:
                try:
                    file_obj = client.get_object(Bucket=bucket_name, Key=file_key)
                    parquet_file = pq.ParquetFile(BytesIO(file_obj["Body"].read()))
                    df = parquet_file.read().to_pandas()
                    # >> can't do 'any' (default) because we lose rows in dim_location
                    df_without_nulls = df.dropna(how="all")
                    # print("df_without_nulls", df_without_nulls)
                    # print("type", type(df_without_nulls))
                    # print(df_without_nulls.columns)
                    dfs[file_key] = df_without_nulls
                except ClientError as e:
                    logger.error(
                        f"Unable to retrieve S3 object {file_key}: {e}", exc_info=True
                    )
                except Exception as e:
                    logger.error(
                        f"Unable to process file {file_key}: {e}", exc_info=True
                    )
        else:
            logger.error(f"No files found in {bucket_name}.", exc_info=True)
            return {}
    except ValueError as value_error:
        logger.error(f"Unable to list objects: {value_error}", exc_info=True)
        raise
    except ClientError as client_error:
        logger.error(f"Unable to list objects: {client_error}", exc_info=True)
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
        "dim_transaction.parquet",  # This one was missing,
        "dim_payment_type.parquet",
    ]
    mutable_df_dict = [
        "dim_currency",
        "fact_sales_order",
        "fact_purchase_order",
        "fact_payment",
    ]
    with db_engine.begin() as connection:
        for file_name, df in dict_of_dfs.items():
            print(df.dtypes, "dtypes")
            print(df.head())
            print(file_name, "<<< FILE NAME")
            print(immutable_df_dict, "<<<IMMUTABLE_DF_DICT")
            if file_name in immutable_df_dict:
                table_name = file_name.split(".")[0]
                print(table_name, "<<<<<")
                try:
                    df.to_sql(
                        table_name,
                        con=connection,
                        schema="project_team_2",
                        if_exists="append",
                        index=False,
                    )
                    upload_status["uploaded"].append(table_name)
                    print(upload_status)
                except Exception as e:
                    logger.error(
                        f"Error uploading dataframe {file_name} to database: {e}",
                        exc_info=True,
                    )
                    raise
            elif file_name.split("/")[0] in mutable_df_dict:
                table_name = file_name.split("/")[0]
                print(table_name, "<<<<<<<TABLE NAME")
                try:
                    df.to_sql(
                        table_name,
                        con=connection,
                        schema="project_team_2",
                        if_exists="append",
                        index=False,
                    )
                    upload_status["uploaded"].append(table_name)
                except Exception as e:
                    logger.error(
                        f"Error uploading dataframe {file_name} to database: {e}",
                        exc_info=True,
                    )
                    raise
            else:
                upload_status["not_uploaded"].append(file_name)
                logger.error(
                    f"{file_name} does not correspond with table in database",
                    exc_info=True,
                )
            print(upload_status)
    db_engine.dispose()
    return upload_status


if __name__ == "__main__":
    lambda_handler(None, None)

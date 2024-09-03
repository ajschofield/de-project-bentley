import json
import boto3
import re
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from src.transform_lambda.dataframes import *
from botocore.exceptions import ClientError
from pg8000.native import Connection, InterfaceError
from datetime import datetime

class DBConnectionException(Exception):
    """Wraps pg8000.native Error or DatabaseError."""

    def __init__(self, e):
        """Initialise with provided error message."""
        self.message = str(e)
        super().__init__(self.message)


logger = logging.getLogger(__name__)

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.DEBUG,
)

logging.getLogger("botocore").setLevel(logging.WARNING)

TABLES = [
    "sales_order",
    "transaction",
    "payment",
    "counterparty",
    "address",
    "staff",
    "purchase_order",
    "department",
    "currency",
    "design",
    "payment_type",
]


def lambda_handler(event, context):
    db = None

    try:
        db = connect_to_database()
        bucket = bucket_name("transform")

        existing_s3_files = list_existing_s3_files(bucket)
        # print(existing_s3_files)

        dict_of_df = read_from_s3_subfolder_to_df(
            TABLES, bucket_name("extract"), client=boto3.client("s3")
        )

        immutable_df_dict = {
            "dim_counterparty": create_dim_counterparty(dict_of_df),
            "dim_date": create_dim_date(dict_of_df),
            "dim_location": create_dim_location(dict_of_df),
            "dim_staff": create_dim_staff(dict_of_df),
            "dim_design": create_dim_design(dict_of_df),
            "dim_transaction": create_dim_transaction(dict_of_df),
            "dim_payment_type": create_dim_payment_type(dict_of_df),
        }

        mutable_df_dict = {
            "fact_sales_order": create_fact_sales_order(dict_of_df),
            "fact_purchase_order": create_fact_purchase_orders(dict_of_df),
            "fact_payment": create_fact_payment(dict_of_df),
            "dim_currency": create_dim_currency(dict_of_df),
        }
        print(immutable_df_dict.values())
        print(mutable_df_dict.values())
        status = process_to_parquet_and_upload_to_s3(
            existing_s3_files, immutable_df_dict, mutable_df_dict, bucket
        )

        if not status["uploaded"]:
            logger.info("No dataframes written to the bucket.")
            return {
                "statusCode": 204,
                "body": json.dumps("No files where uploaded."),
            }

        return {
            "statusCode": 200,
            "body": json.dumps(
                f"""Parquet files processed for {', '.join(status['uploaded'])} and uploaded successfully.{
                'The following tables were not uploaded: '+', '.join([status['not_uploaded']]) if status['not_uploaded'] else ''}"""
            ),
        }

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return {"statusCode": 500, "body": json.dumps("Internal server error.")}
    finally:
        if db:
            db.close()


def process_to_parquet_and_upload_to_s3(
    existing_s3_files,
    immutable_df_dict,
    mutable_df_dict,
    bucket,
    client=boto3.client("s3"),
):
    status = {"uploaded": [], "not_uploaded": []}

    for table_name, df in immutable_df_dict.items():
        if table_name in existing_s3_files:
            status["not_uploaded"].append(table_name)
        else:
            parquet_file = df.to_parquet(
                f"{table_name}.parquet", engine="pyarrow"
            )  # or fastparquet
            # changed parquet_file variable to the file name
            client.upload_file(f"{table_name}.parquet", bucket, f"{table_name}.parquet")
            status["uploaded"].append(table_name)
            print(status)

    for table_name, df in mutable_df_dict.items():
        s3_key = datetime.strftime(
            datetime.today(), f"{table_name}/%Y/%m/%d/{table_name}_%H:%M:%S.parquet"
        )
        print(s3_key, '<<<< this is S3_Key')
        parquet_file = df.to_parquet(
            f"{table_name}.parquet", engine="pyarrow"
        )  # or fastparquet
        client.upload_file(f"{table_name}.parquet", bucket, s3_key)
        status["uploaded"].append(table_name)

    return status


def retrieve_secrets():
    secret_name = "bentley-secrets"
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


def connect_to_database() -> Connection:
    try:
        secrets = json.loads(retrieve_secrets())
        host = secrets["host"]
        port = secrets["port"]
        user = secrets["user"]
        password = secrets["password"]
        database = secrets["database"]

        return Connection(
            database=database, user=user, password=password, host=host, port=port
        )
    except InterfaceError as i:
        logger.error(f"Interface error: {i}")
        raise DBConnectionException("Failed to connect to database")


def read_from_s3_subfolder_to_df(tables, bucket, client=boto3.client("s3")):
    table_dfs = {}
    for table in tables:
        response = client.list_objects_v2(Bucket=bucket, Prefix=table)
        list_of_keys = [
            "s3://" + bucket + "/" + object["Key"] for object in response["Contents"]
        ]
        list_of_df = [pd.read_csv(key) for key in list_of_keys]
        table_dfs[table] = pd.concat(list_of_df)
    return table_dfs


def bucket_name(bucket_prefix, client=boto3.client("s3")):
    response = client.list_buckets()
    bucket_filter = [
        bucket["Name"]
        for bucket in response["Buckets"]
        if bucket_prefix in bucket["Name"]
    ]

    if not bucket_filter:
        raise ValueError(f"No bucket found with prefix: {bucket_prefix}")

    return bucket_filter[0]


def list_existing_s3_files(bucket_name, client=boto3.client("s3")):
    logging.info("Listing existing S3 files")

    try:
        response = client.list_objects_v2(Bucket=bucket_name)

        if "Contents" in response:
            existing_files = [obj["Key"] for obj in response["Contents"]]
        else:
            logger.error("The bucket is empty")
            return []  # changed from None to [] so it is an iterable

    except ClientError as e:
        logger.error(f"Error listing S3 objects: {e}")
        raise e

    return existing_files


if __name__ == "__main__":
    lambda_handler({}, "")

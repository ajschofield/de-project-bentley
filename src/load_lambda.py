import boto3
from botocore.exceptions import ClientError
from pg8000.native import Connection, InterfaceError, identifier
import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO

from botocore.exceptions import ClientError
import logging


logger = logging.getLogger(__name__)

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.DEBUG,
)

logging.getLogger("botocore").setLevel(logging.WARNING)
    
def convert_parquet_files_to_dfs(bucket_name=None, client=None):
    try:
        if client is None:
            client = boto3.client("s3")
        if bucket_name is None:
            bucket_name = "transform_bucket"
        files = client.list_objects_v2(Bucket=bucket_name)

        dfs = []
        for file in files:
            file_key = file['Key']
            try:
                file_obj = client.get_object(Bucket=bucket_name, Key=file_key)
                parquet_file = pq.ParquetFile(BytesIO(file_obj['body'].read()))
                df = parquet_file.read().to_pandas()
                dfs.append(df)
            except ClientError as e:
                logger.error(f"Unable to retrieve S3 object {file_key}: {e}")
    except ValueError as value_error:
        logger.error(f"Unable to list objects: {value_error}")
        raise
    except ClientError as client_error:
        logger.error(f"Unable to list objects: {client_error}")

    return dfs 
 
import boto3
from botocore.exceptions import ClientError
import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO
import logging


logger = logging.getLogger(__name__)

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.DEBUG,
)

logging.getLogger("botocore").setLevel(logging.WARNING)

# list and then retrieve parquet files from S3 bucket
# convert parquet files into dataframes and return a list of dataframes  
def convert_parquet_files_to_dfs(bucket_name=None, client=None):
    try:
        if client is None:
            client = boto3.client("s3")
        if bucket_name is None:
            bucket_name = "transform_bucket"
        files = client.list_objects_v2(Bucket=bucket_name)

        dfs = []
        if "Contents" in files:
            for file in files["Contents"]:
                file_key = file['Key']
                try:
                    file_obj = client.get_object(Bucket=bucket_name, Key=file_key)
                    parquet_file = pq.ParquetFile(BytesIO(file_obj['Body'].read()))
                    df = parquet_file.read().to_pandas()
                    dfs.append(df)
                except ClientError as e:
                    logger.error(f"Unable to retrieve S3 object {file_key}: {e}")
                except Exception as e:
                    logger.error(f"Unable to process file {file_key}: {e}")
        else:
            logger.error(f"No files found in {bucket_name}.")
            return []
    except ValueError as value_error:
        logger.error(f"Unable to list objects: {value_error}")
        raise
    except ClientError as client_error:
        logger.error(f"Unable to list objects: {client_error}")
        raise

    return dfs 

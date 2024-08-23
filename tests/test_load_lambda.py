import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO
from moto import mock_aws
import boto3
import os
import pytest
from src.load_lambda import lambda_handler, connect_to_db_and_return_engine, get_transform_bucket, convert_parquet_files_to_dfs, upload_dfs_to_database

@pytest.fixture(scope="class")
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURIT_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="class")
def s3_client(aws_credentials):
    with mock_aws():
        yield boto3.client("s3")

@pytest.fixture(scope="function")
def s3_mock_bucket(s3_client):
    bucket = s3_client.create_bucket(
        Bucket="transform_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    return bucket


class TestLambdaHandler:
    pass

class TestConnectToDBAndReturnEngine:
    pass

class TestGetTransformBucket:
    def test_get_transform_bucket_returns_string(self, s3_client, s3_mock_bucket):
        result = get_transform_bucket(s3_client)
        assert result == "transform_bucket"

class TestConvertParquetToDfs:
        pass

class TestUploadDfsToDatabase:
    pass
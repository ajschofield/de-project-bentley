import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO
from moto import mock_aws
import boto3
import botocore.exceptions
import os
import pytest
from src.load_lambda import *


@pytest.fixture(scope="class")
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="class")
def mock_s3_client(aws_credentials):
    with mock_aws():
        yield boto3.client("s3")
        

@pytest.fixture(scope="class")
def mock_sm_client(aws_credentials):
    with mock_aws():
        yield boto3.client("secretsmanager")


class TestLambdaHandler:
    pass


class TestRetrieveSecrets:
    def test_retrieve_secrets_returns_dictionary(self, mock_sm_client):
        secret = {
            "cohort_id": "test_cohort_id",
            "user": "test_user_id",
            "password": "test_password",
            "host": "test_host",
            "database": "test_database",
            "port": "test_port",
        }

        secret_name = "test_secret"

        mock_sm_client.create_secret(
            Name=secret_name, SecretString=json.dumps(secret)
        )

        result = retrieve_secrets(mock_sm_client, secret_name)

        assert isinstance(result, dict)

    def test_retrieve_secrets_returns_correct_keys_and_values(self, mock_sm_client):
        secret_name = "test_secret"

        result = retrieve_secrets(mock_sm_client, secret_name)

        assert result["user"] == "test_user_id"
        assert result["password"] == "test_password"

    def test_retrieve_secrets_returns_client_error_if_no_secret(self, mock_sm_client):
        secret_name = "another_test_secret"

        with pytest.raises(botocore.exceptions.ClientError) as error:
            retrieve_secrets(mock_sm_client, secret_name)


class TestConnectToDBAndReturnEngine:
    pass


class TestGetTransformBucket:
    def test_raises_value_error_if_no_buckets(self, mock_s3_client):
        with pytest.raises(ValueError, match="No transform bucket found"):
            get_transform_bucket(mock_s3_client)

    def test_raises_value_error_if_no_transform_bucket(self, mock_s3_client):
        mock_s3_client.create_bucket(
            Bucket="extract_bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        with pytest.raises(ValueError, match="No transform bucket found"):
            get_transform_bucket(mock_s3_client)

    def test_returns_transform_bucket_if_one_bucket(self, mock_s3_client):
        mock_s3_client.create_bucket(
            Bucket="transform_bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        result = get_transform_bucket(mock_s3_client)
        assert result == "transform_bucket"

    def test_only_returns_transform_bucket_if_several_buckets(self, mock_s3_client):
        mock_s3_client.create_bucket(
            Bucket="another_test_bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        result = get_transform_bucket(mock_s3_client)
        assert result == "transform_bucket"


class TestConvertParquetToDfs:
    def test_function_returns_empty_dictionary_if_no_files(self, mock_s3_client):
        mock_s3_client.create_bucket(
            Bucket="transform_bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        result = convert_parquet_files_to_dfs(
            bucket_name="transform_bucket", client=mock_s3_client
        )
        assert result == {}

    # def test_function_returns_dictionary_with_table_with_file_key():
    #     # need to mock parquet file and upload to mock bucket
    #     result = convert_parquet_files_to_dfs(bucket_name="transform_bucket", client=mock_s3_client)
    #     assert "dim_staff" in result


def mock_connect_db(mocker):
    return mocker.patch("src.load_lambda.connect_to_db_and_return_engine")

class TestUploadDfsToDatabase:
    pass
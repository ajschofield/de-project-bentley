import pandas as pd
from moto import mock_aws
import boto3
import botocore.exceptions
import os
import pytest
from src.load_lambda import (
    lambda_handler,
    retrieve_secrets,
    connect_to_db_and_return_engine,
    convert_parquet_files_to_dfs,
    get_transform_bucket,
    upload_dfs_to_database,
)
import tempfile
import json
from unittest.mock import MagicMock, patch


@pytest.fixture(scope="class")
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="class")
def mock_s3_client():
    with mock_aws():
        yield boto3.client("s3")


@pytest.fixture(scope="class")
def mock_sm_client():
    with mock_aws():
        yield boto3.client("secretsmanager")


class TestLambdaHandler:
    @staticmethod
    def test_lambda_handler_returns_200_and_table_name_if_uploaded(mocker):
        mocker.patch(
            "src.load_lambda.upload_dfs_to_database",
            return_value={"uploaded": ["table_one", "table_two"], "not_uploaded": []},
        )
        result = lambda_handler(None, None)
        assert result["statusCode"] == 200
        assert "table_one" in result["body"]
        assert "table_two" in result["body"]

    @staticmethod
    def test_lambda_handler_returns_200_and_table_name_if_not_uploaded(mocker):
        mocker.patch(
            "src.load_lambda.upload_dfs_to_database",
            return_value={"uploaded": [], "not_uploaded": ["table_one"]},
        )
        result = lambda_handler(None, None)
        assert result["statusCode"] == 200
        assert "No dataframes were uploaded" in result["body"]

    @staticmethod
    def test_lambda_handler_returns_error_if_both_lists_empty(mocker):
        mocker.patch(
            "src.load_lambda.upload_dfs_to_database",
            return_value={"uploaded": [], "not_uploaded": []},
        )

        result = lambda_handler(None, None)

        assert result == {"error"}


class TestRetrieveSecrets:
    @staticmethod
    def test_retrieve_secrets_returns_dictionary(mock_sm_client):
        secret = {
            "cohort_id": "test_cohort_id",
            "user": "test_user_id",
            "password": "test_password",
            "host": "test_host",
            "database": "test_database",
            "port": "test_port",
        }

        secret_name = "test_secret"

        mock_sm_client.create_secret(Name=secret_name, SecretString=json.dumps(secret))

        result = json.loads(retrieve_secrets(mock_sm_client, secret_name))

        assert isinstance(result, dict)

    @staticmethod
    def test_retrieve_secrets_returns_correct_keys_and_values(mock_sm_client):
        secret_name = "test_secret"

        result = json.loads(retrieve_secrets(mock_sm_client, secret_name))

        assert result["user"] == "test_user_id"
        assert result["password"] == "test_password"

    @staticmethod
    def test_retrieve_secrets_returns_client_error_if_no_secret(mock_sm_client):
        secret_name = "another_test_secret"

        with pytest.raises(botocore.exceptions.ClientError) as error:
            retrieve_secrets(mock_sm_client, secret_name)


class TestConnectToDBAndReturnEngine:
    @staticmethod
    def test_returns_unsuccessful_connection_when_wrong_credentials():
        sm_secret = {
            "host": "host",
            "port": "port",
            "user": "user",
            "password": "password",
            "database": "database",
        }

        with pytest.raises(Exception):
            connect_to_db_and_return_engine(json.dumps(sm_secret))


class TestGetTransformBucket:
    @staticmethod
    def test_raises_value_error_if_no_buckets(mock_s3_client):
        with pytest.raises(ValueError, match="No transform bucket found"):
            get_transform_bucket(mock_s3_client)

    @staticmethod
    def test_raises_value_error_if_no_transform_bucket(mock_s3_client):
        mock_s3_client.create_bucket(
            Bucket="extract_bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        with pytest.raises(ValueError, match="No transform bucket found"):
            get_transform_bucket(mock_s3_client)

    @staticmethod
    def test_returns_transform_bucket_if_one_bucket(mock_s3_client):
        mock_s3_client.create_bucket(
            Bucket="transform_bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        result = get_transform_bucket(mock_s3_client)
        assert result == "transform_bucket"

    @staticmethod
    def test_only_returns_transform_bucket_if_several_buckets(mock_s3_client):
        mock_s3_client.create_bucket(
            Bucket="another_test_bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        result = get_transform_bucket(mock_s3_client)
        assert result == "transform_bucket"


class TestConvertParquetToDfs:
    @staticmethod
    def test_function_returns_empty_dictionary_if_no_files(mock_s3_client):
        mock_s3_client.create_bucket(
            Bucket="transform_bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        result = convert_parquet_files_to_dfs(
            bucket_name="transform_bucket", client=mock_s3_client
        )
        assert result == {}

    @staticmethod
    def test_function_returns_dictionary_with_file_key_and_dataframe(mock_s3_client):
        with tempfile.TemporaryDirectory() as tmp:
            d = {
                "test": ["Hello", "Bye"],
                "design_id": ["Hello", "Bye"],
                "design_name": ["Hello", "Bye"],
                "file_name": ["Hello", "Bye"],
                "file_location": ["Hello", "Bye"],
                "Hello": ["Hello", "Bye"],
            }

            test_df = pd.DataFrame(data=d)

            path = os.path.join(tmp, "test_parquet.parquet")

            test_df.to_parquet(path, engine="pyarrow")

            with open(path, "rb") as p:
                mock_s3_client.put_object(
                    Bucket="transform_bucket", Key="test_parquet.parquet", Body=p.read()
                )

            result = convert_parquet_files_to_dfs(
                bucket_name="transform_bucket", client=mock_s3_client
            )

            assert "test_parquet.parquet" in result

            pd.testing.assert_frame_equal(result["test_parquet.parquet"], test_df)


class TestUploadDfsToDatabase:
    @pytest.fixture
    def mock_engine(self):
        engine = MagicMock()
        engine.dispose = MagicMock()
        return engine

    @pytest.fixture
    def mock_df(self):
        df = MagicMock(spec=pd.DataFrame)
        df.to_sql = MagicMock()
        return df

    @staticmethod
    def test_function_returns_dictionary_with_uploaded_and_not_uploaded_keys(
        mock_engine, mock_df
    ):
        with patch(
            "src.load_lambda.convert_parquet_files_to_dfs",
            return_value={"dim_counterparty.parquet": mock_df},
        ), patch(
            "src.load_lambda.connect_to_db_and_return_engine", return_value=mock_engine
        ):
            result = upload_dfs_to_database()

            assert "uploaded" in result
            assert "not_uploaded" in result

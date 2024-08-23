import boto3.exceptions
import botocore.exceptions
import pytest
import boto3
from moto import mock_aws
from unittest.mock import patch, MagicMock
from unittest import TestCase
import os
import logging
import json
from pg8000.native import InterfaceError


@pytest.fixture(scope="function", autouse=True)
def aws_mocks():
    with mock_aws():
        yield


@pytest.fixture
def mock_conn():
    with patch("src.extract_lambda.Connection") as mock:
        yield mock


@pytest.fixture(scope="function")
def mock_config():
    env_vars = json.dumps(
        {
            "host": "abc",
            "port": "5432",
            "user": "def",
            "password": "password",
            "database": "db",
        }
    )
    with patch(
        "src.extract_lambda.retrieve_secrets", return_value=env_vars
    ) as mock_config:
        yield mock_config


@pytest.fixture(scope="function", autouse=True)
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3_client(aws_credentials):
    with mock_aws():
        yield boto3.client("s3")


@pytest.fixture(scope="function")
def s3_mock_bucket(s3_client):
    bucket = s3_client.create_bucket(
        Bucket="extract_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    return bucket


from src.extract_lambda import (  # noqa: E402
    list_existing_s3_files,
    connect_to_database,
    DBConnectionException,
    lambda_handler,
    process_and_upload_tables,
    retrieve_secrets,
    extract_bucket,
)


class TestLambdaHandler:
    def test_files_processed_and_uploaded_successfully(self, mocker):
        mock_db = MagicMock()
        mock_db.run.side_effect = [
            [["Fruits"]],
            [["Vegetable", "Sour", "Green"], ["Berry", "Sweet", "Red"]],
            [["Food_type"], ["Flavour"], ["Colour"]],
        ]
        mock_db.columns.return_value = [
            {"name": "Food_type"},
            {"name": "Flavour"},
            {"name": "Colour"},
        ]
        with patch("src.extract_lambda.connect_to_database", return_value=mock_db):
            mock_process_and_upload_tables = mocker.patch(
                "src.extract_lambda.process_and_upload_tables",
                return_value={
                    "updated": ["Fruits"],
                    "no change": ["Vegetable", "Berry"],
                },
            )
            mock_list_existing_s3_files = mocker.patch(
                "src.extract_lambda.list_existing_s3_files", return_value={}
            )
            event = {}
            context = {}
            response = lambda_handler(event, context)
            assert response["statusCode"] == 200
            assert json.loads(response["body"]) == (
                "CSV files processed for Fruits and uploaded successfully."
                "The following tables were not updated: Vegetable, Berry"
            )
            mock_list_existing_s3_files.assert_called_once()
            mock_process_and_upload_tables.assert_called_once_with(mock_db, {})
            mock_db.close.assert_called_once()

    def test_no_changes_detected_no_files_uploaded(self, mocker):
        mock_db = MagicMock()
        mock_db.run.side_effect = [
            [["Fruits"]],
            [["Vegetable", "Sour", "Green"], ["Berry", "Sweet", "Red"]],
            [["Food_type"], ["Flavour"], ["Colour"]],
        ]
        mock_db.columns.return_value = [
            {"name": "Food_type"},
            {"name": "Flavour"},
            {"name": "Colour"},
        ]

        with patch("src.extract_lambda.connect_to_database", return_value=mock_db):
            mock_process_and_upload_tables = mocker.patch(
                "src.extract_lambda.process_and_upload_tables",
                return_value={"updated": [], "no change": ["Fruits"]},
            )
            mock_list_existing_s3_files = mocker.patch(
                "src.extract_lambda.list_existing_s3_files", return_value={}
            )
            event = {}
            context = {}
            response = lambda_handler(event, context)
            assert response["statusCode"] == 200
            assert (
                json.loads(response["body"])
                == "No changes detected, no CSV files were uploaded."
            )
            mock_list_existing_s3_files.assert_called_once()
            mock_process_and_upload_tables.assert_called_once_with(mock_db, {})
            mock_db.close.assert_called_once()

    def test_exception_error(self, mocker):
        with patch(
            "src.extract_lambda.connect_to_database",
            side_effect=Exception("Database connection error"),
        ):
            mock_process_and_upload_tables = mocker.patch(
                "src.extract_lambda.process_and_upload_tables"
            )
            mock_list_existing_s3_files = mocker.patch(
                "src.extract_lambda.list_existing_s3_files"
            )
            event = {}
            context = {}
            response = lambda_handler(event, context)
            assert response["statusCode"] == 500
            assert json.loads(response["body"]) == "Internal server error."
            mock_list_existing_s3_files.assert_not_called()
            mock_process_and_upload_tables.assert_not_called()


class TestExtractBucket:
    def test_extract_bucket_returns_bucket_name(self, s3_client, s3_mock_bucket):
        result = extract_bucket(s3_client)
        assert result == "extract_bucket"

    def test_bucket_returns_first_bucket(self, s3_client):
        # Redefine what the test does
        # Create two buckets and check that only extract_bucket is returned

        s3_client.create_bucket(
            Bucket="extract_bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.create_bucket(
            Bucket="bucket1",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        result = extract_bucket(s3_client)
        assert result == "extract_bucket"

    def test_raises_value_error_if_no_buckets(self, s3_client):
        with pytest.raises(ValueError, match="No extract_bucket found"):
            extract_bucket(s3_client)


class TestListExistingS3Files:
    def test_error_if_no_bucket(self, s3_client, caplog):
        logger = logging.getLogger()
        logger.info("Testing now.")
        caplog.set_level(logging.ERROR)

        # Mock the extract_bucket function to raise a ValueError!
        with patch(
            "src.extract_lambda.extract_bucket",
            side_effect=ValueError("No extract_bucket found"),
        ):
            with pytest.raises(ValueError, match="No extract_bucket found"):
                list_existing_s3_files(client=s3_client)

        assert "Error listing S3 objects" in caplog.text

    def test_error_if_bucket_is_empty(self, s3_client, caplog, s3_mock_bucket):
        list_existing_s3_files("extract_bucket", client=s3_client)
        assert "The bucket is empty" in caplog.text

    def test_retrieves_file_content(self, s3_client, caplog, s3_mock_bucket):
        s3_client.upload_file("tests/dummy.txt", "extract_bucket", "dummy.txt")
        result = list_existing_s3_files("extract_bucket", client=s3_client)
        assert list(result.values()) == ["This is a test file."]


class TestConnectToDatabase:
    def test_connect_to_database(mock_conn, mock_config):
        with patch("src.extract_lambda.Connection", autospec=True) as mock_conn:
            connect_to_database()
            mock_conn.assert_called_with(
                host="abc", user="def", port="5432", password="password", database="db"
            )

    def test_database_error(self, mock_config):  # had mock_config in param
        with pytest.raises(DBConnectionException):
            connect_to_database()

    def test_logs_interface_error(self, caplog, mock_config):
        # Use mock_config fixture which already mocks the retrieve_secrets
        # function to return JSON string with DB connection details
        logger = logging.getLogger()
        logger.info("Testing now.")
        caplog.set_level(logging.ERROR)

        with patch(
            "src.extract_lambda.Connection", side_effect=InterfaceError("Test error")
        ), pytest.raises(DBConnectionException):
            connect_to_database()

        assert "Interface error" in caplog.text


class TestProcessAndUploadTables:
    # Added missing mock_conn fixture
    def test_error_process_and_upload_tables(self, mock_conn, s3_client, caplog):
        caplog.set_level(logging.INFO)

        # Mock return values for database queries
        queries = [
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE';",
            "SELECT * FROM Fruits WHERE last_updated > :latest;",
            "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS where table_name = 'Fruits';",
        ]
        return_values = [
            [["Fruits"]],
            [],  # No new rows with a more recent last_updated timestamp
            [["Food_type"], ["Flavour"], ["Colour"], ["last_updated"]],
        ]
        vals = dict(zip(queries, return_values))

        # Patch the database connection and set return values for queries
        with patch("src.extract_lambda.Connection") as mock_db:
            mock_db().run.side_effect = return_values
            s3_key = "Fruits/2024/08/15/Fruits_16:46:30.csv"
            existing_files = {
                s3_key: "Food_type,Flavour,Colour,last_updated\nVegetable,Sour,Green,2022-11-03 14:20:49.962\nBerry,Sweet,Red,2022-11-03 14:20:49.962"
            }

            # Simulate S3 bucket and file setup
            s3_client.create_bucket(
                Bucket="test_extract_bucket",
                CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
            )
            s3_client.upload_file(
                "tests/dummy_identical.csv", "test_extract_bucket", s3_key
            )

            # Run the process_and_upload_tables function
            process_and_upload_tables(mock_db(), existing_files, client=s3_client)
            # Assert that the log contains "No new data"
            assert "No new data" in caplog.text

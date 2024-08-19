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
from src.extract_lambda import (
    list_existing_s3_files,
    connect_to_database,
    DBConnectionException,
    lambda_handler,
    process_and_upload_tables,
)


@pytest.fixture(scope="class")
def mock_config():
    env_vars = {
        "host": "abc",
        "port": "5432",
        "user": "def",
        "password": "password",
        "database": "db",
    }
    with patch(
        "src.extract_lambda.retrieve_secrets", return_value=env_vars
    ) as mock_config:
        yield mock_config


@pytest.fixture(scope="class")
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="class")
def s3_client(aws_credentials):
    with mock_aws():
        yield boto3.client("s3")


@pytest.fixture(scope="class")
def s3_mock_bucket(s3_client):
    bucket = s3_client.create_bucket(
        Bucket="extract_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    return bucket


class TestLambdaHandler:
    def test_lambda_handler_files_processed_and_uploaded_successfully(self, mocker):
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
                "src.extract_lambda.process_and_upload_tables", return_value=mock_db
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
                == "CSV files processed and uploaded successfully."
            )
            mock_list_existing_s3_files.assert_called_once()
            mock_process_and_upload_tables.assert_called_once_with(mock_db, {})
            mock_db.close.assert_called_once()

    def test_lambda_handler_no_changes_detected_no_files_uploaded(self, mocker):
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
                "src.extract_lambda.process_and_upload_tables", return_value=False
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

    def test_lambda_handler_exception_error(self, mocker):
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


class TestListExistingS3Files:
    def test_error_if_no_bucket(self, s3_client, caplog):
        logger = logging.getLogger()
        logger.info("Testing now.")
        caplog.set_level(logging.ERROR)
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
    # had mock_config in param
    def test_connect_to_database(mock_conn, mock_config):
        with patch("src.extract_lambda.Connection", autospec=True) as mock_conn:
            connect_to_database()
            mock_conn.assert_called_with(
                host="abc", user="def", port="5432", password="password", database="db"
            )

    def test_database_error(self, mock_config):  # had mock_config in param
        with pytest.raises(DBConnectionException):
            connect_to_database()

    def test_logs_interface_error(self, caplog):
        logger = logging.getLogger()
        logger.info("Testing now.")
        caplog.set_level(logging.ERROR)
        with pytest.raises(DBConnectionException):
            connect_to_database()
        assert "Interface error" in caplog.text

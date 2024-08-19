import boto3.exceptions
import botocore.exceptions
import pytest
import boto3
from moto import mock_aws
from unittest.mock import patch, MagicMock
from unittest import TestCase
from src.extract_lambda import (
    list_existing_s3_files,
    connect_to_database,
    DBConnectionException,
    process_and_upload_tables,
    extract_bucket,
)
import logging
import os


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

    # def test_error_retrieving_object(self, s3_client, caplog, s3_mock_bucket):
    #     s3_client.upload_file('tests/dummy.txt', 'extract_bucket', 'dummy.txt')

    #     list_existing_s3_files(bucket_name='extract_bucket', client=s3_client)

    #     assert 'Error retrieving S3 object dummy.txt: ClientError' in caplog.text

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


"""
class TestProcessAndUploadTables:
    def test_error_process_and_upload_tables(mock_conn, mock_config, s3_client, caplog):
        logger = logging.getLogger()
        logger.info('Testing now.')
        caplog.set_level(logging.ERROR)
        ####
        queries = ["SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE';",
                    "SELECT * FROM Fruits;",
                    "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS where table_name = 'Fruits'"]
        return_values = [[['Fruits']],
                        [['Vegetable','Sour','Green'],['Berry','Sweet','Red']],
                        [['Food_type'],['Flavour'],['Colour']]]
        vals = dict(zip(queries,return_values))

        ####
        with patch('src.extract_lambda.connect_to_database') as mock_db:
            mock_db().run.side_effects = return_values
            s3_key = 'Fruits/2024/08/15/Fruits_16:46:30.csv'
            existing_files = {s3_key: 'Food_type,Flavour,Colour\nFruit,Sour,Green\nBerry,Sweet,Red'}
            s3_client.create_bucket(Bucket='extract_bucket', 
                        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
            s3_client.upload_file('tests/dummy_identical.csv', 'extract_bucket', s3_key)
            process_and_upload_tables(mock_db(), existing_files, client=s3_client)
            assert 'No new data.' in caplog.text
"""

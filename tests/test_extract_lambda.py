import pytest
import boto3
from moto import mock_aws
from unittest.mock import patch
from unittest import TestCase
from src.extract_lambda import list_existing_s3_files, connect_to_database, DBConnectionException, process_and_upload_tables
import os 
import logging

@pytest.fixture(scope='class')
def mock_config():
    env_vars = {
        "host": "abc",
        "port": "5432",
        "user": "def",
        "password": "password",
        "database": "db",
    }
    with patch("src.extract_lambda.get_config", return_value=env_vars) as mock_config:
        yield mock_config


@pytest.fixture(scope='class')
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = 'testing'
    os.environ["AWS_SECRET_ACCESS_KEY"] = 'testing'
    os.environ["AWS_SECURIT_TOKEN"] = 'testing'
    os.environ["AWS_SESSION_TOKEN"] = 'testing'
    os.environ["AWS_DEFAULT_REGION"]= 'eu-west-2'

@pytest.fixture(scope='class')
def s3_client(aws_credentials):
    with mock_aws():
        yield boto3.client('s3')

class TestListExistingS3Files:
    def test_error_if_no_bucket(self, s3_client, caplog):

        logger = logging.getLogger()
        logger.info('Testing now.')
        caplog.set_level(logging.ERROR)
        list_existing_s3_files(client=s3_client)
        assert 'Error listing S3 objects' in caplog.text

    def test_error_if_bucket_is_empty(self, s3_client, caplog):

        s3_client.create_bucket(Bucket='extract_bucket', 
                                CreateBucketConfiguration={
                                    'LocationConstraint': 'eu-west-2'
                                })
        list_existing_s3_files(client=s3_client)
        assert 'The bucket is empty' in caplog.text 

    def test_error_retrieving_object(self, s3_client, caplog):
        s3_client.upload_file('tests/dummy.txt', 'extract_bucket', 'dummy.txt')
        list_existing_s3_files(bucket_name='test_bucket', client=s3_client)

        assert 'Error retrieving S3 object ' in caplog.text

    def test_retrieves_file_content(self, s3_client, caplog):
        result = list_existing_s3_files(client=s3_client)

        assert list(result.values()) == ['This is a test file.'] 

class TestConnectToDatabase:
    def test_connect_to_database(mock_conn, mock_config):
        with patch("src.extract_lambda.Connection", autospec=True) as mock_conn:  
            connect_to_database()
            mock_conn.assert_called_with(
            host="abc", user="def", port="5432", password="password", database="db"
            )

    def test_database_error(self, mock_config):
        with pytest.raises(DBConnectionException):
            connect_to_database()

    def test_logs_interface_error(self, caplog):
        logger = logging.getLogger()
        logger.info('Testing now.')
        caplog.set_level(logging.ERROR)
        with pytest.raises(DBConnectionException):
            connect_to_database()
        assert 'Interface error' in caplog.text

class TestProcessAndUploadTables:
    def test_error_process_and_upload_tables(mock_conn, mock_config, s3_client, caplog, mocker):
        logger = logging.getLogger()
        logger.info('Testing now.')
        caplog.set_level(logging.ERROR)

        with patch("src.extract_lambda.Connection", autospec=True) as mock_conn:
            mock_db = connect_to_database()
            # need to add a table 
            s3_key = 'dummy/2024/8/14/dummy_16:46:30.txt'
            mock_existing_files = mocker.Mock(return_value={s3_key: 'This is a test file.' })
            s3_client.create_bucket(Bucket='extract_bucket', 
                        CreateBucketConfiguration={
                            'LocationConstraint': 'eu-west-2'
                        })
            s3_client.upload_file('tests/dummy.txt', 'extract_bucket', s3_key)
            process_and_upload_tables(mock_db, mock_existing_files, client=s3_client)

            assert 'Error uploading to S3' in caplog.text

#@pytest.mark.describe("Helpers")
# @pytest.mark.it("Query processor returns correctly formatted dict")
# def test_process_query():
#     with patch("src.api.helpers.get_db_connection") as mock_conn:
#         mock_conn().run.side_effect = db_data
#         mock_conn().columns = sample_headers
#         result = process_query("test query")
#         assert result == sample_result
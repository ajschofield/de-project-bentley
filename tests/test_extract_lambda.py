import pytest
import boto3
from moto import mock_aws
from src.extract_lambda import list_existing_s3_files #process_and_upload_tables
import os 
import logging


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

class TestListExistings3Files():
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
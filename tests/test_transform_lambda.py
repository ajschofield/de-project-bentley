from src.transform_lambda import read_from_s3_subfolder_to_df
from moto import mock_aws
import pytest
import pandas as pd
import os
import boto3

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
class TestReadFromS3:
    
    def test_returns_dictionary_with_correct_value_pair(self,s3_client):
        s3_client.create_bucket(Bucket = 'dummy_buc',CreateBucketConfiguration={
                                    'LocationConstraint': 'eu-west-2'
                                })
        s3_client.upload_file('tests/dummy_identical.csv', 'dummy_buc', 'Foods/2024/08/21/Foods_12:03:10.csv')
        tables = ['Foods']
        result = read_from_s3_subfolder_to_df(tables,bucket='dummy_buc',client=s3_client)
        print(result)
        assert isinstance(result,dict)
        assert list(result.keys()) == 'Foods'
        assert isinstance(result['Foods'],pd.DataFrame)
        
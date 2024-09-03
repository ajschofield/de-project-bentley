from datetime import datetime
import logging
import io
import os
import numpy as np
from unittest.mock import patch, MagicMock
import boto3
import pandas as pd
from moto import mock_aws
from botocore.exceptions import ClientError
import pytest
from src.transform_lambda.transform_lambda import read_from_s3_subfolder_to_df, list_existing_s3_files, bucket_name, process_to_parquet_and_upload_to_s3, lambda_handler

logger = logging.getLogger()
logger.setLevel(logging.INFO)


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
def mock_extract_bucket(s3_client):
    mock_extract_bucket = s3_client.create_bucket(
        Bucket="dummy_extract_buc",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    return mock_extract_bucket

@pytest.fixture(scope="class")
def mock_transform_bucket(s3_client):
    mock_transform_bucket = s3_client.create_bucket(
        Bucket="dummy_transform_buc",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    return mock_transform_bucket

@pytest.fixture
def mock_db_connection():
    with patch('src.transform_lambda.transform_lambda.connect_to_database') as mock_connect:
        mock_db = MagicMock()
        mock_connect.return_value = mock_db
        yield mock_db

@pytest.fixture(scope="function")
def mock_df_functions():
    with patch('your_module.create_dim_counterparty') as mock_counterparty, \
         patch('your_module.create_dim_date') as mock_date, \
         patch('your_module.create_dim_location') as mock_location, \
         patch('your_module.create_dim_staff') as mock_staff, \
         patch('your_module.create_dim_design') as mock_design, \
         patch('your_module.create_fact_sales_order') as mock_sales, \
         patch('your_module.create_fact_purchase_orders') as mock_purchase, \
         patch('your_module.create_fact_payment') as mock_payment, \
         patch('your_module.create_dim_currency') as mock_currency:
        
        yield {
            'counterparty': mock_counterparty,
            'date': mock_date,
            'location': mock_location,
            'staff': mock_staff,
            'design': mock_design,
            'sales': mock_sales,
            'purchase': mock_purchase,
            'payment': mock_payment,
            'currency': mock_currency
        }


class TestReadFromS3:
    def test_returns_dictionary_with_correct_value_pair(
        self, s3_client, mock_extract_bucket
    ):
        s3_client.upload_file(
            "tests/dummy_identical.csv",
            "dummy_extract_buc",
            "Foods/2024/08/21/Foods_12:03:10.csv",
        )
        tables = ["Foods"]
        result = read_from_s3_subfolder_to_df(
            tables, bucket="dummy_extract_buc", client=s3_client
        )
        print(result)
        expected_df = pd.DataFrame(
            np.array(
                [
                    ["Vegetable", "Sour", "Green", "2022-11-03 14:20:49.962"],
                    ["Berry", "Sweet", "Red", "2022-11-03 14:20:49.962"],
                ]
            ),
            columns=["Food_type", "Flavour", "Colour", "last_updated"],
        )
        assert isinstance(result, dict)
        assert list(result.keys())[0] == "Foods"
        assert isinstance(result["Foods"], pd.DataFrame)
        assert result["Foods"].eq(expected_df, axis="columns").all(axis=None)

    def test_returns_dictionary_of_dataframes_for_multiple_tables(
        self, s3_client, mock_extract_bucket
    ):
        s3_client.upload_file(
            "tests/dummy_2.csv",
            "dummy_extract_buc",
            "Cars/2024/08/21/Cars_14:03:56.csv",
        )
        tables = ["Foods", "Cars"]
        result = read_from_s3_subfolder_to_df(
            tables, bucket="dummy_extract_buc", client=s3_client
        )
        expected_foods_df = pd.DataFrame(
            np.array(
                [
                    ["Vegetable", "Sour", "Green", "2022-11-03 14:20:49.962"],
                    ["Berry", "Sweet", "Red", "2022-11-03 14:20:49.962"],
                ]
            ),
            columns=["Food_type", "Flavour", "Colour", "last_updated"],
        )
        expected_cars_df = pd.DataFrame(
            np.array(
                [
                    ["Truck", "Chevrolet", "Grey"],
                    ["Convertible", "Mercedes", "Red"],
                    ["Van", "Volkswagen", "Blue"],
                ]
            ),
            columns=["Car_type", "Brand", "Colour"],
        )
        assert list(result.keys()) == tables
        assert result["Foods"].eq(expected_foods_df, axis="columns").all(axis=None)


class TestListExistingFiles:
    def test_functions_receives_error_if_no_bucket(self, s3_client, caplog):
        caplog.set_level(logging.INFO)

        with pytest.raises(ClientError):
            list_existing_s3_files("rando_bucket", client=s3_client)

        assert (
            "Error listing S3 objects: An error occurred (NoSuchBucket) when calling the ListObjectsV2 operation: The specified bucket does not exist"
            in caplog.text
        )

    def test_recieves_logger_error_if_no_files_listed(self, s3_client, caplog):
        caplog.set_level(logging.INFO)

        s3_client.create_bucket(
            Bucket="mock_bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        response = list_existing_s3_files("mock_bucket", client=s3_client)
        assert "The bucket is empty" in caplog.text

    def test_retrieves_existing_files(self, s3_client, caplog):
        caplog.set_level(logging.INFO)

        s3_client.upload_file("tests/dummy.txt", "mock_bucket", "dummy.txt")
        result = list_existing_s3_files("mock_bucket", client=s3_client)
        assert result == ["dummy.txt"]


class TestBucketName:
    def test_functions_retrieves__extractbucket(
        self, mock_extract_bucket, mock_transform_bucket, s3_client
    ):
        bucket = bucket_name("dummy_extract_buc", s3_client)
        assert bucket == "dummy_extract_buc"

    def test_transform_bucket_name(
        self, mock_extract_bucket, mock_transform_bucket, s3_client
    ):
        bucket2 = bucket_name("dummy_transform_buc", s3_client)
        assert bucket2 == "dummy_transform_buc"

    def test_recieves_error_when_bucket_doesnt_exist(
        self, mock_extract_bucket, s3_client
    ):
        s3_client.delete_bucket(Bucket="dummy_extract_buc")
        with pytest.raises(ValueError):
            bucket_name("dummy_extract_buc", s3_client)


class TestProcessToParquetUploadS3:
    def test_func_doesnt_upload_if_file_exists(self, mock_transform_bucket, s3_client):
        expected_cars_df = pd.DataFrame(
            np.array(
                [
                    ["Truck", "Chevrolet", "Grey"],
                    ["Convertible", "Mercedes", "Red"],
                    ["Van", "Volkswagen", "Blue"],
                ]
            ),
            columns=["Car_type", "Brand", "Colour"],
        )
        mock_dim_dict = {"car_data": expected_cars_df}

        response = process_to_parquet_and_upload_to_s3(
            ['car_data'], mock_dim_dict, {}, "dummy_transform_buc", s3_client
        )

        object_list = s3_client.list_objects_v2(Bucket='dummy_transform_buc')
        s3_uploaded_files = [obj['Key'] for obj in object_list.get('Contents', [])]
        assert 'car_data.parquet' not in s3_uploaded_files
        assert response == {"uploaded": [], "not_uploaded": ['car_data']}

    def test_func_uploads_data_if_doesnt_exist(self, mock_transform_bucket, s3_client):
        expected_flower_df = pd.DataFrame(
            np.array(
                [
                    ["Daisy", "White", "Edible"],
                    ["Rose", "Red", "Yes"],
                    ["Daffodil", "Yellow", "No"],
                ]
            ),
            columns=["Flower", "Colour", "Edible"],
        )
        mock_dim_dict = {"flower_data": expected_flower_df}


        response = process_to_parquet_and_upload_to_s3(
            ['car_data'], mock_dim_dict, {}, "dummy_transform_buc", s3_client
        )
        object_list = s3_client.list_objects_v2(Bucket='dummy_transform_buc')
        s3_uploaded_files = [obj['Key'] for obj in object_list.get('Contents', [])]
        # print(s3_uploaded_files, '<<<<<< the FILES IN DUMMY TRASN BUC')

        assert "flower_data.parquet" in s3_uploaded_files
        assert response == {"uploaded": ['flower_data'], "not_uploaded": []}
        
    def test_func_uploads_mutable_and_immutable_files(self, mock_transform_bucket, s3_client):
        expected_vegetable_df = pd.DataFrame(
            np.array(
                [
                    ["Carrot", "Orange", "Edible"],
                    ["Broccoli", "Green", "Yes"],
                ]
            ),
            columns=["Vegetable", "Colour", "Edible"],
        )
        
        expected_meat_df = pd.DataFrame(
            np.array(
                [
                    ["Chicken", "White", "Yes"],
                    ["Beef", "Red", "No"],
                ]
            ),
            columns=["Meat", "Colour", "Edible"],
        )

        mock_dim_dict = {"vegetable_data": expected_vegetable_df}
        mock_fact_dict = {"meat_data": expected_meat_df}

        ##mocked an existing file 
        expected_vegetable_df.to_parquet("vegetable_data.parquet", engine="pyarrow")
        s3_client.upload_file("vegetable_data.parquet", 'dummy_transform_buc', "vegetable_data.parquet")

    
        response = process_to_parquet_and_upload_to_s3(
            ['vegetable_data'], mock_dim_dict, mock_fact_dict, "dummy_transform_buc", s3_client
        )
        object_list = s3_client.list_objects_v2(Bucket='dummy_transform_buc')
        s3_uploaded_files = [obj['Key'] for obj in object_list.get('Contents', [])]

        time_prefix = datetime.strftime(datetime.today(), "meat_data/%Y/%m/%d/meat_data_%H:%M:%S.parquet")
        assert any(key.startswith("meat_data/") and key.endswith(".parquet") for key in s3_uploaded_files)
        assert 'vegetable_data.parquet' in s3_uploaded_files
        assert response == {"uploaded": ['meat_data'], "not_uploaded": ['vegetable_data']}

    def test_func_handles_empty_dicts(self, mock_transform_bucket, s3_client):
        response = process_to_parquet_and_upload_to_s3(
            [], {}, {}, 'dummy_transform_buc', s3_client
        )

        assert response == {"uploaded": [], "not_uploaded": []}

class TestLambdaHandler:
    def test_func_reads_from_extract_bucket(self, s3_client, mock_db_connection, mock_extract_bucket, mock_transform_bucket):
        mock_csv = "id,name\n1,Lauryn\n2,Hill"
        s3_client.put_object(Bucket='dummy_extract_buc',
                       Key="mock_table.csv",
                       Body=mock_csv)

        with patch('src.transform_lambda.transform_lambda.read_from_s3_subfolder_to_df') as mock_read, \
            patch('src.transform_lambda.transform_lambda.bucket_name', return_value="dummy_extract_buc") as mock_bucket_name:
            
            mock_read.return_value = {'sample_mock_table': pd.read_csv(io.StringIO(mock_csv))}

            lambda_handler({}, {})
            
            mock_read.assert_called_once()

            args, kwargs = mock_read.call_args
            assert kwargs.get('bucket') == mock_bucket_name.return_value
        


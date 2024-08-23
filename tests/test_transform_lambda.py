from src.transform_lambda import read_from_s3_subfolder_to_df
from moto import mock_aws
import pytest
import pandas as pd
import os
import boto3
import numpy as np


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


class TestReadFromS3:
    def test_returns_dictionary_with_correct_value_pair(self, s3_client):
        s3_client.create_bucket(
            Bucket="dummy_buc",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(
            "tests/dummy_identical.csv",
            "dummy_buc",
            "Foods/2024/08/21/Foods_12:03:10.csv",
        )
        tables = ["Foods"]
        result = read_from_s3_subfolder_to_df(
            tables, bucket="dummy_buc", client=s3_client
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

    def test_returns_dictionary_of_dataframes_for_multiple_tables(self, s3_client):
        s3_client.upload_file(
            "tests/dummy_2.csv", "dummy_buc", "Cars/2024/08/21/Cars_14:03:56.csv"
        )
        tables = ["Foods", "Cars"]
        result = read_from_s3_subfolder_to_df(
            tables, bucket="dummy_buc", client=s3_client
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
        assert result["Cars"].eq(expected_cars_df, axis="columns").all(axis=None)

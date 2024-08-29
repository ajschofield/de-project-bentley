from src.extract_lambda import retrieve_secrets
import boto3
import botocore.exceptions
from moto import mock_aws
import json
import pytest
import os


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def mock_sm_client(aws_credentials):
    with mock_aws():
        yield boto3.client("secretsmanager")


@pytest.fixture(scope="function")
def mock_store_secret(mock_sm_client):
    secret = {
        "cohort_id": "test_cohort_id",
        "user": "test_user_id",
        "password": "test_password",
        "host": "test_host",
        "database": "test_database",
        "port": "test_port",
    }

    secret_name = "test_secret"

    response = mock_sm_client.create_secret(
        Name=secret_name, SecretString=json.dumps(secret)
    )

    return response


@pytest.mark.skip(reason="The test is broken!")
def test_retrieves_secrets_returns_dictionary(mock_sm_client, mock_store_secret):
    secret_name = "test_secret"

    result = retrieve_secrets(mock_sm_client, secret_name)

    assert isinstance(result, dict)


@pytest.mark.skip(reason="The test is broken!")
def test_retrieves_secrets_returns_correct_keys_and_values(
    mock_sm_client, mock_store_secret
):
    secret_name = "test_secret"

    result = retrieve_secrets(mock_sm_client, secret_name)

    assert result["cohort_id"] == "test_cohort_id"
    assert result["user"] == "test_user_id"
    assert result["password"] == "test_password"
    assert result["host"] == "test_host"
    assert result["database"] == "test_database"
    assert result["port"] == "test_port"


@pytest.mark.skip(reason="The test is broken!")
def test_retrieves_secrets_raises_error_if_secret_name_incorrect_data_type(
    mock_sm_client,
):
    secret_name = [1, 2, 3]

    with pytest.raises(botocore.exceptions.ParamValidationError) as error:
        retrieve_secrets(mock_sm_client, secret_name)


@pytest.mark.skip(reason="The test is broken!")
def test_retrieves_secrets_raises_error_if_secret_name_does_not_exist(
    mock_sm_client, mock_store_secret
):
    secret_name = "test_secret_2"

    with pytest.raises(botocore.exceptions.ClientError) as error:
        retrieve_secrets(mock_sm_client, secret_name)

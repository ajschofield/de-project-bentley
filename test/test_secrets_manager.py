from src.secrets_manager import sm_client, create_secret, list_secret
import boto3
from moto import mock_aws
import json
import pytest
import os

pytest.fixture(scope="class")


def mock_aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="class")
def mock_sm_client(mock_aws_credentials):
    with mock_aws():
        yield boto3.client("secretsmanager")


def test_create_secret_stores_secrets(mock_sm_client):
    cohort_id = "test_cohort_id"
    user = "test_user_id"
    password = "test_password"
    host = "test_host"
    database = "test_database"
    port = "test_port"

    secret_name = "test_secret"
    response = create_secret(
        mock_sm_client, secret_name, cohort_id, user, password, host, database, port
    )

    assert response["Name"] == secret_name

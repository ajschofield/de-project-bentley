import boto3
from botocore.exceptions import ClientError
import json


def sm_client():
    sm_client = boto3.client('secretsmanager')
    yield sm_client

def create_secret(sm_client, secret_name, cohort_id, user, password, host, database, port):
    secret = {
        "cohort_id": cohort_id,
        "user": user,
        "password": password,
        "host": host,
        "database": database,
        "port": port
    }

    response = sm_client.create_secret(
        Name = secret_name,
        SecretString = json.dumps(secret)
    )

    print(response)
    return response

def list_secret(sm_client):
    response = sm_client.list_secrets()
    secret_dict = response['SecretList']
    secret_names = []
    for items in secret_dict:
        secret_names.append(items['Name'])
    print(f'{len(secret_names)} secret(s) available')
    for name in secret_names:
        print(name)
    return secret_names

def retrieve_secrets(sm_client):
    response = sm_client.get_secrets(
        
    )



#retrieve secret
#so lambda can access totesy db
#so lambda connect to the db and then retrieve the data
from pg8000.native import Connection, Error, DatabaseError, InterfaceError
from dotenv import load_dotenv
import os
import boto3
import csv
from botocore.exceptions import ClientError

load_dotenv()

def lambda_handler(event, context):

    client = boto3.client('s3')
# temporary credentials for dev- will not have access when uploaded
    
    database = os.getenv('database')
    user = os.getenv('user')
    password = os.getenv('password')
    host = os.getenv('host')
    port = os.getenv('port')

    try:
        db = Connection(
        database=database,
        user=user,
        password=password,
        host=host,
        port=port
        )
    except DatabaseError as e:
        print(e)
    except InterfaceError as i:
        print(i)
    #replace prints with upload to cloudwatch logs 

    tables = db.run("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE';")
    for table in tables:
        table_name = table[0]
        rows = db.run(f"SELECT * FROM {table_name};")
        # this saves the csv files to the repo root before writing to s3, this is unnecessary. how will the lambda behave when it attempts to save files?
        with open(f"{table_name}.csv", "w", newline='') as file:
            writer = csv.writer(file)
            writer.writerow([desc["name"] for desc in db.columns(f"SELECT * FROM {table_name};")])
            writer.writerows(rows)
            try:  
                client.upload_file(file, Bucket='ingestion-bucket', Object_name=table_name)
            
            except ClientError as e:
                print(e)
            #replace print with upload to cloudwatch logs 
    
    if db:
        db.close()

    
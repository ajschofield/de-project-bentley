from pg8000.native import Connection, DatabaseError, InterfaceError
from dotenv import load_dotenv
import os
import boto3
import csv
from botocore.exceptions import ClientError
import logging
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)
load_dotenv()


database = os.getenv('database')
user = os.getenv('user')
password = os.getenv('password')
host = os.getenv('host')
port = os.getenv('port')

def lambda_handler(event, context):
    """This lambda function connects to the Totesys database, lists the contents of the ingestion bucket,
       and converts all tables to CSV and if any of those tables do not exist in, or are different to the ones in s3, it uploads them
       it uses 3 helper functions to achieve these 3 functionalities 
    """
    try:
        db = connect_to_database()
        existing_files = list_existing_s3_files()
        any_changes = process_and_upload_tables(db, existing_files)
        
        if not any_changes:
            logger.info("No changes detected in the database.")
            return {
                'statusCode': 200,
                'body': json.dumps('No changes detected, no CSV files were uploaded.')
            }
        else:
            return {
                'statusCode': 200,
                'body': json.dumps('CSV files processed and uploaded successfully.')
            }
        
    except Exception as e:
        logger.error(f'Error: {e}')
        return {
            'statusCode': 500,
            'body': json.dumps('Internal server error.')
        }
    
    finally:
 
        if db:
            db.close()

def connect_to_database():
    try:
        return Connection(
            database=database,
            user=user,
            password=password,
            host=host,
            port=port
        )
    except DatabaseError as e:
        logger.error(f'Database error: {e}')
        raise
    except InterfaceError as i:
        logger.error(f'Interface error: {i}')
        raise



def list_existing_s3_files():
    """Creates a dictionary and populates it with the 
       results of listing the contents of the s3 bucket, then
       returns the populated dictionary
    """
    client = boto3.client('s3')
    existing_files = {}
    
    try:
        response = client.list_objects_v2(Bucket=ingestion_bucket)
        
        if 'Contents' in response:
            for obj in response['Contents']:
                s3_key = obj['Key']
                try:
                    file_obj = client.get_object(Bucket=ingestion_bucket, Key=s3_key)
                    file_content = file_obj['Body'].read().decode('utf-8')
                    existing_files[s3_key] = file_content
                except ClientError as e:
                    logger.error(f'Error retrieving S3 object {s3_key}: {e}')
    
    except ClientError as e:
        logger.error(f'Error listing S3 objects: {e}')
    
    return existing_files



def process_and_upload_tables(db, existing_files):
    """Creates a list of the tables from a database query and 
       then selects everything from each table in individual queries
       it then writes each table to CSV files and compares with the item 
       in the existing_files dictionary with the same name. If it finds sny changes
       to files, or new tables/files it uploads them to the s3 bucket
    """
    client = boto3.client('s3')
    tables = db.run("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE';")
    
    for table in tables:
        table_name = table[0]
        rows = db.run(f"SELECT * FROM {table_name};")
        

        csv_file_path = f"/tmp/{table_name}.csv"
        with open(csv_file_path, "w", newline='') as file:
            writer = csv.writer(file)
            column_names = [desc["name"] for desc in db.columns(f"SELECT * FROM {table_name};")]
            writer.writerow(column_names)
            writer.writerows(rows)
        
        s3_key = f"{table_name}/latest.csv"
        new_csv_content = open(csv_file_path, "r").read()
        

        if s3_key not in existing_files or existing_files[s3_key] != new_csv_content:
            try:
                client.upload_file(csv_file_path, ingestion_bucket, s3_key)
                logger.info(f"Uploaded {s3_key} to S3.")
            except ClientError as e:
                logger.error(f'Error uploading to S3: {e}')
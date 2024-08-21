#from src.extract_lambda import extract_bucket
import json
import boto3
import re
import io
from io import StringIO
import pandas as pd

def lambda_handler(event, context):
    pass


tables = ['sales_order', 
        'transaction', 
        'payment', 
        'counterparty', 
        'address', 
        'staff', 
        'purchase_order', 
        'department', 
        'currency', 
        'design', 
        'payment_type']

def read_from_s3_subfolder_to_df(tables, bucket, client=boto3.client('s3')):
    table_dfs = {}
    for table in tables:
        response = client.list_objects_v2(Bucket=bucket, Prefix=table)
        list_of_keys = ['s3://'+bucket+'/'+object['Key'] for object in response['Contents']] 
        list_of_df = [pd.read_csv(key) for key in list_of_keys]
        table_dfs[table] = pd.concat(list_of_df)
    return table_dfs

        

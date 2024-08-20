import json
import boto3
import re
import io
from io import StringIO
import pandas as pd

##add trigger window from extract bucket (on console?)
##suffix: must .csv --> reads only this file type that is uploaded to extract
##In-order to use PANDAS module in lambda function, a Lambda Layer needs to be attached to the AWS Lambda Function.
##need a function that normalises the data



s3_resource = boto3.resource('s3') ##need this for a way of reuploading data after transformation

def lambda_handler(event, context):
    s3_client = boto3.client('s3')  

    # tables = ['sales_order', 
    #           'transaction', 
    #           'payment', 
    #           'counterparty', 
    #           'address', 
    #           'staff', 
    #           'purchase_order', 
    #           'department', 
    #           'currency', 
    #           'design', 
    #           'payment_type']
    try:
        s3_bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        s3_file_name = event["Records"][0]["s3"]["object"]["key"]

        ## concatanating the file per table - most recent
        ## iterate through the subfolders
        ## table name prefix to iterate through the files written to that table

        object = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_file_name)
        body = object['Body']
        csv_string = body.read().decode('utf-8')
        dataframe = pd.read_csv(StringIO(csv_string)) ##this is the streaming body
        
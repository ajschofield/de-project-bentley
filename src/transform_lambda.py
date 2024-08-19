import json
import boto3
import io
from io import StringIO
import pandas as pd


##add trigger window from extract bucket (on console?)
##suffix: must .csv --> reads only this file type that is uploaded to extract
##In-order to use PANDAS module in lambda function, a Lambda Layer needs to be attached to the AWS Lambda Function.
##need a function that normalises the data


s3_client = boto3.client('s3')

def lambda_handler(event, context):
    try:
        s3_bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        s3_file_name = event["Records"][0]["s3"]["object"]["key"]
        
        object = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_file_name)
        body = object['Body']
        csv_string = body.read().decode('utf-8')
        dataframe = pd.read_csv(StringIO(csv_string)) ##this is the streaming body
        
        print(dataframe.head(3))

    except Exception as err:
        print(err)
        
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('')
    }
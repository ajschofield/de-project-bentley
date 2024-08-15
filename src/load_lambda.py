### Example taken from https://medium.com/@pranay1001090/how-to-load-data-from-amazon-s3-csv-parquet-to-aws-rds-using-python-3dc51dd2186e

### THIS IS AN EXAMPLE CODE WE CAN PICK FROM, NONE OF THIS HAS BEEN CUSTOMISED YET

import boto3
import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO
from sqlalchemy import create_engine

# AWS credentials and region
aws_access_key = '<your-access-key>'
aws_secret_key = '<your-secret-key>'
region_name = '<your-region>'

# S3 bucket and file details
bucket_name = '<your-bucket-name>'
file_prefix = '<your-file-prefix>'
s3_client = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=region_name)

# RDS connection details
database_name = '<your-database-name>'
table_name = '<your-table-name>'
rds_host = '<your-rds-host>'
rds_port = '<your-rds-port>'
rds_user = '<your-rds-username>'
rds_password = '<your-rds-password>'
# Function to load Parquet files into a Pandas DataFrame
def load_parquet_data(s3_bucket, s3_prefix):
    file_objects = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)['Contents']
    dfs = []
    for file_object in file_objects:
        file_key = file_object['Key']
        file_obj = s3_client.get_object(Bucket=s3_bucket, Key=file_key)
        parquet_file = pq.ParquetFile(BytesIO(file_obj['Body'].read()))
        df = parquet_file.read().to_pandas()
        dfs.append(df)
    return pd.concat(dfs)

# Load Parquet data from S3 into a Pandas DataFrame
df = load_parquet_data(bucket_name, file_prefix)
# Connect to RDS
conn_str = f'mysql+pymysql://{rds_user}:{rds_password}@{rds_host}:{rds_port}/{database_name}'
engine = create_engine(conn_str)

# Write the DataFrame to RDS
df.to_sql(table_name, con=engine, if_exists='replace', index=False)

# Closing the connection
engine.dispose()

print('Data loaded successfully!')
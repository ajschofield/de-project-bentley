import json
import boto3
import re
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from src.extract_lambda import extract_bucket
from src.fact_purchase_table import *
from src.fact_sales_order import create_dim_staff, create_dim_design, create_fact_sales_order


tables = [
    "sales_order",
    "transaction",
    "payment",
    "counterparty",
    "address",
    "staff",
    "purchase_order",
    "department",
    "currency",
    "design",
    "payment_type",
]

def lambda_handler(event, context):
    dict_of_df = read_from_s3_subfolder_to_df(tables, extract_bucket(), client=boto3.client("s3"))
    common_df_list = [create_dim_counterparty(dict_of_df), 
                      create_dim_date(dict_of_df), 
                      create_dim_location(dict_of_df), 
                      create_dim_currency(dict_of_df), 
                      create_dim_staff(dict_of_df)] 
    
    create_fact_purchase_order()

    f_sales_list = [create_fact_sales_order(),
                    create_dim_design()]
                    
    
    '''
    #dict{
        sales_schema: {
            Table_name: df_value, 
            ...}
        payment_schema: 
            Table_name: df_value, 
            ...}
        purchase_schema: 
            Table_name: df_value, 
            ...}
    }

    for schema in dict:
        for table_name, df_value in schema.items():
            parquet_file = df_value.to_parquet(f'{table_name}.parquet', engine='pyarrow'/'fastparquet'(?)) #we don't know the engine

            s3_key = datetime.strftime(
                        datetime.today(), f"{schema}/%Y/%m/%d/{table_name}_%H:%M:%S.parquet"
                    )

            client.upload_file(
            parquet_file, transform_bucket(), s3_key)
            ##might need seperate function for easier testing##
    '''



def read_from_s3_subfolder_to_df(tables, bucket, client=boto3.client("s3")):
    table_dfs = {}
    for table in tables:
        response = client.list_objects_v2(Bucket=bucket, Prefix=table)
        list_of_keys = [
            "s3://" + bucket + "/" + object["Key"] for object in response["Contents"]
        ]
        list_of_df = [pd.read_csv(key) for key in list_of_keys]
        table_dfs[table] = pd.concat(list_of_df)
    return table_dfs

def transform_bucket(client=boto3.client("s3")):
    response = client.list_buckets()
    bucket_filter = [
        bucket["Name"] for bucket in response["Buckets"] if "transform" in bucket["Name"]
    ]

    return bucket_filter[0]

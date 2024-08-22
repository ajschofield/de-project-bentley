from src.transform_lambda import read_from_s3_subfolder_to_df, tables
from src.extract_lambda import extract_bucket
import json
import boto3
import re
import pandas as pd


dict_of_df = read_from_s3_subfolder_to_df(tables, extract_bucket(), client=boto3.client("s3"))


# iterates through each dataframe in the list of dataframes and assigns them to a variable
df_staff = dict_of_df['staff'] ##no change
df_currency = dict_of_df['currency'] ##scraping API 
df_counterparty = dict_of_df['counterparty']
df_address = dict_of_df['address']
df_department = dict_of_df['department']
df_purchase_order = dict_of_df['purchase_order']

## dim_staff table is the same across the schemas (no change)

## dim_counterparty table

## dim_location df_currency --> drops 2 columns
dim_location = df_address.drop(labels=['created_at', 'last_updated'], axis=1).rename(columns={'address_id': 'location_id'})

## dim_counterparty 
df_prefixed_address = df_address.add_prefix('counterparty_legal_', axis=1) 
pd.merge(df_counterparty, 
         df_prefixed_address, 
         left_on="legal_address_id", 
         right_on="address_id", 
         how="outer")


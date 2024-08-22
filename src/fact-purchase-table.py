from src.transform_lambda import read_from_s3_subfolder_to_df, tables
from src.extract_lambda import extract_bucket
import json
import boto3
import re
import pandas as pd


# iterates through each dataframe in the list of dataframes and assigns them to a variable 
def get_dfs_from_dict(tables,dictionary=dict_of_df):
    for table in tables:
    df_staff = dict_of_df['staff'] ##no change
    df_currency = dict_of_df['currency'] ##scraping API 
    df_counterparty = dict_of_df['counterparty']
    df_address = dict_of_df['address']
    df_department = dict_of_df['department']
    df_purchase_order = dict_of_df['purchase_order']

## dim_staff table is the same across the schemas (no change)

## dim_location from address --> drops 2 columns
def create_dim_location(dict_of_df):
    dim_location = dict_of_df['address'].drop(labels=['created_at', 'last_updated'], axis=1).rename(columns={'address_id': 'location_id'})
    return dim_location

## dim_counterparty from address and counterparty
def create_dim_counterparty(dict_of_df):
    df_prefixed_address = dict_of_df['address'].add_prefix('counterparty_legal_', axis=1) 
    pd.merge(dict_of_df['counterparty'], 
            df_prefixed_address, 
            left_on="legal_address_id", 
            right_on="address_id", 
            how="outer")

def create_fact_purchase_order(dict_of_df):
    df_po = dict_of_df['purchase_order']
    df_po.index.name = 'purchase_record_id'
    #df_po['create_date'] = df_po['create_at'].date()
    #df_po['create_time'] = df_po['create_at'].time()
    df_po['agreed_delivery_date'] = 
    df_po['agreed_payment_date']
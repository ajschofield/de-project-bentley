from src.transform_lambda import read_from_s3_subfolder_to_df, tables
from src.extract_lambda import extract_bucket
import json
import boto3
import re
import pandas as pd
from datetime import datetime as dt
import requests
from bs4 import BeautifulSoup


## dim_staff table is the same across the schemas (no change)

## dim_location from address --> drops 2 columns
def create_dim_location(dict_of_df):
    df_loc = dict_of_df['address'].drop(labels=['created_at', 'last_updated'], axis=1).rename(columns={'address_id': 'location_id'}).set_index('location_id')
    return df_loc

## dim_counterparty from address and counterparty
def create_dim_counterparty(dict_of_df):
    df_prefixed_address = dict_of_df['address'].add_prefix('counterparty_legal_', axis=1) 
    df_cp = pd.merge(dict_of_df['counterparty'], 
            df_prefixed_address, 
            left_on="legal_address_id", 
            right_on="address_id", 
            how="outer").set_index('counterparty_id')
    return df_cp

## fact_purchase_order from purchase_order
def create_fact_purchase_order(dict_of_df):
    df_po = dict_of_df['purchase_order']
    df_po.index.name = 'purchase_record_id'
    df_po['created_date'] = df_po['created_at'].date()
    df_po['created_time'] = df_po['created_at'].dt.time
    df_po['last_updated_date'] = df_po['last_updated_at'].date()
    df_po['last_updated_time'] = df_po['last_updated_at'].dt.time
    df_po['agreed_delivery_date'] = pd.to_datetime(df_po['agreed_delivery_date'],format="%Y-%m-%d")
    df_po['agreed_payment_date'] = pd.to_datetime(df_po['agreed_payment_date'],format="%Y-%m-%d")
    df_po.drop(labels=['created_at','last_updated_at'],axis=1,inplace=True)
    return df_po

## dim_date from purchase_order
def create_dim_date(dict_of_df):
    sr_date = pd.concat([df['created_date'],df['last_updated_date'],df['agreed_delivery_date'],df['agreed_payment_date']]).sort()
    df_date = pd.DataFrame(sr_date,columns='date_id')
    df_date['year'] = df_date['date_id'].dt.year
    df_date['month'] = df_date['date_id'].dt.month
    df_date['day'] = df_date['date_id'].dt.day
    df_date['day_of_week'] = df_date['date_id'].dt.dayofweek
    df_date['day_name'] = df_date['date_id'].dt.day_name
    df_date['month_name'] = df_date['date_id'].dt.month_name
    df_date['quarter'] = df_date['date_id'].dt.quarter
    df_date.set_index('date_id')

def scrape_currency_names():
    response = requests.get('https://www.xe.com/currency/').content
    soup = BeautifulSoup(response,'html.parser')
    currency = [item.text for item in soup.findAll('a', attrs={'class' : "sc-299dec64-6 fZPTSw"})]
    sr = pd.Series(currency)
    df_cur = sr.str.split(pat=" - ",expand=True).rename({0:'currency_code',1:'currency_name'},axis=1)
    return df_cur

def create_dim_currency(dict_of_df,names=scrape_currency_names()):
    df_cur = dict_of_df['currency'].drop(labels=['created_at', 'last_updated'], axis=1)
    dim_cur = pd.merge(df_cur,names,left_on='currency_code',right_on='currency_code',how='inner').set_index('currency_id')
    return dim_cur






import pandas as pd
from bs4 import BeautifulSoup
from src.transform_lambda import read_from_s3_subfolder_to_df, tables
from src.extract_lambda import extract_bucket
import json
import boto3
import re
from datetime import datetime as dt
import requests

#Table names:
# fact_sales_order
# fact_purchase_orders
# fact_payment
# dim_transaction
# dim_staff
# dim_payment_type
# dim_location
# dim_design
# dim_date
# dim_currency
# dim_counterparty

def create_dim_transaction(dict_of_df):
    pass

def create_fact_sales_order(dict_of_df):
    df_sales = dict_of_df["sales_order"]
    df_sales.index.name = "sales_record_id"
    df_sales["created_date"] = pd.to_datetime(df_sales["created_at"]).dt.date
    df_sales["created_time"] = pd.to_datetime(df_sales["created_at"]).dt.time
    df_sales["last_updated_date"] = pd.to_datetime(df_sales["last_updated"]).dt.date
    df_sales["last_updated_time"] = pd.to_datetime(df_sales["last_updated"]).dt.time
    pd.merge(dict_of_df["staff"], df_sales["sales_staff_id"], on="staff_id", how="left")
    # df_sales.rename(columns={"staff_id": "sales_staff_id"})
    fact_sales_order = df_sales.loc[:,[
        "sales_record_id",
        "sales_order_id",
        "created_date",
        "created_time",
        "last_updated_date",
        "last_updated_time",
        "sales_staff_id",
        "counterparty_id",
        "units_sold",
        "unit_price",
        "currency_id",
        "design_id",
        "agreed_payment_date",
        "agreed_delivery_date",
        "agreed_delivery_location_id"
    ]]
    return fact_sales_order

## fact_purchase_order from purchase_order
def create_fact_purchase_orders(dict_of_df):
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


def create_fact_payment(dict_of_df):
    df_payment = dict_of_df["payment"]
    df_payment.index.name = "payment_record_id"
    df_payment["created_date"] = pd.to_datetime(df_payment["created_at"]).dt.date
    df_payment["created_time"] = pd.to_datetime(df_payment["created_at"]).dt.time
    df_payment["last_updated_date"] = pd.to_datetime(df_payment["last_updated"]).dt.date
    df_payment["last_updated_time"] = pd.to_datetime(df_payment["last_updated"]).dt.time
    fact_payment = df_payment.loc[:,[
        "payment_record_id",
        "payment_id",
        "created_date",
        "created_time",
        "last_updated_date",
        "last_updated_time",
        "transaction_id",
        "counterparty_id",
        "payment_amount",
        "currency_id",
        "payment_type_id",
        "paid",
        "payment_date"
    ]]
    return fact_payment

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


## dim_date from purchase_order
def create_dim_date(dict_of_df):
    sr_date = pd.concat([dict_of_df['created_date'],dict_of_df['last_updated_date'],dict_of_df['agreed_delivery_date'],dict_of_df['agreed_payment_date']]).sort()
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







def create_dim_payment_type(dict_of_df):
    df_payment_type = dict_of_df["payment_type"]
    dim_payment_type = df_payment_type.loc[:, ["payment_type_id", "payment_type_name"]]
    return dim_payment_type

def create_fact_payment(dict_of_df):
    df_payment = dict_of_df["payment"]
    df_payment.index.name = "payment_record_id"
    df_payment["created_date"] = pd.to_datetime(df_payment["created_at"]).dt.date
    df_payment["created_time"] = pd.to_datetime(df_payment["created_at"]).dt.time
    df_payment["last_updated_date"] = pd.to_datetime(df_payment["last_updated"]).dt.date
    df_payment["last_updated_time"] = pd.to_datetime(df_payment["last_updated"]).dt.time
    fact_payment = df_payment.loc[:,[
        "payment_record_id",
        "payment_id",
        "created_date",
        "created_time",
        "last_updated_date",
        "last_updated_time",
        "transaction_id",
        "counterparty_id",
        "payment_amount",
        "currency_id",
        "payment_type_id",
        "paid",
        "payment_date"
    ]]
    return fact_payment

def create_dim_design(dict_of_df):
    df_design = dict_of_df["design"]
    dim_design = df_design.loc[:, ["design_id", "design_name", "file_name", "file_location"]]
    return dim_design

def create_dim_staff(dict_of_df):
    staff_department = pd.merge(dict_of_df["staff"], dict_of_df["department"], on='department_id', how="left")
    dim_staff = staff_department.loc[:, ['staff_id', 'first_name', 'last_name', 'department_name', 'location', 'email_address']]
    return dim_staff

def create_dim_currency(dict_of_df):
    df_currency = dict_of_df["currency"]
    dim_currency = df_currency.loc[:, ["currency_id", "currency_code"]]
    mappings = {
        "GBP": "Pound",
        "USD": "US Dollar",
        "EUR": "Euro"
    }
    dim_currency["currency_name"] = dim_currency["currency_code"].map(mappings)
    return dim_currency


def create_dim_date(dict_of_df):
    df_sales = dict_of_df["sales"]
    df_sales = df_sales.loc[:, ["agreed_delivery_date"]]
    df_sales["agreed_delivery_date"] = pd.to_datetime["agreed_delivery_date"]
    df_sales["year"] = df_sales["agreed_delivery_date"].dt.year
    df_sales["month"] = df_sales["agreed_delivery_date"].dt.month
    df_sales["day"] = df_sales["agreed_delivery_date"].dt.day
    df_sales["day_of_week"] = df_sales["agreed_delivery_date"].dt.dayofweek
    df_sales["day_name"] = df_sales["agreed_delivery_date"].dt.day_name()
    df_sales["month_name"] = df_sales["agreed_delivery_date"].dt.month_name()
    df_sales["quarter"] = df_sales["agreed_delivery_date"].dt.quarter()
    dim_date = ["date_id", "year", "month", "day", "day_of_week", "day_name", "month_name", "quarter"]   #series.dt.quarter()
    return dim_date


# TO DO:                                    
# complete dim_date from merged fact table
# merge dataframes into one dataframe
# remove duplicates
# test dim_date and fact_sales_order

def create_sales_star_schema(dict_of_df):
    dim_design = create_dim_design(dict_of_df)
    dim_staff = create_dim_staff(dict_of_df)
    dim_currency = create_dim_currency(dict_of_df)
    dim_date = create_dim_date(dict_of_df)
    
    fact_sales_order = create_fact_sales_order(dict_of_df)
    
    fact_sales_order = fact_sales_order.merge(dim_design, on='design_id', how='left')
    fact_sales_order = fact_sales_order.merge(dim_staff, left_on='sales_staff_id', right_on='staff_id', how='left')
    fact_sales_order = fact_sales_order.merge(dim_currency, on='currency_id', how='left')
    fact_sales_order = fact_sales_order.merge(dim_date, left_on='agreed_delivery_date', right_on='date_id', how='left')
    
    return fact_sales_order



def create_dim_payment_type(dict_of_df):
    df_payment_type = dict_of_df["payment_type"]
    dim_payment_type = df_payment_type.loc[:, ["payment_type_id", "payment_type_name"]]
    return dim_payment_type






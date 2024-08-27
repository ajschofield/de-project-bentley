import pandas as pd
from bs4 import BeautifulSoup
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


#no test, same as fact_payment
def create_fact_sales_order(dict_of_df):
    df_sales = dict_of_df["sales_order"]
    df_sales.index.name = "sales_record_id"
    df_sales["created_date"] = pd.to_datetime(df_sales["created_at"].dt.date,format='%Y-%m-%d')
    df_sales["created_time"] = df_sales["created_at"].dt.floor('s').dt.time
    df_sales["last_updated_date"] = pd.to_datetime(df_sales["last_updated"].dt.date,format='%Y-%m-%d')
    df_sales["last_updated_time"] = df_sales["last_updated"].dt.floor('s').dt.time
    df_sales['agreed_delivery_date'] = pd.to_datetime(df_sales['agreed_delivery_date'],format="%Y-%m-%d")
    df_sales['agreed_payment_date'] = pd.to_datetime(df_sales['agreed_payment_date'],format="%Y-%m-%d")
    df_sales.drop(labels=['created_at','last_updated'],axis=1,inplace=True)
    df_sales.reset_index(inplace=True)
    return df_sales

#no test, same as fact_payment
def create_fact_purchase_orders(dict_of_df):
    df_po = dict_of_df['purchase_order']
    df_po.index.name = 'purchase_record_id'
    df_po['created_date'] = pd.to_datetime(df_po['created_at'].dt.date,format='%Y-%m-%d')
    df_po['created_time'] = df_po['created_at'].dt.floor('s').dt.time
    df_po['last_updated_date'] = pd.to_datetime(df_po['last_updated'].dt.date,format='%Y-%m-%d')
    df_po['last_updated_time'] = df_po['last_updated'].dt.floor('s').dt.time
    df_po['agreed_delivery_date'] = pd.to_datetime(df_po['agreed_delivery_date'],format="%Y-%m-%d")
    df_po['agreed_payment_date'] = pd.to_datetime(df_po['agreed_payment_date'],format="%Y-%m-%d")
    df_po.drop(labels=['created_at','last_updated'],axis=1,inplace=True)
    df_po.reset_index(inplace=True)
    return df_po

#test passed
def create_fact_payment(dict_of_df):
    df_payment = dict_of_df["payment"]
    df_payment.index.name = "payment_record_id"
    df_payment["created_date"] = pd.to_datetime(df_payment["created_at"].dt.date,format='%Y-%m-%d')
    df_payment["created_time"] = df_payment["created_at"].dt.floor('s').dt.time
    df_payment["last_updated_date"] = pd.to_datetime(df_payment["last_updated"].dt.date,format='%Y-%m-%d')
    df_payment["last_updated_time"] = df_payment["last_updated"].dt.floor('s').dt.time
    df_payment['payment_date'] = pd.to_datetime(df_payment['payment_date'],format="%Y-%m-%d")
    df_payment.drop(labels=['created_at','last_updated'],axis=1,inplace=True)
    df_payment.reset_index(inplace=True)
    return df_payment

#test passed
def create_dim_transaction(dict_of_df):
    df_transaction = dict_of_df["transaction"].drop(labels=['created_at', 'last_updated'], axis=1)
    return df_transaction

#test passed
def create_dim_location(dict_of_df):
    df_loc = dict_of_df['address'].drop(labels=['created_at', 'last_updated'], axis=1).rename(columns={'address_id': 'location_id'})
    return df_loc


def create_dim_counterparty(dict_of_df):
    df_prefixed_address = dict_of_df['address'].add_prefix('counterparty_legal_', axis=1) 
    df_cp = pd.merge(dict_of_df['counterparty'], 
            df_prefixed_address, 
            left_on="legal_address_id", 
            right_on="counterparty_legal_address_id", 
            how="outer")
    df_cp.drop(columns=["legal_address_id","counterparty_legal_address_id"],inplace=True)
    return df_cp

#test passed
def create_dim_date(dict_of_df):
    fact_dfs = [create_fact_payment(dict_of_df), create_fact_purchase_orders(dict_of_df), create_fact_sales_order(dict_of_df)]
    list_of_date_columns = []
    for df in fact_dfs:
        date_col_names = [col_name for col_name in list(df.columns) if 'date' in col_name]
        for col in date_col_names:
            list_of_date_columns.append(df[col])
    sr_date = pd.array(pd.concat(list_of_date_columns),dtype='datetime64[ns]')
    df_date = pd.DataFrame(data=sr_date,columns=['date_id'])
    df_date.drop_duplicates(inplace=True)
    df_date['year'] = df_date['date_id'].dt.year
    df_date['month'] = df_date['date_id'].dt.month
    df_date['day'] = df_date['date_id'].dt.day
    df_date['day_of_week'] = df_date['date_id'].dt.dayofweek
    df_date['day_name'] = df_date['date_id'].dt.day_name()
    df_date['month_name'] = df_date['date_id'].dt.month_name()
    df_date['quarter'] = df_date['date_id'].dt.quarter 
    return df_date

#tests passed
def scrape_currency_names():
    response = requests.get('https://www.xe.com/currency/').content
    soup = BeautifulSoup(response,'html.parser')
    currency = [item.text for item in soup.findAll('a', attrs={'class' : "sc-299dec64-6 fZPTSw"})]
    sr = pd.Series(currency)
    df_cur = sr.str.split(pat=" - ",expand=True).rename({0:'currency_code',1:'currency_name'},axis=1)
    return df_cur

#tests passed
def create_dim_currency(dict_of_df,names=scrape_currency_names()):
    df_cur = dict_of_df['currency'].drop(labels=['created_at', 'last_updated'], axis=1)
    dim_cur = pd.merge(df_cur,names,left_on='currency_code',right_on='currency_code',how='inner')
    return dim_cur

#tests passed
def create_dim_payment_type(dict_of_df):
    df_payment_type = dict_of_df["payment_type"]
    dim_payment_type = df_payment_type.loc[:, ["payment_type_id", "payment_type_name"]]
    return dim_payment_type

#tests passed
def create_dim_design(dict_of_df):
    df_design = dict_of_df["design"]
    dim_design = df_design.loc[:, ["design_id", "design_name", "file_name", "file_location"]]
    return dim_design
#tests passed
def create_dim_staff(dict_of_df):
    staff_department = pd.merge(dict_of_df["staff"], dict_of_df["department"], on='department_id', how="left")
    dim_staff = staff_department.loc[:, ['staff_id', 'first_name', 'last_name', 'department_name', 'location', 'email_address']]
    return dim_staff


















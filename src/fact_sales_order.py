import pandas as pd


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

def create_fact_sales_order(dict_of_df):
    df_sales = dict_of_df["sales_order"]
    df_sales.index.name = "sales_record_id"
    df_sales["created_date"] = pd.to_datetime(df_sales["created_at"]).dt.date
    df_sales["created_time"] = pd.to_datetime(df_sales["created_at"]).dt.time
    df_sales["last_updated_date"] = pd.to_datetime(df_sales["last_updated"]).dt.date
    df_sales["last_updated_time"] = pd.to_datetime(df_sales["last_updated"]).dt.time
    df_sales.rename(columns={"staff_id": "sales_staff_id"})
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

# TO DO:                                    
# complete dim_date from merged fact table
# merge dataframes into one dataframe
# remove duplicates
# test dim_date and fact_sales_order







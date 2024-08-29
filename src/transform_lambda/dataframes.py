import pandas as pd
from bs4 import BeautifulSoup
import requests

# Table names:
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


# no test, same as fact_payment
def create_fact_sales_order(dict_of_df):
    df_sales = dict_of_df["sales_order"].rename(columns={"staff_id": "sales_staff_id"})

    df_sales["created_date"] = df_sales["created_at"].astype("datetime64[ns]").dt.date
    df_sales["created_time"] = (
        df_sales["created_at"].astype("datetime64[ns]").dt.floor("s").dt.time
    )
    df_sales["last_updated_date"] = (
        df_sales["last_updated"].astype("datetime64[ns]").dt.date
    )
    df_sales["last_updated_time"] = (
        df_sales["last_updated"].astype("datetime64[ns]").dt.floor("s").dt.time
    )
    df_sales["agreed_delivery_date"] = pd.to_datetime(
        df_sales["agreed_delivery_date"], format="%Y-%m-%d"
    )
    df_sales["agreed_payment_date"] = pd.to_datetime(
        df_sales["agreed_payment_date"], format="%Y-%m-%d"
    )
    fact_sales = df_sales.loc[
        :,
        [
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
            "agreed_delivery_location_id",
        ],
    ]
    fact_sales.convert_dtypes()
    fact_sales.index = pd.RangeIndex(1, len(fact_sales.index) + 1)
    fact_sales.index.name = "sales_record_id"
    fact_sales.reset_index(inplace=True)
    fact_sales.dropna(inplace=True)
    return fact_sales


# no test, same as fact_payment


def create_fact_purchase_orders(dict_of_df):
    df_po = dict_of_df["purchase_order"]
    df_po["created_date"] = df_po["created_at"].astype("datetime64[ns]").dt.date
    df_po["created_time"] = (
        df_po["created_at"].astype("datetime64[ns]").dt.floor("s").dt.time
    )
    df_po["last_updated_date"] = df_po["last_updated"].astype("datetime64[ns]").dt.date
    df_po["last_updated_time"] = (
        df_po["last_updated"].astype("datetime64[ns]").dt.floor("s").dt.time
    )
    df_po["agreed_delivery_date"] = pd.to_datetime(
        df_po["agreed_delivery_date"], format="%Y-%m-%d"
    )
    df_po["agreed_payment_date"] = pd.to_datetime(
        df_po["agreed_payment_date"], format="%Y-%m-%d"
    )
    fact_purchase_order = df_po.loc[
        :,
        [
            "purchase_order_id",
            "created_date",
            "created_time",
            "last_updated_date",
            "last_updated_time",
            "staff_id",
            "counterparty_id",
            "item_code",
            "item_quantity",
            "item_unit_price",
            "currency_id",
            "agreed_delivery_date",
            "agreed_payment_date",
            "agreed_delivery_location_id",
        ],
    ]
    fact_purchase_order.convert_dtypes()
    fact_purchase_order.index = pd.RangeIndex(1, len(fact_purchase_order.index) + 1)
    fact_purchase_order.index.name = "purchase_record_id"
    fact_purchase_order.reset_index(inplace=True)
    fact_purchase_order.dropna(inplace=True)
    return fact_purchase_order


# test passed


def create_fact_payment(dict_of_df):
    df_payment = dict_of_df["payment"]
    df_payment["created_date"] = (
        df_payment["created_at"].astype("datetime64[ns]").dt.date
    )
    df_payment["created_time"] = (
        df_payment["created_at"].astype("datetime64[ns]").dt.floor("s").dt.time
    )
    df_payment["last_updated_date"] = (
        df_payment["last_updated"].astype("datetime64[ns]").dt.date
    )
    df_payment["last_updated_time"] = (
        df_payment["last_updated"].astype("datetime64[ns]").dt.floor("s").dt.time
    )
    df_payment["payment_date"] = pd.to_datetime(
        df_payment["payment_date"], format="%Y-%m-%d"
    )
    fact_payment = df_payment.loc[
        :,
        [
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
            "payment_date",
        ],
    ]
    fact_payment.convert_dtypes()
    fact_payment.index = pd.RangeIndex(1, len(fact_payment.index) + 1)
    fact_payment.index.name = "payment_record_id"
    fact_payment.reset_index(inplace=True)
    fact_payment.dropna(inplace=True)
    fact_payment = fact_payment.astype({"currency_id": "int", "payment_id": "int"})
    return fact_payment


# test passed


def create_dim_transaction(dict_of_df):
    dim_transaction = dict_of_df["transaction"].loc[
        :, ["transaction_id", "transaction_type", "sales_order_id", "purchase_order_id"]
    ]
    # dim_transaction = dim_transaction.astype({"sales_order_id":"Int64","purchase_order_id":"Int64"})
    return dim_transaction


# test passed


def create_dim_location(dict_of_df):
    dim_location = (
        dict_of_df["address"]
        .drop(labels=["created_at", "last_updated"], axis=1)
        .rename(columns={"address_id": "location_id"})
    )
    return dim_location


def create_dim_counterparty(dict_of_df):
    df_prefixed_address = (
        dict_of_df["address"]
        .drop(labels=["created_at", "last_updated"], axis=1)
        .rename(columns={"phone": "phone_number"})
        .add_prefix("counterparty_legal_", axis=1)
    )
    df_cp = pd.merge(
        dict_of_df["counterparty"],
        df_prefixed_address,
        left_on="legal_address_id",
        right_on="counterparty_legal_address_id",
        how="inner",
    )  # .dropna(inplace=True)
    dim_counterparty = df_cp.drop(
        labels=[
            "legal_address_id",
            "counterparty_legal_address_id",
            "created_at",
            "last_updated",
            "commercial_contact",
            "delivery_contact",
        ],
        axis=1,
    )
    return dim_counterparty


# test passed


def create_dim_date(dict_of_df):
    fact_dfs = [
        create_fact_payment(dict_of_df),
        create_fact_purchase_orders(dict_of_df),
        create_fact_sales_order(dict_of_df),
    ]
    list_of_date_columns = []
    for df in fact_dfs:
        date_col_names = [
            col_name for col_name in list(df.columns) if "_date" in col_name
        ]
        for col in date_col_names:
            list_of_date_columns.append(df[col])
    sr_date = pd.array(pd.concat(list_of_date_columns), dtype="datetime64[ns]")
    df_date = pd.DataFrame(data=sr_date, columns=["date_id"])
    df_date.drop_duplicates(inplace=True)
    # df_date.dropna(inplace=True)
    df_date["year"] = df_date["date_id"].dt.year
    df_date["month"] = df_date["date_id"].dt.month
    df_date["day"] = df_date["date_id"].dt.day
    df_date["day_of_week"] = df_date["date_id"].dt.dayofweek
    df_date["day_name"] = df_date["date_id"].dt.day_name()
    df_date["month_name"] = df_date["date_id"].dt.month_name()
    df_date["quarter"] = df_date["date_id"].dt.quarter
    return df_date


# tests passed


def scrape_currency_names():
    response = requests.get("https://www.xe.com/currency/").content
    soup = BeautifulSoup(response, "html.parser")
    currency = [
        item.text for item in soup.findAll("a", attrs={"class": "sc-299dec64-6 fZPTSw"})
    ]
    sr = pd.Series(currency)
    df_cur = sr.str.split(pat=" - ", expand=True).rename(
        {0: "currency_code", 1: "currency_name"}, axis=1
    )
    return df_cur


# tests passed


def create_dim_currency(dict_of_df, names=scrape_currency_names()):
    df_cur = dict_of_df["currency"].drop(labels=["created_at", "last_updated"], axis=1)
    dim_currency = pd.merge(
        df_cur, names, left_on="currency_code", right_on="currency_code", how="left"
    )
    dim_currency.drop_duplicates(inplace=True)
    dim_currency.astype({"currency_name": "string", "currency_code": "string"})
    print(dim_currency.dtypes, "<<<<<<<<<Dtype")
    return dim_currency


# tests passed


def create_dim_payment_type(dict_of_df):
    df_payment_type = dict_of_df["payment_type"]
    dim_payment_type = df_payment_type.loc[:, ["payment_type_id", "payment_type_name"]]
    return dim_payment_type


# tests passed


def create_dim_design(dict_of_df):
    df_design = dict_of_df["design"]
    dim_design = df_design.loc[
        :, ["design_id", "design_name", "file_name", "file_location"]
    ]
    return dim_design


# tests passed


def create_dim_staff(dict_of_df):
    staff_department = pd.merge(
        dict_of_df["staff"], dict_of_df["department"], on="department_id", how="left"
    )
    dim_staff = staff_department.loc[
        :,
        [
            "staff_id",
            "first_name",
            "last_name",
            "department_name",
            "location",
            "email_address",
        ],
    ]
    return dim_staff

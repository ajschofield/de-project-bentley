import pandas as pd

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

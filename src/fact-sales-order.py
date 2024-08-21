import pandas as pd
from src.transform_lambda import get_dataframes

# {"design": "design dataframe", "address": "address dataframe", ....}
dict_of_df = get_dataframes()


# iterates through each dataframe in the list of dataframes and assigns them to a variable
df_design = dict_of_df[design]
df_currency = dict_of_df[currency]
df_address = dict_of_df[address]
df_staff = dict_of_df[staff]
df_department = dict_of_df[department]
df_counterparty = dict_of_df[counterparty]
df_sales = dict_of_df[sales]

# creates the dim_design dataframe
dim_design = df_design["design_id", "design_name", "file_name", "file_location"]

# creates the dim_staff dataframe
staff_department = pd.merge(df_staff, df_department, on="department_id", how="outer")
dim_staff = staff_department[
    "staff_id",
    "first_name",
    "last_name",
    "department_name",
    "location",
    "email_address",
]

# creates the dim_currency dataframe
# currency names currently hardcoded and not taken from database, is this viable/how else to do this?
d = {
    "currency_id": [1, 2, 3],
    "currency_code": ["GBP", "USD", "EUR"],
    "currency_name": ["Pound", "US Dollar", "Euro"],
}
currency_names = pd.DataFrame(data=d)
join_currency = pd.merge(df_currency, currency_names, on="currency_name", how="outer")
dim_currency = join_currency["currency_id", "currency_code", "currency_name"]

# Using .map to add currency_name column and link it to the currency code
# dim_currency = df_currency["currency_id", "currency_code"]
# mappings = {
#     "GBP": "Pound",
#     "USD": "US Dollar",
#     "EUR": "Euro"
# }
# dim_currency["currency_name"] = dim_currency["currency_code"].map(mappings)


# creates the dim_location dataframe
# need to change address id to location id
"dim_location dataframe: (location_id, address_line_1, address_line_2, district, city, postal code, country, phone)"
df_address.rename(columns={"address_id": "location_id"})
dim_location = df_address[
    "location_id",
    "address_line_1",
    "address_line_2",
    "district",
    "city",
    "postal_code" "country",
    "phone",
]

# creates the dim_counterparty dataframe
counterparty_address = pd.merge(
    df_counterparty,
    df_address,
    left_on="legal_address_id",
    right_on="address_id",
    how="outer",
)
counterparty_address.rename(
    columns={
        "address_line_1": "counterparty_legal_address_line_1",
        "address_line_2": "counterparty_legal_address_line_2",
        "district": "counterparty_legal_district",
        "city": "counterparty_legal_city",
        "postal_code": "counterparty_postal_code",
        "country": "counterparty_legal_country",
        "phone": "counterparty_legal_phone_number",
    }
)

dim_counterparty = df_counterparty[
    "counterparty_id",
    "counterparty_legal_name",
    "counterparty_legal_address_line_1",
    "counterparty_legal_address_line_2",
    "counterparty_legal_district",
    "counterpart_legal_city",
    "counterparty_legal_postal_code",
    "counterparty_legal_country",
    "counterparty_legal_phone_number",
]

# creates the dim_date dataframe
df_sales = df_sales["agreed_delivery_date"]
df_sales["agreed_delivery_date"] = pd.to_datetime["agreed_delivery_date"]
df_sales["year"] = df_sales["agreed_delivery_date"].dt.year
df_sales["month"] = df_sales["agreed_delivery_date"].dt.month
df_sales["day"] = df_sales["agreed_delivery_date"].dt.day
df_sales["day_of_week"] = df_sales["agreed_delivery_date"].dt.dayofweek
df_sales["day_name"] = df_sales["agreed_delivery_date"].dt.day_name()
df_sales["month_name"] = df_sales["agreed_delivery_date"].dt.month_name()
df_sales["quarter"] = df_sales["agreed_delivery_date"].dt.quarter()

dim_date = [
    "date_id",
    "year",
    "month",
    "day",
    "day_of_week",
    "day_name",
    "month_name",
    "quarter",
]  # series.dt.quarter()


# TO DO:
# fact_sales_order

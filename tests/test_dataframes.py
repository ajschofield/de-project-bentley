from src.transform_lambda.dataframes import *
import pandas as pd
from unittest.mock import patch
from datetime import datetime as dt


class TestCreateDimDesign:
    def test_dim_design_returns_dataframe(self):
        d = {
            "test": ["Hello", "Bye"],
            "design_id": ["Hello", "Bye"],
            "design_name": ["Hello", "Bye"],
            "file_name": ["Hello", "Bye"],
            "file_location": ["Hello", "Bye"],
            "Hello": ["Hello", "Bye"],
        }
        test_df = {"design": pd.DataFrame(data=d)}
        result = create_dim_design(test_df)
        assert isinstance(result, pd.DataFrame)

    def test_dim_design_returns_correct_columns_and_values(self):
        d = {
            "test": ["Hello", "Bye"],
            "design_id": ["Hello", "Bye"],
            "design_name": ["Hello", "Bye"],
            "file_name": ["Hello", "Bye"],
            "file_location": ["Hello", "Bye"],
            "Hello": ["Hello", "Bye"],
        }
        test_df = {"design": pd.DataFrame(data=d)}
        result = create_dim_design(test_df)
        d2 = {
            "design_id": ["Hello", "Bye"],
            "design_name": ["Hello", "Bye"],
            "file_name": ["Hello", "Bye"],
            "file_location": ["Hello", "Bye"],
        }
        expected_df = pd.DataFrame(data=d2)
        expected_result = expected_df.copy()
        assert result.equals(expected_result)


class TestCreateDimStaff:
    def test_dim_staff_returns_dataframe(self):
        d = {
            "staff_id": ["Hello", "Bye"],
            "first_name": ["Hello", "Bye"],
            "last_name": ["Hello", "Bye"],
            "department_id": ["Hello", "Bye"],
        }
        d2 = {
            "department_name": ["Hello", "Bye"],
            "location": ["Hello", "Bye"],
            "email_address": ["Hello", "Bye"],
            "department_id": ["Hello", "Bye"],
        }
        test_df = {"staff": pd.DataFrame(
            data=d), "department": pd.DataFrame(data=d2)}
        result = create_dim_staff(test_df)
        assert isinstance(result, pd.DataFrame)

    def test_dim_staff_returns_correct_columns_and_values(self):
        d = {
            "staff_id": ["Hello", "Bye"],
            "first_name": ["Hello", "Bye"],
            "last_name": ["Hello", "Bye"],
            "department_id": ["Hello", "Bye"],
        }
        d2 = {
            "department_name": ["Hello", "Bye"],
            "location": ["Hello", "Bye"],
            "email_address": ["Hello", "Bye"],
            "department_id": ["Hello", "Bye"],
        }

        test_df = {"staff": pd.DataFrame(
            data=d), "department": pd.DataFrame(data=d2)}

        result = create_dim_staff(test_df)
        expected_d = {
            "staff_id": ["Hello", "Bye"],
            "first_name": ["Hello", "Bye"],
            "last_name": ["Hello", "Bye"],
            "department_name": ["Hello", "Bye"],
            "location": ["Hello", "Bye"],
            "email_address": ["Hello", "Bye"],
        }
        expected_df = pd.DataFrame(data=expected_d)
        expected_result = expected_df.copy()
        assert result.equals(expected_result)


class TestCreatePaymentType:
    def test_create_dim_payment_type_returns_correct_columns_and_values(self):
        d = {"payment_type_id": ["Hello", "Bye"],
             "payment_type_name": ["Hello", "Bye"]}

        test_df = {"payment_type": pd.DataFrame(data=d)}
        result = create_dim_payment_type(test_df)
        expected_columns = ["payment_type_id", "payment_type_name"]
        expected_d = {
            "payment_type_id": ["Hello", "Bye"],
            "payment_type_name": ["Hello", "Bye"],
        }
        expected_df = pd.DataFrame(data=expected_d)
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == expected_columns
        assert result.equals(expected_df)


class TestCreateDimCounterparty:
    def test_create_dim_counterparty_type_returns_correct_columns_and_object(self):
        data_l = pd.DataFrame(
            data={
                "counterparty_id": ["Hello", "Bye"],
                "counterparty_legal_name": ["Hello", "Bye"],
                "commercial_contact": ["Hello", "Bye"],
                "legal_address_id": ["bond street", "regent street"],
            }
        )
        data_a = pd.DataFrame(
            data={
                "address_id": ["bond street", "regent street"],
                "postcode": [98365, 93753],
            }
        )
        test_df = {"address": data_a, "counterparty": data_l}
        result = create_dim_counterparty(test_df)

        expected_columns = [
            "counterparty_id",
            "counterparty_legal_name",
            "commercial_contact",
            "counterparty_legal_postcode",
        ]
        print(data_l)
        print(data_a)
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == expected_columns


class TestCreateDimCurrency:
    def test_dim_currency_returns_columns_and_values(self):
        nones = [None, None, None]
        d = {
            "currency_id": [1, 2, 3],
            "currency_code": ["USD", "EUR", "GBP"],
            "created_at": nones,
            "last_updated": nones,
        }
        test_df = {"currency": pd.DataFrame(data=d)}
        scraper_output = pd.DataFrame(
            {
                "currency_code": ["RUS", "USD", "PHP", "GBP", "EUR"],
                "currency_name": ["Rubble", "US Dollar", "Peso", "Pound", "Euro"],
            }
        )
        result = create_dim_currency(test_df, names=scraper_output).sort_values(
            by="currency_code", axis=0
        )
        expected_d = {
            "currency_id": [1, 2, 3],
            "currency_code": ["USD", "EUR", "GBP"],
            "currency_name": ["US Dollar", "Euro", "Pound"],
        }
        expected_df = pd.DataFrame(data=expected_d).sort_values(
            by="currency_code", axis=0
        )
        assert isinstance(result, pd.DataFrame)
        assert result.equals(expected_df)

    def test_scrape_currency_names_returns_dataframe_with_correct_collumns(self):
        result = scrape_currency_names()
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ["currency_code", "currency_name"]


class TestCreateDimDate:
    def test_returns_required_columns(self):
        df_one = pd.DataFrame(
            data={
                "updated_date": dt(2020, 5, 17),
                "created_date": dt(2021, 5, 13),
                "not_dat": None,
            },
            index=[0],
        )
        df_two = pd.DataFrame(
            data={"updated_date": dt(2020, 5, 17),
                  "created_date": dt(2021, 9, 13)},
            index=[0],
        )
        df_three = pd.DataFrame(
            data={"updated_date": dt(2022, 5, 17),
                  "created_date": dt(2023, 5, 13)},

            index=[0],
        )
        expected_df = pd.DataFrame(
            data=[
                [dt(2020, 5, 17), 2020, 5, 17, 6, "Sunday", "May", 2],
                [dt(2021, 5, 13), 2021, 5, 13, 3, "Thursday", "May", 2],
                [dt(2021, 9, 13), 2021, 9, 13, 0, "Monday", "September", 3],
                [dt(2022, 5, 17), 2022, 5, 17, 1, "Tuesday", "May", 2],
                [dt(2023, 5, 13), 2023, 5, 13, 5, "Saturday", "May", 2],
            ],
            columns=[
                "date_id",
                "year",
                "month",
                "day",
                "day_of_week",
                "day_name",
                "month_name",
                "quarter",
            ],
        )
        with patch("src.dataframes.create_fact_payment") as mock_fp:
            with patch("src.dataframes.create_fact_purchase_orders") as mock_fpo:
                with patch("src.dataframes.create_fact_sales_order") as mock_fso:
                    mock_fp.return_value = df_one
                    mock_fpo.return_value = df_two
                    mock_fso.return_value = df_three
                    result = create_dim_date({"dum": 0})
                    result.reset_index(inplace=True, drop=True)
                    assert result.eq(
                        expected_df, axis="columns").all(axis=None)


class TestCreateDimLocation:
    def test_returns_correct_columns_lo(self):
        dict_df = {
            "address": pd.DataFrame(
                data=[["some_time", "some_other_time", 1, "SE18 9QO"]],
                columns=["created_at", "last_updated",
                         "address_id", "postal_code"],

            )
        }
        result = create_dim_location(dict_df)
        assert list(result.columns) == ["location_id", "postal_code"]


class TestCreateDimTransaction:
    def test_returns_correct_columns_tr(self):
        dict_df = {
            "transaction": pd.DataFrame(
                data=[["some_time", "some_other_time", 1, "SE18 9QO"]],
                columns=[
                    "created_at",
                    "last_updated",
                    "transaction_id",
                    "some_other_id",
                ],
            )
        }
        result = create_dim_transaction(dict_df)
        assert list(result.columns) == ["transaction_id", "some_other_id"]


class TestCreateFactPayment:
    def test_returns_correct_columns_payment(self):
        dict_df = {
            "payment": pd.DataFrame(
                data=[
                    [
                        dt.strptime(
                            "2022-11-03 14:20:49.962846", "%Y-%m-%d %H:%M:%S.%f"
                        ),
                        dt.strptime(
                            "2022-12-14 16:20:49.962194", "%Y-%m-%d %H:%M:%S.%f"
                        ),
                        1,
                        "SE18 9QO",
                        "2020-07-16",
                    ]
                ],
                columns=[
                    "created_at",
                    "last_updated",
                    "payment_id",
                    "some_other_id",
                    "payment_date",
                ],
            )
        }
        expected_cols = [
            "payment_record_id",
            "created_date",
            "created_time",
            "last_updated_date",
            "last_updated_time",
            "payment_date",
            "payment_id",
            "some_other_id",
        ]
        result = create_fact_payment(dict_df)
        assert isinstance(result, pd.DataFrame)
        for col in list(result.columns):
            assert col in expected_cols
        for col in expected_cols:


if "_date" or "_time" in col:
    assert result[col].dtype == "O"

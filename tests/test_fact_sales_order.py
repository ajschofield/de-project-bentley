import pandas as pd
from fact_sales_order import create_dim_design, create_dim_staff, create_dim_currency
from src.fact_sales_order import (
    create_dim_design,
    create_dim_staff,
    create_dim_currency,
)
<< << << < Updated upstream
== == == =
>>>>>> > Stashed changes


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


class TestCreateDimCurrency:
    def test_dim_currency_returns_dataframe(self):
        d = {"currency_id": [1, 2, 3], "currency_code": ["USD", "EUR", "GBP"]}
        test_df = {"currency": pd.DataFrame(data=d)}
        result = create_dim_currency(test_df)
        assert isinstance(result, pd.DataFrame)

    def test_dim_currency_returns_columns_and_values(self):
        d = {"currency_id": [1, 2, 3], "currency_code": ["USD", "EUR", "GBP"]}
        test_df = {"currency": pd.DataFrame(data=d)}
        result = create_dim_currency(test_df)
        expected_d = {
            "currency_id": [1, 2, 3],
            "currency_code": ["USD", "EUR", "GBP"],
            "currency_name": ["US Dollar", "Euro", "Pound"],
        }
        expected_df = pd.DataFrame(data=expected_d)
        expected_result = expected_df.copy()
        assert result.equals(expected_result)

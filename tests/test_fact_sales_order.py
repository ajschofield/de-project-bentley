from src.dataframes import create_dim_design, create_dim_staff, create_dim_payment_type, create_dim_counterparty, create_dim_currency
import pandas as pd
from unittest.mock import patch

class TestCreateDimDesign:
    def test_dim_design_returns_dataframe(self):
        d = {"test": ["Hello", "Bye"], "design_id": ["Hello", "Bye"], "design_name": ["Hello", "Bye"], 
                "file_name": ["Hello", "Bye"], "file_location": ["Hello", "Bye"], "Hello": ["Hello", "Bye"]}
        test_df = {"design": pd.DataFrame(data=d)}
        result = create_dim_design(test_df)
        assert isinstance(result, pd.DataFrame)

    def test_dim_design_returns_correct_columns_and_values(self):
        d = {"test": ["Hello", "Bye"], "design_id": ["Hello", "Bye"], "design_name": ["Hello", "Bye"], 
                "file_name": ["Hello", "Bye"], "file_location": ["Hello", "Bye"], "Hello": ["Hello", "Bye"]}
        test_df = {"design": pd.DataFrame(data=d)}
        result = create_dim_design(test_df)
        d2 = {"design_id": ["Hello", "Bye"], "design_name": ["Hello", "Bye"], "file_name": ["Hello", "Bye"], 
            "file_location": ["Hello", "Bye"]}
        expected_df = pd.DataFrame(data=d2)
        expected_result = expected_df.copy()
        assert result.equals(expected_result)

class TestCreateDimStaff:
    def test_dim_staff_returns_dataframe(self):
        d = {"staff_id": ["Hello", "Bye"], "first_name": ["Hello", "Bye"], "last_name": ["Hello", "Bye"], "department_id": ["Hello", "Bye"]}
        d2 = {"department_name": ["Hello", "Bye"], "location": ["Hello", "Bye"], "email_address": ["Hello", "Bye"], "department_id": ["Hello", "Bye"]}
        test_df = {"staff": pd.DataFrame(data=d), "department": pd.DataFrame(data=d2)}
        result = create_dim_staff(test_df)
        assert isinstance(result, pd.DataFrame)  

    def test_dim_staff_returns_correct_columns_and_values(self):
        d = {"staff_id": ["Hello", "Bye"], "first_name": ["Hello", "Bye"], "last_name": ["Hello", "Bye"], "department_id": ["Hello", "Bye"]}
        d2 = {"department_name": ["Hello", "Bye"], "location": ["Hello", "Bye"], "email_address": ["Hello", "Bye"], "department_id": ["Hello", "Bye"]}
        test_df = {"staff": pd.DataFrame(data=d), "department": pd.DataFrame(data=d2)}
        result = create_dim_staff(test_df)
        expected_d = {"staff_id": ["Hello", "Bye"], "first_name": ["Hello", "Bye"], "last_name": ["Hello", "Bye"], "department_name": ["Hello", "Bye"], "location": ["Hello", "Bye"], "email_address": ["Hello", "Bye"]}
        expected_df = pd.DataFrame(data=expected_d)
        expected_result = expected_df.copy()
        assert result.equals(expected_result)  

class TestCreatePaymentType:
    def test_create_dim_payment_type_returns_correct_columns_and_values(self):
        d = {"payment_type_id": ["Hello", "Bye"], "payment_type_name": ["Hello", "Bye"]}
        test_df = {"payment_type": pd.DataFrame(data=d)}
        result = create_dim_payment_type(test_df)
        expected_columns = ["payment_type_id", "payment_type_name"]
        expected_d = {"payment_type_id": ["Hello", "Bye"], "payment_type_name": ["Hello", "Bye"]}
        expected_df = pd.DataFrame(data=expected_d)
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == expected_columns
        assert result.equals(expected_df)

class TestCreateDimCounterparty:
    def test_create_dim_counterparty_type_returns_correct_columns_and_values(self):
        data_d = {"counterparty_id": ["Hello", "Bye"], 
             "counterparty_legal_name": ["Hello", "Bye"], 
             "counterparty_legal_address_line_1": ["Hello", "Bye"], 
             }
        data_a = {"address_id":
                  "address",
                  }
        test_df = {"address": pd.DataFrame(data=data_a)}
        test_df = {}
        result = create_dim_counterparty(test_df)

        expected_columns = ["counterparty_id", 
             "counterparty_legal_name", 
             "counterparty_legal_address_line_1", 
             "counterparty_legal_address_line_2", 
             "counterparty_legal_district",
             "counterparty_legal_city",
             "counterparty_legal_postal_code",
             "counterparty_legal_postal_code", 
             "counterparty_legal_phone_number"]
        expected_d = {"counterparty_id": ["Hello", "Bye"], 
             "counterparty_legal_name": ["Hello", "Bye"], 
             "counterparty_legal_address_line_1": ["Hello", "Bye"], 
             "counterparty_legal_address_line_2": ["Hello", "Bye"], 
             "counterparty_legal_district": ["Hello", "Bye"],
             "counterparty_legal_city": ["Hello", "Bye"],
             "counterparty_legal_postal_code": ["Hello", "Bye"],
             "counterparty_legal_postal_code": ["Hello", "Bye"], 
             "counterparty_legal_phone_number": ["Hello", "Bye"]}
        expected_df = pd.DataFrame(data=expected_d)
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == expected_columns
        assert result.equals(expected_df)

# # figuring out how to mock currency scraper functiom
# class TestCreateDimCurrency:
#     @patch("src.dataframes.scrape_currency_names")  
#     def test_dim_currency_returns_columns_and_values(self):
#         d = {"currency_id": [1, 2, 3], "currency_code": ["USD", "EUR", "GBP"]}
#         test_df = {"currency": pd.DataFrame(data=d)}
#         result = create_dim_currency(test_df)
#         expected_d = {"currency_id": [1, 2, 3], "currency_code": ["USD", "EUR", "GBP"], "currency_name": ["US Dollar", "Euro", "Pound"]}
#         expected_df = pd.DataFrame(data=expected_d)
#         expected_result = expected_df.copy()
#         assert result.equals(expected_result)  

#     def test_dim_currency_returns_dataframe(self):
#         d = {"currency_id": [1, 2, 3], "currency_code": ["USD", "EUR", "GBP"]}
#         test_df = {"currency": pd.DataFrame(data=d)}
#         result = create_dim_currency(test_df)
#         assert isinstance(result, pd.DataFrame)  
        
    

    
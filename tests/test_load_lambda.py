import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO
from src.load_lambda import convert_parquet_files_to_dfs

class TestConvertParquetToDFs:
    def test_convert_parquet_to_dfs_returns_df():
        
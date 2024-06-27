import pandas as pd
import pytest
import pyarrow.parquet as pq

# Path to the Parquet file generated by previous task
parquet_file = "/opt/airflow/data/processed_data.parquet"

# Fixture to load the Parquet file as a Pandas DataFrame
@pytest.fixture(scope="module")

def load_parquet():
    df = pd.read_parquet(parquet_file)
    yield df

# Example of data quality checks using pytest
def test_column_existence(load_parquet):
    expected_columns = ['reporting_entity_name', 'reporting_entity_type', 'plan_name', 'plan_id', 'plan_id_type',
                        'plan_market_type', 'description', 'location']
    assert all(col in load_parquet.columns for col in expected_columns), "Missing columns in Parquet file."

def test_null_values(load_parquet):
    assert load_parquet.isnull().sum().sum() == 0, "Null values found in Parquet file."

def test_data_types(load_parquet):
    expected_data_types = {
        'reporting_entity_name': object,
        'reporting_entity_type': object,
        'plan_name': object,
        'plan_id': pd.Int64Dtype(),
        'plan_id_type': object,
        'plan_market_type': object,
        'description': object,
        'location': object
    }
    for col, dtype in expected_data_types.items():
        assert load_parquet[col].dtype == dtype, f"Unexpected data type for column {col}."


if __name__ == "__main__":
    pytest.main()

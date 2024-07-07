import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
import subprocess
import pandas as pd
import polars as pl
from cuallee import Check, CheckLevel
# Function to check URL using Selenium with a remote ChromeDriver
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities


def check_url_with_selenium(url, timeout=10):
    """Check if a URL loads successfully using Selenium with a remote ChromeDriver."""

    remote_webdriver_url = 'http://remote_chromedriver:4444/wd/hub'  # Update with your remote WebDriver URL

    # Set up Chrome options if needed
    chrome_options = webdriver.ChromeOptions()
    # Add any desired options here

    driver = None
    try:
        # Use webdriver.Remote to connect to the remote ChromeDriver
        driver = webdriver.Remote(
            command_executor=remote_webdriver_url,
            options=chrome_options
        )

        driver.get(url)
        # Optionally, wait for the page to load completely
        driver.implicitly_wait(timeout)

        # Get the response code
        response_code = driver.execute_script("return document.readyState")

        print(f"Response code for {url}: {response_code}")

    except Exception as e:
        print(f"Error checking URL {url}: {e}")
        return False

    finally:
        if driver is not None:
            driver.quit()

    return True

# Path to the Parquet file generated by previous task
today_date = datetime.now().strftime('%Y%m%d')
parquet_file = f"/opt/airflow/data/processed/processed_data_{today_date}.parquet"
def perform_data_validation(parquet_file):
    # Load Parquet file into Polars DataFrame
    df = pl.read_parquet(parquet_file)

    # List of column names to check completeness
    columns_to_check = [
        "reporting_entity_name",
        "reporting_entity_type",
        "plan_name",
        "plan_id",
        "plan_id_type",
        "plan_market_type",
        "description",
        "location"
    ]

    def check_completeness(pl_df, column_name):
        check = Check(CheckLevel.ERROR, "Completeness")
        validation_results_df = check.is_complete(column_name).validate(pl_df)
        return validation_results_df["status"].to_list()

    def null_value_check(pl_df, column_name):
        check = Check()
        check.is_complete(column_name)
        validation_results_df = check.validate(pl_df)
        return validation_results_df["status"].to_list()

    completeness_status = check_completeness(df, columns_to_check)
    null_value_status = null_value_check(df, "plan_id")

    # Combine results into a single DataFrame
    completeness_df = pd.DataFrame({
        'Check Type': ['Completeness'] * len(completeness_status),
        'Status': completeness_status
    })

    null_value_df = pd.DataFrame({
        'Check Type': ['Null Value Check'] * len(null_value_status),
        'Status': null_value_status
    })

    combined_df = pd.concat([completeness_df, null_value_df], ignore_index=True)

    if 'FAIL' in combined_df['Status'].values:
        return 'Data Validation check Failed'
    else:
        return 'Data Validation Check Passed'


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': None,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'UHG_web_scrap',
    default_args=default_args,
    description='A DAG to download files on a schedule',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Task to check URL before extraction
check_url_task = PythonOperator(
    task_id='check_url',
    python_callable=check_url_with_selenium,
    op_kwargs={'url': 'https://transparency-in-coverage.uhc.com/'},
    dag=dag,
)

# Task to extract data
Extract = BashOperator(
    task_id="Extract",
    bash_command="python /opt/airflow/dags/scripts/spark/Extract_Json.py",
    dag=dag,
)

flat = BashOperator(
    task_id="flattened_json",
    bash_command="python /opt/airflow/dags/scripts/spark/flattened_json.py",
    dag=dag,
)

data_validation_task = PythonOperator(
    task_id='data_validation',
    python_callable=perform_data_validation,
    op_kwargs={'parquet_file': parquet_file},
    provide_context=True,  # Ensure this is set to True when passing kwargs
    dag=dag,
)

load_data = BashOperator(
    task_id="load_data",
    bash_command="python /opt/airflow/dags/scripts/spark/Load_data.py",
    dag=dag,
)


check_url_task >> Extract >> flat >> data_validation_task >> load_data


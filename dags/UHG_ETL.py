import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Function to check URL using Selenium with a remote ChromeDriver
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

from selenium import webdriver


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
    'download_dag',
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

test = BashOperator(
    task_id="Quality_test",
    bash_command="python /opt/airflow/dags/scripts/spark/test_data.py",
    dag=dag,
)

# Set task dependencies
check_url_task >> Extract >> flat >> test


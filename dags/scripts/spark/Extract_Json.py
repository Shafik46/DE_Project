import os
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time
import logging
import sys

# Setup logger
error_log_directory = "/opt/airflow/logs/"
status_log_directory = "/opt/airflow/logs/"
os.makedirs(error_log_directory, exist_ok=True)
os.makedirs(status_log_directory, exist_ok=True)

error_log_file = os.path.join(error_log_directory, "download_errors.log")
status_log_file = os.path.join(status_log_directory, "download_status.log")

# Create and configure error logger
error_logger = logging.getLogger("error_logger")
error_handler = logging.FileHandler(error_log_file)
error_handler.setLevel(logging.ERROR)
error_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
error_handler.setFormatter(error_formatter)
error_logger.addHandler(error_handler)
error_logger.setLevel(logging.ERROR)

# Create and configure status logger
status_logger = logging.getLogger("status_logger")
status_handler = logging.FileHandler(status_log_file)
status_handler.setLevel(logging.INFO)
status_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
status_handler.setFormatter(status_formatter)
status_logger.addHandler(status_handler)
status_logger.setLevel(logging.INFO)

# Constants for configuration
URL = "https://transparency-in-coverage.uhc.com/"
DIRECTORY = "/opt/airflow/data/raw/"
TIMEOUT = 200

def fetch_webpage(url, timeout=200):
    """Fetch the webpage content using Selenium and return the page source."""
    try:
        remote_webdriver_url = 'http://remote_chromedriver:4444/wd/hub'  # Update with your remote WebDriver URL

        chrome_options = webdriver.ChromeOptions()
        # Add any desired options here

        # Use webdriver.Remote to connect to the remote ChromeDriver
        driver = webdriver.Remote(command_executor=remote_webdriver_url, options=chrome_options)
        driver.get(url)
        time.sleep(timeout)
        html = driver.page_source

    except Exception as e:
        error_logger.error(f"Error fetching webpage from {url}: {e}")
        html = None

    finally:
        if 'driver' in locals():
            driver.quit()

    return html

def parse_links(html, start_url):
    """Parse the webpage to find all download links starting with the given URL."""
    try:
        soup = BeautifulSoup(html, features="html.parser")
        download_links = [link["href"] for link in soup.find_all("a") if link.get("href", "").startswith(start_url)]
        return download_links
    except Exception as e:
        error_logger.error(f"Error parsing links: {e}")
        return []

def download_file(link, directory, index):
    """Download a file from the given link and save it to the specified directory."""
    try:
        response = requests.get(link, timeout=TIMEOUT)
        response.raise_for_status()

        filename = os.path.basename(link.split('?')[0])
        file_path = os.path.join(directory, filename)

        with open(file_path, "wb") as file:
            file.write(response.content)

        status_logger.info(f"{index}: {filename} - Downloaded successfully")
        print(f"{index}: {filename} - Downloaded successfully")

    except requests.HTTPError as e:
        error_logger.error(f"{index}: {link} - HTTP error occurred: {e.response.status_code} - {e}")
        raise  # Raise the exception to propagate it up
    except requests.ConnectionError:
        error_logger.error(f"{index}: {link} - Connection error occurred.")
        raise
    except requests.Timeout:
        error_logger.error(f"{index}: {link} - Timeout occurred.")
        raise
    except requests.RequestException as e:
        error_logger.error(f"{index}: {link} - Error during request: {e}")
        raise
    except OSError as e:
        error_logger.error(f"{index}: {filename} - OS error when saving the file: {e}")
        raise
    except Exception as e:
        error_logger.error(f"{index}: {link} - Unexpected error: {e}")
        raise

def main():
    """Main function to orchestrate the download process."""
    try:
        html = fetch_webpage(URL)
        if not html:
            raise ValueError(f"Empty HTML content retrieved from {URL}")

        start_url = "https://mrfstorageprod.blob.core.windows.net/public-mrf"
        download_links = parse_links(html, start_url)
        first_10_links = download_links[:10]  # Download the first 10 links

        for i, link in enumerate(first_10_links):
            download_file(link, DIRECTORY, i)

        # Set a flag indicating downloads are completed
        downloads_completed = True

    except Exception as e:
        error_logger.error(f"Error in main process: {e}")
        downloads_completed = False

    finally:
        logging.shutdown()

        # Terminate the script if downloads are completed successfully
        if downloads_completed:
            sys.exit(0)  # Signal to Airflow that the task is complete
        else:
            sys.exit(1)  # Signal to Airflow that the task has failed

if __name__ == "__main__":
    main()

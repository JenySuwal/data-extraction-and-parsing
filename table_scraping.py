import boto3
from botocore.exceptions import NoCredentialsError
import undetected_chromedriver as uc
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import shutil
from selenium.webdriver.common.keys import Keys
import os
import time
import random
from urllib.parse import urlparse
from tqdm import tqdm  


SLEEP_TIME_MIN = 2
SLEEP_TIME_MAX = 5

s3_client = boto3.client("s3", region_name="ap-south-1")  
S3_BUCKET_NAME = "scraped-unstructured-data"  

def upload_to_s3(local_file, s3_path):
    try:
        s3_client.upload_file(local_file, S3_BUCKET_NAME, s3_path)
        print(f"File uploaded to S3: s3://{S3_BUCKET_NAME}/{s3_path}")
        return f"s3://{S3_BUCKET_NAME}/{s3_path}"
    except NoCredentialsError:
        print("AWS credentials not found. Make sure they are configured.")
        return None
    except Exception as e:
        print(f"Error uploading file to S3: {e}")
        return None

def sleep_random():
    time.sleep(random.uniform(SLEEP_TIME_MIN, SLEEP_TIME_MAX))

def check_visibility(driver, xpath):
    try:
        return len(driver.find_elements(By.XPATH, xpath)) > 0
    except:
        return False

def scroll_to_view(driver, element):
    driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", element)
    sleep_random()

def scroll_whole_page(driver):
    """Scrolls the page to load all tables."""
    if not check_visibility(driver, "//table"):
        print("No tables found.")
        return False

    tables = driver.find_elements(By.TAG_NAME, "table")
    print(f"Total Tables Found Initially: {len(tables)}")

    for table in tables:
        try:
            scroll_to_view(driver, table)
        except:
            driver.execute_script("window.scrollBy(0, 400);")
            sleep_random()

    
    return True

def fetch_tables_html(url,crawl_id):
    try:
        chrome_options = Options()
        chrome_options.add_argument("--headless")  
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--remote-debugging-port=9222")
        chrome_options.binary_location = "/usr/bin/google-chrome"

        chromedriver_path = shutil.which("chromedriver")
        if not chromedriver_path:
            raise FileNotFoundError("Chromedriver not found. Make sure it's installed and in PATH.")

        # print(f"Using Chromedriver: {chromedriver_path}")

        driver = uc.Chrome(options=chrome_options, driver_executable_path=chromedriver_path)
        # print("Chrome launched successfully.")

        driver.get(url)
        sleep_random()

        scroll_whole_page(driver)

        tables = driver.find_elements(By.TAG_NAME, "table")
        tables_html = "\n".join([table.get_attribute("outerHTML") for table in tables])
        driver.quit()

        domain = urlparse(url).netloc.replace(".", "_")
        folder_path = os.path.join(os.getcwd(), domain)
        os.makedirs(folder_path, exist_ok=True)

        
        file_name = f"tables_{crawl_id}.html"
        local_file_path = os.path.join(folder_path, file_name)
        
        with open(local_file_path, "w", encoding="utf-8") as f:
            f.write(f"<html><body>{tables_html}</body></html>")

        print(f"Tables saved to: {local_file_path}")

        s3_path = f"{domain}/{file_name}"
        s3_url = upload_to_s3(local_file_path, s3_path)

        return s3_url

    except Exception as e:
        print(f"Error fetching tables from {url}: {e}")
        return None

if __name__ == "__main__":
    url = input("Enter the URL to scrape tables from: ")
    result_file = fetch_tables_html(url)
    if result_file:
        print(f"Tables have been successfully saved to {result_file}")
    else:
        print("An error occurred while fetching tables.")

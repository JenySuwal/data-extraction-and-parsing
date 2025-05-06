import boto3
from botocore.exceptions import NoCredentialsError
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import os
import time
from selenium.webdriver.chrome.options import Options
import random


SLEEP_TIME_MIN = 2
SLEEP_TIME_MAX = 5


# AWS Setup
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
def scroll_to_view(driver, element):
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
    time.sleep(0.5)
def sleep_random():
    time.sleep(random.uniform(SLEEP_TIME_MIN, SLEEP_TIME_MAX))

import undetected_chromedriver as uc
def scrape_tables_with_delivery(url):
    chrome_options = Options()
    # chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--remote-debugging-port=9222")
    chrome_options.binary_location = "/usr/bin/google-chrome"
    chromedriver_path = "/usr/local/bin/chromedriver"  
    # chromium_path = "/snap/bin/chromium"
    if not chromedriver_path:
        raise FileNotFoundError("Chromedriver not found. Make sure it's installed and in PATH.")

    driver = uc.Chrome(options=chrome_options, driver_executable_path=chromedriver_path)

    driver.get(url)
    sleep_random()
    scroll_whole_page(driver)
    # driver = webdriver.Chrome(options=options)
    # driver.get(url)

    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.TAG_NAME, 'table'))
        )

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        tables = soup.find_all('table')
        processed_tables = []

        for table in tables:
            rows = table.find_all('tr')
            processed_rows = []

            for row in rows:
                cells = row.find_all(['td', 'th'])
                part_links = []
                delivery_dates = []

                for cell in cells:
                    part_links.extend(cell.find_all('a', class_='PartNbrLnk'))

                for part_link in part_links:
                    part_number = part_link.text.strip()
                    delivery_date = "Delivery info not found"

                    try:
                        # Find and click part number
                        part_number_element = WebDriverWait(driver, 20).until(
                            EC.presence_of_element_located((By.XPATH, f"//a[contains(text(), '{part_number}')]"))
                        )
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", part_number_element)
                        part_number_element.click()

                        # Handle quantity input
                        quantity_input = WebDriverWait(driver, 15).until(
                            EC.presence_of_element_located((By.XPATH, "//input[starts-with(@id,'qtyInp')]"))
                        )
                        quantity_input.clear()
                        quantity_input.send_keys("1")
                        
                        # Wait for button to be clickable
                        add_to_order = WebDriverWait(driver, 15).until(
                            EC.element_to_be_clickable((By.XPATH,
                            "//button[contains(@class,'button-add-to-order-inline add-to-order')]"))
                        )
                        
                        # Try direct click first, fallback to JS click
                        try:
                            add_to_order.click()
                        except:
                            driver.execute_script("arguments[0].click();", add_to_order)
                        
                        # Get delivery message
                        delivery_msg = WebDriverWait(driver, 15).until(
                            EC.presence_of_element_located((By.XPATH,
                            "//div[contains(@class,'InLnOrdWebPartLayout_ItmAddedMsg')]"))
                        )
                        delivery_text = delivery_msg.text.split('\n')
                        if len(delivery_text) > 1:
                            delivery_date = delivery_text[1].strip()

                        # Try to close the dialog
                        try:
                            close_btn = driver.find_element(By.XPATH, "//button[contains(@class,'close-button')]")
                            driver.execute_script("arguments[0].click();", close_btn)
                        except:
                            driver.back()

                    except Exception as e:
                        print(f"Error processing part {part_number}: {str(e)}")
                        try:
                            driver.back()
                        except:
                            pass

                    delivery_dates.append(delivery_date)

                processed_cells = [str(cell) for cell in cells]

                for date in delivery_dates:
                    delivery_cell = soup.new_tag("td")
                    delivery_cell.string = date
                    delivery_cell['class'] = 'delivery-date'
                    processed_cells.append(str(delivery_cell))

                processed_rows.append(f"<tr>{''.join(processed_cells)}</tr>")

            processed_tables.append(f"<table>{''.join(processed_rows)}</table>")

        return "\n".join(processed_tables)

    finally:
        driver.quit()

def fetch_tables_html(url, crawl_id):
    try:
        html_content = scrape_tables_with_delivery(url)

        domain = urlparse(url).netloc.replace(".", "_")
        folder_path = os.path.join(os.getcwd(), domain)
        os.makedirs(folder_path, exist_ok=True)

        file_name = f"tables_{crawl_id}.html"
        local_file_path = os.path.join(folder_path, file_name)

        with open(local_file_path, "w", encoding="utf-8") as f:
            f.write(f"<html><body>{html_content}</body></html>")

        s3_path = f"{domain}/{file_name}"
        s3_url = upload_to_s3(local_file_path, s3_path)

        return s3_url

    except Exception as e:
        print(f"Error fetching tables from {url}: {e}")
        return None


if __name__ == "__main__":
    url = input("Enter the URL to scrape tables from: ")
    crawl_id = input("Enter the crawl ID: ")
    result_file = fetch_tables_html(url, crawl_id)

    if result_file:
        print(f"Tables have been successfully saved to {result_file}")
    else:
        print("An error occurred while fetching tables.")

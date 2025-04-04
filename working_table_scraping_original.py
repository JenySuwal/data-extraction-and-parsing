from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import os
import time
import random
from urllib.parse import urlparse
from tqdm import tqdm
import undetected_chromedriver as uc

SLEEP_TIME_MIN = 2
SLEEP_TIME_MAX = 5

def sleep_random():
    time.sleep(random.uniform(SLEEP_TIME_MIN, SLEEP_TIME_MAX))

def scroll_to_view(driver, element):
    driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", element)
    sleep_random()

def check_visibility(driver, xpath):
    try:
        return len(driver.find_elements(By.XPATH, xpath)) > 0
    except:
        return False

def scroll_whole_page(driver):
    """Scrolls the page to load all tables."""
    if not check_visibility(driver, "//table"):
        print("No tables found.")
        return False

    tables = driver.find_elements(By.TAG_NAME, "table")
    print(f"Total Tables Found Initially: {len(tables)}")

    for table in tqdm(tables, desc="Scrolling through tables"):
        try:
            scroll_to_view(driver, table)
        except:
            driver.execute_script("window.scrollBy(0, 400);")
            sleep_random()

    print("Finished scrolling.")
    return True

def fetch_tables_html(url):
    """Fetch tables from a webpage and save as HTML."""
    chrome_options = Options()
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    # chrome_options.add_argument("--headless")  # Run in headless mode
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    chromedriver_path = "chromedriver.exe"

    driver = uc.Chrome(driver_executable_path=chromedriver_path, options=chrome_options)
    driver.get(url)

    sleep_random()
    scroll_whole_page(driver)

    tables = driver.find_elements(By.TAG_NAME, "table")
    tables_html = "\n".join([table.get_attribute("outerHTML") for table in tables])

    driver.quit()
    print(f"Total tables extracted: {len(tables)}")

    domain = urlparse(url).netloc.replace(".", "_")
    folder_path = os.path.join(os.getcwd(), domain)
    os.makedirs(folder_path, exist_ok=True)

    file_path = os.path.join(folder_path, f"tables_{int(time.time())}.html")
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(f"<html><body>{tables_html}</body></html>")

    print(f"Tables saved to: {file_path}")

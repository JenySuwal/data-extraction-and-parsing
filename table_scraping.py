from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import os
import time
import random
import undetected_chromedriver as uc
from urllib.parse import urlparse
from tqdm import tqdm

SLEEP_TIME_MIN = 2
SLEEP_TIME_MAX = 5

def sleep_random():
    """Sleep for a random time between defined min and max values."""
    time.sleep(random.uniform(SLEEP_TIME_MIN, SLEEP_TIME_MAX))

def scroll_to_view(driver, element):
    """Scroll an element into view."""
    driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", element)
    sleep_random()

def check_visibility(driver, xpath):
    """Check if an element is visible on the page."""
    try:
        return len(driver.find_elements(By.XPATH, xpath)) > 0
    except:
        return False

def scroll_whole_page(driver):
    """Scrolls through the page to ensure all tables are loaded."""
    body = driver.find_element(By.TAG_NAME, "body")
    active_element = driver.switch_to.active_element

    if not check_visibility(driver, "//table"):
        print("Could not find tables on the page.")
        return False

    tables = driver.find_elements(By.TAG_NAME, "table")
    table_count = len(tables)
    print(f"Total Tables Found Initially: {table_count}")

    print("Scrolling through the page to load all tables...")

    for index in tqdm(range(table_count), desc="Loading Tables"):
        try:
            tables = driver.find_elements(By.TAG_NAME, "table")
            scroll_to_view(driver, tables[index])
        except:
            body.click()
            sleep_random()
            active_element.send_keys(Keys.PAGE_DOWN)

    print("Finished scrolling the entire page.")
    return True

def fetch_tables_html(url):
    """Main function to fetch tables and save as HTML."""
    chrome_options = Options()
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    driver = uc.Chrome(options=chrome_options)
    print(f"Loading: {url}")
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

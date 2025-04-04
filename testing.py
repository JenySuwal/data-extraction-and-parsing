import undetected_chromedriver as uc
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time
import random
import os
import shutil
from urllib.parse import urlparse

SLEEP_TIME_MIN = 2
SLEEP_TIME_MAX = 5

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

    print("Finished scrolling.")
    return True

def fetch_tables_html(url):
    try:
        # Configure Chrome options for WSL
        chrome_options = Options()
        # chrome_options.add_argument("--headless")  # Uncomment if running in headless mode
        chrome_options.add_argument("--disable-gpu")  # Disable GPU acceleration
        chrome_options.add_argument("--no-sandbox")  # Required for running in WSL
        chrome_options.add_argument("--disable-dev-shm-usage")  # Prevents crashes in WSL
        chrome_options.add_argument("--remote-debugging-port=9222")  # Debugging support
        chrome_options.binary_location = "/usr/bin/google-chrome"  # or wherever Chrome is installed
        # Check if chromedriver exists in PATH
        chromedriver_path = shutil.which("chromedriver")#/usr/local/bin/chromedriver
        if not chromedriver_path:
            raise FileNotFoundError("Chromedriver not found. Make sure it's installed and in PATH.")

        print(f"Using Chromedriver: {chromedriver_path}")

        # Launch undetected Chrome driver
        driver = uc.Chrome(options=chrome_options, driver_executable_path=chromedriver_path)
        print("Chrome launched successfully.")

        driver.get(url)
        sleep_random()

        # Scroll the page to ensure all tables are loaded
        scroll_whole_page(driver)

        # Extract tables
        tables = driver.find_elements(By.TAG_NAME, "table")
        tables_html = "\n".join([table.get_attribute("outerHTML") for table in tables])
        driver.quit()

        # Create output directory
        domain = urlparse(url).netloc.replace(".", "_")
        folder_path = os.path.join(os.getcwd(), domain)
        os.makedirs(folder_path, exist_ok=True)

        # Save the tables to an HTML file
        file_path = os.path.join(folder_path, f"tables_{int(time.time())}.html")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(f"<html><body>{tables_html}</body></html>")

        print(f"Tables saved to: {file_path}")
        return file_path

    except Exception as e:
        print(f"Error fetching tables from {url}: {e}")
        return None

if __name__ == "__main__":
    # URL to scrape
    url = input("Enter the URL to scrape tables from: ")

    # Fetch and save tables
    result_file = fetch_tables_html(url)
    if result_file:
        print(f"Tables have been successfully saved to {result_file}")
    else:
        print("An error occurred while fetching tables.")

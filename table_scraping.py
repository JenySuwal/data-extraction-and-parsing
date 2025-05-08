# scraping_module.py
import os
import re
import time
import random
import logging
from datetime import datetime
from urllib.parse import urlparse

import boto3
import requests
from botocore.exceptions import NoCredentialsError
from selenium.common.exceptions import (StaleElementReferenceException, 
                                     NoSuchElementException,
                                     TimeoutException)
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from seleniumbase import SB

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
SLEEP_TIME_MIN = 2
SLEEP_TIME_MAX = 5
S3_BUCKET_NAME = "scraped-unstructured-data"
S3_REGION = "ap-south-1"
DEBUG_MODE = True  # Set to False for production

# AWS Setup
s3_client = boto3.client("s3", region_name=S3_REGION)

def sleep_random():
    """Sleep for a random interval between min and max values."""
    delay = random.uniform(SLEEP_TIME_MIN, SLEEP_TIME_MAX)
    logger.debug(f"Sleeping for {delay:.2f} seconds")
    time.sleep(delay)

def upload_to_s3(local_file, s3_path):
    """Upload a file to S3 bucket."""
    try:
        s3_client.upload_file(local_file, S3_BUCKET_NAME, s3_path)
        logger.info(f"File uploaded to S3: s3://{S3_BUCKET_NAME}/{s3_path}")
        return f"s3://{S3_BUCKET_NAME}/{s3_path}"
    except NoCredentialsError:
        logger.error("AWS credentials not found")
        return None
    except Exception as e:
        logger.error(f"Error uploading file to S3: {e}")
        return None

def get_address():
    """Get geographical location information."""
    try:
        response = requests.get("http://ip-api.com/json", timeout=5)
        if response.status_code == 200:
            data = response.json()
            return f"{data.get('city')}, {data.get('regionName')}, {data.get('country')}"
    except Exception as e:
        logger.warning(f"Couldn't get location: {e}")
        return f"Error: {str(e)}"
    return "Address Not Available"

def scroll_to_view(driver, element):
    """Scroll to make an element visible in the viewport."""
    try:
        driver.execute_script(
            "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", 
            element
        )
        sleep_random()
    except Exception as e:
        logger.warning(f"Couldn't scroll to element: {e}")

def check_visibility(driver, xpath):
    """Check if element is visible on the page."""
    try:
        elements = driver.find_elements(By.XPATH, xpath)
        visible = len(elements) > 0
        logger.debug(f"Visibility check for {xpath}: {visible}")
        return visible
    except Exception as e:
        logger.warning(f"Visibility check failed: {e}")
        return False

def scroll_whole_page(driver):
    """Scrolls the page to load all content."""
    if not check_visibility(driver, "//table"):
        logger.warning("No tables found during initial check")
        return False
    
    last_height = driver.execute_script("return document.body.scrollHeight")
    scroll_attempts = 0
    
    while scroll_attempts < 3:  # Limit scroll attempts
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        sleep_random()
        new_height = driver.execute_script("return document.body.scrollHeight")
        
        if new_height == last_height:
            break
        last_height = new_height
        scroll_attempts += 1
    
    logger.info(f"Finished scrolling after {scroll_attempts} attempts")

def scrape_tables_with_delivery(url):
    """Scrape tables from URL including delivery information."""
    logger.info(f"Starting to scrape: {url}")
    
    with SB(
        uc=True, 
        incognito=True, 
        maximize=True, 
        locale_code="en", 
        skip_js_waits=True, 
        headless=True,  # Run in headful mode if DEBUG_MODE=True
        agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    ) as sb:
        try:
            # Track timing
            start_time = time.time()
            
            # Navigate to URL
            logger.info(f"Loading URL: {url}")
            sb.driver.get(url)
            sleep_random()
            
            # Scroll to load content
            logger.info("Scrolling page to load content")
            scroll_whole_page(sb.driver)
            
            # Verify tables exist
            if not check_visibility(sb.driver, "//table"):
                logger.error("No tables found after scrolling")
                return None
            
            # Get address info
            address = get_address()
            logger.info(f"Detected location: {address}")
            
            # Find all tables
            tables = sb.driver.find_elements(By.XPATH, "//table[contains(@class,'hn')]")
            logger.info(f"Found {len(tables)} tables to process")
            
            processed_tables = []
            
            for table_idx, table in enumerate(tables, 1):
                try:
                    logger.info(f"Processing table {table_idx}/{len(tables)}")
                    
                    # Get table title/header if available
                    try:
                        material_div = table.find_element(
                            By.XPATH,
                            ".//preceding::div[contains(@class,'aq') and contains(@class,'id') and contains(@class,'ju')][1]"
                        )
                        material_text = material_div.find_element(By.XPATH, ".//span").text.strip()
                        type_html = f"<tr><th colspan='100%'>{material_text}</th></tr>" if material_text else ""
                        logger.debug(f"Found table header: {material_text}")
                    except Exception as e:
                        type_html = ""
                        logger.debug("No table header found")
                    
                    # Process rows
                    rows = table.find_elements(By.XPATH, ".//tr")
                    logger.debug(f"Found {len(rows)} rows in table")
                    processed_rows = []
                    
                    for row_idx, row in enumerate(rows, 1):
                        try:
                            logger.debug(f"Processing row {row_idx}/{len(rows)}")
                            
                            # Get all cells and part links
                            cells = row.find_elements(By.XPATH, ".//td|.//th")
                            part_links = row.find_elements(By.XPATH, ".//a[contains(@class,'PartNbrLnk')]")
                            delivery_dates = []
                            
                            logger.debug(f"Found {len(part_links)} part links in row")
                            
                            for link_idx, part_link in enumerate(part_links, 1):
                                part_number = part_link.text.strip()
                                delivery_date = "Delivery info not found"
                                
                                try:
                                    logger.debug(f"Processing part {link_idx}/{len(part_links)}: {part_number}")
                                    
                                    # Click part link
                                    scroll_to_view(sb.driver, part_link)
                                    part_link.click()
                                    logger.debug("Clicked part link")
                                    
                                    # Handle quantity input
                                    try:
                                        quantity_input = WebDriverWait(sb.driver, 15).until(
                                            EC.presence_of_element_located((By.XPATH, "//input[starts-with(@id,'qtyInp')]"))
                                        )
                                        quantity_input.clear()
                                        quantity_input.send_keys("1")
                                        logger.debug("Quantity set to 1")
                                    except TimeoutException:
                                        logger.warning("Timeout waiting for quantity input")
                                        raise
                                    
                                    # Click add to order
                                    try:
                                        add_to_order = WebDriverWait(sb.driver, 15).until(
                                            EC.element_to_be_clickable((By.XPATH,
                                            "//button[contains(@class,'button-add-to-order-inline add-to-order')]"))
                                        )
                                        add_to_order.click()
                                        logger.debug("Clicked add to order button")
                                    except:
                                        sb.driver.execute_script("arguments[0].click();", add_to_order)
                                        logger.debug("Used JS to click add to order")
                                    
                                    # Get delivery message
                                    try:
                                        delivery_msg = WebDriverWait(sb.driver, 15).until(
                                            EC.presence_of_element_located((By.XPATH,
                                            "//div[contains(@class,'InLnOrdWebPartLayout_ItmAddedMsg')]"))
                                        )
                                        delivery_text = delivery_msg.text.split('\n')
                                        if len(delivery_text) > 1:
                                            delivery_date = delivery_text[1].strip()
                                        logger.debug(f"Got delivery date: {delivery_date}")
                                    except TimeoutException:
                                        logger.warning("Timeout waiting for delivery message")
                                        raise
                                    
                                    # Try to close the dialog
                                    try:
                                        close_btn = sb.driver.find_element(By.XPATH, "//button[contains(@class,'close-button')]")
                                        sb.driver.execute_script("arguments[0].click();", close_btn)
                                        logger.debug("Closed dialog")
                                    except:
                                        sb.driver.back()
                                        logger.debug("Used back navigation instead")
                                        
                                except Exception as e:
                                    logger.warning(f"Error processing part {part_number}: {str(e)}")
                                    try:
                                        sb.driver.back()
                                        logger.debug("Navigated back after error")
                                    except:
                                        logger.warning("Couldn't navigate back")
                                
                                delivery_dates.append(delivery_date)
                            
                            # Build row HTML
                            cell_html = "".join([cell.get_attribute('outerHTML') for cell in cells])
                            delivery_html = "".join([f"<td class='delivery-date'>{date}</td>" for date in delivery_dates])
                            processed_rows.append(f"<tr>{cell_html}{delivery_html}</tr>")
                        
                        except StaleElementReferenceException:
                            logger.warning("Stale element reference in row processing")
                            continue
                    
                    # Add table to results
                    processed_tables.append(f"<table>{type_html}{''.join(processed_rows)}</table>")
                    logger.info(f"Completed processing table {table_idx}")
                
                except Exception as e:
                    logger.error(f"Error processing table {table_idx}: {e}")
                    continue
            
            elapsed_time = time.time() - start_time
            logger.info(f"Finished scraping in {elapsed_time:.2f} seconds")
            
            return {
                "tables": "\n".join(processed_tables),
                "address": address,
                "timestamp": int(datetime.now().timestamp()),
                "url": url
            }
            
        except Exception as e:
            logger.error(f"Fatal error during scraping: {str(e)}")
            return None

def fetch_tables_html(url, crawl_id):
    """Main function to fetch tables and upload to S3."""
    try:
        logger.info(f"Starting fetch_tables_html for crawl_id: {crawl_id}")
        
        if not isinstance(url, list):
            url = [url]
        
        all_results = []
        
        for single_url in url:
            logger.info(f"Processing URL: {single_url}")
            result = scrape_tables_with_delivery(single_url)
            
            if result:
                all_results.append(result)
                logger.info(f"Successfully processed URL: {single_url}")
            else:
                logger.warning(f"Failed to process URL: {single_url}")
        
        if not all_results:
            logger.error("No results obtained from any URLs")
            return None
        
        # Prepare HTML content
        html_content = []
        for result in all_results:
            html_content.append(f"""
            <div class="scraped-result">
                <h3>Scraped from: <a href="{result['url']}">{result['url']}</a></h3>
                <p>Location: {result['address']}</p>
                <p>Timestamp: {result['timestamp']}</p>
                <div class="tables">{result['tables']}</div>
            </div>
            """)
        
        full_html = f"""
        <html>
            <head>
                <title>Scraped Data - {crawl_id}</title>
                <style>
                    table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
                    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                    th {{ background-color: #f2f2f2; }}
                    .delivery-date {{ color: #006600; font-weight: bold; }}
                    .scraped-result {{ margin-bottom: 30px; border: 1px solid #ccc; padding: 15px; }}
                </style>
            </head>
            <body>
                <h1>Scraped Data - {crawl_id}</h1>
                {''.join(html_content)}
            </body>
        </html>
        """
        
        # Save to local file
        domain = urlparse(url[0]).netloc.replace(".", "_")
        folder_path = os.path.join(os.getcwd(), domain)
        os.makedirs(folder_path, exist_ok=True)

        file_name = f"tables_{crawl_id}.html"
        local_file_path = os.path.join(folder_path, file_name)

        with open(local_file_path, "w", encoding="utf-8") as f:
            f.write(full_html)
        logger.info(f"Saved HTML to local file: {local_file_path}")

        # Upload to S3
        s3_path = f"{domain}/{file_name}"
        s3_url = upload_to_s3(local_file_path, s3_path)

        if s3_url:
            logger.info(f"Successfully uploaded to S3: {s3_url}")
        else:
            logger.error("Failed to upload to S3")

        return s3_url

    except Exception as e:
        logger.error(f"Error in fetch_tables_html: {e}")
        return None
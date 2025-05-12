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
    try:
        response = requests.get("http://ip-api.com/json", timeout=5)
        if response.status_code == 200:
            data = response.json()
            city = data.get("city", "City not available")
            region = data.get("regionName", "Region not available")
            country = data.get("country", "Country not available")
            return f"<td>{city}, {region}, {country}</td>"
        else:
            return "<td>Unknown Location</td>"
    except requests.RequestException:
        return "<td>Unknown Location</td>"

def scroll_to_view(driver, element):
    driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", element)
    sleep_random()

def scrape_tables_with_delivery(url):
    """Scrape tables from URL including delivery information."""
    logger.info(f"Starting to scrape: {url}")
    all_tables_html = []

    with SB(
        uc=True,
        incognito=True,
        maximize=True,
        locale_code="en",
        skip_js_waits=True,
        headless=False,
    ) as sb:
        try:
            start_time = time.time()
            sb.open(url)
            sleep_random()

            address = get_address()
            logger.info(f"Detected location: {address}")

            original_window = sb.driver.current_window_handle

            def get_stable_tables():
                """Get fresh table references with retry logic"""
                for _ in range(3):
                    try:
                        tables = sb.find_elements("table.hn, table[class^='ProductTable_table']")
                        if tables:
                            return tables
                    except Exception as e:
                        logger.warning(f"Error getting tables: {str(e)}")
                    sleep_random(0.5)
                raise Exception("Failed to get stable table references")

            tables = get_stable_tables()
            logger.info(f"Found {len(tables)} tables to process")

            for table_idx in range(len(tables)):
                try:
                    logger.info(f"Processing table {table_idx + 1}/{len(tables)}")
                    
                    # Get fresh table reference
                    tables = get_stable_tables()
                    table = tables[table_idx]

                    # Get material info
                    try:
                        material_div = table.find_element(
                            By.XPATH,
                            ".//preceding::div[contains(@class,'aq') and contains(@class,'id') and contains(@class,'ju')][1]"
                        )
                        span = material_div.find_element(By.XPATH, ".//span")
                        material_text = span.text.strip()
                        type_of_material_html = f"<tr><th>{material_text}</th></tr>"
                    except Exception:
                        type_of_material_html = ""

                    # Get stable rows reference
                    def get_stable_rows():
                        for _ in range(3):
                            try:
                                tables = get_stable_tables()
                                table = tables[table_idx]
                                rows = table.find_elements(By.XPATH, ".//tr")
                                if rows:
                                    return rows
                            except Exception as e:
                                logger.warning(f"Error getting rows: {str(e)}")
                            sleep_random(0.5)
                        raise Exception("Failed to get stable row references")

                    rows = get_stable_rows()
                    all_row_data = []
                    max_parts_in_any_row = 0

                    for row_idx in range(len(rows)):
                        try:
                            # Refresh row reference
                            rows = get_stable_rows()
                            if row_idx >= len(rows):
                                logger.warning(f"Row index {row_idx} out of bounds (max {len(rows)-1}), skipping")
                                continue
                                
                            r = rows[row_idx]
                            all_details = []
                            
                            # Find part numbers in current row
                            part_number_elements = r.find_elements(
                                By.XPATH,
                                ".//a[contains(@class,'PartNbrLnk')] | "
                                ".//a[starts-with(@class, 'PartNumberCell_partNumberLink')]//span"
                            )
                            part_count = len(part_number_elements)
                            max_parts_in_any_row = max(max_parts_in_any_row, part_count)

                            for part_idx, part_number_el in enumerate(part_number_elements):
                                try:
                                    # Refresh element reference in case it's stale
                                    rows = get_stable_rows()
                                    r = rows[row_idx]
                                    part_number_elements = r.find_elements(
                                        By.XPATH,
                                        ".//a[contains(@class,'PartNbrLnk')] | "
                                        ".//a[starts-with(@class, 'PartNumberCell_partNumberLink')]//span"
                                    )
                                    part_number_el = part_number_elements[part_idx]
                                    part_number = part_number_el.text.strip()
                                    if not part_number:
                                        continue

                                    logger.debug(f"Processing part number: {part_number}")

                                    # Click part number with retry
                                    clicked = False
                                    for _ in range(3):
                                        try:
                                            scroll_to_view(sb.driver, part_number_el)
                                            part_number_el.click()
                                            clicked = True
                                            break
                                        except Exception as e:
                                            logger.warning(f"Click attempt failed: {str(e)}")
                                            sleep_random(0.5)
                                            # Refresh references
                                            tables = get_stable_tables()
                                            table = tables[table_idx]
                                            rows = get_stable_rows()
                                            r = rows[row_idx]
                                            part_number_elements = r.find_elements(
                                                By.XPATH,
                                                ".//a[contains(@class,'PartNbrLnk')] | "
                                                ".//a[starts-with(@class, 'PartNumberCell_partNumberLink')]//span"
                                            )
                                            if part_idx < len(part_number_elements):
                                                part_number_el = part_number_elements[part_idx]

                                    if not clicked:
                                        logger.warning(f"Failed to click part number: {part_number}")
                                        all_details.append("<td>Click failed</td><td>0</td>")
                                        continue

                                    # Handle new window if opened
                                    if len(sb.driver.window_handles) > 1:
                                        sb.driver.switch_to.window(sb.driver.window_handles[-1])

                                    # Wait for quantity input and add to cart
                                    try:
                                        WebDriverWait(sb.driver, 10).until(
                                            lambda d: d.find_elements(By.XPATH, "//input[contains(@id,'qty')]") or
                                                    d.find_elements(By.XPATH, "//button[contains(.,'Add to Cart') or contains(.,'Add to Order')]")
                                        )

                                        qty_input = WebDriverWait(sb.driver, 10).until(
                                            EC.presence_of_element_located((
                                                By.XPATH,
                                                "//input[contains(@id,'qty') or contains(@name,'quantity')]"
                                            ))
                                        )
                                        qty_input.clear()
                                        qty_input.send_keys("1")

                                        add_button = WebDriverWait(sb.driver, 15).until(
                                            EC.element_to_be_clickable((
                                                By.XPATH,
                                                "//button[contains(.,'Add to Cart') or "
                                                "contains(.,'Add to Order') or "
                                                "contains(@id,'addToCart') or "
                                                "contains(@id,'addToOrder') or "
                                                "contains(@class,'add-to-cart') or "
                                                "contains(@class,'add-to-order')]"
                                            ))
                                        )
                                        sb.driver.execute_script("arguments[0].click();", add_button)

                                        # Get delivery message
                                        delivery_msg = WebDriverWait(sb.driver, 10).until(
                                            EC.visibility_of_element_located((
                                                By.XPATH,
                                                "//div[contains(@class,'delivery')] | "
                                                "//span[contains(@class,'delivery')] | "
                                                "//div[contains(@class,'ship-date')] | "
                                                "//div[contains(@class,'InLnOrdWebPartLayout_ItmAddedMsg')] | "
                                                "//span[starts-with(@class,'DeliveryMessage_deliveryMessage')]"
                                            ))
                                        ).text
                                        date_lines = delivery_msg.split('\n')
                                        delivery_date = f"<td>{date_lines[1].strip()}</td>" if len(date_lines) > 1 else f"<td>{delivery_msg.strip()}</td>"
                                    except Exception as e:
                                        logger.warning(f"Error in delivery processing: {str(e)}")
                                        delivery_date = "<td>Delivery date not available</td>"

                                    extracted_date = f"<td>{int(datetime.now().timestamp())}</td>"
                                    all_details.append(f"{delivery_date}{extracted_date}")

                                    # Close popup or return to main page
                                    try:
                                        close_button = WebDriverWait(sb.driver, 5).until(
                                            EC.element_to_be_clickable((
                                                By.XPATH,
                                                "//div[contains(@class,'InLnOrdWebPartLayout_CloseIcon')] | "
                                                "//img[starts-with(@class,'ClosingIcon_buttonImage')]"
                                            ))
                                        )
                                        close_button.click()
                                        WebDriverWait(sb.driver, 5).until_not(
                                            EC.presence_of_element_located((
                                                By.XPATH,
                                                "//div[contains(@class,'InLnOrdWebPartLayout_CloseIcon')] | "
                                                "//img[starts-with(@class,'ClosingIcon_buttonImage')]"
                                            ))
                                        )
                                    except:
                                        pass

                                except Exception as e:
                                    logger.warning(f"Part processing error: {str(e)}")
                                    all_details.append("<td>Part processing error</td><td>0</td>")
                                finally:
                                    try:
                                        if len(sb.driver.window_handles) > 1:
                                            sb.driver.close()
                                            sb.driver.switch_to.window(original_window)
                                        else:
                                            sb.go_back()
                                            # Wait for table to reload
                                            WebDriverWait(sb.driver, 10).until(
                                                EC.presence_of_element_located((
                                                    By.XPATH,
                                                    "//table[contains(@class,'hn') or starts-with(@class,'ProductTable_table')]"
                                                ))
                                            )
                                            # Refresh all references
                                            tables = get_stable_tables()
                                            table = tables[table_idx]
                                            rows = get_stable_rows()
                                    except Exception as e:
                                        logger.warning(f"Error returning from part page: {e}")

                            # Get fresh row HTML and append details
                            rows = get_stable_rows()
                            r = rows[row_idx]
                            row_html = r.get_attribute('outerHTML')
                            if all_details:
                                row_html = row_html.replace("</tr>", f"{address}{''.join(all_details)}</tr>")
                            all_row_data.append(row_html)

                        except Exception as e:
                            logger.warning(f"Row processing error: {str(e)}")
                            all_row_data.append("<tr><td>Row processing error</td></tr>")

                    # Process table HTML
                    thead_rows = all_row_data[:2]
                    tbody_rows = all_row_data[2:]

                    if max_parts_in_any_row == 1:
                        extra_headers = "<th>Address</th><th>Delivery Date</th><th>Extracted Date</th>"
                    else:
                        extra_headers = "<th>Address</th>"
                        for i in range(1, max_parts_in_any_row + 1):
                            extra_headers += f"<th>Delivery Date {i}</th><th>Extracted Date {i}</th>"

                    for i, row in enumerate(thead_rows):
                        if "<th" in row:
                            thead_rows[i] = row.replace("</tr>", f"{extra_headers}</tr>")
                            break

                    table_html = f"""
                        <table>
                            <thead>
                                {type_of_material_html}
                                {''.join(thead_rows)}
                            </thead>
                            <tbody>
                                {''.join(tbody_rows)}
                            </tbody>
                        </table>
                    """
                    all_tables_html.append(table_html)

                except Exception as e:
                    logger.error(f"Error in table {table_idx + 1}: {str(e)}")
                    continue

            elapsed_time = time.time() - start_time
            logger.info(f"Finished scraping in {elapsed_time:.2f} seconds")

            return {
                "status": "success",
                "tables": all_tables_html,
                "url": url,
                "elapsed_time": elapsed_time
            }

        except Exception as e:
            logger.error(f"Fatal error during scraping: {str(e)}", exc_info=True)
            return {
                "status": "failed",
                "error": str(e),
                "url": url
            }

def fetch_tables_html(url, crawl_id):
    """Main function to fetch tables and upload to S3."""
    try:
        logger.info(f"Starting fetch_tables_html for crawl_id: {crawl_id}")
        
        if not isinstance(url, list):
            url = [url]
        
        all_tables_html = []
        error_reports = []
        
        for single_url in url:
            logger.info(f"Processing URL: {single_url}")
            result = scrape_tables_with_delivery(single_url)
            
            if result and result.get("status") == "success" and result.get("tables"):
                all_tables_html.extend(result['tables'])
                logger.info(f"Successfully processed URL: {single_url}")
            else:
                error_msg = result.get("error", "No tables found") if result else "Scraping failed"
                error_reports.append(f"URL: {single_url} - Error: {error_msg}")
                logger.warning(f"Failed to process URL: {single_url} - {error_msg}")
        
        if not all_tables_html:
            error_message = "No results obtained from any URLs. Details:\n" + "\n".join(error_reports)
            logger.error(error_message)
            return {
                "status": "failed",
                "error": error_message,
                "crawl_id": crawl_id
            }
        
        # Prepare HTML content
        full_html = f"""
        <!DOCTYPE html>
        <html>
            <head>
                <title>{url[0]}</title>
                <style>
                    table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
                    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                    th {{ background-color: #f2f2f2; }}
                </style>
            </head>
            <body>
                <h1>Scraped Data from {url[0]}</h1>
                <p>Crawl ID: {crawl_id}</p>
                <p>Scraped at: {datetime.now().isoformat()}</p>
                {''.join(all_tables_html)}
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
            return {
                "status": "success",
                "s3_url": s3_url,
                "crawl_id": crawl_id,
                "local_path": local_file_path
            }
        else:
            logger.error("Failed to upload to S3")
            return {
                "status": "failed",
                "error": "S3 upload failed",
                "crawl_id": crawl_id,
                "local_path": local_file_path
            }

    except Exception as e:
        logger.error(f"Error in fetch_tables_html: {str(e)}", exc_info=True)
        return {
            "status": "failed",
            "error": str(e),
            "crawl_id": crawl_id
        }
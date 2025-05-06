from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import boto3
from botocore.exceptions import NoCredentialsError

 
def scroll_to_view(driver, element):
    """Scroll to make element visible"""
    driver.execute_script("arguments[0].scrollIntoView(true);", element)
    time.sleep(0.5)

def scrape_tables_with_delivery(url):
    # Set up Chrome options
    options = webdriver.ChromeOptions()
    options.add_argument('--disable-notifications')
    options.add_argument('--start-maximized') 
    
    driver = webdriver.Chrome(options=options)
    driver.get(url)
    
    try:
        # Wait for initial page load
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, 'table'))
        )
        
        # Get the page source and parse it with BeautifulSoup
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        
        # Find all tables on the page
        tables = soup.find_all('table')
        processed_tables = []
        
        for table in tables:
            rows = table.find_all('tr')
            processed_rows = []
            
            for row in rows:
                cells = row.find_all(['td', 'th'])
                part_links = []
                delivery_dates = []
                
                # Find all part numbers in this row
                for cell in cells:
                    part_links.extend(cell.find_all('a', class_='PartNbrLnk'))
                
                # Process each part number
                for part_link in part_links:
                    part_number = part_link.text.strip()
                    delivery_date = "Delivery info not found"
                    
                    try:
                        # Find the part number element in Selenium
                        part_number_element = WebDriverWait(driver, 15).until(
                            EC.presence_of_element_located((By.XPATH, f"//a[contains(text(), '{part_number}')]"))
                        )
                        
                        # Scroll to the element and click
                        scroll_to_view(driver, part_number_element)
                        part_number_element.click()
                        
                        # Wait for quantity input and set quantity
                        quantity_input = WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.XPATH, "//input[starts-with(@id,'qtyInp')]"))
                        )
                        quantity_input.clear()
                        quantity_input.send_keys("1")
                        time.sleep(1)
                        
                        # Click add to order button
                        add_to_order = driver.find_element(By.XPATH, 
                            "//button[contains(@class,'button-add-to-order-inline add-to-order')]")
                        add_to_order.click()
                        
                        # Get delivery date from confirmation message
                        delivery_msg = WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.XPATH, 
                            "//div[contains(@class,'InLnOrdWebPartLayout_ItmAddedMsg')]"))
                        )
                        delivery_text = delivery_msg.text.split('\n')
                        if len(delivery_text) > 1:
                            delivery_date = delivery_text[1].strip()
                        
                        # Close the dialog or go back
                        try:
                            close_btn = driver.find_element(By.XPATH, "//button[contains(@class,'close-button')]")
                            close_btn.click()
                        except:
                            driver.back()
                        
                    except Exception as e:
                        print(f"Error processing part {part_number}: {str(e)}")
                        try:
                            driver.back()
                        except:
                            pass
                    
                    delivery_dates.append(delivery_date)
                
                # Reconstruct the row
                processed_cells = [str(cell) for cell in cells]
                
                # Add delivery dates as new cells if available
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

# Example usage
if __name__ == "__main__":
    url = "https://www.mcmaster.com/products/screws/shoulder-screws-2~/shoulder-screws-with-hex-drive-tip/"  # Replace with your target URL
    scraped_tables = scrape_tables_with_delivery(url)
    
    with open("scraped_tables_with_delivery_hex.html", "w", encoding="utf-8") as f:
        f.write(scraped_tables)
    
    print("Scraping completed. Results saved to scraped_tables_with_delivery.html")
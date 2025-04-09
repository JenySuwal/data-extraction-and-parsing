from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time

def scrape_and_save_links(url, output_file):
    options = Options()
    options.add_argument('--headless')  # Comment out if you want to see the browser window
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920x1080')

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    try:
        driver.get(url)

        # Wait for the page to load completely
        time.sleep(3)  # Adjust as needed based on the site’s load time

        # General XPath to scrape all links across sections
        xpath_pattern = "//section//a"  # This will select all <a> tags inside <section> tags

        # Find all the <a> elements that match the generalized XPath
        link_elements = driver.find_elements(By.XPATH, xpath_pattern)

        # Extract href attribute from all <a> tags
        links = [link.get_attribute("href") for link in link_elements if link.get_attribute("href")]

        if links:
            print(f"\n🔗 Found {len(links)} links.")
            
            # Save the links to a text file with the desired format
            with open(output_file, 'w') as f:
                for i, link in enumerate(links, 1):
                    f.write(f'"{links}",\n')
                     

            print(f"\n🔗 Links saved in {output_file}")
        else:
            print("\n🔗 No valid links found.")

        return links

    finally:
        driver.quit()

# Run the function
if __name__ == "__main__":
    url = "https://www.mcmaster.com/products/screws/thumb-screws-2~/"  
    output_file = "Carriage_bolt.txt"  
    scrape_and_save_links(url, output_file)

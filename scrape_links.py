from fastapi import FastAPI, Query
from pydantic import HttpUrl
from typing import List
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time
import os

app = FastAPI()

def scrape_links_from_url(url: str) -> List[str]:
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920x1080')

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    try:
        driver.get(url)
        time.sleep(30)

        xpath_pattern = "//*[starts-with(@id, 'Abbr_')]"

        link_elements = driver.find_elements(By.XPATH, xpath_pattern)
        links = [link.get_attribute("href") for link in link_elements if link.get_attribute("href")]

        return links
    finally:
        driver.quit()
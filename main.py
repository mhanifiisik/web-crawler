from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
import json
import pandas as pd
from collections import OrderedDict
from scrapper import JobScraper


if __name__ == "__main__":
    url = "https://justjoin.it/job-offers/warszawa?experience-level=junior,mid&orderBy=DESC&sortBy=published"
    scraper = JobScraper(url, headless=True)
    jobs = scraper.scrape()
    scraper.save_to_json()
    scraper.save_to_csv()



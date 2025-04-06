import time
import json
import pandas as pd
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.firefox import GeckoDriverManager
from utils.logger import Logger

logger = Logger()

class ThirdJobsScraper:


    def __init__(self, headless=True):
        self.base_url = "https://www.make-it-in-germany.com/en/working-in-germany/job-listings?tx_solr%5Bfilter%5D%5B0%5D=topjobs%3A4"
        self.headless = headless
        self.driver = self.setup_driver()  # Initialize the web driver
        self.jobs = []  # List to store job data

    def setup_driver(self):
        firefox_options = Options()
        if self.headless:
            firefox_options.add_argument("--headless")
        firefox_options.add_argument("--width=1920")
        firefox_options.add_argument("--height=1080")
        firefox_options.set_preference("dom.webnotifications.enabled", False)
        return webdriver.Firefox(service=Service(GeckoDriverManager().install()), options=firefox_options)

    def scrape(self):
        self.driver.get(self.base_url)
        WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'h1.h2')))  # Wait for the page to load

        # Accept cookies if the button is present
        try:
            cookie_button = WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[data-testid="uc-accept-all-button"]')))
            cookie_button.click()
            time.sleep(2)  # Wait for the cookie acceptance to process
        except Exception as e:
            logger.warning("Cookie acceptance button not found or already accepted.")

        # Determine total pages
        total_pages = self.get_total_pages()

        # Scrape each page
        for page in range(1, total_pages + 1):
            page_url = f"{self.base_url}&tx_solr%5Bpage%5D={page}"
            self.driver.get(page_url)
            time.sleep(5)  # Wait for the page to load
            self.extract_jobs()  # Call the method to extract job data

        self.save_to_json()  # Save data to JSON
        self.save_to_csv()   # Save data to CSV
        self.driver.quit()  # Close the driver after scraping
        logger.info("Scraping finished")

    def get_total_pages(self):
        # Navigate to the first page to get the total number of pages
        self.driver.get(self.base_url)
        WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, '.pagination__item--last a')))
        total_pages = self.driver.find_element(By.CSS_SELECTOR, '.pagination__item--last a').text.strip()
        return int(total_pages)

    def extract_jobs(self):
        job_elements = self.driver.find_elements(By.CSS_SELECTOR, '#list45536 > div.job-category__main > ul > li.list__item:not(.list__item--newsletter):not(.list__item--customercenter)')
        if not job_elements:
            logger.warning("No job elements found on the current page.")
            return

        for job_element in job_elements:
            try:
                title_element = job_element.find_element(By.CSS_SELECTOR, 'h3 a')
                title = title_element.text.strip()
                url = title_element.get_attribute('href')  # Get the job URL
                company = job_element.find_element(By.CSS_SELECTOR, 'p').text.strip()
                location = job_element.find_element(By.CSS_SELECTOR, '.icon--before.icon--pin .element').text.strip()
                date = job_element.find_element(By.CSS_SELECTOR, '.icon--before.icon--calendar time').get_attribute('datetime').strip()
                job_data = {
                    'title': title,
                    'url': url,  # Include the job URL
                    'company': company,
                    'location': location,
                    'date': date,
                    'scraped_at': time.strftime("%Y-%m-%d %H:%M:%S")
                }
                self.jobs.append(job_data)  # Append job data to the list
                logger.info(f"Extracted job: {title}")
            except Exception as e:
                logger.warning(f"Failed to extract job details: {str(e)}")

        print(f"Extracted {len(job_elements)} jobs from the current page.")

    def save_to_json(self):
        with open('jobs3.json', 'w', encoding='utf-8') as f:
            json.dump(self.jobs, f, ensure_ascii=False, indent=4)
        logger.info("Data saved to jobs3.json")

    def save_to_csv(self):
        df = pd.DataFrame(self.jobs)
        df.to_csv('jobs3.csv', index=False, encoding='utf-8')
        logger.info("Data saved to jobs3.csv")

def main():
    scrapper = ThirdJobsScraper(headless=True)
    scrapper.scrape()



if __name__ == "__main__":
    main()

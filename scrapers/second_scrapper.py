import time
import json
import pandas as pd
from collections import OrderedDict
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.firefox import GeckoDriverManager
from utils.logger import Logger

logger = Logger()

class SecondScrapper:
    def __init__(self, url, headless=True):
        self.url = url
        self.jobs = OrderedDict()
        firefox_options = Options()
        if headless:
            firefox_options.add_argument("--headless")
        firefox_options.add_argument("--width=1920")
        firefox_options.add_argument("--height=1080")
        firefox_options.set_preference("dom.webnotifications.enabled", False)
        firefox_options.set_preference("app.update.enabled", False)
        logger.info("Setting up Firefox WebDriver...")
        self.driver = webdriver.Firefox(service=Service(GeckoDriverManager().install()), options=firefox_options)
        self.current_page = 1

    def scrape(self):
        try:
            logger.info(f"Navigating to {self.url}")
            self.driver.get(self.url)
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#offers-list > div:nth-child(4)"))
            )
            logger.info("Page loaded successfully")
            remove_cookie = self.driver.find_element(By.CSS_SELECTOR, '.cookies_aropjbf > div:nth-child(1) > button:nth-child(1)')
            remove_cookie.click()
            time.sleep(2)
            self._extract_visible_jobs()
            max_page_elem = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div.listing_n1mxvncp button:last-of-type'))
            )
            max_page_text = max_page_elem.text.strip()
            max_page = int(max_page_text) if max_page_text else 1
            logger.info(f"Total pages found: {max_page}")

            for page in range(2, max_page + 1):
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'div.listing_a1ftse4d'))
                )
                try:
                    page_button = self.driver.find_element(
                        By.CSS_SELECTOR, f'button[data-test="bottom-pagination-button-page-{page}"]'
                    )
                    self.driver.execute_script("arguments[0].click();", page_button)
                    logger.info(f"Clicked page button for page {page}")
                except Exception as e:
                    logger.warning(f"Page button for page {page} not clickable, using Next button. Error: {e}")
                    next_button = self.driver.find_element(
                        By.CSS_SELECTOR, 'button[data-test="bottom-pagination-button-next"]'
                    )
                    self.driver.execute_script("arguments[0].click();", next_button)
                    logger.info(f"Clicked Next button for page {page}")

                self.current_page = page
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#offers-list > div:nth-child(4)"))
                )
                time.sleep(2)
                self._extract_visible_jobs()

            self._save_to_csv()
            self._save_to_json()
            self.driver.quit()
            logger.info("COMPLETE", "Scraping finished")
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            self.driver.quit()

    def _extract_visible_jobs(self):
        offers = self.driver.find_elements(By.CSS_SELECTOR, 'div[data-test="default-offer"]')
        logger.info(f"Extracting {len(offers)} offers from page {self.current_page}")
        for offer in offers:
            try:
                offer_id = offer.get_attribute("data-test-offerid")
            except Exception as e:
                logger.error(f"Failed to extract offer ID: {e}")
                offer_id = None

            try:
                title_elem = offer.find_element(By.CSS_SELECTOR, '[data-test="offer-title"] a')
                title = title_elem.text.strip()
                job_url = title_elem.get_attribute("href")
            except Exception:
                title = "N/A"
                job_url = "N/A"

            try:
                salary_elem = offer.find_element(By.CSS_SELECTOR, '[data-test="offer-salary"]')
                salary = salary_elem.text.strip()
            except Exception:
                salary = "N/A"

            try:
                company_elem = offer.find_element(By.XPATH, '//*[@id="offers-list"]/div[4]/div[1]/div/div/div[1]/div[2]/div[1]/div/div/div[2]/div[1]/a[1]/h3')
                company = company_elem.text.strip()
            except Exception:
                company = "N/A"

            try:
                location_elem = offer.find_element(By.XPATH, '//*[@id="offers-list"]/div[4]/div[5]/div/div/div[1]/div[2]/div[1]/div/div/div[2]/div[2]/h4')
                location = location_elem.text.strip()
            except Exception:
                location = "N/A"

            try:
                date_elem = offer.find_element(By.CSS_SELECTOR, '[data-test="text-added"]')
                published = date_elem.text.strip()
            except Exception:
                published = "N/A"

            try:
                job_type_elem = offer.find_elements(By.XPATH, '//*[@id="offers-list"]/div[4]/div[6]/div/div/div[1]/div[2]/div[1]/div/ul/li[1]')
                job_type = job_type_elem[0].text.strip() if job_type_elem else "N/A"
            except Exception as e:
                logger.error(f"Failed to extract job type: {e}")
                job_type = "N/A"

            try:
                contract_type_elem = offer.find_elements(By.XPATH, '//*[@id="offers-list"]/div[4]/div[6]/div/div/div[1]/div[2]/div[1]/div/ul/li[3]')
                contract_type = contract_type_elem[0].text.strip() if contract_type_elem else "N/A"
            except Exception:
                contract_type = "N/A"

            try:
                work_conditions_elem = offer.find_elements(By.CSS_SELECTOR, 'li[data-test="offer-additional-info-4"]')
                work_conditions = work_conditions_elem[0].text.strip() if work_conditions_elem else "N/A"
            except Exception:
                work_conditions = "N/A"

            try:
                technologies_elem = offer.find_elements(By.CSS_SELECTOR, '[data-test="technologies-list"] span')
                technologies = [tech.text.strip() for tech in technologies_elem]
            except Exception:
                technologies = []

            try:
                short_description_elem = offer.find_element(By.CSS_SELECTOR, '[data-test="section-short-description"] .invisible')
                short_description = short_description_elem.text.strip()
            except Exception:
                short_description = "N/A"

            job = {
                "offer_id": offer_id,
                "title": title,
                "url": job_url,
                "salary": salary,
                "company": company,
                "location": location,
                "published": published,
                "job_type": job_type,
                "contract_type": contract_type,
                "work_conditions": work_conditions,
                "technologies": technologies,
                "short_description": short_description,
                'scraped_at': time.strftime("%Y-%m-%d %H:%M:%S")
            }
            key = offer_id if offer_id else title
            self.jobs[key] = job
            logger.info(f"Offer {key} extracted successfully")

    def _save_to_json(self):
        with open("jobs2.json", "w", encoding="utf-8") as f:
            json.dump(self.jobs, f, indent=4)
        logger.info("Data saved to jobs2.json")

    def _save_to_csv(self):
        df = pd.DataFrame(self.jobs.values())
        df.to_csv("jobs2.csv", index=False, encoding="utf-8")
        logger.info("Data saved to jobs2.csv")


def main():
    scrapper = SecondScrapper("", headless=True)
    scrapper.scrape()



if __name__ == "__main__":
    main()

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
from utils.logger import Logger  # Import custom Loguru logger

logger = Logger()  # Initialize logger


class FirstScraper:
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
        self.max_jobs = 1000
        self.last_seen_index = -1
        logger.info("Firefox WebDriver setup complete")

    def scrape(self, scroll_pause_time=2):
        try:
            logger.info(f"Navigating to {self.url}")
            self.driver.get(self.url)

            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "[data-index]"))
            )
            logger.info("Page loaded successfully")

            scroll_count = 0
            no_new_jobs_count = 0

            while len(self.jobs) < self.max_jobs and no_new_jobs_count < 5:
                current_job_count = len(self.jobs)
                self._extract_visible_jobs()

                if len(self.jobs) > current_job_count:
                    logger.info(f"Found {len(self.jobs) - current_job_count} new jobs. Total: {len(self.jobs)}")
                    no_new_jobs_count = 0
                else:
                    no_new_jobs_count += 1
                    logger.warning(f"No new jobs found. Attempt {no_new_jobs_count}/5")

                self.driver.execute_script("window.scrollBy(0, window.innerHeight);")
                scroll_count += 1
                logger.info(f"Scrolling... (#{scroll_count})")
                time.sleep(scroll_pause_time)

            logger.info(f"Scraping finished. Total jobs collected: {len(self.jobs)}")
            return list(self.jobs.values())

        except Exception as e:
            logger.error(f"An error occurred during scraping: {str(e)}")
            return list(self.jobs.values())

        finally:
            self.driver.quit()
            logger.info("Browser closed")

    def _extract_visible_jobs(self):
        try:
            job_elements = self.driver.find_elements(By.CSS_SELECTOR, "[data-index]")
            new_jobs = 0

            for job_element in job_elements:
                try:
                    data_index = job_element.get_attribute("data-index")
                    if data_index in self.jobs:
                        continue

                    index_num = int(data_index)
                    self.last_seen_index = max(self.last_seen_index, index_num)
                    job_data = self._parse_job_element(job_element, data_index)

                    if job_data:
                        self.jobs[data_index] = job_data
                        new_jobs += 1
                except Exception as e:
                    logger.warning(f"Failed to parse job {data_index}: {str(e)}")
                    continue

            if new_jobs > 0:
                logger.info(f"Extracted {new_jobs} new job listings")

        except Exception as e:
            logger.error(f"Error extracting jobs: {str(e)}")

    def _parse_job_element(self, job_element, data_index):
        """Parse a job element to extract all relevant data"""
        try:
            title_element = job_element.find_element(By.CSS_SELECTOR, "h3")
            title = title_element.text if title_element else "N/A"

            company = "N/A"
            try:
                company_element = job_element.find_element(By.CSS_SELECTOR, "div.MuiBox-root.css-1kb0cuq > span:nth-child(2)")
                company = company_element.text if company_element else "N/A"
            except:
                pass

            salary = "N/A"
            try:
                salary_container = job_element.find_element(By.CSS_SELECTOR, "div.MuiBox-root.css-18ypp16")
                if "Undisclosed Salary" in salary_container.text:
                    salary = "Undisclosed Salary"
                else:
                    spans = salary_container.find_elements(By.TAG_NAME, "span")
                    if len(spans) >= 3:
                        min_salary = spans[0].text.strip()
                        max_salary = spans[1].text.strip()
                        currency = spans[2].text.strip()
                        salary = f"{min_salary} - {max_salary} {currency}"
                    else:
                        salary = salary_container.text.strip()
            except:
                pass

            location = "N/A"
            try:
                location_element = job_element.find_element(By.CSS_SELECTOR, "span.css-1o4wo1x")
                location = location_element.text if location_element else "N/A"
            except:
                pass

            remote_status = "Not specified"
            try:
                remote_element = job_element.find_element(By.XPATH, ".//span[contains(text(), 'Fully remote')] | .//span[contains(text(), 'remote')]")
                remote_status = remote_element.text if remote_element else "Not specified"
            except:
                pass

            skills = []
            try:
                skill_elements = job_element.find_elements(By.CSS_SELECTOR, "div.skill-tag-1 div, div.skill-tag-2 div, div.skill-tag-3 div")
                for skill in skill_elements:
                    skill_text = skill.text.strip()
                    if skill_text and skill_text.lower() != "new":
                        skills.append(skill_text)
            except:
                pass

            job_url = "N/A"
            try:
                link_element = job_element.find_element(By.CSS_SELECTOR, "a")
                job_url = link_element.get_attribute("href")
            except:
                pass

            company_logo = "N/A"
            try:
                company_logo_element = job_element.find_element(By.CSS_SELECTOR, "img#offerCardCompanyLogo")
                company_logo = company_logo_element.get_attribute("src")
            except:
                pass

            return {
                'data_index': data_index,
                'title': title,
                'company': company,
                'company_logo': company_logo,
                'salary': salary,
                'location': location,
                'remote_status': remote_status,
                'skills': skills,
                'url': job_url,
                'scraped_at': time.strftime("%Y-%m-%d %H:%M:%S")
            }
        except Exception as e:
            logger.error(f"Failed to parse job element {data_index}: {str(e)}")
            return None

    def save_to_json(self, filename="jobs.json"):
        try:
            job_list = list(self.jobs.values())
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(job_list, f, ensure_ascii=False, indent=4)
            logger.info(f"Successfully saved {len(job_list)} jobs to {filename}")
        except Exception as e:
            logger.error(f"Failed to save JSON file: {str(e)}")

    def save_to_csv(self, filename="jobs.csv"):
        try:
            job_list = list(self.jobs.values())
            df = pd.DataFrame(job_list)
            df['skills'] = df['skills'].apply(lambda x: ', '.join(x) if x else '')
            df.to_csv(filename, index=False, encoding='utf-8')
            logger.info(f"Successfully saved {len(job_list)} jobs to {filename}")
        except Exception as e:
            logger.error(f"Failed to save CSV file: {str(e)}")


if __name__ == "__main__":
    main()

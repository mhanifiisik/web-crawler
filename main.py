import os
import time
import argparse
import psycopg2
from dotenv import load_dotenv
from scrapers.first_scrapper import FirstScraper
from scrapers.second_scrapper import SecondScrapper
from scrapers.third_jobs_scrapper import ThirdJobsScraper
from utils.logger import Logger
import json
import pandas as pd
from datetime import datetime

# Load environment variables
load_dotenv()

# Fetch database variables
USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")
FIRST_PAGE_URL = os.getenv("first_page_url")
SECOND_PAGE_URL = os.getenv("second_page_url")
THIRD_PAGE_URL = os.getenv("third_page_url")

logger = Logger()

def get_scraper(scraper_name, headless=True, url=None):
    # Define scraper classes
    scrapers = {
        'second_page': SecondScrapper,
        'third_page': ThirdJobsScraper
    }

    if scraper_name not in scrapers:
        raise ValueError(f"Unknown scraper: {scraper_name}. Available scrapers: {', '.join(scrapers.keys())}")

    # Initialize the scraper with the appropriate parameters
    if scraper_name == 'third_page':
        return scrapers[scraper_name](headless=headless)
    else:
        if not url:
            url = os.getenv(f"{scraper_name}_url")
        return scrapers[scraper_name](url=url, headless=headless)

def connect_to_database():
    try:
        connection = psycopg2.connect(
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT,
            dbname=DBNAME
        )
        logger.info("Database connection successful!")
        return connection
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise

def check_and_update_table_structure(connection):
    """Check the existing table structure and update it if needed"""
    try:
        cursor = connection.cursor()

        # Check if the table exists
        cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = 'jobs'
        );
        """)
        table_exists = cursor.fetchone()[0]

        if not table_exists:
            logger.info("Jobs table does not exist. Creating it...")
            setup_database(connection)
            return

        # Get the current columns in the table
        cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'jobs';
        """)
        existing_columns = {row[0]: row[1] for row in cursor.fetchall()}

        # Define the required columns and their data types
        required_columns = {
            'id': 'integer',
            'job_id': 'character varying',
            'title': 'character varying',
            'company': 'character varying',
            'location': 'character varying',
            'salary': 'character varying',
            'url': 'text',
            'description': 'text',
            'published_date': 'character varying',
            'job_type': 'character varying',
            'contract_type': 'character varying',
            'remote_status': 'character varying',
            'technologies': 'ARRAY',
            'source': 'character varying',
            'scraped_at': 'timestamp without time zone',
            'created_at': 'timestamp without time zone'
        }

        # Check for missing columns and add them
        for column, data_type in required_columns.items():
            if column not in existing_columns:
                logger.info(f"Adding missing column: {column}")
                if data_type == 'ARRAY':
                    cursor.execute(f"ALTER TABLE jobs ADD COLUMN {column} TEXT[];")
                else:
                    cursor.execute(f"ALTER TABLE jobs ADD COLUMN {column} {data_type};")

        # Check if the index exists
        cursor.execute("""
        SELECT EXISTS (
            SELECT FROM pg_indexes
            WHERE tablename = 'jobs' AND indexname = 'idx_jobs_job_id_source'
        );
        """)
        index_exists = cursor.fetchone()[0]

        if not index_exists:
            logger.info("Creating index on job_id and source")
            cursor.execute("""
            CREATE INDEX idx_jobs_job_id_source ON jobs(job_id, source);
            """)

        connection.commit()
        logger.info("Table structure check and update completed successfully")
    except Exception as e:
        logger.error(f"Error checking and updating table structure: {e}")
        connection.rollback()
        raise
    finally:
        cursor.close()

def setup_database(connection):
    """Create the jobs table if it doesn't exist"""
    try:
        cursor = connection.cursor()

        # Create jobs table with a structure that can accommodate all scrapers
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id SERIAL PRIMARY KEY,
            job_id VARCHAR(255),
            title VARCHAR(255),
            company VARCHAR(255),
            location VARCHAR(255),
            salary VARCHAR(255),
            url TEXT,
            description TEXT,
            published_date VARCHAR(255),
            job_type VARCHAR(255),
            contract_type VARCHAR(255),
            remote_status VARCHAR(255),
            technologies TEXT[],
            source VARCHAR(50),
            scraped_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        connection.commit()
        logger.info("Jobs table created or already exists")

        # Now create the index after the table exists
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_jobs_job_id_source ON jobs(job_id, source);
        """)

        connection.commit()
        logger.info("Database setup completed successfully")
    except Exception as e:
        logger.error(f"Error setting up database: {e}")
        connection.rollback()
        raise
    finally:
        cursor.close()

def save_jobs_to_database(connection, jobs, source):
    """Save scraped jobs to the database"""
    try:
        cursor = connection.cursor()
        jobs_added = 0

        for job in jobs:
            # Extract job data based on the source
            if source == 'justjoin_categories':
                job_id = job.get('data_index', '')
                title = job.get('title', '')
                company = job.get('company', '')
                location = job.get('location', '')
                salary = job.get('salary', '')
                url = job.get('url', '')
                description = ''
                published_date = None  # Use None for empty dates
                job_type = ''
                contract_type = ''
                remote_status = job.get('remote_status', '')
                technologies = job.get('skills', [])
                scraped_at = job.get('scraped_at', time.strftime("%Y-%m-%d %H:%M:%S"))
                position = title  # Use title as position
                source_url = url  # Use url as source_url
                status = 'new'  # Default status for newly scraped jobs

            elif source == 'second_page':
                job_id = job.get('offer_id', '')
                title = job.get('title', '')
                company = job.get('company', '')
                location = job.get('location', '')
                salary = job.get('salary', '')
                url = job.get('url', '')
                description = job.get('short_description', '')

                # Handle published date properly
                published_str = job.get('published', '')
                if published_str and published_str != 'N/A':
                    try:
                        # Try to parse the date string
                        published_date = datetime.strptime(published_str, '%Y-%m-%d').date()
                    except ValueError:
                        published_date = None
                else:
                    published_date = None

                job_type = job.get('job_type', '')
                contract_type = job.get('contract_type', '')
                remote_status = ''  # Pracuj doesn't provide remote status in the current scraping
                technologies = job.get('technologies', [])
                scraped_at = job.get('scraped_at', time.strftime("%Y-%m-%d %H:%M:%S"))
                position = title  # Use title as position
                source_url = url  # Use url as source_url
                status = 'new'  # Default status for newly scraped jobs

            elif source == 'third_page':
                job_id = str(hash(f"{job.get('title', '')}{job.get('company', '')}"))  # Create a unique ID
                title = job.get('title', '')
                company = job.get('company', '')
                location = job.get('location', '')
                salary = ''  # Germany scraper doesn't provide salary in the current scraping
                url = job.get('url', '')
                description = ''  # Germany scraper doesn't provide description in the current scraping

                # Handle published date properly
                date_str = job.get('date', '')
                if date_str:
                    try:
                        # Try to parse the date string
                        published_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                    except ValueError:
                        published_date = None
                else:
                    published_date = None

                job_type = ''  # Germany scraper doesn't provide job type in the current scraping
                contract_type = ''  # Germany scraper doesn't provide contract type in the current scraping
                remote_status = ''  # Germany scraper doesn't provide remote status in the current scraping
                technologies = []  # Germany scraper doesn't provide technologies in the current scraping
                scraped_at = job.get('scraped_at', time.strftime("%Y-%m-%d %H:%M:%S"))
                position = title  # Use title as position
                source_url = url  # Use url as source_url
                status = 'new'  # Default status for newly scraped jobs

            # Check if job already exists to avoid duplicates
            cursor.execute(
                "SELECT id FROM jobs WHERE job_id = %s AND source = %s",
                (job_id, source)
            )

            if cursor.fetchone() is None:
                # Insert the job into the database
                cursor.execute("""
                INSERT INTO jobs (
                    job_id, title, company, position, location, salary, url, description,
                    published_date, job_type, contract_type, remote_status,
                    technologies, source, scraped_at, source_url, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    job_id, title, company, position, location, salary, url, description,
                    published_date, job_type, contract_type, remote_status,
                    technologies, source, scraped_at, source_url, status
                ))
                jobs_added += 1

        connection.commit()
        logger.info(f"Added {jobs_added} new jobs to the database from {source}")
    except Exception as e:
        logger.error(f"Error saving jobs to database: {e}")
        connection.rollback()
        raise
    finally:
        cursor.close()

def scrape_justjoin_categories(headless=True):
    """Scrape multiple job categories from justjoin.it"""
    categories = [
        "javascript",
        "python",
        "data",
        "devops"
    ]

    base_url = "https://justjoin.it/job-offers/all-locations/{category}?experience-level=junior,mid&orderBy=DESC&sortBy=published"
    all_jobs = {}

    for category in categories:
        url = base_url.format(category=category)
        logger.info(f"Scraping {category} jobs from {url}")

        # Create FirstScraper instance directly for category scraping
        scraper = FirstScraper(url=url, headless=headless)
        jobs = scraper.scrape()

        # Add category information to each job
        for job in jobs:
            job['category'] = category

        # Merge jobs into the main dictionary
        all_jobs.update({f"{category}_{job['data_index']}": job for job in jobs})

        logger.info(f"Found {len(jobs)} jobs for {category}")

    return all_jobs

def main():
    try:
        # Connect to the database
        connection = connect_to_database()

        # Check and update the table structure
        check_and_update_table_structure(connection)

        # Set up argument parser
        parser = argparse.ArgumentParser(description='Web Job Scraper')
        parser.add_argument('--scraper', type=str, required=False,
                          choices=['second_page', 'third_page', 'all', 'justjoin_categories'],
                          default='all',
                          help='Choose which scraper to run (default: all)')
        parser.add_argument('--headless', action='store_true',
                          help='Run browser in headless mode')
        args = parser.parse_args()

        # Create output directory if it doesn't exist
        output_dir = "output"
        os.makedirs(output_dir, exist_ok=True)

        # Define scrapers to run
        if args.scraper == 'justjoin_categories':
            scrapers_to_run = ['justjoin_categories']
        elif args.scraper == 'all':
            scrapers_to_run = ['second_page', 'third_page']
        else:
            scrapers_to_run = [args.scraper]

        total_jobs = 0
        start_time = time.time()

        # Run each scraper
        for i, scraper_name in enumerate(scrapers_to_run, 1):
            scraper_start_time = time.time()
            logger.info(f"Starting {scraper_name} scraper ({i}/{len(scrapers_to_run)})...")

            if scraper_name == 'justjoin_categories':
                # Handle justjoin categories scraping
                jobs = scrape_justjoin_categories(headless=args.headless)
                jobs_list = list(jobs.values())
                total_jobs += len(jobs_list)

                # Save results
                timestamp = time.strftime("%Y%m%d_%H%M%S")
                json_path = os.path.join(output_dir, f"justjoin_categories_{timestamp}.json")
                csv_path = os.path.join(output_dir, f"justjoin_categories_{timestamp}.csv")

                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(jobs_list, f, ensure_ascii=False, indent=4)

                df = pd.DataFrame(jobs_list)
                df.to_csv(csv_path, index=False, encoding='utf-8')

                # Save to database
                save_jobs_to_database(connection, jobs_list, 'justjoin_categories')
            else:
                # Initialize the chosen scraper
                scraper = get_scraper(scraper_name, headless=args.headless)

                # Scrape the jobs
                logger.info(f"Scraping jobs from {scraper_name}...")
                scraper.scrape()

                # Get the scraped jobs
                if scraper_name == 'third_page':
                    jobs = scraper.jobs  # Germany scraper stores jobs in self.jobs
                else:
                    jobs = list(scraper.jobs.values())  # JustJoin and Pracuj store jobs in self.jobs dictionary

                # Save the jobs to the database
                logger.info(f"Saving {len(jobs)} jobs from {scraper_name} to the database...")
                save_jobs_to_database(connection, jobs, scraper_name)

                # Calculate progress and time
                total_jobs += len(jobs)
                scraper_end_time = time.time()
                scraper_duration = scraper_end_time - scraper_start_time

                # Estimate remaining time
                remaining_scrapers = len(scrapers_to_run) - i
                avg_time_per_scraper = (scraper_end_time - start_time) / i
                estimated_remaining_time = avg_time_per_scraper * remaining_scrapers

                logger.info(f"{scraper_name} scraper completed in {scraper_duration:.2f} seconds")
                logger.info(f"Progress: {i}/{len(scrapers_to_run)} scrapers ({(i/len(scrapers_to_run))*100:.1f}%)")
                logger.info(f"Estimated time remaining: {estimated_remaining_time:.1f} seconds")

        # Calculate total time
        end_time = time.time()
        total_duration = end_time - start_time

        logger.info(f"All scrapers completed successfully!")
        logger.info(f"Total jobs scraped: {total_jobs}")
        logger.info(f"Total time: {total_duration:.2f} seconds")
        logger.info(f"Average time per job: {total_duration/total_jobs:.2f} seconds")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise
    finally:
        # Close database connection if it exists
        if 'connection' in locals():
            connection.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    main()

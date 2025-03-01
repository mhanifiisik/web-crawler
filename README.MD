# Job Scraper

A Python-based web scraper that extracts job listings from JustJoin.it using Selenium WebDriver. The scraper collects detailed information about IT job postings including titles, companies, salaries, locations, and required skills.

## Data Collected

- Job title
- Company name
- Company logo URL
- Salary information
- Location
- Remote work status
- Required skills
- Job posting URL
- Timestamp of scraping

## Installation

```bash
git clone https://github.com/mhanifiisik/web-crawler.git

cd web-crawler
```

## Example Json Value

```json
{
  "data_index": "151",
  "title": "Middle AQA Engineer",
  "company": "N-iX",
  "company_logo": "https://...",
  "salary": "12 128 - 18 192 PLN",
  "location": "Warszawa",
  "remote_status": "Not specified",
  "skills": ["Selenium", "C#", "SQL"],
  "url": "https://justjoin.it/...",
  "scraped_at": "2024-03-01 18:02:30"
}
```

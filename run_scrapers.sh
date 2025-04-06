#!/bin/bash

# Activate virtual environment
source venv/bin/activate

# Function to run scraper and check for errors
run_scraper() {
    local scraper_name=$1
    local display_name=$2
    echo "Starting $display_name..."
    if python main.py --scraper "$scraper_name" --headless; then
        echo "$display_name completed successfully."
    else
        echo "Error: $display_name failed."
        exit 1
    fi
}

# Run the JustJoin categories scraper
run_scraper "justjoin_categories" "JustJoin Categories scraper"

# Run the second scraper
run_scraper "second_page" "Pracuj scraper"

# Run the third scraper
run_scraper "third_page" "Germany Jobs scraper"

# Deactivate virtual environment
deactivate

echo "All scrapers have completed their execution."

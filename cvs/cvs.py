import os
import json
import logging
import asyncio
from cvs_scrape import async_scrape
def logs():
    # Configure logging to write to both a file and the console
    os.makedirs("logs", exist_ok=True)  # Ensure the logs directory exists

    # Create a file handler to write logs to a file
    file_handler = logging.FileHandler("logs/scraper.log", encoding="utf-8")
    file_handler.setLevel(logging.INFO)  # Log INFO and above to the file

    # Create a stream handler to display logs in the console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)  # Log INFO and above to the console

    # Define a common log format
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Configure the root logger
    logging.basicConfig(
        level=logging.INFO,  # Log INFO and above
        handlers=[file_handler, console_handler],  # Add both handlers
    )

async def main(THREADS, PAGES):
    logs()
    formatted_data, bad_data = await async_scrape(PAGES, THREADS)
    formatted_count = sum(len(jobs) for jobs in formatted_data.values())
    bad_count = sum(len(jobs) for jobs in bad_data.values())
    logging.info(f"Total jobs in formatted_data: {formatted_count}")
    logging.info(f"Total jobs in bad_data: {bad_count}")

    save(formatted_data, bad_data)
    

def save(formatted_data, bad_data):
    """
    Saves formatted and bad (no address) data to separate JSON files.

    Args:
        formatted_data (list): List of jobs with valid addresses.
        bad_data (list): List of jobs using location instead of addresses.
    """
    os.makedirs("data", exist_ok=True)  # Ensure the data directory exists

    # Transform formatted data into the desired structure
    formatted_output = [
        {
            "address": address,
            "jobs": [
                {
                    "jobLink": job.get("job_link"),
                    "title": job.get("title"),
                    "description": job.get("description"),
                    "hourlyRate": job.get("hourly_rate"),
                    "salary": job.get("salary"),
                    "types": job.get("types"),
                }
                for job in jobs
            ],
        }
        for address, jobs in formatted_data.items()
    ]

    # Save formatted data to a JSON file
    with open("data/cvs_jobs_formatted.json", "w", encoding="utf-8") as file:
        json.dump(formatted_output, file, ensure_ascii=False, indent=4)

    # Transform bad data into the desired structure
    bad_output = [
        {
            "location": location,
            "jobs": [
                {
                    "jobLink": job.get("job_link"),
                    "title": job.get("title"),
                    "description": job.get("description"),
                    "salary": job.get("salary"),
                    "types": job.get("types"),
                }
                for job in jobs
            ],
        }
        for location, jobs in bad_data.items()
    ]

    # Save bad data to a separate JSON file
    with open("data/cvs_jobs_bad.json", "w", encoding="utf-8") as file:
        json.dump(bad_output, file, ensure_ascii=False, indent=4)
if __name__ == "__main__":
    asyncio.run(main(16, 160))
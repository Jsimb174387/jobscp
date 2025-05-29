import logging
import asyncio
from collections import defaultdict
from playwright.async_api import async_playwright
import pyap


async def async_scrape(total_pages, num_scrapers, proxies=None, query="", retries=5):
    """
    Asynchronous scraping using Playwright.

    Args:
        total_pages (int): Total number of pages to scrape.
        num_scrapers (int): Number of concurrent scrapers to use.
        proxies (list): List of proxies to use for scraping.
        query (str): Search query for the Walmart careers page.
        retries (int): Number of retries for failed requests.

    Returns:
        tuple: A tuple containing:
            - result_formatted (dict): Jobs grouped by address.
            - result_bad (dict): Jobs with missing or unknown addresses.
    """
    logging.info(f"Starting async_scrape with {total_pages} pages, {num_scrapers} scrapers, and query '{query}'")
    result_formatted = defaultdict(list)
    result_bad = defaultdict(list)

    async def scrape_pages(start_page, end_page, proxy=None):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, proxy={"server": proxy} if proxy else None)
            context = await browser.new_context()
            page = await context.new_page()

            for page_num in range(start_page, end_page + 1):
                url = f"https://careers.walmart.com/results?q={query}&page={page_num}"
                logging.info(f"Scraping URL: {url}")
                for attempt in range(retries):
                    try:
                        await page.goto(url)
                        await page.wait_for_selector(".job-listing")  # Wait for job listings to load

                        jobs_all = await page.query_selector_all(".job-listing")
                        jobs = []
                        #filter out the 'recent-jobs' tab
                        for job in jobs_all:
                            in_recent = await job.evaluate("node => node.closest('.recent-jobs') !== null")
                            if not in_recent:
                                jobs.append(job)
                        
                        logging.info(f"Found {len(jobs)} jobs on page {page_num}")
                        for job in jobs:
                            title_element = await job.query_selector(".job-listing__link")
                            location_element = await job.query_selector(".job-listing__location")

                            title = await title_element.inner_text() if title_element else "N/A"
                            link = await title_element.get_attribute("href") if title_element else "N/A"
                            location = await location_element.inner_text() if location_element else "Unknown"

                            # Extract detailed job information
                            job_details = await extract_job_details(page, link)
                            addresses = parse_addresses(job_details["description"])

                            job_data = {
                                "title": title,
                                "job_link": link,
                                "description": job_details["description"],
                                "hourly_rate": job_details["hourly_rate"],
                                "salary": job_details["salary"],
                                "types": job_details["employment_type"],
                            }

                            if addresses:
                                for address in addresses:
                                    result_formatted[address].append(job_data)
                            else:
                                result_bad[location].append(job_data)
                        break
                    except Exception as e:
                        logging.warning(f"Attempt {attempt + 1} failed: {e}")
                        if attempt < retries - 1:
                            await asyncio.sleep(2 ** attempt)  # Exponential backoff
                        else:
                            logging.error(f"Failed to scrape page {page_num} after {retries} attempts.")

            await browser.close()

    tasks = []
    pages_per_scraper = total_pages // num_scrapers
    page_ranges = [
        (i * pages_per_scraper + 1, (i + 1) * pages_per_scraper)
        for i in range(num_scrapers)
    ]
    for i, (start_page, end_page) in enumerate(page_ranges):
        proxy = proxies[i % len(proxies)] if proxies else None
        tasks.append(scrape_pages(start_page, end_page, proxy))

    await asyncio.gather(*tasks)
    return dict(result_formatted), dict(result_bad)


async def extract_job_details(page, job_link):
    """
    Extracts detailed job information from the job details page.

    Args:
        page (Page): The Playwright page object.
        job_link (str): The URL of the job details page.

    Returns:
        dict: A dictionary containing job details.
    """
    details_page = await page.context.new_page()
    try:
        await details_page.goto(job_link)
        await details_page.wait_for_selector(".job-data")

        description_element = await details_page.query_selector(".job-description")
        description = await description_element.inner_text() if description_element else "N/A"

        salary = None
        hourly_rate = None
        employment_type = []

        lines = description.splitlines()
        for line in lines:
            if "$" in line and "annual salary range" in line:
                salary = line.split("$", 1)[1].strip()
            elif "$" in line and "hourly wage range" in line:
                hourly_rate = line.split("$", 1)[1].strip()
    
        return {
            "description": description,
            "salary": salary,
            "hourly_rate": hourly_rate,
            "employment_type": employment_type,
        }
    except Exception as e:
        logging.error(f"Error extracting job details from {job_link}: {e}")
        return {}
    finally: 
        await details_page.close()


def parse_addresses(description, country="US"):
    """
    Parses addresses from the job description using the pyap library.

    Args:
        description (str): The job description text.
        country (str): The country code for address parsing (default is "US").

    Returns:
        list: A list of parsed addresses as strings.
    """
    try:
        # Use pyap to parse addresses from the description
        addresses = pyap.parse(description, country=country)
        # Convert parsed addresses to a list of full addresses
        return [str(addr.full_address) for addr in addresses]
    except Exception as e:
        logging.error(f"Error parsing addresses: {e}")
        return []

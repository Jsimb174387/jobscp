import logging
import asyncio
from collections import defaultdict
from playwright.async_api import async_playwright
import pyap

async def async_scrape(total_pages, num_scrapers, proxies = None, retries = 5):
    logging.info(f"Starting CVS Scraper")
    result_formatted = defaultdict(list)
    result_bad = defaultdict(list)

    async def scrape_pages(start_page, end_page, proxy = None):
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless= True,
                proxy={"server": proxy} if proxy else None)
            context = await browser.new_context()
            page = await context.new_page()

            for page_num in range(start_page, end_page + 1):
                # 10 jobs per page. Indexed by job start number. 
                url = f"https://jobs.cvshealth.com/us/en/search-results?from={page_num * 10}&s=1"
                for attempt in range(retries):
                    try:
                        await page.goto(url)
                        # selector for jobs
                        await page.wait_for_selector("h3.phw-pr-4 a")
                        jobs = await page.query_selector_all("h3.phw-pr-4 a")
                        for job in jobs:
                            href = await job.get_attribute("href")
                            if not href:
                                continue                            
                            details = await extract_details(page, href)
                            addresses = []
                            location = []
                            for loc in details["address"]:
                                temp = parse_addresses(loc)
                                if temp:
                                    addresses.append(loc)
                                else:
                                    location.append(loc)

                            job_data = {
                                "title": details['title'],
                                "job_link": href,
                                "description": details["description"],
                                "salary": details['salary'],
                                "types": details["employment_type"],
                            }
                            
                            if addresses:
                                for address in addresses:
                                    result_formatted[address].append(job_data)
                            if location:
                                for loc in location:
                                    result_bad[loc].append(job_data)
                        break
                    except Exception as e:
                        logging.warning(f"Attempt {attempt + 1} failed: {e}")
                        if attempt < retries - 1:
                            await asyncio.sleep (2 ** attempt)
                        else:
                            logging.error(f"Failed")
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

async def extract_details(page, job_link):
    details_page = await page.context.new_page()
    try:
        await details_page.goto(job_link)
        # Job info ribbon at the top of the page
        container = await details_page.wait_for_selector("div.job-info-wrapper.g-col-8.g-col-lg-12")
        h1 = await container.query_selector("h1.phw-g-h1-widget-title-dark.phw-pb-1.job-title")
        title = await h1.inner_text() if h1 else "N/A"

        location = [None]
        location_element = await details_page.query_selector('[data-ph-at-text="location"]')
        if not location_element:
            location_element = await details_page.query_selector('[data-ph-at-text="multi_location"]')
            if location_element:
                button = await details_page.query_selector("#multi_location")
                if button:
                    await button.click()
                    await details_page.wait_for_selector('[data-ps="370cdfc3-span-27"]')
                    elements = await details_page.query_selector_all('[data-ps="370cdfc3-span-27"]')
                    multi_locations = [await el.inner_text() for el in elements]
                    dialog_close = await details_page.query_selector(".dialog-close.phw-modal-close")
                    if dialog_close:
                        await dialog_close.click()
                    location = multi_locations
        else:
            location = [await location_element.inner_text()]
        
        description_element = await details_page.query_selector(".phw-job-description")
        description = await description_element.inner_text() if description_element else "N/A"

        salary = None
        employment_type = "Full time"

        lines = description.splitlines()
        for line in lines:
            if '$' in line:
                salary = line
            if line == "Part time" or line == "Full time":
                employment_type = line

        def clean_loc(loc):
            if loc:
                return loc.replace("Location:\n", "").strip()
            return loc
        location = [clean_loc(loc) for loc in location]
        return {
            "description": description,
            "salary": salary,
            "employment_type": employment_type,
            "title": title,
            "address": location
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

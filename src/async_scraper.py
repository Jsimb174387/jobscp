import logging
import concurrent.futures
from collections import defaultdict
from queue import Queue

from scrape import Scraper


def a_scrape(
    total_pages,
    num_scrapers,
    proxy_pool,
    query="",
    date_sort=False,
    expand="department,brand,type,rate",
    job_career_area="all",
    employment_type=None,
    retries=5,
    country="US",
):
    """
    Scrapes asynchronously. `num_scrapers` sets the max number of workers
    (capped to 61 on Windows). Divides the pages between the workers, who
    handle all the jobs on their assigned pages to divide the work evenly.

    Args:
        total_pages (int): Total number of pages to scrape.
        num_scrapers (int): Number of concurrent scrapers to use.
        proxy_pool (list): List of proxies to use for scraping.
        query (str): Search query for the Walmart careers page.
        date_sort (bool): Whether to sort results by date.
        expand (str): Additional parameters for expanding search results.
        job_career_area (str): Job career area filter.
        employment_type (str): Employment type filter.
        retries (int): Number of retries for failed requests.
        country (str): Country code for parsing addresses (e.g., 'US').

    Returns:
        tuple: A tuple containing:
            - result_formatted (dict): Jobs grouped by address.
            - result_bad (dict): Jobs with missing or unknown addresses.
    """
    proxy_queue = Queue()
    for proxy in proxy_pool:
        proxy_queue.put(proxy)

    def get_proxy():
        """
        Thread-safe function to get a proxy from the pool.
        Passed as a function into scrapers.

        Returns:
            str: A proxy URL in the format protocol://ip:port.
        """
        proxy = proxy_queue.get()
        proxy_queue.put(proxy)
        return proxy

    # Divide pages evenly among scrapers
    pages_per_scraper = total_pages // num_scrapers
    page_ranges = [
        (i * pages_per_scraper + 1, (i + 1) * pages_per_scraper)
        for i in range(num_scrapers)
    ]

    # Handle any remainder pages
    if total_pages % num_scrapers != 0:
        page_ranges[-1] = (page_ranges[-1][0], total_pages)

    result_formatted = defaultdict(list)
    result_bad = defaultdict(list)

    # Use ThreadPoolExecutor for concurrent scraping
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_scrapers) as executor:
        future_to_scraper = {
            executor.submit(
                run_scraper,
                get_proxy,
                start_page,
                end_page,
                query,
                date_sort,
                expand,
                job_career_area,
                employment_type,
                retries,
                country,
            ): (start_page, end_page)
            for start_page, end_page in page_ranges
        }

        for future in concurrent.futures.as_completed(future_to_scraper):
            start_page, end_page = future_to_scraper[future]
            try:
                formatted, bad = future.result()
                logging.info(f"Scraper finished {start_page} to {end_page}")

                for item in formatted:
                    result_formatted[item["address"]].extend(item["jobs"])

                for item in bad:
                    result_bad[item["location"]].extend(item["jobs"])

            except Exception as e:
                logging.error(
                    f"Scraper failed for {start_page} to {end_page}, message: {e}"
                )

    return dict(result_formatted), dict(result_bad)


def run_scraper(
    get_proxy,
    start_page,
    end_page,
    query="",
    date_sort=False,
    expand="department,brand,type,rate",
    job_career_area="all",
    employment_type=None,
    retries=5,
    country="US",
):
    """
    Runs a single scraper instance for a given page range.

    Args:
        get_proxy (function): Function to retrieve a proxy.
        start_page (int): Starting page number for the scraper.
        end_page (int): Ending page number for the scraper.
        query (str): Search query for the Walmart careers page.
        date_sort (bool): Whether to sort results by date.
        expand (str): Additional parameters for expanding search results.
        job_career_area (str): Job career area filter.
        employment_type (str): Employment type filter.
        retries (int): Number of retries for failed requests.
        country (str): Country code for parsing addresses (e.g., 'US').

    Returns:
        tuple: A tuple containing:
            - formatted_data (list): Jobs grouped by address.
            - bad_data (list): Jobs with missing or unknown addresses.
    """
    scraper = Scraper(
        get_proxy,
        query,
        date_sort,
        expand,
        job_career_area,
        employment_type,
        retries,
        country,
    )
    formatted_data, bad_data = scraper.get_jobs(start_page, end_page)
    return formatted_data, bad_data

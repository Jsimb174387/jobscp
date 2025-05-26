import time
import logging
import html

from playwright.sync_api import sync_playwright
import pyap


class Scraper:
    def __init__(
        self,
        query="",
        date_sort=False,
        expand="department,brand,type,rate",
        job_career_area="all",
        employment_type=None,
        retries=5,
        country="US",
    ):
        """
        Initializes the Scraper instance.

        Args:
            query (str): Search query for the Walmart careers page.
            date_sort (bool): Whether to sort results by date.
            expand (str): Additional parameters for expanding search results.
            job_career_area (str): Job career area filter.
            employment_type (str): Employment type filter.
            retries (int): Number of retries for failed requests.
            country (str): Country code for parsing addresses (e.g., 'US').
        """
        self.query = query
        self.date_sort = date_sort
        self.expand = expand
        self.job_career_area = job_career_area
        self.employment_type = employment_type
        self.retries = retries
        self.country = country

    def get_walmart_careers(self, start_pages, max_pages):
        """
        Scrapes Walmart careers pages using Playwright.

        Args:
            start_pages (int): Starting page number.
            max_pages (int): Maximum page number.

        Returns:
            dict: Dictionary of jobs grouped by location.
        """
        url = "https://careers.walmart.com/results"
        jobs_by_location = {}

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()

            for page_num in range(start_pages, max_pages + 1):
                payload = (
                    f"?q={self.query}&page={page_num}&sort=rank&expand={self.expand}"
                    f"&jobCareerArea={self.job_career_area}"
                )
                if self.employment_type:
                    payload += f"&EmploymentType={self.employment_type}"

                for attempt in range(self.retries):
                    try:
                        logging.info(f"Scraping page {page_num} (attempt {attempt + 1})")
                        page.goto(url + payload)

                        # Sort by date if required
                        if self.date_sort:
                            sort_button = page.locator(
                                "label.search__sort__option__label[title='Job Post Date']"
                            )
                            sort_button.click()

                        # Extract job listings
                        job_list = page.locator(".job-listing")
                        for job in job_list.element_handles():
                            title = job.locator(".job-listing__link").inner_text()
                            link = job.locator(".job-listing__link").get_attribute("href")
                            location = job.locator(".job-listing__location").inner_text()

                            if location not in jobs_by_location:
                                jobs_by_location[location] = []

                            jobs_by_location[location].append(
                                {"title": title, "link": link}
                            )

                        break  # Exit retry loop if successful
                    except Exception as e:
                        logging.warning(f"Attempt {attempt + 1} failed: {e}")
                        if attempt < self.retries - 1:
                            time.sleep(2 ** attempt)  # Exponential backoff
                        else:
                            logging.error(f"Failed to scrape page {page_num}. Skipping.")
                            break

            browser.close()

        return jobs_by_location

    def extract_job_details(self, job_link):
        """
        Extracts job details from the job details page.

        Args:
            job_link (str): URL of the job details page.

        Returns:
            dict: Extracted job details.
        """
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()

            try:
                page.goto(job_link)
                job_data = page.locator(".job-data")

                # Extract details
                location = job_data.locator(".job-data__location").inner_text()
                description = job_data.locator(".job-description").inner_text()
                salary = None
                hourly_rate = None

                # Parse salary or hourly rate from description
                for line in description.splitlines():
                    if "$" in line and "annual salary range" in line:
                        salary = line.split("$", 1)[1].strip()
                    elif "$" in line and "hourly wage range" in line:
                        hourly_rate = line.split("$", 1)[1].strip()

                return {
                    "location": location,
                    "description": description,
                    "salary": salary,
                    "hourly_rate": hourly_rate,
                }
            except Exception as e:
                logging.error(f"Error extracting job details from {job_link}: {e}")
                return None
            finally:
                browser.close()

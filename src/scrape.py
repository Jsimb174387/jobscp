import time
import logging
import html

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from webdriver_manager.chrome import ChromeDriverManager
import pyap


class Scraper:
    def __init__(
        self,
        get_proxy,
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
            get_proxy (function): Function to retrieve a proxy.
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
        self.get_proxy = get_proxy

    def get_jobs(self, start_page, end_page):
        """
        Retrieves job postings within a specified range and processes the data.

        Args:
            start_page (int): The starting index for job postings to retrieve.
            end_page (int): The ending index for job postings to retrieve.

        Returns:
            tuple: A tuple containing:
                - formatted_data (list): A list of processed job data.
                - bad_data (list): A list of job data entries that could not
                  be processed.
        """
        jobs_by_location = self.get_walmart_careers(
            self.query,
            self.date_sort,
            self.expand,
            self.job_career_area,
            self.employment_type,
            start_page,
            end_page,
            self.retries,
        )
        formatted_data, bad_data = self.get_career_info(
            jobs_by_location, self.country, self.retries
        )
        return formatted_data, bad_data

    def get_walmart_careers(
        self,
        query,
        date_sort,
        expand,
        job_career_area,
        employment_type,
        start_pages,
        max_pages,
        retries,
    ):
        """
        Webscraper for Walmart career page. Utilizes the search queries
        provided to it, along with the proxy pool given. Pages scraped are
        based on the start and end page indices. Retries are implemented with
        exponential backoff based on the maximum retries set. Proxy switched
        when failing to access pages.

        Args:
            query (str): Search query for the Walmart careers page.
            date_sort (bool): Whether to sort results by date.
            expand (str): Additional parameters for expanding search results.
            job_career_area (str): Job career area filter.
            employment_type (str): Employment type filter.
            start_pages (int): Starting page number.
            max_pages (int): Maximum page number.
            retries (int): Number of retries for failed requests.

        Returns:
            dict: Dictionary of jobs grouped by location.
        """
        url = "https://careers.walmart.com/results"
        jobs_by_location = {}

        driver = None
        proxy_address = self.get_proxy()

        try:
            for page in range(start_pages, max_pages + 1):
                payload = (
                    f"?q={query}&page={page}&sort=rank&expand={expand}"
                    f"&jobCareerArea={job_career_area}"
                )
                if employment_type:
                    payload += f"&EmploymentType={employment_type}"

                for attempt in range(retries):
                    try:
                        if not driver:
                            driver = self.proxy_driver(proxy_address)
                            logging.info(f"Using proxy: {proxy_address}")

                        driver.get(url + payload)

                        if date_sort:
                            button = WebDriverWait(driver, 10).until(
                                EC.element_to_be_clickable(
                                    (
                                        By.CSS_SELECTOR,
                                        "label.search__sort__option__label"
                                        "[title='Job Post Date']",
                                    )
                                )
                            )
                            button.click()

                        job_list = WebDriverWait(driver, 20).until(
                            EC.presence_of_all_elements_located(
                                (By.CLASS_NAME, "job-listing")
                            )
                        )

                        for job in job_list:
                            try:
                                listing_data = job.find_element(
                                    By.CLASS_NAME, "job-listing__link"
                                )
                                location_data = job.find_element(
                                    By.CLASS_NAME, "job-listing__location"
                                )

                                link = listing_data.get_attribute("href")
                                title = listing_data.text.strip()
                                location = location_data.text.strip()

                                if not title or not link or not location:
                                    raise ValueError(
                                        f"Missing or incomplete job data, "
                                        f"on page {page}"
                                    )

                                if location not in jobs_by_location:
                                    jobs_by_location[location] = []

                                jobs_by_location[location].append(
                                    {"title": title, "link": link}
                                )
                            except Exception as e:
                                logging.error(f"Error during parsing job: {e}")
                                continue

                        break
                    except Exception as e:
                        logging.warning(f"Attempt {attempt + 1} failed: {e}")
                        if attempt < retries - 1:
                            backoff_time = 2**attempt
                            proxy_address = self.get_proxy()
                            if driver:
                                driver.quit()
                                driver = None
                            logging.info(f"Switching to new Proxy: {proxy_address}")
                            logging.info(f"Sleeping for {backoff_time} seconds...")
                            time.sleep(backoff_time)
                        else:
                            logging.error(
                                f"Failed to access page {page}. Switching proxy."
                            )
                            proxy_address = self.get_proxy()
                            if driver:
                                driver.quit()
                                driver = None
        finally:
            if driver:
                driver.quit()
            return jobs_by_location

    def proxy_driver(self, proxy_address):
        """
        Initializes a Selenium WebDriver with optional proxy settings.

        Args:
            proxy_address (str): Proxy address to use.

        Returns:
            WebDriver: Configured Selenium WebDriver instance.
        """
        options = Options()
        options.add_argument("--headless=new")
        prefs = {
            "profile.default_content_setting_values.geolocation": 2
        }
        options.add_experimental_option("prefs", prefs)
        if proxy_address:
            options.add_argument(f"--proxy-server={proxy_address}")
            logging.info(f"Using proxy: {proxy_address}")
        try:
            driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()),
                options=options,
            )
            return driver
        except Exception as e:
            logging.error(
                f"Failed to initialize WebDriver with proxy {proxy_address}: {e}"
            )
            raise

    def get_career_info(self, jobs_by_location, country, retries=5):
        """
        Processes job listings by extracting detailed information for each job.
        Uses a proxy pool to handle requests and retries on failure.

        Args:
            jobs_by_location (dict): Dictionary of jobs grouped by location.
                Obtained from get_walmart_careers.
            country (str): Country code for parsing addresses (e.g., 'US').
                These are specified from the module pyap.
            retries (int): Number of retries for each job in case of failure.

        Returns:
            tuple: A tuple containing:
                - formatted_data (list): List of jobs grouped by address.
                - bad_data (list): List of jobs with missing or unknown
                  addresses.
        """
        jobs_by_address = {}  # Dictionary to store jobs grouped by address
        unknown_address = {}  # Dictionary to store jobs with missing addresses
        driver = None  # Initialize the WebDriver
        proxy_address = self.get_proxy()  # Get the first proxy from the pool

        try:
            # Iterate through each location and its associated job list
            for area, job_list in jobs_by_location.items():
                for job in job_list:
                    job_link = job.get("link")  # Extract the job link
                    title = job.get("title")  # Extract the job title

                    # Skip jobs with missing data
                    if not job_link or not title:
                        logging.warning(f"Skipping job due to missing data: {job}")
                        continue

                    # Retry logic for extracting job details
                    for attempt in range(retries):
                        try:
                            # Initialize the WebDriver if not already initialized
                            if not driver:
                                driver = self.proxy_driver(proxy_address)
                                logging.info(
                                    f"Initialized WebDriver with proxy: "
                                    f"{proxy_address}"
                                )

                            # Extract job details from the job page
                            logging.info(f"Extracting details for job: {title}")
                            job_details = self.extract_job_details(
                                driver, job_link, country
                            )

                            # If job details could not be extracted, retry
                            if not job_details:
                                raise ValueError(f"Failed to extract details.")

                            # Extract the address from the job details
                            address = job_details["address"]
                            if address:
                                # If address is found, group jobs by address
                                for addr in address:
                                    if addr not in jobs_by_address:
                                        jobs_by_address[addr] = []
                                    jobs_by_address[addr].append(
                                        {
                                            "job_link": job_link,
                                            "title": title,
                                            "description": job_details["description"],
                                            "hourly_rate": job_details["hourly_rate"],
                                            "salary": job_details["salary"],
                                            "types": job_details["employment_type"],
                                        }
                                    )
                                    logging.info(f"Added job to address: {addr}")
                            else:
                                # If no address is found, group jobs by location
                                location = job_details["location"]
                                if location not in unknown_address:
                                    unknown_address[location] = []
                                unknown_address[location].append(
                                    {
                                        "job_link": job_link,
                                        "title": title,
                                        "description": job_details["description"],
                                        "hourly_rate": job_details["hourly_rate"],
                                        "salary": job_details["salary"],
                                        "types": job_details["employment_type"],
                                    }
                                )
                                logging.info(f"Added job to location: {location}")
                            break  # Break out of the retry loop if successful
                        except Exception as e:
                            # Log a warning if an attempt fails
                            logging.warning(
                                f"Attempt {attempt + 1} failed for job: "
                                f"{job_link}. Error: {e}"
                            )
                            if attempt < retries - 1:
                                # Wait before retrying (exponential backoff)
                                backoff_time = 2 ** attempt
                                proxy_address = self.get_proxy()
                                if driver:
                                    driver.quit()
                                    driver = None
                                logging.info(
                                    f"Retrying in {backoff_time} seconds..."
                                )
                                time.sleep(backoff_time)
                            else:
                                # Log an error if all retries fail
                                logging.error(
                                    f"Failed to process job: {job_link} after "
                                    f"{retries} retries. Skipping."
                                )
                                proxy_address = self.get_proxy()
                                if driver:
                                    driver.quit()
                                    driver = None
                                break
                logging.info(f"Done with {area}")
        finally:
            # Ensure the WebDriver is closed after processing
            if driver:
                driver.quit()

            # Format the data for jobs with valid addresses
            formatted_data = [
                {"address": address, "jobs": jobs}
                for address, jobs in jobs_by_address.items()
            ]
            # Format the data for jobs with missing or unknown addresses
            bad_data = [
                {"location": location, "jobs": jobs}
                for location, jobs in unknown_address.items()
            ]

            # Log the results
            logging.info(
                f"Formatted data: {len(formatted_data)} addresses processed."
            )
            logging.info(
                f"Bad data: {len(bad_data)} locations with missing addresses."
            )
            return formatted_data, bad_data

    def extract_job_details(self, driver, jobLink, country_):
        """
        Extracts job details from the job details page.
        """
        try:
            # Navigate to the job link
            driver.get(jobLink)

            # Wait for the job details container to load
            job_data = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CLASS_NAME, "job-data"))
            )

            # Initialize fields
            location = None
            employment_type = []
            salary = None
            hourly_rate = None
            description = None
            address = []

            # Extract location, position type, and other details
            job_elements = job_data.find_elements(By.CLASS_NAME, "job-data__element")
            for element in job_elements:
                try:
                    title = (
                        element.find_element(By.CLASS_NAME, "job-data__title")
                        .get_attribute("innerHTML")
                        .strip()
                    )
                    value = (
                        element.find_element(By.CLASS_NAME, "job-data__value")
                        .get_attribute("innerHTML")
                        .strip()
                    )

                    if title == "Location":
                        location = value
                    elif title == "Employment Type":
                        raw_types = html.unescape(value).split("&")
                        for raw in raw_types:
                            cleaned = raw.strip()
                            if cleaned == "Regular/Permanent":
                                cleaned = "Full Time"
                            if cleaned == "Full":
                                cleaned = "Full Time"
                            employment_type.append(cleaned)
                except Exception as e:
                    logging.warning(f"Error extracting job element: {e}")

            # Extract job description
            job_description = driver.find_element(By.CLASS_NAME, "job-description")
            description = job_description.text
            address = pyap.parse(description, country=country_)
            # Convert parsed addresses to a list of full addresses
            if address:
                address = [str(addr.full_address) for addr in address]
            # Extract salary or hourly rate
            lines = description.splitlines()
            for i, text in enumerate(lines):
                # Check for salary information
                if "$" in text and "annual salary range" in text:
                    salary = text.split("$", 1)[1].strip()
                elif "$" in text and "hourly wage range" in text:
                    hourly_rate = text.split("$", 1)[1].strip()

                # Check for "Primary Location..." and get the next line as the address
                if "Primary Location..." in text:
                    if i + 1 < len(lines):  # Ensure there is a next line
                        full_addr = lines[
                            i + 1
                        ].strip()  # Get the next line and strip whitespace
                        found = False
                        if address != []:
                            for addr in address:
                                if addr in full_addr:
                                    found = True
                        if not found:
                            address.append(full_addr)

            # Return extracted details
            return {
                "location": location,
                "employment_type": employment_type,
                "salary": salary,
                "hourly_rate": hourly_rate,
                "description": description,
                "address": address,
            }

        except Exception as e:
            logging.error(f"Error extracting job details from {jobLink}: {e}")
            return None

import time
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pyap

# Walmart jobs webscraper
def get_walmart_careers(proxy_pool, query='', date_sort = False, expand='department,brand,type,rate',
                        jobCareerArea='all', EmploymentType=None, start_pages=1, max_pages=5,
                        retries=5):
    """
    Webscraper for walmart career page. Utilizes the search queries provided to it, along with the proxy pool given. 
    Pages scraped is based on the start and end page indicies. 
    Retries are implemented with a exponential backoff based on the maximum retries set. 
    Proxy switched when failing to access pages. 
    """
    url = "https://careers.walmart.com/results"
    jobs_by_location = {}

    driver = None  # Initialize driver variable
    proxy_address = next(proxy_pool, None) if proxy_pool else None  # Start with the first proxy

    try:
        for page in range(start_pages, max_pages + 1):
            # Create the payload of search filters
            payload = f"?q={query}&page={page}&sort=rank&expand={expand}&jobCareerArea={jobCareerArea}"
            if EmploymentType:
                payload += f"&EmploymentType={EmploymentType}"

            # Retry mechanism with exponential backoff
            for attempt in range(retries):
                try:
                    # Initialize the WebDriver with the current proxy
                    if not driver:  # Only create a driver if one doesn't exist
                        driver = proxy_driver(proxy_address)
                        logging.info(f"Using proxy: {proxy_address}")

                    #logging.info(f"Loading URL: {url + payload}")
                    driver.get(url + payload)

                    # The career page ignores payload passed sorting requests, so you need to click the button. The default is Rank (best match).
                    # If date_sort == True then sort by date. 
                    # This can break things, with it resorting back to page 1, so this needs to be used with care. 
                    if date_sort:
                        button = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "label.search__sort__option__label[title='Job Post Date']")))
                        button.click()

                    # Wait for the job listings to load
                    job_list = WebDriverWait(driver, 20).until(
                        EC.presence_of_all_elements_located((By.CLASS_NAME, "job-listing")))
                    # Parse job listings
                    for job in job_list:
                        try:
                            listing_data = job.find_element(By.CLASS_NAME, "job-listing__link")
                            location_data = job.find_element(By.CLASS_NAME, "job-listing__location")

                            link = listing_data.get_attribute('href') if listing_data else None
                            title = listing_data.text.strip() if listing_data else None
                            location = location_data.text.strip() if location_data else None

                            if not title or not link or not location:
                                raise ValueError(f"Missing or incomplete job data, on page {page}")

                            if not jobs_by_location.get(location):
                                jobs_by_location[location] = []

                            jobs_by_location[location].append({
                                'title': title,
                                'link': link,
                            })
                        except Exception as e:
                            logging.error(f"Error during parsing job: {e}")
                            continue

                    # If successful, break out of the retry loop
                    break
                except Exception as e:
                    logging.warning(f"Attempt {attempt + 1} failed: {e}")
                    if attempt < retries - 1:
                        backoff_time = 2 ** attempt  # Exponential backoff
                        logging.info(f"Retrying in {backoff_time} seconds...")
                        time.sleep(backoff_time)
                    else:
                        logging.error(f"Failed to access page {page}. Switching proxy.")
                        proxy_address = next(proxy_pool, None)  # Switch to the next proxy
                        if driver:
                            driver.quit()  # Close the current driver
                            driver = None  # Reset driver to avoid reusing a closed instance
    finally:
        if driver:
            driver.quit()
        return jobs_by_location

def proxy_driver(proxy_address):
    """
    Initializes a Selenium WebDriver with optional proxy settings.
    """
    options = Options()
    options.add_argument("--headless=new")  # Removes GUI for better performance
    prefs = {
        "profile.default_content_setting_values.geolocation": 2  # 2 to block it
    }
    options.add_experimental_option("prefs", prefs) # Blocking location data, as chrome keeps pulling location data into the search
    if proxy_address:
        options.add_argument(f"--proxy-server={proxy_address}")
        logging.info(f"Using proxy: {proxy_address}")
    try:
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options
        )
        return driver
    except Exception as e:
        logging.error(f"Failed to initialize WebDriver with proxy {proxy_address}: {e}")
        raise

def get_career_info(jobs_by_location, proxy_pool, country, retries=5):
    """
    Processes job listings by extracting detailed information for each job.
    Uses a proxy pool to handle requests and retries on failure.

    Args:
        jobs_by_location (dict): Dictionary of jobs grouped by location. Obtained from get_walmart_careers.
        proxy_pool (iterator): Iterator of proxy addresses for rotating proxies.
        country (str): Country code for parsing addresses (e.g., 'US'). These are specified from the module pyap.
        retries (int): Number of retries for each job in case of failure.

    Returns:
        tuple: A tuple containing:
            - formatted_data (list): List of jobs grouped by address.
            - bad_data (list): List of jobs with missing or unknown addresses.
    """
    jobs_by_address = {}  # Dictionary to store jobs grouped by address
    unknown_address = {}  # Dictionary to store jobs with missing addresses
    driver = None  # Initialize the WebDriver
    proxy_address = next(proxy_pool, None) if proxy_pool else None  # Get the first proxy from the pool

    try:
        # Iterate through each location and its associated job list
        for area, job_list in jobs_by_location.items():
            for job in job_list:
                jobLink = job.get("link")  # Extract the job link
                title = job.get("title")  # Extract the job title

                # Skip jobs with missing data
                if not jobLink or not title:
                    logging.warning(f"Skipping job due to missing data: {job}")
                    continue

                # Retry logic for extracting job details
                for attempt in range(retries):
                    try:
                        # Initialize the WebDriver if not already initialized
                        if not driver:
                            driver = proxy_driver(proxy_address)
                            logging.info(f"Initialized WebDriver with proxy: {proxy_address}")

                        # Extract job details from the job page
                        logging.info(f"Extracting details for job: {title}")
                        job_details = extract_job_details(driver, jobLink, country)

                        # If job details could not be extracted, log a warning and retry
                        if not job_details:
                            logging.warning(f"Failed to extract details for job: {title}")
                            continue

                        # Extract the address from the job details
                        address = job_details["address"]
                        if address:
                            # If address is found, group jobs by address
                            for addr in address:
                                if not jobs_by_address.get(addr):
                                    jobs_by_address[addr] = []
                                jobs_by_address[addr].append({
                                    "jobLink": jobLink,
                                    "title": title,
                                    "description": job_details["description"],
                                    "hourlyRate": job_details["hourly_rate"],
                                    "salary": job_details["salary"],
                                    "types": [job_details["employment_type"]]
                                })
                                logging.info(f"Added job to address: {addr}")
                        else:
                            # If no address is found, group jobs by location
                            location = job_details["location"]
                            if not unknown_address.get(location):
                                unknown_address[location] = []
                            unknown_address[location].append({
                                "jobLink": jobLink,
                                "title": title,
                                "description": job_details["description"],
                                "hourlyRate": job_details["hourly_rate"],
                                "salary": job_details["salary"],
                                "types": [job_details["employment_type"]]
                            })
                            logging.info(f"Added job to location: {location}")
                        break  # Break out of the retry loop if successful
                    except Exception as e:
                        # Log a warning if an attempt fails
                        logging.warning(f"Attempt {attempt + 1} failed for job: {jobLink}. Error: {e}")
                        if attempt < retries - 1:
                            # Wait before retrying (exponential backoff)
                            backoff_time = 2 ** attempt
                            logging.info(f"Retrying in {backoff_time} seconds...")
                            time.sleep(backoff_time)
                        else:
                            # Log an error if all retries fail
                            logging.error(f"Failed to process job: {jobLink} after {retries} retries. Skipping.")
                            break
            logging.info(f"Done with {area}")
    finally:
        # Ensure the WebDriver is closed after processing
        if driver:
            driver.quit()

        # Format the data for jobs with valid addresses
        formatted_data = [{"address": address, "jobs": jobs} for address, jobs in jobs_by_address.items()]
        # Format the data for jobs with missing or unknown addresses
        bad_data = [{"location": location, "jobs": jobs} for location, jobs in unknown_address.items()]

        # Log the results
        logging.info(f"Formatted data: {len(formatted_data)} addresses processed.")
        logging.info(f"Bad data: {len(bad_data)} locations with missing addresses.")
        return formatted_data, bad_data

def extract_job_details(driver, jobLink, country_):
    """
    Extracts job details from the job details page.
    """
    try:
        # Navigate to the job link
        driver.get(jobLink)

        # Wait for the job details container to load
        job_data = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "job-data"))
        )

        # Initialize fields
        location = None
        employment_type = None
        salary = None
        hourly_rate = None
        description = None
        address = []

        # Extract location, position type, and other details
        job_elements = job_data.find_elements(By.CLASS_NAME, "job-data__element")
        for element in job_elements:
            try:
                title = element.find_element(By.CLASS_NAME, "job-data__title").get_attribute("innerHTML").strip()
                value = element.find_element(By.CLASS_NAME, "job-data__value").get_attribute("innerHTML").strip()

                if title == "Location":
                    location = value
                elif title == "Employment Type":
                    employment_type = value
            except Exception as e:
                logging.warning(f"Error extracting job element: {e}")

        # Extract job description
        job_description = driver.find_element(By.CLASS_NAME, "job-description")
        description = job_description.text
        address = pyap.parse(description, country = country_)
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
                    full_addr = lines[i + 1].strip()  # Get the next line and strip whitespace
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
            "address": address
        }

    except Exception as e:
        logging.error(f"Error extracting job details from {jobLink}: {e}")
        return None

class Scraper:
    def __init__(self, proxy_pool, query='', date_sort = False, expand='department,brand,type,rate',
                        jobCareerArea='all', EmploymentType=None, retries=5, country = 'US'):
        self.proxy_pool = proxy_pool
        self.query = query
        self.date_sort = date_sort
        self.expand = expand
        self.jobCareerArea = jobCareerArea
        self.EmploymentType = EmploymentType
        self.retries = retries
        self.country = country

    def get_jobs(self, page):
        jobs_by_location = get_walmart_careers(self.proxy_pool, self.query, self.date_sort, 
                                               self.expand, self.jobCareerArea, self.EmploymentType, page, page, self.retries)
        careers = get_career_info(jobs_by_location, self.proxy_pool, self.country, self.retries)
        

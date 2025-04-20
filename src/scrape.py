import requests
import time
import json
import logging
import itertools
from bs4 import BeautifulSoup

# Walmart jobs webscraper

PROXIES = [
    "",
]

proxy_pool = itertools.cycle(PROXIES)

def get_walmart_careers(query = '', sort = 'date', expand = 'department,brand,type,rate',
              jobCareerArea = 'all', EmploymentType = None, start_pages = 1, max_pages = 5,
              retries = 5):
    url = https://careers.walmart.com/results
    jobs = []

    jobs_by_location = {}

    for page in range(start_pages, max_pages + 1):
        payload = {
            'q': query,
            'page': page,
            'sort': sort,
            'expand': expand,
            'jobCareerArea': jobCareerArea,
        }

        if EmploymentType:
            payload['EmploymentType'] = EmploymentType
    
        for attempt in range(retries):
            proxy = next(proxy_pool)
            try:
                response = requests.get(url, params = payload, proxies = {'http': proxy, 'https': proxy}, timeout = 10)
                if response.status_code == 200:
                    break
                else:
                    logging.warning(f"Attempt {attempt + 1} failed. Status: {response.status_code}.")
                    time.sleep(2)
            except requests.exceptions.RequestException as e:
                sleep_time = 2 ** attempt
                logging.error(f"Request failed: {e}")
                time.sleep(sleep_time)
        else:
            logging.error(f"Failed to fetch {page}")
            continue
    
        soup = BeautifulSoup(response.text, 'html.parser')
        
        job_list = soup.find_all('li', class_= 'search-result job-listing')

        for job in job_list:
            listing_data = job.find('a', class_= "job-listing__link")
            title_tag = job.find('a', class_= "job-listing__link").text.stip()
            location_data = job.find('span', class_= "job-listing__location" )

            if listing_data:
                link = listing_data['href']
                title = listing_data.text.strip()
            else:
                link = None
                title = None

            if location_data:
                location = location_data.text.strip()
            else:
                location = None

            if not jobs_by_location.get(location):
                jobs_by_location[location] = []
            
            jobs_by_location[location].append({
                'title': title,
                'link': link,
            })
    return jobs_by_location

            

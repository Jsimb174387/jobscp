# jobscp

# Documentation

## Approach
The `jobscp` project is designed to scrape job postings from Walmart's career page and organize them into structured JSON files. 
The project uses Python and Selenium for web scraping, along with a proxy pool to handle requests efficiently.

The main features implemented are, and named according to the Assignment page:
- Advanced Error Handling: on failed requests, an exponential backoff system is implemented, based on the retry parameter.
Time waited between attempts is 2 ** attempt_number.
Additionally, each attempt switches proxies.
After enough failed attempts, the thread moves on to the next page.

- Pagination Handling: The scraper handles multiple pages. There are two main steps:
the listing page, where 25 job listings are displayed per page. And the job info page, 
where more details such as salary and address are found. The scraper goes through the 
given number of listing pages, then extracts the details on each job info pages.

- Real-Time Logging: Info and above level logging is used to track the progress of 
the scraper. Whenever a thread is finished with a location, it logs that it is done with 
said location. Additionally, information such as errors, sleep information, and
the proxy used are provided in logs. 

- IP Rotation: Each thread uses a proxy. Whenever an error occurs, it switches proxies
as part of the error handling. The Proxy pool is shared between all Threads, with
a thread safe Queue to handle thread access to the pool. 

- Asynchronous scraping: Through the use of Python's concurrent.futures module, the workload is divided between multiple threads to enable faster scraping.
src/scrape.py holds the core scraping logic, fetching job listings, parsing job details, and then grouping the jobs for a set range of pages.
This is implimented in the Scraper class. 
src/async_scraper.py creates multiple instances of the scraper class, then divides the pages between all of the scrapers for them each to handle.
Then, it combines the results. This drastically improves throughput. 

The main entry point is [`src/main.py`](src/main.py), which orchestrates the scraping process by:
- Fetching proxies from an external API.
- Running the asynchronous scraper with specified parameters.
- Saving the results into two JSON files:
  - `data/walmart_jobs_formatted.json`: Jobs grouped by address.
  - `data/walmart_jobs_bad.json`: Jobs with missing or unknown addresses.


## Challenges Faced
The main challenges was dealing with odd behaviors that were implemented in the
Walmart Careers website. This is not the full list, but the ones I remember off the top of my head.

Location: When opening the career website, sometimes the page pulls location data and inputs it into the search filters. 
Solution: By disabling geolocation data in the selenium settings, the page was unable to do this.

Unexpected Behaviors: Pagination with page button is the only way to use the sorting.
i.e, in order to say sort by date, you cannot use url + payload. This means I would not be 
able to efficiently implement async scraping, so I decided to abandon sorting to gain speed.

Slow scraping: The scraping speed is pretty slow, especially when using free proxies.
To improve this, I used async scrapers to load multiple pages at a time. 

Scraping details was nontrivial: the data is stored in a bunch of #text blocks rather than in a html paragraph.
This was a bit tricky to get the exact details I needed, but by loading all the lines into a list
then comparing it to the start of what I am looking for I extracted the information. For example, 
if the previous line is "Primary Location:" then the next line is the address.

Sometimes, the address is also stored in places other than "Primary Location:". To handle this,
I imported the pyap module to try and identify these addresses, which it does a good job at doing. 
However, some addresses pyap misses, and so I cannot rely on it solely. Which is why 
I always use the line after Primary Location, which is always an address (in the data I have checked at least). 

## Example Data
Example JSON can be found in the data folder. walmart_jobs_formatted holds jobs that could be identified to an address,
walmart_jobs_bad holds jobs that could only be identified by a general location. 
logs are found in the logs file, under scraper.logs.

Note: The data found in data was generated without using proxies. While I initially tried generating the data with proxies, 
The free proxy list I was using seemed pretty low quality and more often than not wouldn't give good responses back, leading to skipping 
more jobs than I would like. 

In the jobs JSON, there is 505 jobs in walmart_jobs_formatted, and 3495 jobs in walmart_jobs_bad. 
I added to the search query "IT". I was hoping that would help filter for IT jobs, but instead it picked up
many Cashier Roles. Regardless, the data is still good. 

Logs should (probably) be gitignored, but as it is part of the assessment I kept them visable.

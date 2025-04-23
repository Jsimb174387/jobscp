from scrape import *
import json
import requests
import os
import logging
import itertools

#Job Career Area settings

JCA_TECH = "0000015e-b97d-d143-af5e-bd7da8ca0000"

JCA_CORPORATE = """00000159-7597-d2b4-abdd-f5b79c9d0000,00000159-759e-d286-a3f9-7fbe44710000,00000159-759f-d286-a3f9-7fbf0f6d0000,00000159-759f-d2b4-abdd-f5bfa22d0000,00000159-75a1-d286-a3f9-7fa114fa0000,00000159-75a2-d286-a3f9-7fa2bac60000,00000159-75a3-d286-a3f9-7fa3cf810000,00000159-75a3-d2b4-abdd-f5a734fa0000,00000159-75a4-d286-a3f9-7fa4503d0000,00000159-75a5-d2b4-abdd-f5a758f40000,00000159-75a6-d2b4-abdd-f5a7fdd80000,00000159-75a7-d286-a3f9-7fa7a04f0000,00000159-75a8-d286-a3f9-7fa8f5c00000,00000159-75a8-d2b4-abdd-f5af3e670000,00000159-75a9-d286-a3f9-7fa9c9e30000,00000159-7627-d286-a3f9-7ea7d10c0000,0000015e-f1eb-d841-a15f-f7eb14100000,00000161-8bda-d3dd-a1fd-bbda62130000,0000017b-11e6-d257-afff-33f6155f0000&jobSubCategory=0000015a-a525-d1ef-a3db-ff7fe69d0000,0000015a-a525-d435-a1da-bfa5662d0000,0000018b-4850-dbdf-a19b-6fde19570000,0000018b-4852-d283-abeb-5a5ec36b0000,0000018b-485a-d283-abeb-5a5e8ca20000,0000018b-486d-dbdf-a19b-6fff77110000,0000015a-a527-d752-a75a-f7f73e820000,00000188-e346-df4d-affe-effebb320000,0000018b-48a2-d283-abeb-5afe854e0000,0000018b-492e-da98-adfb-79ef66c60000,0000018b-482f-da98-adfb-78ef98a30000,0000018b-4854-da98-adfb-78f76e7a0000,0000018b-489b-da98-adfb-78ffda970000,0000018b-482e-dbdf-a19b-6fbe62a80000,0000018b-484d-d494-a3cb-eddf78a40000,0000018b-4865-da98-adfb-78e71d930000,0000018b-48a0-dd60-a3af-6dbde9960000,0000018b-492b-de4a-ad9f-59ff1a230000,0000018b-4855-de4a-ad9f-58dfb73c0000,0000018b-48a7-d494-a3cb-edb72e1d0000,0000018b-48df-da98-adfb-78ff931c0000,0000018b-4917-dd60-a3af-6d9ff6590000,0000018b-4922-dd60-a3af-6dbf97a90000,0000018b-48a4-dd60-a3af-6dbd95fa0000,0000018b-4925-d283-abeb-5b7dd8f60000,0000015a-a52e-d06d-af5f-f5bf8ae90000,0000015a-a530-d06d-af5f-f5b37f790000,00000161-57de-dc3b-a7ed-57ff9cbf0000,0000018b-4862-dd60-a3af-6dff47da0000,0000018b-4869-dbdf-a19b-6fff19a30000,0000018b-491d-de4a-ad9f-59df9d780000,0000018b-4925-d494-a3cb-edb73a780000,0000018b-4920-d283-abeb-5b7c3b370000,0000018b-4923-de4a-ad9f-59ff545c0000,0000018b-4829-dd60-a3af-6dbdc6a80000,0000018b-4859-dbdf-a19b-6fdf90790000,0000018b-486c-d283-abeb-5a7c50220000,0000018b-484c-de4a-ad9f-58df75a80000,0000018b-491b-d494-a3cb-ed9b5efc0000,0000018b-491e-de4a-ad9f-59df415f0000,0000018b-491f-d494-a3cb-ed9f2fff0000,0000018b-4921-dbdf-a19b-6fbf76cc0000,0000018b-4828-dbdf-a19b-6fbeac050000,0000018b-48a3-da98-adfb-78e737460000,0000018b-4919-d494-a3cb-ed9b5e5c0000,0000018b-4928-d283-abeb-5b7c4c660000,0000015a-a52a-d06d-af5f-f5bb06450000,0000018b-4831-d283-abeb-5a7d7b260000,0000018b-485e-da98-adfb-78ff5f550000,0000018b-485f-dbdf-a19b-6fdf0e7e0000,0000018b-4860-d283-abeb-5a7c5d410000,0000018b-4834-dd60-a3af-6dbd06e20000,0000018b-4836-d494-a3cb-edb77a690000,0000018b-4839-d283-abeb-5a7d89870000,0000018b-4864-dd60-a3af-6dfd18390000,0000018b-48aa-de4a-ad9f-58ffddaa0000,0000018b-4e48-da98-adfb-7eef99190000,0000015a-a52e-d752-a75a-f7fee63c0000&expand=department,0000015e-b97d-d143-af5e-bd7da8ca0000,00000161-7bff-da32-a37b-fbffc8c10000,00000159-7574-d286-a3f9-7ff45f640000,00000159-7597-d2b4-abdd-f5b79c9d0000,00000159-759e-d286-a3f9-7fbe44710000,00000159-759f-d2b4-abdd-f5bfa22d0000,00000159-75a1-d286-a3f9-7fa114fa0000,00000159-75a2-d286-a3f9-7fa2bac60000,00000159-75a3-d286-a3f9-7fa3cf810000,00000159-75a3-d2b4-abdd-f5a734fa0000,00000159-75a5-d2b4-abdd-f5a758f40000,00000159-75a6-d2b4-abdd-f5a7fdd80000,00000159-75a7-d286-a3f9-7fa7a04f0000,00000159-75a8-d286-a3f9-7fa8f5c00000,00000159-75a8-d2b4-abdd-f5af3e670000,00000159-75a9-d286-a3f9-7fa9c9e30000,00000159-7627-d286-a3f9-7ea7d10c0000,0000015e-f1eb-d841-a15f-f7eb14100000,0000017b-11e6-d257-afff-33f6155f0000"""

# Configure logging to write to both a file and the console
os.makedirs('logs', exist_ok=True)  # Ensure the logs directory exists

# Create a file handler to write logs to a file
file_handler = logging.FileHandler('logs/scraper.log', encoding='utf-8')
file_handler.setLevel(logging.INFO)  # Log INFO and above to the file

# Create a stream handler to display logs in the console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)  # Log INFO and above to the console

# Define a common log format
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Configure the root logger
logging.basicConfig(
    level=logging.INFO,  # Log INFO and above
    handlers=[file_handler, console_handler]  # Add both handlers
)

def main():
    proxies = get_proxies()
 
    p_pool = itertools.cycle(proxies)
    #JobCareerArea for tech: 0000015e-b97d-d143-af5e-bd7da8ca0000
    jobs_by_location = get_walmart_careers(None, start_pages = 1, max_pages = 270)

    formatted_data, bad_data = get_career_info(jobs_by_location, None, 'US')

    save(formatted_data, bad_data)







def get_proxies():
    url = "https://proxylist.geonode.com/api/proxy-list?country=US&google=false&limit=500&page=1&sort_by=responseTime&sort_type=asc"
    res = requests.get(url)
    if res.status_code == 200:
        proxies = res.json().get('data', [])
        logging.info(f"Fetched {len(proxies)} proxies.")
        # Filter for proxies that support HTTP or HTTPS
        return [
            f"{protocol}://{proxy['ip']}:{proxy['port']}"
            for proxy in proxies for protocol in proxy.get('protocols', [])
            if protocol in ['http', 'https']
        ]
    else:
        logging.error(f"Failed to get proxies, code: {res.status_code}")
        return []

def save(formatted_data, bad_data):
    # Save formatted and bad (no address) data to seperate JSON files
    os.makedirs('data', exist_ok=True)  # Ensure the data directory exists
    with open('data/walmart_jobs_formatted.json', 'w', encoding='utf-8') as file:
        json.dump(formatted_data, file, ensure_ascii=False, indent=4)
        
    with open('data/walmart_jobs_bad.json', 'w', encoding='utf-8') as file:
        json.dump(bad_data, file, ensure_ascii=False, indent=4)

if __name__ == "__main__":
    main()


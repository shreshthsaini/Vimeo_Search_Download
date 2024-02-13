""" 
    Getting the search query results from Vimeo 
    
    Base URL: https://vimeo.com/search?hdr=hdr&price=free&q=
    NOTE: in the base url, hdr=hdr is for high dynamic range videos and price=free is for free videos and q= is for the search query keyword; We take all CC licensed videos from Vimeo. 
    NOTE: You can change the base url to get different results; just apply filter on vimeo and copy the url and paste it here.

    Avoid getting blocked due to making too many requests: 
    NOTE: Use a random user agent for each request; see get_random_user_agent() function
    NOTE: Use a random delay between requests; see random_delay() function; typical human behavior is 0.5-10 seconds between requests. 

    Usage:
    searcher = VimeoSearch()
    results = searcher.search_vimeo("nature documentary")
    searcher.write_to_csv(results)
    
    return: search results in json format

    - Shreshth Saini, December 2023

"""

import requests
from urllib.parse import quote_plus
from json import loads
import csv
import pandas as pd
import time
import random
from fake_useragent import UserAgent
from bs4 import BeautifulSoup

class VimeoSearch():
    """ 
        # based on the combination of hdr, price and license, the url will be generated 
        # hdr: any, hdr, hdr10, dolby_vision, 
        # price: any, free, paid
        # resolution: any, 4k
        # license: any, by, cc0, by-nd, by-nc, by-sa, by-nc-nd, by-nc-sa, allCC
        # NOTE: You can change the base url to get different results; just apply filter on vimeo and copy the url and paste it here.

    """
    def __init__(self, base_vimeo_url="https://vimeo.com/search", hdr='hdr', price="free", license="allCC", resolution="any"):

        self.cc= ['by', 'cc0', 'by-nd', 'by-nc', 'by-sa', 'by-nc-nd', 'by-nc-sa']
        self.license = license
        
        self.url_components = '?'
        if hdr != 'any': 
            self.url_components += f"hdr={hdr}&"
        if price != "any":
            self.url_components += f"price={price}&"
        if license != "any" and license != "allCC":
            self.url_components += f"license={license}&"
        if resolution != "any":
            self.url_components += f"resolution={resolution}&"
        if base_vimeo_url != "https://vimeo.com/search":
            self.base_url = base_vimeo_url
            self.url_components = ''
        else:
            self.base_url = base_vimeo_url
        self.user_agent = UserAgent()
        self.all_results = []
        self.collected_ids = set()

    """
        Get the total number of pages for a given search query; 
        TODO: Edits function based on the Vimeo page structure. 
    """   
    def get_total_pages(self, search_terms):
        '''
        Returns the total number of pages for a given search query

        Args:
        - search_terms (str): The search query

        Returns:
        - int: The total number of pages
        
        '''
        encoded_search = quote_plus(search_terms)
        url = f"{self.base_url}{encoded_search}&page=1"
        headers = {'user-agent': self.get_random_user_agent()}
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            print(f"Error: HTTP {response.status_code}")
            return None

        soup = BeautifulSoup(response.text, 'html.parser')

        # This part is hypothetical and needs to be adjusted based on Vimeo's actual page structure
        # Example: Find an element or script tag that contains total results or pages information
        total_results_tag = soup.find("div", class_="total_results")
        if total_results_tag:
            total_results = int(total_results_tag.text)
            results_per_page = 10  # This is an assumption; adjust based on Vimeo's actual structure
            total_pages = (total_results + results_per_page - 1) // results_per_page
            return total_pages
        else:
            return None

    # Get a random user agent 
    def get_random_user_agent(self):
        '''
        Returns a random user agent string

        Returns:
        - str: A random user agent string 
        '''
        return self.user_agent.random

    # Random delay between requests
    def random_delay(self):
        '''
        Sleeps for a random amount of time between 0.0 and 0.1 seconds

        Returns:
        - float: The amount of time slept
        '''
        time.sleep(random.uniform(0.0, 0.1))

    # Query Vimeo search results for a given search query
    def query_vimeo(self, url_comp, encoded_search, license):
        '''
        Queries Vimeo search results for a given search query

        Args:
        - url_comp (str): The url components
        - encoded_search (str): The search query
        - license (str): The license type

        Returns:
        - list: A list of dictionaries containing the search results
        '''
        page = 1
        end_count = 0
        
        # Loop through all pages
        while True:
            # Get the search results for the current page
            temp_results = []
            # if page is 1 then url is simpler 
            if page == 1:
                url = f"{self.base_url}{url_comp}q={encoded_search}"
            else:
                if self.url_components == '':
                    url = f"{self.base_url.split('?')[0]}/page:{page}?{self.base_url.split('?')[-1]}{url_comp}q={encoded_search}"
                else: 
                    url = f"{self.base_url}/page:{page}{url_comp}q={encoded_search}"
            print(url)
            
            # Make the request
            headers = {'user-agent': self.get_random_user_agent()}
            response = requests.get(url, headers=headers)

            # Handle HTTP Status Codes
            if response.status_code == 429:
                print("Rate limited. Waiting to retry...")
                time.sleep(60)  # longer delay if rate limited
                continue
            elif response.status_code == 400:
                print(f"Reached the end of the results! Stopping...")
                break
            elif response.status_code != 200:
                print(f"Error: HTTP {response.status_code}")
                print(f"Unknow error! Stopping...")
                break
            
            # Parse the response
            try:
                index1 = response.text.index("vimeo.config = ")
                index1 = response.text.index("[", index1)
                index2 = response.text.index("}}}]", index1) + 4
                text = response.text[index1:index2]
                dicti = loads(text)

                for content in dicti:
                    title = content["clip"]["name"] + " "
                    while title in [video["title"] for video in self.all_results]:
                        title += "I"

                    temp_results.append({
                        "url": content["clip"]["link"],
                        "id": content["clip"]["link"].split("/")[-1],
                        "play_time": content["clip"]["duration"],
                        "date": content["clip"]["created_time"][:7],
                        "channel": content["clip"]["user"]["name"],
                        "title": title, 
                        "license": license,
                        "keyword": encoded_search
                    })
                    
            except Exception as e:
                print(f"An error occurred: {e}")
                # if license is allCC then break the loop else continue
                if self.license == "allCC":
                    print(f"No data for this license type: {license}")
                    break

                print("Response text:", response.text)
                # error is related to no more pages to scrape then break the loop else continue 
                if "No results found" in response.text:
                    break
                else:
                    continue                
            
            # Check if the IDs are unique; else break while loop
            if len(temp_results) == 0:
                print('Length of the dataframe: ',len(temp_results))
                break
            
            # check if all ids are already collected then break the loop
            temp_ids = []
            for result in temp_results:
                # check each id in temp with collected_ids; continue if id is already collected else add to collected_ids and append to all_results; if all ids are already collected then break the loop 
                if result['url'].split('/')[-1] in self.collected_ids:
                    end_count += 1
                    temp_ids.append(result['url'].split('/')[-1])
                    continue
                else:
                    self.collected_ids.add(result['url'].split('/')[-1])
                    self.all_results.append(result)
            
            #print(end_count, len(temp_results))
            #print(collected_ids, temp_ids)
                    
            # if all ids are already collected then break the loop            
            if end_count == len(temp_results):
                break
            
            # Random delay after each request
            self.random_delay()
            print(f"Scrapped page {page}...")
            page += 1
        
        return 
        
    # Search Vimeo for a given search query with built urls
    def search_vimeo(self, search_terms):
        '''
        Searches Vimeo for a given search query

        Args:
        - search_terms (str): The search query

        Returns:
        - list: A list of dictionaries containing the search results
        '''
        
        # encode search query
        encoded_search = quote_plus(search_terms)
        
        # if license is allCC then loop through all cc licenses else query for the given license type
        if self.license == "allCC":
            for li in self.cc:
                url_comp = self.url_components+f"license={li}&"
                self.query_vimeo(url_comp, encoded_search,li)
        else: 
            url_comp = self.url_components
            self.query_vimeo(url_comp, encoded_search, self.license)
        
        # return list of dictionaries containing the search results         
        return self.all_results

    # Convert the search results to a Pandas DataFrame 
    def results_to_df(self, results):
        '''
        Converts the search results to a Pandas DataFrame

        Returns:
        - DataFrame: A Pandas DataFrame containing the search results
        '''
        return pd.DataFrame(results)


    # Write the search results to a CSV file
    def write_to_csv(self, data, filename):
        '''
        Writes the search results to a CSV file

        Args:
        - data (list): The search results
        - filename (str): The CSV filename
        '''
        if not data:
            print("No data to write")
            return
        with open(filename, 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)


"""
# Usage
searcher = VimeoSearch(license="by", hdr='hdr')
keyword = "falling"
total_pages = searcher.get_total_pages(keyword)
if total_pages:
    print(f"Total pages: {total_pages}")
else:
    print("Could not determine the total number of pages.")

results = searcher.search_vimeo(keyword)
searcher.write_to_csv(results, "vimeo_videos.csv")
df = searcher.results_to_df(results)
print(df.head())
"""
'''
USDA_API Class
    - Built to encapsulate functionality of USDA_API methods
    - API website: https://quickstats.nass.usda.gov/api
    - handles most errors that were caught: API Throttling, unauthorized users, etc...
    - Used to capture information from API which then moves the information into MongoDB
    - Date Added: 04/04/2025
'''

import requests
import os
import sys
import time

config_path = os.getcwd().replace('\\scripts','')
sys.path.insert(0, config_path)

from config import settings 

class USDA_API():
    def __init__(self, key):
        self.url = 'https://quickstats.nass.usda.gov/api'
        self.key = key
        self.params = ''
        self.session = requests.Session()
        self.commodity_list = settings.usda_commodity_list
        
        
    def add_params(self, fieldname, value):
        self.params += f'&{fieldname}={value}'
    
    def return_params(self):
        return self.params
    
    def return_call(self):
        return self.url + '/api_GET/?' + f'key={self.key}' + f'{self.params}'

    def remove_params(self, fieldname):
        if len(self.params.split('&')) > 1:
            new_params = ''
            size = 1
            remove_params = [item for item in self.params.split('&') if fieldname not in item]
        
            for item in remove_params:
                if len(remove_params) > 1 and len(item) != 0 and size < len(remove_params) - 1:
                    new_params += item + '&'
                    size = size + 1
                else:
                    new_params  += item
            self.params = '&' + new_params
        else:
            self.params = self.params
            print('No Parameters to remove')

    def call(self, max_retries=10):
        retry_delay = 1
        for attempt in range(max_retries):
            try:
                response = self.session.get(self.return_call())
                response.raise_for_status()

                if response.status_code == 429:
                    time.sleep(retry_delay)
                    retry_delay *= 2

                if response.status_code == 200:
                        
                    # comment out lines 70 - 74 in case get_counts is failing or extract isn't running within 1.5 hours.

                    # get_counts = self.session.get(f'{self.url}/get_counts/?key={self.key}{self.params}').json()
            
                    # if get_counts['count'] >= 50000:
                    #     return f'Unable to Process Request. Request is greater than 50000 rows'
                    # else:
                        return response.json()['data']
                
            except requests.exceptions.HTTPError:
                return f"HTTP Error: {response.status_code}"
            
    
    def get_param_values(self, field):
        if field in self.commodity_list:
            return requests.get(f'{self.url}/get_param_values/?key={self.key}&param={field}').json()[field]
        else:
            return 'Invalid Field!'

def create_mongo_year_list(start_year):
    data = USDA_API(settings.usda_key)

    mongo_year_list = []
    for i in data.get_param_values('year'):
        if int(i) >= start_year:
            mongo_year_list.append(int(i))
    return mongo_year_list
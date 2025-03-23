#API Extraction into MongoDB
''' List of dependencies used in data extraction and ingestion into MongoDB '''
import requests
import pandas as pd
import os
from pymongo.mongo_client import MongoClient
import pymongo
import re
import sys
import time
import random

'''Adds in settings from the config file to be able to have cleaner code.'''
config_path = os.getcwd().replace('\\scripts','')
sys.path.insert(0, config_path)

from config import settings
'''
USDA_API

USDA_API class built from http://quickstats.nass.usda.gov/api
- functions are created from the functionality of the API.
 
- add_params will append arguments to API call.
- return_params will return the parameters that have been added for the call.
- return_call will return the entire url that get command uses to retrieve information
- remove_params will remove parameters from the api arguments list.
- call makes a request to the API which returns as a JSON object, handles errors including api throttling issues.
- get_param_values will list all possible values for any specific filter to be added onto API
'''



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
                    get_counts = self.session.get(f'{self.url}/get_counts/?key={self.key}{self.params}').json()
            
                    if get_counts['count'] >= 50000:
                        return f'Unable to Process Request. Request is greater than 50000 rows'
                    else:
                        return response.json()['data']
                
            except requests.exceptions.HTTPError:
                return f"HTTP Error: {response.status_code}"
            
    
    def get_param_values(self, field):
        if field in self.commodity_list:
            return requests.get(f'{self.url}/get_param_values/?key={self.key}&param={field}').json()[field]
        else:
            return 'Invalid Field!'


'''
MongoDB

- MongoDB Class created to direct JSON responses into USDA Cluster.
- USDA Cluster is free tier, with max 528 mb storage.
- test_connectivity will ping the cluster to ensure there's a proper connection.
- initialize will initialize a new collection in MongoDB, with the name of the specific commodity desc
- add_new_col will add new collection to the database if it doesn't already exist within the database.
- add_record will add a new document into the current collection.

'''

class MongoDB():
    def __init__(self, username, password, default_clusterName, default_db='', default_col='', schema={}):
        self.username = username
        self.password = password
        self.cluster_name = default_clusterName
        self.db_name = default_db
        self.col_name = default_col
        self.schema = schema
        self.client = MongoClient(settings.mongo_client)
        
    def test_connectivity(self):
        try:
            self.client.admin.command('ping')
            return '1'
        except Exception as e:
            print(e)

    def initialize(self):
        try:
            self.client[self.db_name][self.col_name].insert_one(self.schema)
        except:
            return 'MongoDB already initialized'

    def drop_col(self, name):
        if name in self.client[self.db_name].list_collection_names():
            self.client[self.db_name][name].drop()
        else:
            return 'collection does not exist'

    def add_new_col(self, col_name):
        if col_name not in self.client[self.db_name].list_collection_names():
            client = self.client[self.db_name][col_name]
            client.insert_one(self.schema)
        else:
            return 'collection already exists'
            
    def add_record(self, data, col):
        self.client[self.db_name][col].insert_many(data)

def create_mongo_year_list(start_year):
    data = USDA_API(settings.usda_key)

    mongo_year_list = []
    for i in data.get_param_values('year'):
        if int(i) >= start_year:
            mongo_year_list.append(int(i))
    return mongo_year_list

def drop_db_if_exists(db_name):
    if db_name in MongoClient(settings.mongo_client).list_database_names():
        MongoClient(settings.mongo_client).drop_database(db_name)
    else:
        return 0
    
drop_db_if_exists(settings.mongo_default_db)

mongo_instance = MongoDB(username= settings.mongo_username,
                         password= settings.mongo_password, \
                         default_db = settings.mongo_default_db, \
                         default_col = settings.mongo_default_colname, \
                         default_clusterName= settings.mongo_default_clusterName, \
                         schema = settings.mongo_default_schema)

mongo_instance.test_connectivity()
mongo_instance.initialize()
'''
Populate_nosql()

- Function that dynamically creates new collections and adds in the relevant records to those collections.
- uses a session object so as to not generate new TCP connections to the database, effectively handling API Throttling.

'''

def populate_nosql():
    data = USDA_API(settings.usda_key)
    data.add_params('state_alpha','US')

    for commodity_desc in data.get_param_values('commodity_desc'):
        for year in create_mongo_year_list(2015):
            try:
                col_title = re.sub(r"[ ,&()]","", commodity_desc).replace(" ", "_")
                data.add_params('commodity_desc', commodity_desc)
                data.add_params('year', year)

                current_doc = data.call()
                connection = mongo_instance.test_connectivity()

                if  mongo_instance.test_connectivity() == '1' and type(current_doc) != str:
                    mongo_instance.add_new_col(col_title)
                    mongo_instance.add_record(current_doc, col_title)
                    mongo_instance.drop_col(settings.mongo_default_colname)

                data.remove_params('commodity_desc')
                data.remove_params('year')
            except Exception as e:
                print(f"Error {e}, {data.call()}")
                data.remove_params('commodity_desc')
                data.remove_params('year')       

populate_nosql()
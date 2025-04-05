'''
MongoDB

- MongoDB Class created to direct JSON responses into USDA Cluster.
- USDA Cluster is free tier, with max 528 mb storage.
- test_connectivity will ping the cluster to ensure there's a proper connection.
- initialize will initialize a new collection in MongoDB, with the name of the specific commodity desc
- add_new_col will add new collection to the database if it doesn't already exist within the database.
- add_record will add a new document into the current collection.

'''

from pymongo.mongo_client import MongoClient
import os
import sys

config_path = os.getcwd().replace('\\scripts','')
sys.path.insert(0, config_path)

from config import settings 

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


def drop_db_if_exists(db_name):
    if db_name in MongoClient(settings.mongo_client).list_database_names():
        MongoClient(settings.mongo_client).drop_database(db_name)
    else:
        return 0
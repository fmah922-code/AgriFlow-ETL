'''
Extract.py

- Migrates data from the USDA API on https://quickstats.nass.usda.gov/api to Mongo database
- Retrieves the last 10 years worth of data, from 2015 to 2025 and creates collections for each commodity (i.e Broccoli, Blueberries, Rice, etc...)
- If adding in collection is a problem, for explainable reasons, it won't move a specific subset of usda api to a mongo db.
'''


import re
import os
import sys

sys.path.append(os.getcwd().replace('\\etl_components',''))

from config import settings

sys.path.append(os.getcwd().replace('\\etl_components',''))

from scripts import Mongo_DB as mdb
from scripts import USDA_API as usda   

def initialize_mongo():
    mongo_instance = mdb.MongoDB(username= settings.mongo_username,
                         password= settings.mongo_password, \
                         default_db = settings.mongo_default_db, \
                         default_col = settings.mongo_default_colname, \
                         default_clusterName= settings.mongo_default_clusterName, \
                         schema = settings.mongo_default_schema)

    mongo_instance.test_connectivity()
    mongo_instance.initialize()

    return mongo_instance

def drop_db_if_exists(mongo_conn, db_name):
    if db_name in mongo_conn.client.list_database_names():
        mongo_conn.client.drop_database(db_name)
        print('Database dropped!')
    else:
        print('Database not found!')

def extract(mongo_conn):
    data = usda.USDA_API(settings.usda_key)
    data.add_params('state_alpha','US')

    for commodity_desc in data.get_param_values('commodity_desc'):
        for year in usda.create_mongo_year_list(2015):
            try:
                col_title = re.sub(r"[ ,&()]","", commodity_desc).replace(" ", "_")
                data.add_params('commodity_desc', commodity_desc)
                data.add_params('year', year)

                current_doc = data.call()

                if  type(current_doc) != str:
                    mongo_conn.add_new_col(col_title)
                    mongo_conn.add_record(current_doc, col_title)
                    mongo_conn.drop_col(settings.mongo_default_colname)

                data.remove_params('commodity_desc')
                data.remove_params('year')
            except Exception as e:
                print(f"Error {e}, {data.call()}")
                data.remove_params('commodity_desc')
                data.remove_params('year')     

    return 1
'''
AgriFlow.py

ETL pipeline which uses the etl_components and scripts directories to load data from USDA_API to finally the PostgresQL DB.

- Steps (from start to finish)
    - Initialize Mongo Database in MongoDB using personal AWS cluster purposefully started beforehand.
    - MongoDB collections data is brought into psypark dataframes for transformations, (filtering, defining schema, NULL handling) which is then converted into panda dataframes.
    - Panda dataframes are then unioned together to create a single dataframe.
    - Using library PSYCOG2, defined the schema, and dropped the existing landing table, and using the execute_values function to bulk insert dataframe rows into PostgresQL db.
'''

'''
Important Notes: 
- The get_counts function sometimes fails silently, causing the ETL process to run indefinitely while pulling data from the USDA API into MongoDB.
    - To fix this, try commenting out lines 70-74 in USDA_API.py (in .scripts). This function uses requests.get, with the endpoint URL including get_counts.
'''

'''
Future updates

- Using docker to containerize applications.
- Using apache airflow to schedule etl.
- Using DBT to ensure data integrity.
'''

import os
import psycopg2
import sys
import time

sys.path.append(os.path.join(os.getcwd(), 'etl_components'))

from etl_components import extract as e, \
                           transform as t, \
                           load as l

mongo_instance = e.initialize_mongo()
e.drop_db_if_exists(mongo_instance, 'USDA')

print('Initializing mongo instance!')
e.extract(mongo_instance)
print('Data extracted from QuickStats API')
time.sleep(5)

spark_rdd_list = t.transform(mongo_instance)
print('Data transformed successfully!')

rdd = t.union_rdd_list(spark_rdd_list)
print('Data unioned to singular df succesfully!')

conn, cur = l.return_connection_details()
conn.autocommit=True

with conn.cursor() as cursor:
    l.create_staging_table(cursor)
print('Schema created for landing table!')

l.populate_data(rdd, 'ag.CropPrices', conn=conn)
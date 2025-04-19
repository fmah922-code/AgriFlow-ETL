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

l.populate_data(rdd,'ag.CropPrices', conn, cur)
print('PostgreSQL successfully populated!')
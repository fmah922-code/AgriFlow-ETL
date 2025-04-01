'''
Retrieves collection names and inserts the pyspark dataframe into postgresql which will then be automated by Apache Airflow.
'''

import sys
import os
from pymongo.mongo_client import MongoClient

import data_transform

config_path = os.getcwd().replace('\\scripts','')
sys.path.insert(0, config_path)

from config import settings 

for collection in data_transform.spark_rdd_list:
    data_transform.spark_rdd_list[collection].write.format("jdbc")\
        .mode('overwrite') \
        .option("url", "jdbc:postgresql://localhost:5432/USDA_DB") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", f"ag.{collection.replace('-','_')}") \
        .option("user", f"{settings.pgadmin_user}").option("password", f"{settings.pgadmin_password}") \
        .save()
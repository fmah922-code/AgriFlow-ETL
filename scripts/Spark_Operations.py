import os
import sys
from pyspark.sql import SparkSession
from pymongo.mongo_client import MongoClient

config_path = os.getcwd().replace('\\scripts','')
sys.path.insert(0, config_path)

from config import settings 

spark = SparkSession.builder \
        .config("spark.jars", "C:\jdbc\postgresql-42.7.5.jar") \
        .getOrCreate()


mongo_conn = MongoClient(settings.mongo_client)[settings.mongo_default_db]

def stringify_id(row):
    return str(row['_id'])

def drop_first_row(collection):
    altered_col = collection.iloc[1:]
    return altered_col 
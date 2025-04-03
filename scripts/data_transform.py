'''
Transformation 1: Removing any collections from MongoDB that have ambiguous names. 
Transformation 2: Dropping off the first row which was a dummy null row
Transformation 3: Conversion collection columns to more appropriate datatypes. (not all strings)
'''

import sys
import os
from pymongo.mongo_client import MongoClient
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType, DateType, FloatType, DecimalType
import numpy as np

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

spark_rdd_list = {}
def populate_spark_rdd_list():
    for collection in mongo_conn.list_collection_names():
        test_df = drop_first_row(pd.DataFrame([item for item in mongo_conn.get_collection(collection).find()]))

        #Converts id column to a string to make it easier to be created as a pyspark rdd.
        test_df['id'] = test_df.apply(stringify_id, axis=1)
        test_df = test_df.drop('_id',axis=1)

        #Moves id to the front of the dataframe
        id = test_df['id']
        test_df.drop(labels=['id'], axis=1,inplace=True)
        test_df.insert(0, 'id', id)


        spark_df = spark.createDataFrame(test_df)

        spark_df = spark_df.where(~col('Value').like('%(%'))
        spark_df = spark_df.where(~col('CV (%)').like('%(%'))

        spark_df = spark_df.withColumn("Value",
                                spark_df['Value']
                                .cast('float')) \
                            .withColumn("CV (%)",
                                spark_df['CV (%)']
                                .cast('float')) \
                            .withColumn('year',
                               spark_df['year']
                               .cast('int')) \
                            .withColumn('zip_5',
                               spark_df['zip_5'] \
                               .cast('int')) \
                            .withColumn('load_time',
                               spark_df['load_time'] \
                               .cast('date')) \
                            

        spark_rdd_list[collection] = spark_df

populate_spark_rdd_list()
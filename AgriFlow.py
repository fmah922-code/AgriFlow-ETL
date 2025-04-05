'''
AgriFlow ETL Pipeline

    - Pipeline which has multiple steps
        - loads api responses into MongoDB.
        - Coverts MongoDB collections into pyspark dataframes where schema is established
        - utilizing a .jar file which allows data ingestion into PostgreSQL database USDA.

    - Notes:
        - limiting down with state_alpha = 'US'. Individual states wouldn't fit into 
        MongoDB Cluster.
'''


import re
import os
import pandas as pd
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pymongo.mongo_client import MongoClient

from scripts import USDA_API as usda
from scripts import Mongo_DB as mdb
from scripts import Spark_Operations as so

from config import settings

mdb.drop_db_if_exists(settings.mongo_default_db)

def extract():
    mongo_instance = mdb.MongoDB(username= settings.mongo_username,
                         password= settings.mongo_password, \
                         default_db = settings.mongo_default_db, \
                         default_col = settings.mongo_default_colname, \
                         default_clusterName= settings.mongo_default_clusterName, \
                         schema = settings.mongo_default_schema)

    mongo_instance.test_connectivity()
    mongo_instance.initialize()

    data = usda.USDA_API(settings.usda_key)
    data.add_params('state_alpha','US')

    for commodity_desc in data.get_param_values('commodity_desc'):
        for year in usda.create_mongo_year_list(2015):
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

    return 1
def transform_and_load(_int):
    if _int == 1:
        spark = SparkSession.builder \
            .config("spark.jars", os.getcwd() + '\\' + 'postgresql-42.7.5.jar') \
            .getOrCreate()


        mongo_conn = MongoClient(settings.mongo_client)[settings.mongo_default_db]

        spark_rdd_list = {}
        for collection in mongo_conn.list_collection_names():
            if collection not in settings.excluded_commodities:
                test_df = so.drop_first_row(pd.DataFrame([item for item in mongo_conn.get_collection(collection).find()]))

                #Converts id column to a string to make it easier to be created as a pyspark rdd.
                test_df['id'] = test_df.apply(so.stringify_id, axis=1)
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
        
        for collection in spark_rdd_list:
            spark_rdd_list[collection].write.format("jdbc")\
            .mode('overwrite') \
            .option("url", "jdbc:postgresql://localhost:5432/USDA_DB") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", f"ag.{collection.replace('-','_')}") \
            .option("user", f"{settings.pgadmin_user}").option("password", f"{settings.pgadmin_password}") \
            .save()

        spark.close()

    else:
        print('Error: Upstream Task Failed!')


e = extract()
if e == 1:
    print('Data Extraction Completed!')
    transform_and_load(e)
    print('Data Transformation and Loading Completed!')
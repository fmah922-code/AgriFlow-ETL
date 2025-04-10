import os
import sys
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

sys.path.append(os.getcwd().replace('\\etl_components',''))

from config import settings
from scripts import Spark_Operations as so

sys.path.append(os.getcwd().replace('\\scripts',''))

from config import settings



def transform(mongo_conn):
    spark = SparkSession.builder \
            .config("spark.jars", os.getcwd() + '\\connections\\' + 'postgresql-42.7.5.jar') \
            .config("spark.network.timeout", "120s") \
            .config("spark.sql.debug.maxToStringFields", 25) \
            .config('spark.driver.memory', '8g') \
            .config("spark.network.timeout", "300s") \
            .config("spark.executor.heartbeatInterval", "60s") \
            .getOrCreate()
        
    col_list = mongo_conn.client[settings.mongo_default_db].list_collection_names()
    
    spark_rdd_list = []
    for collection in col_list:
        if collection not in settings.excluded_commodities:
            try: 
                if(mongo_conn.client.admin.command('ping')['ok'] == 1):

                    collection_data = list(mongo_conn.client[settings.mongo_default_db][collection].find())
                    
                    test_df = pd.DataFrame(collection_data)
                    test_df = so.drop_first_row(test_df)

                    test_df['id'] = test_df.apply(so.stringify_id, axis=1)
                    test_df.drop('_id', axis=1, inplace=True)

                    id_col = test_df.pop('id')
                    test_df.insert(0, 'id', id_col)

                    spark_df = spark.createDataFrame(test_df)

                    spark_df = spark_df.where(~col('Value').like('%(%')) \
                                       .where(~col('CV (%)').like('%(%')) \
                                       .withColumn("Value", col("Value").cast("float")) \
                                       .withColumn("CV (%)", col("CV (%)").cast("float")) \
                                       .withColumn("year", col("year").cast("int")) \
                                       .withColumn("zip_5", col("zip_5").cast("int"))

                    spark_rdd_list.append(spark_df)
            except:
                print('Collection not added: ' + str(collection))
                continue
    return spark_rdd_list

import os
import sys

sys.path.append(os.getcwd().replace('\\etl_components',''))

from config import settings


def load(spark_rdd_list):
    base_df = spark_rdd_list[0]

    i = 1
    while i < len(spark_rdd_list):
        base_df = base_df.union(spark_rdd_list[i])
        i = i + 1

    base_df.write.format("jdbc")\
        .mode('overwrite') \
        .option("url", "jdbc:postgresql://localhost:5432/USDA_DB?tcpKeepAlive=true") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "ag.CropPrices") \
        .option("user", f"{settings.pgadmin_user}").option("password", f"{settings.pgadmin_password}") \
        .save()
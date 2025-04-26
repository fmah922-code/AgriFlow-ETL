# AgriFlow ETL Pipeline

## Overview
AgriFlow is a fully Dockerized ETL pipeline built to streamline the extraction, transformation, and loading of USDA data. By leveraging MongoDB, PySpark, Pandas, and PostgreSQL, the pipeline ensures an efficient and reliable flow of data from the USDA API into a centralized database, ready for analysis. It relies on the organized structure of the `etl_components` and `scripts` directories to tie all stages of the process together seamlessly.

https://hub.docker.com/layers/fmahmud922/agriflow/latest/images/sha256-e2563b2ca293120246653ece9e15c9e4ab646c9949c98b17d2a22702d02876c2

## Workflow
1. **MongoDB Initialization**: The process begins with setting up a MongoDB instance, hosted in a personal AWS cluster.
2. **Data Transformation**: Raw data from MongoDB collections is imported into PySpark for cleaning, schema definition, and NULL value handling. The data is then converted into Pandas dataframes for further manipulation.
3. **Dataframe Union**: All Pandas dataframes are combined into a single, unified dataframe to ensure consistency and simplify loading.
4. **Database Loading**: The unified dataframe is prepared for PostgreSQL by defining the schema, clearing any existing landing tables, and using the `execute_values` function for efficient bulk insertion.

---

## Instructions  

**1.** Ensure all environment variables are correctly configured in a `.env` file within the project root directory before running the pipeline.    

```env
# USDA API Key  
usda_key=XXXXXXX  

# MongoDB Credentials  
mongo_username=XXXXXXX  
mongo_password=XXXXXXX  
mongo_default_clusterName = XXXXXXXX

# MongoDB Connection URI  
mongo_client=XXXXXXXX  

# Mongo Cluster Name
mongo_default_clusterName=XXXXXXXX

# PostgreSQL (pgAdmin4) Login Credentials  
pgadmin_user=XXXXXXX  
pgadmin_password=XXXXXXX

#Must define landing db, table, and schema names to be dumped in PostgreSQL.
landing_db=XXXXXX
landing_table=XXXXXXX (with format [table_schema].[table_name])
```

**2.** Have Docker Engine installed on your local machine with this link, and run the following commands in the command line. \
https://docs.docker.com/engine/install/
```
#Run this code first
docker pull fmahmud922/agriflow:lts 

#Run this afterwards.
docker run fmahmud922/agriflow:lts
```


## Important Notes
- **Silent Issue in `get_counts`**: Occasionally, the `get_counts` function, responsible for fetching data from the USDA API, may fail silently and cause the process to stall indefinitely.  
  - **Temporary Fix**: To avoid this, comment out lines 70–74 in `USDA_API.py` located in the `scripts` directory.

---

## Future Updates
Looking ahead, the following enhancements are planned for AgriFlow:
- **Apache Airflow**: Integrating Airflow to automate and schedule ETL workflows.
- **DBT Integration**: Incorporating DBT to reinforce data consistency and maintain high data quality standards.

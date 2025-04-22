# AgriFlow ETL Pipeline

## Overview
AgriFlow is a fully Dockerized ETL pipeline built to streamline the extraction, transformation, and loading of USDA data. By leveraging MongoDB, PySpark, Pandas, and PostgreSQL, the pipeline ensures an efficient and reliable flow of data from the USDA API into a centralized database, ready for analysis. It relies on the organized structure of the `etl_components` and `scripts` directories to tie all stages of the process together seamlessly.

https://hub.docker.com/layers/fmahmud922/agriflow/v1/images/sha256-83ac80bde02b4895da2d9e21b11270f5b8a13988829935799c3c3c8619254d51

## Workflow
1. **MongoDB Initialization**: The process begins with setting up a MongoDB instance, hosted in a personal AWS cluster.
2. **Data Transformation**: Raw data from MongoDB collections is imported into PySpark for cleaning, schema definition, and NULL value handling. The data is then converted into Pandas dataframes for further manipulation.
3. **Dataframe Union**: All Pandas dataframes are combined into a single, unified dataframe to ensure consistency and simplify loading.
4. **Database Loading**: The unified dataframe is prepared for PostgreSQL by defining the schema, clearing any existing landing tables, and using the `execute_values` function for efficient bulk insertion.

---

## Instructions  

Ensure all environment variables are correctly configured in a `.env` file within the project root directory before running the pipeline.    

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
```


## Important Notes
- **Silent Issue in `get_counts`**: Occasionally, the `get_counts` function, responsible for fetching data from the USDA API, may fail silently and cause the process to stall indefinitely.  
  - **Temporary Fix**: To avoid this, comment out lines 70–74 in `USDA_API.py` located in the `scripts` directory.

---

## Future Updates
Looking ahead, the following enhancements are planned for AgriFlow:
- **Apache Airflow**: Integrating Airflow to automate and schedule ETL workflows.
- **DBT Integration**: Incorporating DBT to reinforce data consistency and maintain high data quality standards.

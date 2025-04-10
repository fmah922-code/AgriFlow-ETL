import os
import papermill as pm
import datetime
from airflow.decorators import dag, tasks

# from extract import initialize_mongo, drop_db_if_exists, extract
# from transform import transform
# from load import load

# mongo_instance = initialize_mongo()
# drop_db_if_exists(mongo_instance, 'USDA')
# extract(mongo_instance)
# spark_rdd_list = transform(mongo_instance)
# load(spark_rdd_list)


from etl_components import extract as e, \
                           transform as t, \
                           load as l

@dag(schedule_interval='@monthly',
     start_date=datetime(2025,4,10),
     tags='AgriFlow')

def agriflow_etl_pipeline():
    @task
    def setup_mongo_task():
        mongo_instance = e.initialize_mongo()
        e.drop_db_if_exists(mongo_instance, 'USDA')
        return mongo_instance

    @task
    def extract_task(mongo_instance):
        e.extract(mongo_instance)
        return mongo_instance

    @task
    def transform_and_load(mongo_instance):
        """
        Executes the AgriFlow Pipeline using papermill.
        """
        notebook_path = os.path.join(os.getcwd(), 'etl_components', 'agriflow_pipeline.ipynb')  
        output_path = os.path.join(os.getcwd(), 'results', 'run_output.ipynb')

        print("Starting execution of Jupyter Notebook...")
        pm.execute_notebook(
            notebook_path,
            output_path,
            kernel_name='python3'
        )

        print("Agriflow executed successfully!")

    mongo_instance = setup_mongo_task()
    mongo_instance = extract_task(mongo_instance)
    transform_and_load(mongo_instance)

agriflow_etl_pipeline()


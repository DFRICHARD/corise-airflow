from datetime import datetime
from typing import List
from zipfile import ZipFile

import pandas as pd
from airflow.decorators import dag, task # DAG and task decorators for interfacing with the TaskFlow API
from airflow.providers.google.cloud.hooks.gcs import GCSHook # GCSHook for interfacing with GCS

@dag(
    dag_id="energy_dataset_dag",
    schedule_interval="@daily",
    start_date=datetime(2021, 2, 12),
    catchup=False,
    default_args={
        "owner": "Dze Richard",
        "retries": 2, # If a task fails, it will retry 2 times.
    },
    tags=['corise']) # If set, this tag is shown in the DAG view of the Airflow UI
def energy_dataset_dag():
    """
    ### Basic ETL Dag
    This is a simple ETL data pipeline example that demonstrates the use of
    the TaskFlow API using two simple tasks to extract data from a zipped folder
    and load it to GCS.

    """

    @task
    def extract() -> List[pd.DataFrame]:
        """
        #### Extract task
        A simple task that loads each file in the zipped file into a dataframe,
        building a list of dataframes that is returned.

        """
        # Extract csv files from zip and return a list of dataframes
        with ZipFile("dags/data/energy-consumption-generation-prices-and-weather.zip", 'r') as energy_data_zip:
            # Get a list of all archived file names from the zip
            filenames_list = energy_data_zip.namelist()
            # Iterate over the file names
            dataframes = []
            for file_name in filenames_list:
                # Check filename endswith csv
                if file_name.endswith('.csv'):
                    # Extract a single file from zip
                    dataframes.append(pd.read_csv(energy_data_zip.open(file_name)))
            print(dataframes)
        return dataframes

        
    @task
    def load(unzip_result: List[pd.DataFrame]):
        """
        #### Load task
        A simple "load" task that takes in the result of the "transform" task, prints out the 
        schema, and then writes the data into GCS as parquet files.
        """

        data_types = ['generation', 'weather']

        # GCSHook uses google_cloud_default connection by default, so we can easily create a GCS client using it
        # https://github.com/apache/airflow/blob/207f65b542a8aa212f04a9d252762643cfd67a74/airflow/providers/google/cloud/hooks/gcs.py#L133

        # The google cloud storage github repo has a helpful example for writing from pandas to GCS:
        # https://github.com/googleapis/python-storage/blob/main/samples/snippets/storage_fileio_pandas.py
        
        client = GCSHook()
        bucket_name = "corise-airflow-dfr"
        for data_type, df in zip(data_types, unzip_result):
            # Write the dataframe to GCS as a parquet file
            client.upload(bucket_name= bucket_name, object_name=data_type, data=df.to_parquet())
            # Print the schema of the dataframe
            print(df.dtypes)
            print(f"The {data_type} dataframe was successfully written to the {bucket_name} bucket in GCS")
  

    # Set extract and load tasks to run in parallel
    load(extract())


energy_dataset_dag = energy_dataset_dag()
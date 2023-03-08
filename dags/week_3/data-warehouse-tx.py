from datetime import datetime, timedelta
from typing import List

import pandas as pd
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator

from common.week_3.config import DATA_TYPES, NORMALIZED_COLUMNS


PROJECT_ID = "airflow-week"
DESTINATION_BUCKET = 'corise-airflow-dfr'
BQ_DATASET_NAME = "energy_data"

default_args={"owner": "Dze Richard",
              "retries": 2, # If a task fails, it will retry 2 times. 
              "retry_delay": timedelta(seconds=60)                       
              }

@dag(
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args=default_args
    )
def data_warehouse_transform_dag():
    """
    ### Data Warehouse Transform DAG
    This DAG performs four operations:
        1. Extracts zip file into two dataframes
        2. Loads these dataframes into parquet files on GCS, with valid column names
        3. Builds external tables on top of these parquet files
        4. Builds normalized views on top of the external tables
        5. Builds a joined view on top of the normalized views, joined on time
    """


    @task
    def extract() -> List[pd.DataFrame]:
        """
        #### Extract task
        A simple task that loads each file in the zipped file into a dataframe,
        building a list of dataframes that is returned


        """
        from zipfile import ZipFile
        filename = "/usr/local/airflow/dags/data/energy-consumption-generation-prices-and-weather.zip"
        dfs = [pd.read_csv(ZipFile(filename).open(i)) for i in ZipFile(filename).namelist()]
        return dfs


    @task
    def load(unzip_result: List[pd.DataFrame]):
        """
        #### Load task
        A simple "load" task that takes in the result of the "extract" task, formats
        columns to be BigQuery-compliant, and writes data to GCS.

        Args:
            unzip_result (List[pd.DataFrame]): A list of dataframes, one for each file in the zip file
        """

        from airflow.providers.google.cloud.hooks.gcs import GCSHook
        
        client = GCSHook().get_conn()       
        bucket = client.get_bucket(DESTINATION_BUCKET)

        for index, df in enumerate(unzip_result):
            df.columns = df.columns.str.replace(" ", "_")
            df.columns = df.columns.str.replace("/", "_")
            df.columns = df.columns.str.replace("-", "_")
            bucket.blob(f"week-3/{DATA_TYPES[index]}.parquet").upload_from_string(df.to_parquet(), "text/parquet")
            print(df.dtypes)

    @task_group
    def create_bigquery_dataset():
        '''
        #### Create BigQuery Dataset
        This task group creates a BigQuery dataset if one does not already exist
        '''
        
        from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
        
        BigQueryCreateEmptyDatasetOperator(dataset_id=BQ_DATASET_NAME, 
                                           project_id=PROJECT_ID,
                                           location="us-east1",
                                           task_id="create_bigquery_dataset_if_not_exists",
                                           if_exists="ignore")
        print(f"---- Successfully created `{BQ_DATASET_NAME}` ----")
 

    @task_group
    def create_external_tables():
        '''
        #### Create External Tables
        This task group creates external tables on top of the parquet files stored in GCS
        '''
        from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
        from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator

        # Produce two external tables, one for each data type, referencing the data stored in GCS
        # When using the BigQueryCreateExternalTableOperator, it's suggested you use the table_resource
        # field to specify DDL configuration parameters. If you don't, then you will see an error
        # related to the built table_resource specifying csvOptions even though the desired format is 
        # PARQUET.

        for data_type in DATA_TYPES:
            try:
                BigQueryCreateExternalTableOperator(
                    task_id=f"create_external_table_for_{data_type}",
                    table_resource={
                        "type": "EXTERNAL",
                        "sourceFormat": "PARQUET",
                        "sourceUris": [
                            f"gs://{DESTINATION_BUCKET}/week-3/{data_type}.parquet"
                        ],
                    },
                    dataset_id=BQ_DATASET_NAME,
                    table_id=f"{data_type}_external",
                    project_id=PROJECT_ID,
                    location="us-east1",
                    description=f"External table for {data_type} data")
                
                print(f"---- Successfully created external table for {data_type} ----")
            
            except Exception as e:
                print(f"---- Failed to create external table for {data_type} ----")
                print(e)
                pass


    def produce_select_statement(timestamp_column: str, columns: List[str]) -> str:
        """
        #### Produce Select Statement
        This function produces a select statement that can be used to create a normalized view
        on top of an external table. The function accepts the timestamp column, and a list of
        columns to select. It then programmatically builds a select statement that can be used
        to create a normalized view.

        Args:
            timestamp_column (str): The name of the timestamp column
            columns (List[str]): A list of columns to select

        Returns:
            str: A select statement that can be used to create a normalized view
        """
        query = f"SELECT CAST({timestamp_column} AS TIMESTAMP) as {timestamp_column}, {', '.join(columns)}"
        
        return query

    @task_group
    def produce_normalized_views():
        '''
        #### Produce Normalized Views
        This task group produces normalized views on top of the external tables
          
        The normalized views are created using the produce_select_statement function, which
        accepts the timestamp column and a list of columns to select. The produce_select_statement
        function then programmatically builds a select statement that can be used to create
        a normalized view.
        '''
        
        from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator

        for data_type in DATA_TYPES:
            view_query = produce_select_statement(timestamp_column=NORMALIZED_COLUMNS[data_type]["time"],
                                         columns=NORMALIZED_COLUMNS[data_type]["columns"])
            try:
                BigQueryCreateEmptyTableOperator(
                    task_id=f"create_normalized_view_for_{data_type}",
                    dataset_id=BQ_DATASET_NAME,
                    table_id=f"{data_type}_normalized_view",
                    project_id=PROJECT_ID,
                    location="us-east1",
                    description=f"Normalized view for {data_type} data",
                    view={
                        "query": view_query ,
                        "useLegacySql": False
                    })
                
                print(f"---- Successfully created normalized view for {data_type} ----")
            
            except Exception as e:
                print(f"---- Failed to create normalized view for {data_type} ----")
                print(e)
                pass

    @task_group
    def produce_joined_view():
        '''
        #### Produce Joined View
        This task group produces a joined view on top of the normalized views
        '''
        from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
        
        # Produce a view that joins the two normalized views on time
        joined_view_query = f"""
                                SELECT g.*, w.* 
                                FROM `{PROJECT_ID}.{BQ_DATASET_NAME}.{DATA_TYPES[0]}_normalized_view` AS g
                                JOIN `{PROJECT_ID}.{BQ_DATASET_NAME}.{DATA_TYPES[1]}_normalized_view` AS w
                                ON g.time = w.dt_iso
                                """
        
        joined_view_name = f"{DATA_TYPES[0]}_{DATA_TYPES[1]}_joined_view"

        try:
            BigQueryCreateEmptyTableOperator(
                task_id=f"create_joined_view",
                dataset_id=BQ_DATASET_NAME,
                table_id= joined_view_name,
                project_id=PROJECT_ID,
                location="us-east1",
                description=f"Joined view for {DATA_TYPES[0]} and {DATA_TYPES[1]} data",
                view={
                    "query": joined_view_query ,
                    "useLegacySql": False
                })
            
            print(f"---- Successfully created joined view ----")

        except Exception as e:
            print(f"---- Failed to create joined view ----")
            print(e)
            pass


    unzip_task = extract()
    load_task = load(unzip_task)
    create_bigquery_dataset_task = create_bigquery_dataset()
    load_task >> create_bigquery_dataset_task
    external_table_task = create_external_tables()
    create_bigquery_dataset_task >> external_table_task
    normal_view_task = produce_normalized_views()
    external_table_task >> normal_view_task
    joined_view_task = produce_joined_view()
    normal_view_task >> joined_view_task


data_warehouse_transform_dag = data_warehouse_transform_dag()
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Union

import numpy as np
import pandas as pd
import xgboost as xgb
from airflow.operators.empty import EmptyOperator
from airflow.models.dag import DAG
from airflow.decorators import task, task_group

from common.week_2.model import multivariate_data, train_xgboost

from common.week_2.feature_engineering import join_data_and_add_features

TRAINING_DATA_PATH = 'week-2/price_prediction_training_data.csv'
DATASET_NORM_WRITE_BUCKET = 'corise-airflow-dfr'
VAL_END_INDEX = 31056


@task
def read_dataset_norm():
    """
    Read dataset norm from storage

    Returns:
        np.ndarray: Normalized dataset
    """

    from airflow.providers.google.cloud.hooks.gcs import GCSHook
    import io
    client = GCSHook().get_conn()
    read_bucket = client.bucket(DATASET_NORM_WRITE_BUCKET)
    dataset_norm = pd.read_csv(io.BytesIO(read_bucket.blob(TRAINING_DATA_PATH).download_as_bytes())).to_numpy()

    return dataset_norm


@task
def produce_indices(num_pairs=10) -> List[Tuple[np.ndarray, np.ndarray]]:
    """
    Produce zipped list of training and validation indices

    The number of pairs produced here will be equivalent to the number of 
    mapped 'format_data_and_train_model' tasks you have.

    For each indices pairs, we will assign 10% validation and the remaining 90% for training  

    Args:
        num_pairs (int): Number of pairs of (training and validation) indices to produce. By default it's 10

    Returns:
        List[Tuple[np.ndarray, np.ndarray]]: List of tuples, each containing the corresponding training and validation indices

 """
    data_size = VAL_END_INDEX  # Compute size of dataset available for splitting
    val_size = int(VAL_END_INDEX / num_pairs)  # Compute size of validation set
    train_size = data_size - val_size  # Compute size of training set


    # Initialize empty lists to store the training and validation indices
    train_indices = []
    val_indices = []

    # Loop through each fold and generate the corresponding indices
    for fold in range(num_pairs):
        # Compute the start and end indices for the validation set
        val_start = fold * val_size
        val_end = (fold + 1) * val_size
        
        # Generate the validation indices
        val_indices.append(np.arange(val_start, val_end))
        
        # Generate the training indices
        train_indices.append(np.concatenate([np.arange(0, val_start), np.arange(val_end, data_size)]))

    # Return a list of tuples, each containing the corresponding training and validation indices
    return list(zip(train_indices, val_indices))


@task
def format_data_and_train_model(dataset_norm: np.ndarray,
                                indices: Tuple[np.ndarray, np.ndarray]) -> xgb.Booster:
    """
    Extract training and validation sets and labels, and train a model with a given
    set of training and validation indices

    Args:
        dataset_norm (np.ndarray): Normalized dataset
        indices (Tuple[np.ndarray, np.ndarray]): Tuple containing the training and validation indices

    Returns:
        xgb.Booster: Trained XGBoost model
    """
    past_history = 24
    future_target = 0
    train_indices, val_indices = indices
    print(f"train_indices is {train_indices}, val_indices is {val_indices}")
    X_train, y_train = multivariate_data(dataset_norm, train_indices, past_history, future_target, step=1, single_step=True)
    X_val, y_val = multivariate_data(dataset_norm, val_indices, past_history, 
                                     future_target, step=1, single_step=True)
    model = train_xgboost(X_train, y_train, X_val, y_val)
    print(f"Model eval score is {model.best_score}")

    return model


@task
def select_best_model(models: List[xgb.Booster]):
    """
    Given a list of XGBoost models, select the best model based on the validation score and write this to GCS.

    Args:
        models (List[xgb.Booster]): List of XGBoost models
    
    Returns:
    

    """
    
    if not models:
        raise ValueError("List of models is empty")

    # Initialize the best model to the first model
    best_model = models[0]
    # Initialize the best score to the first model's score
    best_score = models[0].best_score

    for model in models:
        val_score = model.best_score
        if val_score > best_score:
            best_model = model
            best_score = val_score

    print(f'The best model has a validation score of {best_score}')

    best_model.save_model("best_model.json")

    print("Best model saved to local directory")
    
    from airflow.providers.google.cloud.hooks.gcs import GCSHook
    client = GCSHook()
    client.upload(bucket_name=DATASET_NORM_WRITE_BUCKET,
                  object_name="week-2/best_model.json",
                  file_name="best_model.json")

    print("Best model saved to GCS :) ")


@task_group
def train_and_select_best_model():
    """
    Task group responsible for training XGBoost models to predict energy prices, including:
       1. Reading the dataset norm from GCS
       2. Producing a list of training and validation indices numpy array tuples,  
       3. Mapping each element of that list onto the indices argument of format_data_and_train_model
       4. Calling select_best_model on the output of all of the mapped tasks to select the best model and 
          write it to GCS 

    Using different train/val splits, train multiple models and select the one with the best evaluation score.
    """

    past_history = 24
    future_target = 0

    dataset_norm = read_dataset_norm()
    indices = produce_indices()
    trained_models = format_data_and_train_model.partial(dataset_norm = dataset_norm).expand(indices = indices)
    best_model = select_best_model(trained_models)

    return best_model


default_args={"owner": "Dze Richard",
              "retries": 2, # If a task fails, it will retry 2 times. 
              "retry_delay": timedelta(seconds=60)                       
              }

with DAG("energy_price_prediction",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    default_args=default_args,
    tags=['model_training'],
    render_template_as_native_obj=True,
    concurrency=5
    ) as dag:

        group_1 = join_data_and_add_features()
        group_2 = train_and_select_best_model()
        group_1 >> group_2
 
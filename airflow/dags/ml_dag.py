from airflow.decorators import dag, task # pyright: ignore[reportMissingImports]
from datetime import datetime, timedelta
from source.ml.train import train_model
from source.ml.predict import get_predictions
from source.etl.load import load_to_database
import pandas as pd # pyright: ignore[reportMissingModuleSource]
from pathlib import Path
import shutil

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id='ml_dag',
    default_args=default_args,
    description='ML DAG that trains model and loads predictions to database',
    schedule_interval=None,
    start_date=datetime(2026, 1, 18),
    catchup=False
)
def ml_pipeline():
    """
    ML Pipeline DAG for NBA MVP Predictor project.

    This DAG trains the machine learning model using the features dataset and generates player MVP predictions.
    The predictions are then loaded into a PostgreSQL database.

    Steps:
    1. Train the machine learning model using the features dataset.
    2. Generate MVP predictions for players based on the trained model.
    3. Load the predictions into the PostgreSQL database.
    4. Clean up temporary files.
    """

    @task
    def train():
        """Trains the machine learning model."""

        train_model()


    @task
    def predict():
        """
        Generates player MVP predictions and saves them as a parquet file.

        Returns:
            str: Path to the saved predictions parquet file.
        """

        file_path = f'/opt/airflow/data/predictions.parquet'
        predictions = get_predictions()
        predictions.to_parquet(file_path, index=False)

        return file_path


    @task
    def load(file_path: str):
        """
        Loads the player MVP predictions into the PostgreSQL database.

        Params:
            file_path (str): Path to the predictions parquet file.
        """
        
        predictions = pd.read_parquet(file_path)
        load_to_database(predictions, 'player_predictions', 'predictions')


    @task(trigger_rule='all_success')
    def clean_up():
        """
        Remove intermediate parquet files in the data directory after training and prediction.
        Executes only if all upstream tasks succeed, allowing for debugging in case of failures.
        """

        directory = Path('/opt/airflow/data')

        for item in directory.iterdir():
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()


    train_task = train()

    predictions = predict()

    train_task >> predictions

    load(predictions) >> clean_up()


ml_dag = ml_pipeline()
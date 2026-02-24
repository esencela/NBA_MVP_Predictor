from airflow.decorators import dag, task # pyright: ignore[reportMissingImports]
from datetime import datetime, timedelta
from source.ml.train import train_model
import logging
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id='model_training_dag',
    default_args=default_args,
    description='ML DAG that trains model on historic data',
    schedule_interval=None,
    start_date=datetime(2026, 1, 18),
    catchup=False
)
def ml_pipeline():
    """
    ML Pipeline DAG for NBA MVP Predictor project.

    This DAG trains the machine learning model using the features dataset in Postgres.
    """

    logger = logging.getLogger(__name__)

    @task
    def train():
        """Trains the machine learning model."""

        start_time = time.time()

        logger.info('Starting model training')

        train_model()

        logger.info('Model training completed in %.2f seconds', time.time() - start_time)


    train()


ml_dag = ml_pipeline()
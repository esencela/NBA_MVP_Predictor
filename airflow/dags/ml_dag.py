from airflow import DAG
from airflow.operators.python import PythonOperator # pyright: ignore[reportMissingImports]
from datetime import datetime, timedelta
from source.ml.train import train_model
from source.ml.predict import get_predictions
from source.etl.load import load_to_database


def ml_pipeline():
    train_model()
    predictions = get_predictions()
    load_to_database(predictions, 'player_predictions', 'predictions')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

ml_dag = DAG(
    dag_id='ml_dag',
    default_args=default_args,
    description='ML pipeline dag that trains and loads predictions to database',
    schedule_interval=None,
    start_date=datetime(2026, 1, 19),
    catchup=False
)

run_ml = PythonOperator(
    task_id='run_etl_pipeline',
    python_callable=ml_pipeline,
    dag=ml_dag
)
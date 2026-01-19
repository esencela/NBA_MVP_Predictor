from airflow import DAG
from airflow.operators.python import PythonOperator # pyright: ignore[reportMissingImports]
from source.etl.extract import extract_season_data
from source.etl.transform import transform_data
from source.etl.load import load_to_database
from datetime import datetime, timedelta
import pandas as pd # pyright: ignore[reportMissingModuleSource]
from source.config.settings import (
    CURRENT_SEASON,
    MIN_SEASON
)


def etl_pipeline():
    features_list = []
    stats_list = []

    for season in range(MIN_SEASON, CURRENT_SEASON + 1):
        season_data = extract_season_data(season)
        features_data, stats_data = transform_data(season_data.per_game, 
                                                season_data.advanced, 
                                                season_data.team, 
                                                season_data.mvp, 
                                                season)
            
        features_list.append(features_data)
        stats_list.append(stats_data)
            
    df_features = pd.concat(features_list, axis=0).reset_index(drop=True)
    df_stats = pd.concat(stats_list, axis=0).reset_index(drop=True)

    load_to_database(df_features, 'player_features', 'stats')
    load_to_database(df_stats, 'player_stats', 'stats')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

etl_dag = DAG(
    dag_id='etl_dag',
    default_args=default_args,
    description='ETL Dag for NBA MVP Project',
    schedule_interval=None,
    start_date=datetime(2026, 1, 18),
    catchup=False
)

run_etl = PythonOperator(
    task_id='run_etl_pipeline',
    python_callable=etl_pipeline,
    dag=etl_dag
)
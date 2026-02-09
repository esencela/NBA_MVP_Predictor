from airflow.decorators import dag, task # pyright: ignore[reportMissingImports]
from sqlalchemy import text # pyright: ignore[reportMissingImports]
from datetime import datetime, timedelta
from source.etl.extract import extract_season_data
from source.etl.transform import transform_season_data
from source.etl.load import load_to_database
from source.ml.predict import get_predictions
from source.db.utils import remove_season_data, create_serving_view
from source.db.connection import get_engine
import pandas as pd # pyright: ignore[reportMissingModuleSource]
from pathlib import Path
import shutil
import time
from source.config.settings import (
    CURRENT_SEASON
)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}


def update_success(**kwargs):
    
    update_id = kwargs['ti'].xcom_pull(task_ids='start')

    engine = get_engine(user='ml')
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE metadata.update_runs
            SET status = 'success', 
                end_time = NOW()
            WHERE update_id = :update_id;
            """),
            {
                'update_id': update_id
            }
        )


def update_failure(**kwargs):
    
    update_id = kwargs['ti'].xcom_pull(task_ids='start')
    error_message = str(kwargs.get('exception'))[:500] # Truncate error message to limit data usage

    engine = get_engine(user='ml')
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE metadata.update_runs
            SET status = 'failed', 
                end_time = NOW(),
                error_message = :error_message
            WHERE update_id = :update_id;
            """),
            {
                'update_id': update_id,
                'error_message': error_message
            }
        )


@dag(
    dag_id='update_dag',
    default_args=default_args,
    description='Update DAG that updates current season data and predictions',
    schedule_interval='@weekly',
    start_date=datetime(2026, 1, 26),
    catchup=False,
    on_success_callback=update_success,
    on_failure_callback=update_failure
)
def update_pipeline():

    @task
    def start(**kwargs):
        dag_run = kwargs['dag_run']

        trigger_type = 'manual' if dag_run.external_trigger else 'scheduled'

        engine = get_engine(user='ml')

        with engine.begin() as conn:
            result = conn.execute(text("""
                INSERT INTO metadata.update_runs (
                    dag_id,
                    run_id,
                    start_time,
                    status,
                    trigger_type
                )
                VALUES (
                    :dag_id,
                    :run_id,
                    NOW(),
                    'running',
                    :trigger_type
                )
                RETURNING update_id;
            """), 
            {
                'dag_id': kwargs['dag'].dag_id,
                'run_id': kwargs['run_id'],
                'trigger_type': trigger_type
            })

            update_id = result.scalar()
        
        return update_id


    @task(pool='api_pool')
    def extract(season: int) -> dict:
        """
        Extract season data and save as parquet files for transformation.

        This function fetches:
        - Per Game statistics
        - Advanced statistics
        - Team statistics (Eastern and Western Conferences)
        - MVP voting results

        Params:
            season (int): The NBA season year (e.g., 2023 for the 2022-2023 season).

        Returns:
            dict: Paths to the saved parquet files for each dataset and season.
                {
                    'per_game': str,
                    'advanced': str,
                    'team': {
                        'east': str,
                        'west': str
                    },
                    'mvp': str,
                    'season': int
                }
        """

        data = extract_season_data(season)

        df_per_game = data['per_game']
        df_advanced = data['advanced']
        df_east = data['team']['east']
        df_west = data['team']['west']
        df_mvp = data['mvp']

        per_game_path = f'/opt/airflow/data/per_game_{season}.parquet'
        advanced_path = f'/opt/airflow/data/advanced_{season}.parquet'
        east_path = f'/opt/airflow/data/east_{season}.parquet'
        west_path = f'/opt/airflow/data/west_{season}.parquet'
        mvp_path = f'/opt/airflow/data/mvp_{season}.parquet'

        # Ensure path for data folder exists
        data_dir = Path('/opt/airflow/data')
        data_dir.mkdir(parents=True, exist_ok=True)

        df_per_game.to_parquet(per_game_path, index=False)
        df_advanced.to_parquet(advanced_path, index=False)
        df_east.to_parquet(east_path, index=False)
        df_west.to_parquet(west_path, index=False)
        df_mvp.to_parquet(mvp_path, index=False)

        return {
            'per_game': per_game_path,
            'advanced': advanced_path,
            'team': {
                'east': east_path,
                'west': west_path
            },
            'mvp': mvp_path,
            'season': season
        }
    

    @task
    def transform(paths: dict) -> dict:
        """
        Transforms raw data into a into features dataset for model training and a stats dataset for serving.
        
        Reads the extracted parquet files, processes the data, saves the transformed datasets as parquet files,
        and returns the paths to the transformed files.

        Params:
            paths (dict): Paths to the extracted parquet files.

        Returns:
            dict: Paths to the transformed parquet files.
                {
                    'features': str,
                    'stats': str
                }
        """

        per_game = pd.read_parquet(paths['per_game'])
        advanced = pd.read_parquet(paths['advanced'])
        team = {
            'east': pd.read_parquet(paths['team']['east']),
            'west': pd.read_parquet(paths['team']['west'])
        }
        mvp = pd.read_parquet(paths['mvp'])
        season = paths['season']

        transformed_data = transform_season_data(per_game, advanced, team, mvp, season)

        features_data = transformed_data['features']
        stats_data = transformed_data['stats']

        features_path = f'/opt/airflow/data/features_{season}.parquet'
        stats_path = f'/opt/airflow/data/stats_{season}.parquet'
        
        features_data.to_parquet(features_path, index=False)
        stats_data.to_parquet(stats_path, index=False)

        return {
            'features': features_path,
            'stats': stats_path
        }
    

    @task 
    def load_player_data(paths: list[dict]):
        """
        Updates current season data in the PostgreSQL database.

        Reads the transformed parquet files for all seasons, concatenates them into single DataFrames,
        removes data from current season and loads current data into the PostgreSQL database, 
        appending existing data.

        Params:
            paths (list[dict]): List of paths to the transformed parquet files for each season.
        """

        df_features = pd.read_parquet(paths['features'])
        df_stats = pd.read_parquet(paths['stats'])

        remove_season_data(CURRENT_SEASON)

        load_to_database(df_features, user='etl', table_name='player_features', schema='stats', append=True)
        load_to_database(df_stats, user='etl', table_name='player_stats', schema='stats', append=True)


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
    def load_predictions(file_path: str):
        """
        Loads the player MVP predictions into the PostgreSQL database.

        Params:
            file_path (str): Path to the predictions parquet file.
        """
        
        predictions = pd.read_parquet(file_path)
        load_to_database(predictions, user='ml', table_name='mvp_predictions', schema='predictions')


    @task(trigger_rule='all_success')
    def clean_up():
        """
        Remove intermediate parquet files in the data directory after process completion.
        Executes only if all upstream tasks succeed, allowing for debugging in case of failures.
        """

        directory = Path('/opt/airflow/data')

        for item in directory.iterdir():
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()

    start()

    extract_paths = extract(CURRENT_SEASON)

    transform_paths = transform(extract_paths)

    predictions = predict()

    load_player_data(transform_paths) >> predictions

    load_predictions(predictions) >> clean_up()


update_dag = update_pipeline()
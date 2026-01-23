from airflow.decorators import dag, task # pyright: ignore[reportMissingImports]
from source.etl.extract import extract_season_data
from source.etl.transform import transform_season_data
from source.etl.load import load_to_database
from datetime import datetime, timedelta
import pandas as pd # pyright: ignore[reportMissingModuleSource]
from pathlib import Path
import shutil
import time
from source.config.settings import (
    CURRENT_SEASON,
    MIN_SEASON
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id='etl_dag',
    default_args=default_args,
    description='ETL Dag for NBA MVP Project',
    schedule_interval=None,
    start_date=datetime(2026, 1, 18),
    catchup=False
)
def etl_pipeline():
    """
    ETL Pipeline DAG for NBA MVP Predictor project.

    This DAG extracts season data through web scraping, stages raw datasets as parquet files,
    transforms the data to create a features dataset for model training and a stats dataset for serving,
    and loads the final datasets into a PostgreSQL database.

    Steps:
    1. Extract season data for specified seasons and save as parquet files.
    2. Wait for the extraction to complete and verify file existence.
    3. Transform the extracted data to create features and stats datasets.
    4. Wait for the transformation to complete and verify file existence.
    5. Load the final datasets into the PostgreSQL database.
    6. Clean up temporary files.

    The pipeline is designed to handle multiple seasons in parallel using Airflow's task mapping feature,
    and ensures idempotency by replacing existing data in the PostgreSQL database.
    """

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
    def wait_for_extract(paths: dict) -> dict:
        """
        Wait for extracted parquet files to be available.

        Checks filesystem for the existence of extracted parquet files before downstream transformation.
        Raises FileNotFoundError if files are not found within the timeout period.

        Params:
            paths (dict): Paths to the extracted parquet files.

        Returns:
            dict: The same paths dictionary if all files are found.

        Raises:
            FileNotFoundError: If any of the expected files are not found within the timeout period.
        """

        timeout = 60
        interval = 5        

        for file_path in [
            paths['per_game'],
            paths['advanced'],
            paths['team']['east'],
            paths['team']['west'],
            paths['mvp']
        ]:
            p = Path(file_path)
            time_waited = 0

            while not p.exists():
                if time_waited >= timeout:
                    raise FileNotFoundError(f'No file found at {file_path}')
                time.sleep(interval)
                time_waited += interval
        
        return paths


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
    def wait_for_transform(paths: dict) -> dict:
        """
        Wait for transformed parquet files to be available.

        Checks filesystem for the existence of transformed parquet files before downstream loading.
        Raises FileNotFoundError if files are not found within the timeout period.

        Params:
            paths (dict): Paths to the transformed parquet files.
        Returns:
            dict: The same paths dictionary if all files are found.

        Raises:
            FileNotFoundError: If any of the expected files are not found within the timeout period.
        """

        timeout = 60
        interval = 5

        for file_path in [
            paths['features'],
            paths['stats']
        ]:
            p = Path(file_path)
            time_waited = 0

            while not p.exists():
                if time_waited >= timeout:
                    raise FileNotFoundError(f'No file found at {file_path}')
                time.sleep(interval)
                time_waited += interval
        
        return paths


    @task 
    def load(paths: list[dict]):
        """
        Load the final datasets into the PostgreSQL database.

        Reads the transformed parquet files for all seasons, concatenates them into single DataFrames,
        and loads them into the PostgreSQL database, replacing existing data.

        Params:
            paths (list[dict]): List of paths to the transformed parquet files for each season.
        """

        features_list = [pd.read_parquet(path['features']) for path in paths]
        stats_list = [pd.read_parquet(path['stats']) for path in paths]

        df_features = pd.concat(features_list, axis=0).reset_index(drop=True)
        df_stats = pd.concat(stats_list, axis=0).reset_index(drop=True)

        load_to_database(df_features, 'player_features', 'stats')
        load_to_database(df_stats, 'player_stats', 'stats')


    @task(trigger_rule='all_success')
    def clean_up():
        """
        Remove intermediate parquet files in the data directory after ETL process completion.
        Executes only if all upstream tasks succeed, allowing for debugging in case of failures.
        """

        directory = Path('/opt/airflow/data')

        for item in directory.iterdir():
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()


    seasons = list(range(MIN_SEASON, CURRENT_SEASON + 1))

    extract_paths = extract.expand(season=seasons)

    extract_wait = wait_for_extract.expand(paths=extract_paths)

    transform_paths = transform.expand(paths=extract_wait)

    transform_wait = wait_for_transform.expand(paths=transform_paths)

    load(transform_wait) >> clean_up()


etl_dag = etl_pipeline()
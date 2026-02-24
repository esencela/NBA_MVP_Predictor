from airflow.decorators import dag, task # pyright: ignore[reportMissingImports]
from datetime import datetime, timedelta
from source.etl.extract import extract_season_data
from source.etl.transform import transform_season_data
from source.etl.load import load_to_database
from source.ml.predict import get_predictions
from source.db.utils import remove_season_data
from source.airflow.utils import log_data_freshness
import pandas as pd # pyright: ignore[reportMissingModuleSource]
from pathlib import Path
import shutil
import logging
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


@dag(
    dag_id='mvp_prediction_dag',
    default_args=default_args,
    description='Update DAG that performs etl on current season data and generates mvp predictions',
    schedule_interval='@weekly',
    start_date=datetime(2026, 1, 26),
    catchup=False
)
def update_pipeline():
    """
    Update Pipeline DAG for NBA MVP Predictor project.

    This DAG performs an update process that performs ETL on current season data, generates MVP predictions,
    and loads the predictions into a PostgreSQL database on a weekly basis.

    Steps:
    1. Extract current season data and save as parquet files for transformation.
    2. Transform the raw data into a features dataset for model training and a stats dataset for serving.
    3. Load the current season data into the PostgreSQL database.
    4. Generate player MVP predictions based on the current season data and save them as a parquet file.
    5. Load the player MVP predictions into the PostgreSQL database.
    6. Log the data freshness of the extracted data.
    7. Clean up intermediate parquet files.
    """

    logger = logging.getLogger(__name__)

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

        start_time = time.time()

        logger.info('Starting data extraction for %s season', season)

        data = extract_season_data(season)

        df_per_game = data['per_game']
        df_advanced = data['advanced']
        df_east = data['team']['east']
        df_west = data['team']['west']
        df_mvp = data['mvp']       

        data_freshness = data['last_update']

        logger.info('Rows extracted - per_game: %s, advanced: %s, east: %s, west: %s, mvp: %s',
                    len(df_per_game), 
                    len(df_advanced), 
                    len(df_east), 
                    len(df_west), 
                    len(df_mvp))
        
        # Empty MVP vote data is expected for current season
        if df_mvp.empty:
            logger.warning('No MVP voting data found for %s season', season)

        logger.info('Saving extracted data to parquet files')

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

        logger.info('Parquet files saved for %s season', season)

        logger.info('Completed extraction for %s season in %.2f seconds', 
                    season, 
                    time.time() - start_time)

        return {
            'per_game': per_game_path,
            'advanced': advanced_path,
            'team': {
                'east': east_path,
                'west': west_path
            },
            'mvp': mvp_path,
            'season': season,
            'data_freshness': data_freshness.isoformat() # Convert to ISO format string for JSON serialization
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

        start_time = time.time()

        logger.info('Starting data transformation for %s season', paths['season'])

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

        logger.info('Rows transformed for %s season - features: %s, stats: %s',
                    season,
                    len(features_data),
                    len(stats_data))

        logger.info('Saving transformed data to parquet files')

        features_path = f'/opt/airflow/data/features_{season}.parquet'
        stats_path = f'/opt/airflow/data/stats_{season}.parquet'
        
        features_data.to_parquet(features_path, index=False)
        stats_data.to_parquet(stats_path, index=False)

        logger.info('Parquet files saved')

        logger.info('Completed transformation for %s season in %.2f seconds',
                    season, 
                    time.time() - start_time)

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

        start_time = time.time()

        logger.info('Starting data loading')

        df_features = pd.read_parquet(paths['features'])
        df_stats = pd.read_parquet(paths['stats'])

        logger.info('Removing existing data for %s season from database', CURRENT_SEASON)

        remove_season_data(CURRENT_SEASON)

        logger.info('Loading current season data into database')

        load_to_database(df_features, user='etl', table_name='player_features', schema='stats', append=True)
        load_to_database(df_stats, user='etl', table_name='player_stats', schema='stats', append=True)

        logger.info('Data loading completed in %.2f seconds', time.time() - start_time)


    @task
    def predict():
        """
        Generates player MVP predictions and saves them as a parquet file.

        Returns:
            str: Path to the saved predictions parquet file.
        """
        start_time = time.time()

        logger.info('Starting MVP predictions generation')

        file_path = f'/opt/airflow/data/predictions.parquet'
        predictions = get_predictions()

        logger.info('Loading predictions into parquet file at %s', file_path)

        predictions.to_parquet(file_path, index=False)

        logger.info('MVP predictions generated and saved to %s in %.2f seconds', 
                    file_path, 
                    time.time() - start_time)

        return file_path
    

    @task
    def load_predictions(file_path: str):
        """
        Loads the player MVP predictions into the PostgreSQL database.

        Params:
            file_path (str): Path to the predictions parquet file.
        """
        start_time = time.time()

        logger.info('Starting loading of MVP predictions into database')

        predictions = pd.read_parquet(file_path)
        load_to_database(predictions, user='ml', table_name='mvp_predictions', schema='predictions')

        logger.info('MVP predictions loaded into database in %.2f seconds', time.time() - start_time)


    @task(trigger_rule='all_success')
    def log_freshness(extract_output, **kwargs):
        log_data_freshness(extract_output['data_freshness'], **kwargs)


    @task(trigger_rule='all_success')
    def clean_up():
        """
        Remove intermediate parquet files in the data directory after process completion.
        Executes only if all upstream tasks succeed, allowing for debugging in case of failures.
        """

        logger.info('Starting cleanup of intermediate files in data directory')

        directory = Path('/opt/airflow/data')

        for item in directory.iterdir():
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()

        logger.info('Cleanup completed, all intermediate files removed')


    extract_paths = extract(CURRENT_SEASON)

    transform_paths = transform(extract_paths)

    predictions = predict()

    load_player_data(transform_paths) >> predictions

    load_predictions(predictions) >> log_freshness(extract_paths) >> clean_up()


update_dag = update_pipeline()
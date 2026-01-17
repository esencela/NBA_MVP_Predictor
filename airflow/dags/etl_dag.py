from airflow import DAG
from airflow.operators.python import PythonOperator
from source.etl.extract import extract
from source.etl.transform import transform
from source.etl.load import load_to_database
from datetime import datetime
import pandas as pd # pyright: ignore[reportMissingModuleSource]
from source.config.settings import (
    CURRENT_SEASON,
    MIN_SEASON
)

def etl_pipeline():
    features_list = []
    stats_list = []

    for season in range(MIN_SEASON, CURRENT_SEASON + 1):
        season_data = extract(season)
        features_data, stats_data = transform(season_data.per_game, 
                                                season_data.advanced, 
                                                season_data.team, 
                                                season_data.mvp, 
                                                season)
            
        features_list.append(features_data)
        stats_list.append(stats_data)
        print(season)
            
    df_features = pd.concat(features_list, axis=0).reset_index(drop=True)
    df_stats = pd.concat(stats_list, axis=0).reset_index(drop=True)

    load_to_database(df_features, 'player_features', 'stats')
    load_to_database(df_stats, 'player_stats', 'stats')
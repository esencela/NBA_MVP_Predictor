from source.etl.extract import extract_team_season_data, extract_mvp_vote_data, extract_per_game_season_data, retrieve_last_update_time
#from source.etl.transform import transform_season_data
#from source.etl.load import load_to_database
import pandas as pd # pyright: ignore[reportMissingModuleSource]
from pathlib import Path
from source.config.settings import (
    CURRENT_SEASON,
    MIN_SEASON
)

#print(pd.read_parquet(f'airflow/data/features_2003.parquet').isna().sum())

#df = pd.read_parquet(f'airflow/data/stats_2004.parquet')

#print(df[df['Player'].notna()])

print(retrieve_last_update_time(Path('html_snapshots/NBA_2026_per_game.html')))
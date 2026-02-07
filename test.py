from source.etl.extract import extract_team_season_data, extract_mvp_vote_data, extract_per_game_season_data
#from source.etl.transform import transform_season_data
#from source.etl.load import load_to_database
import pandas as pd # pyright: ignore[reportMissingModuleSource]
from source.config.settings import (
    CURRENT_SEASON,
    MIN_SEASON
)

#print(pd.read_parquet(f'airflow/data/features_2003.parquet').isna().sum())

df = pd.read_parquet(f'airflow/data/stats_2004.parquet')

print(df[df['Player'].notna()])
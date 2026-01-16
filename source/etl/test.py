from source.etl.extract import extract_per_game_season_data, extract_advanced_season_data, extract_team_season_data, extract_mvp_vote_data
from source.etl.transform import transform
from source.etl.load import load_to_database
import pandas as pd # pyright: ignore[reportMissingModuleSource]
import time

season = 2024

player_data = extract_per_game_season_data(season)
time.sleep(5)
print('Sleeping')
adv_data = extract_advanced_season_data(season)
time.sleep(5)
print('Sleeping')
team_data = extract_team_season_data(season)
time.sleep(5)
print('Sleeping')
mvp_data = extract_mvp_vote_data(season)
time.sleep(5)
print('Sleeping')

df_features, df_stats = transform(player_data, adv_data, team_data, mvp_data, season)

load_to_database(df_stats, 'player_stats', 'stats')
load_to_database(df_features, 'player_features', 'stats')
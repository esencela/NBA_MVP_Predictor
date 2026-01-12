from extract import extract_per_game_season_data, extract_advanced_season_data, extract_team_season_data, extract_mvp_vote_data
from transform import transform
import pandas as pd
import time

season = 2023

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

transform(player_data, adv_data, team_data, mvp_data, season).to_csv('test2023.csv', index=False)
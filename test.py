from source.etl.extract import extract, extract_team_season_data
from source.etl.transform import transform
from source.etl.load import load_to_database
import pandas as pd # pyright: ignore[reportMissingModuleSource]
from source.config.settings import (
    CURRENT_SEASON,
    MIN_SEASON
)


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

'''
season_data = extract_team_season_data(2009)

print(season_data.team[0].sort_values(by='W/L%', ascending=False).head())
print(season_data.team[1].sort_values(by='W/L%', ascending=False).head())

features_data, stats_data = transform(season_data.per_game, 
                                            season_data.advanced, 
                                            season_data.team, 
                                            season_data.mvp,
                                            2009)

print(features_data.head())
print(stats_data.head())
'''
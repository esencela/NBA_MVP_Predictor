from source.etl.extract import extract
from source.etl.transform import transform
from source.etl.load import load_to_database
import pandas as pd # pyright: ignore[reportMissingModuleSource]

season = 2023

data = extract(season)

df_features, df_stats = transform(data.per_game, data.advanced, data.team, data.mvp, season)

load_to_database(df_stats, 'player_stats', 'stats')
load_to_database(df_features, 'player_features', 'stats')